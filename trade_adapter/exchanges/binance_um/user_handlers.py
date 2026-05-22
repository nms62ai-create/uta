"""Phase 3g: ``USER_DATA_STREAM`` → :class:`PositionManager` + bus + risk wiring.

Factory that produces the ``handlers`` mapping expected by
:class:`UserDataStreamClient`. Translates raw Binance frames into UTA
event types via :mod:`translators`, writes them into the position store
via :class:`PositionManager`, publishes serialised events on the
embedded :class:`EventBus`, and (optionally) feeds realised PnL into
:class:`RiskState` for daily-loss accounting.

Venue-specific: lives next to :mod:`translators` because it is where
Binance frame field names (``"e"`` / ``"a.B"`` / ``"o.rp"``) leak
through. Bybit will get its own equivalent in Phase 4.

The handlers are designed to be liberal about what they accept:

* A translator-level :class:`ValueError` is logged and the offending
  frame skipped. Propagating it would tear down the entire user-data
  stream over a single malformed event, which is worse than missing
  one position update.
* Anything else (programmer errors, ``CancelledError``) propagates so
  the supervisor can reconnect cleanly.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from ...bus.event_bus import EventBus
from ...core.outcome import OutcomeEmitter
from ...core.position_manager import PositionManager
from ...core.risk import RiskState
from ...serialization import (
    fill_to_wire,
    order_update_to_wire,
    position_update_to_wire,
)
from ...types import EventType
from .translators import (
    account_update_to_position_updates,
    order_trade_update_to_fill,
    order_trade_update_to_order_update,
)

_log = logging.getLogger(__name__)

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]


# Stable-coin assets we treat as "USD" for equity accounting on Binance
# USD-M Futures. Anything else in the wallet-balance array is ignored
# (foreign-currency collateral can't be added to a USD bucket without a
# price feed; cross-collateral users get a slightly stale equity until
# the next REST reconcile picks it up via ``totalWalletBalance``).
_USD_STABLE_ASSETS = frozenset({"USDT", "BUSD", "USDC", "FDUSD", "USD"})


def make_user_data_handlers(
    *,
    position_manager: PositionManager,
    event_bus: EventBus | None = None,
    risk_state: RiskState | None = None,
    outcome_emitter: OutcomeEmitter | None = None,
) -> dict[str, EventHandler]:
    """Return the ``handlers`` dict for :class:`UserDataStreamClient`.

    Wires:

    * ``ORDER_TRADE_UPDATE`` → publish :class:`OrderUpdate` on the bus;
      if the event is a trade execution, record the :class:`Fill` on
      the :class:`PositionManager`, publish the :class:`Fill` on the
      bus, and add the trade's realised PnL to the :class:`RiskState`
      daily bucket.
    * ``ACCOUNT_UPDATE`` → apply each per-symbol :class:`PositionUpdate`
      to the manager, publish the same on the bus, and update the
      manager's equity from the wallet-balance array (sum of stable-coin
      ``wb`` fields).

    Parameters
    ----------
    position_manager:
        Where to write the translated events. Its ``venue`` is used to
        tag realised-PnL bucket entries on :class:`RiskState`.
    event_bus:
        Optional. When given, every translated event is also published
        on the appropriate :class:`EventType` topic. None ⇒ silent.
    risk_state:
        Optional. When given, realised-PnL deltas from
        ``ORDER_TRADE_UPDATE`` fills are added to the daily bucket so
        the daily-loss cap (Phase 3f) can trip on its own.
    outcome_emitter:
        Optional. When given, every fill and position update is also
        pushed into the emitter (A.2) so per-position
        :class:`OutcomeReport` payloads can be produced on close. Push
        happens *before* the matching bus publish so the BBO snapshot
        held by :class:`BboTracker` is still live at the moment the
        emitter samples it.
    """

    venue = position_manager.venue

    async def on_order_trade_update(frame: dict[str, Any]) -> None:
        try:
            update = order_trade_update_to_order_update(frame)
        except ValueError as e:
            _log.warning(
                "malformed ORDER_TRADE_UPDATE; skipping. error=%s frame=%r",
                e,
                frame,
            )
            return
        if event_bus is not None:
            event_bus.publish(
                EventType.ORDER_UPDATE.value,
                order_update_to_wire(update),
            )
        try:
            fill = order_trade_update_to_fill(frame)
        except ValueError as e:
            _log.warning(
                "malformed ORDER_TRADE_UPDATE fill; skipping. error=%s frame=%r",
                e,
                frame,
            )
            return
        if fill is None:
            return
        position_manager.apply_fill(fill)
        if outcome_emitter is not None:
            outcome_emitter.apply_fill(fill)
        if event_bus is not None:
            event_bus.publish(EventType.FILL.value, fill_to_wire(fill))
        # Realised PnL is on the order sub-object as ``rp``. Per Binance
        # docs it is per-trade (not cumulative); 0 on non-closing fills,
        # signed (negative = loss) on closing fills. Skip the 0 case so
        # we don't pollute the bucket with zero-deltas.
        o = frame.get("o") or {}
        rp_raw = o.get("rp")
        pnl: float | None = None
        if rp_raw is not None:
            try:
                pnl = float(rp_raw)
            except (TypeError, ValueError):
                pnl = 0.0
        if risk_state is not None and pnl is not None and pnl != 0.0:
            risk_state.record_realized_pnl(venue, pnl)
        if outcome_emitter is not None and pnl is not None and pnl != 0.0:
            outcome_emitter.record_realized_pnl(
                fill.venue, fill.symbol, pnl
            )

    async def on_account_update(frame: dict[str, Any]) -> None:
        try:
            updates = account_update_to_position_updates(frame)
        except ValueError as e:
            _log.warning(
                "malformed ACCOUNT_UPDATE; skipping. error=%s frame=%r",
                e,
                frame,
            )
            return
        for u in updates:
            position_manager.apply_position_update(u)
            # Outcome push happens before the bus publish so the BBO
            # snapshot held by ``BboTracker`` is still live when the
            # emitter samples it on close.
            if outcome_emitter is not None:
                outcome_emitter.apply_position_update(u)
            if event_bus is not None:
                event_bus.publish(
                    EventType.POSITION_UPDATE.value,
                    position_update_to_wire(u),
                )
        equity = _extract_equity_usd(frame)
        if equity is not None:
            try:
                position_manager.apply_equity_update(equity)
            except ValueError as e:
                _log.warning("invalid equity update: %s", e)

    return {
        "ORDER_TRADE_UPDATE": on_order_trade_update,
        "ACCOUNT_UPDATE": on_account_update,
    }


def _extract_equity_usd(frame: dict[str, Any]) -> float | None:
    """Sum stable-coin wallet balances from an ``ACCOUNT_UPDATE`` ``a.B`` array.

    Returns ``None`` if the frame doesn't carry a balance array or none
    of the assets are recognised USD-equivalents. We sum (rather than
    pick a single asset) so users with both USDT and BUSD collateral
    see the combined equity.
    """

    a = frame.get("a")
    if not isinstance(a, dict):
        return None
    balances = a.get("B")
    if not isinstance(balances, list):
        return None
    total = 0.0
    matched = False
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        asset = str(entry.get("a", "")).upper()
        if asset not in _USD_STABLE_ASSETS:
            continue
        wb_raw = entry.get("wb")
        if wb_raw is None:
            continue
        try:
            total += float(wb_raw)
            matched = True
        except (TypeError, ValueError):
            continue
    return total if matched else None


__all__ = ["EventHandler", "make_user_data_handlers"]
