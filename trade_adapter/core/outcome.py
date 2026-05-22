"""A.2: per-position lifecycle tracker → :class:`OutcomeReport` emitter.

Listens to the same :class:`PositionUpdate` / :class:`Fill` stream the
:class:`PositionManager` consumes, accumulates the per-position
envelope, and emits an :class:`OutcomeReport` on the embedded
:class:`EventBus` (topic ``EventType.OUTCOME_REPORT``) when the
``(venue, symbol)`` transitions back to flat (``qty == 0``).

Design
------

Push interface, not bus consumption. The venue-side user-data handler
(:mod:`exchanges.binance_um.user_handlers`) calls
:meth:`apply_position_update` and :meth:`apply_fill` directly with the
typed dataclasses *before* publishing the wire-form on the bus. Two
reasons:

* The bus carries wire-form ``dict`` payloads so the future gateway WS
  feed can forward them without re-serialising. Consuming the bus would
  force the emitter to deserialise on every event, which is wasted work
  when the user-data handler already has the dataclass in hand.
* The emitter has to read the BBO snapshot at the moment of close
  *before* :class:`BboTracker` tears it down. By pushing the close
  event into the emitter synchronously, ahead of the bus publish, we
  guarantee the snapshot is still live when we read it.

Per-lifetime accounting
-----------------------

* ``opened_at`` — timestamp of the first non-flat ``PositionUpdate``.
* ``entry_price`` — ``entry_price`` from that first update (Binance's
  ``ep`` weighted-average; preferred over the first fill price because
  it survives later position additions cleanly).
* ``direction`` / ``signal_id`` / ``correlation_id`` — captured from the
  opening update.
* For every :class:`Fill` that arrives while the lifetime is open:
  - ``fees_usd`` += fill fee
  - ``realized_pnl_usd`` is tracked from the venue's per-trade realised
    P&L (passed via :meth:`record_realized_pnl`); if no venue value was
    recorded for the lifetime, we fall back to the signed sum of
    ``(price * qty * side_sign)`` minus fees, which matches the spec
    formula for spot/futures market orders without funding.
  - first / last fill prices are remembered for slippage and exit-price
    calculation.
  - the last closing fill's ``client_order_id`` suffix
    (``-sl`` / ``-tp`` / else) seeds the inferred
    :class:`CloseReason`.
* On the flat ``PositionUpdate``:
  - MFE / MAE in bps relative to ``first_mid`` of the
    :class:`PositionBboSnapshot` (if a :class:`BboTracker` is wired).
    Sign convention matches A22: positive ``mfe_bps`` means the market
    moved favourably; positive ``mae_bps`` means it moved against.
  - ``slippage_bps`` = ``|first_fill_price - first_mid| / first_mid *
    10000``. The first BBO mid stands in for the operator's intended
    price for market-order entries — a better proxy would require the
    accept-time mid to be stored on :class:`OrderRequest`, which is
    deferred.
  - ``holding_time_s`` = ``closed_at - opened_at``.
  - The dataclass is emitted on :class:`EventBus` and stored as
    :attr:`last_report` for tests.

The emitter is venue-scoped (constructed once per
:class:`TradeAdapter`); a future hub adapter can build N emitters and
fan them into a shared bus.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from ..bus.event_bus import EventBus
from ..marketdata.bbo_tracker import BboTracker, PositionBboSnapshot
from ..serialization import outcome_report_to_wire
from ..types import (
    CloseReason,
    Direction,
    EventType,
    Fill,
    OrderSide,
    OutcomeReport,
    PositionState,
    PositionUpdate,
    Venue,
)

_log = logging.getLogger(__name__)


_Clock = Callable[[], float]


def _default_clock() -> float:
    return time.time()


@dataclass(slots=True)
class _Lifetime:
    """Mutable per-(venue, symbol) lifetime bookkeeping."""

    venue: Venue
    symbol: str
    direction: Direction
    signal_id: str
    correlation_id: str | None
    opened_at: float
    entry_price: float
    max_qty: float
    last_qty: float
    fees_usd: float = 0.0
    # Signed running PnL from fills (BUY contributes negative cash
    # outflow, SELL positive cash inflow; difference = realised PnL
    # before fees for the round trip).
    fill_cash_signed: float = 0.0
    venue_realized_pnl_usd: float = 0.0
    venue_realized_pnl_recorded: bool = False
    first_fill_price: float | None = None
    first_fill_qty: float = 0.0
    last_fill_price: float | None = None
    last_fill_ts: float | None = None
    last_close_coid: str | None = None
    close_reason_override: CloseReason | None = None
    fill_count: int = 0


@dataclass(slots=True)
class _EmitStats:
    reports_emitted: int = 0
    fills_unmatched: int = 0
    last_report: OutcomeReport | None = None


@dataclass(slots=True)
class OutcomeEmitter:
    """Tracks per-position lifetimes and emits :class:`OutcomeReport` on close.

    Wired by the venue user-data handler (push interface, see module
    docstring). The :class:`TradeAdapter` accepts an instance via its
    constructor; the embedded API exposes it back to consumers as
    :attr:`TradeAdapter.outcome_emitter`.
    """

    venue: Venue
    event_bus: EventBus | None = None
    bbo_tracker: BboTracker | None = None
    clock: _Clock = field(default=_default_clock)

    _lifetimes: dict[tuple[Venue, str], _Lifetime] = field(default_factory=dict)
    _stats: _EmitStats = field(default_factory=_EmitStats)

    # ------------------------------------------------------------------
    # Push interface
    # ------------------------------------------------------------------

    def apply_position_update(self, pu: PositionUpdate) -> OutcomeReport | None:
        """Track a position transition.

        Opens a new lifetime when ``(venue, symbol)`` crosses into
        ``qty > 0``; closes the lifetime (and emits) when it returns to
        flat. Returns the emitted :class:`OutcomeReport` (or ``None``
        for intermediate updates).
        """

        key = (pu.venue, pu.symbol)
        is_open = pu.qty > 0 and pu.state is not PositionState.IDLE
        lifetime = self._lifetimes.get(key)

        if is_open:
            if lifetime is None:
                self._lifetimes[key] = _Lifetime(
                    venue=pu.venue,
                    symbol=pu.symbol,
                    direction=pu.direction,
                    signal_id=pu.signal_id or "",
                    correlation_id=pu.correlation_id,
                    opened_at=pu.ts,
                    entry_price=pu.entry_price,
                    max_qty=pu.qty,
                    last_qty=pu.qty,
                )
            else:
                # Growing position — refresh the weighted-average entry
                # price from the venue. Reductions don't change cost
                # basis so we leave it untouched.
                if pu.qty > lifetime.max_qty:
                    lifetime.max_qty = pu.qty
                    lifetime.entry_price = pu.entry_price
                lifetime.last_qty = pu.qty
                # Late-arriving signal_id (some venues stamp it only on
                # the very first update); keep the first non-empty.
                if not lifetime.signal_id and pu.signal_id:
                    lifetime.signal_id = pu.signal_id
                if lifetime.correlation_id is None:
                    lifetime.correlation_id = pu.correlation_id
            return None

        # Flat. Emit only if we had been tracking the lifetime.
        if lifetime is None:
            return None
        report = self._build_report(lifetime, closed_at=pu.ts)
        del self._lifetimes[key]
        self._emit(report)
        return report

    def apply_fill(self, fill: Fill) -> None:
        """Record a fill against the active lifetime (if any)."""

        key = (fill.venue, fill.symbol)
        lifetime = self._lifetimes.get(key)
        if lifetime is None:
            # Fill arrived before the corresponding ACCOUNT_UPDATE —
            # rare but possible because Binance multiplexes both into
            # the user-data stream. We could buffer, but the spec
            # explicitly favours simplicity: count the miss and move on.
            # The position manager already handled the fill correctly,
            # so the only lost field is fees/slippage for that one
            # fill if a lifetime opens later.
            self._stats.fills_unmatched += 1
            return
        side_sign = 1.0 if fill.side is OrderSide.SELL else -1.0
        lifetime.fill_cash_signed += side_sign * fill.qty * fill.price
        lifetime.fees_usd += fill.fee_usd
        if lifetime.first_fill_price is None:
            lifetime.first_fill_price = fill.price
            lifetime.first_fill_qty = fill.qty
        lifetime.last_fill_price = fill.price
        lifetime.last_fill_ts = fill.ts
        lifetime.last_close_coid = fill.client_order_id
        lifetime.fill_count += 1

    def record_realized_pnl(
        self, venue: Venue, symbol: str, pnl: float
    ) -> None:
        """Accumulate venue-reported per-trade realised PnL.

        For Binance UM this is the ``o.rp`` field on
        ``ORDER_TRADE_UPDATE``. Captures funding & cross-margin effects
        the fill-cash heuristic can miss. No-op if the lifetime has not
        been opened yet (matches the simplification in
        :meth:`apply_fill`).
        """

        lifetime = self._lifetimes.get((venue, symbol))
        if lifetime is None:
            return
        lifetime.venue_realized_pnl_usd += pnl
        lifetime.venue_realized_pnl_recorded = True

    def record_close_reason_hint(
        self, venue: Venue, symbol: str, reason: CloseReason
    ) -> None:
        """Override the inferred :class:`CloseReason` for the active lifetime.

        Used by the position manager on reconcile-driven closes and by
        the venue handler on liquidation events; everything else is
        inferred from the closing fill's ``client_order_id``.
        """

        lifetime = self._lifetimes.get((venue, symbol))
        if lifetime is None:
            return
        lifetime.close_reason_override = reason

    # ------------------------------------------------------------------
    # Read-only introspection
    # ------------------------------------------------------------------

    @property
    def reports_emitted(self) -> int:
        return self._stats.reports_emitted

    @property
    def fills_unmatched(self) -> int:
        return self._stats.fills_unmatched

    @property
    def last_report(self) -> OutcomeReport | None:
        return self._stats.last_report

    def is_tracking(self, venue: Venue, symbol: str) -> bool:
        return (venue, symbol) in self._lifetimes

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_report(
        self, lifetime: _Lifetime, *, closed_at: float
    ) -> OutcomeReport:
        snapshot: PositionBboSnapshot | None = None
        if self.bbo_tracker is not None:
            snapshot = self.bbo_tracker.get_snapshot(
                lifetime.venue, lifetime.symbol
            )
        mfe_bps, mae_bps = _compute_excursion_bps(
            lifetime.direction, lifetime.entry_price, snapshot
        )
        slippage_bps = _compute_slippage_bps(
            lifetime.first_fill_price, snapshot
        )
        exit_price = (
            lifetime.last_fill_price
            if lifetime.last_fill_price is not None
            else lifetime.entry_price
        )
        realized_pnl = (
            lifetime.venue_realized_pnl_usd
            if lifetime.venue_realized_pnl_recorded
            else lifetime.fill_cash_signed - lifetime.fees_usd
        )
        close_reason = (
            lifetime.close_reason_override
            if lifetime.close_reason_override is not None
            else _infer_close_reason(lifetime.last_close_coid)
        )
        holding_time_s = max(0.0, closed_at - lifetime.opened_at)
        return OutcomeReport(
            signal_id=lifetime.signal_id,
            venue=lifetime.venue,
            symbol=lifetime.symbol,
            direction=lifetime.direction,
            entry_price=lifetime.entry_price,
            exit_price=exit_price,
            qty=lifetime.max_qty,
            realized_pnl_usd=realized_pnl,
            fees_usd=lifetime.fees_usd,
            slippage_bps=slippage_bps,
            holding_time_s=holding_time_s,
            mfe_bps=mfe_bps,
            mae_bps=mae_bps,
            close_reason=close_reason,
            opened_at=lifetime.opened_at,
            closed_at=closed_at,
            correlation_id=lifetime.correlation_id,
        )

    def _emit(self, report: OutcomeReport) -> None:
        self._stats.reports_emitted += 1
        self._stats.last_report = report
        if self.event_bus is None:
            return
        self.event_bus.publish(
            EventType.OUTCOME_REPORT.value, outcome_report_to_wire(report)
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _infer_close_reason(coid: str | None) -> CloseReason:
    """Map a closing fill's ``client_order_id`` suffix to a reason.

    Mirrors the suffix convention used by :mod:`core.signal_router`
    (``-sl`` / ``-tp`` / ``-entry``). Anything else (operator closed via
    the venue UI, foreign system, reduce-only TWAP, ...) is reported as
    :attr:`CloseReason.MANUAL`. Reconcile and liquidation are not
    inferred from the coid and must be set via
    :meth:`OutcomeEmitter.record_close_reason_hint`.
    """

    if not coid:
        return CloseReason.MANUAL
    lower = coid.lower()
    if lower.endswith("-sl"):
        return CloseReason.SL
    if lower.endswith("-tp"):
        return CloseReason.TP
    if lower.endswith("-entry"):
        return CloseReason.SIGNAL
    return CloseReason.MANUAL


def _compute_excursion_bps(
    direction: Direction,
    entry_price: float,
    snapshot: PositionBboSnapshot | None,
) -> tuple[float, float]:
    """Return (mfe_bps, mae_bps) given the position direction.

    Returns (0.0, 0.0) when no BBO ticks were observed during the
    lifetime — the snapshot is missing entirely or carries zero ticks.

    Sign convention matches A22 in the spec:

    * For a LONG position, MFE is the maximum upward excursion of the
      ask price (best price the position could have been closed at);
      MAE is the maximum downward excursion of the bid (worst price).
    * For a SHORT position, MFE is the maximum downward excursion of
      the bid; MAE is the maximum upward excursion of the ask.

    Both are reported as positive bps; if the market moved entirely
    against the position, ``mfe_bps`` is 0 (we don't report negative
    favourable excursions).
    """

    if snapshot is None or snapshot.tick_count == 0 or entry_price <= 0:
        return 0.0, 0.0
    if direction is Direction.LONG:
        mfe = max(snapshot.max_ask - entry_price, 0.0)
        mae = max(entry_price - snapshot.min_bid, 0.0)
    else:
        mfe = max(entry_price - snapshot.min_bid, 0.0)
        mae = max(snapshot.max_ask - entry_price, 0.0)
    return (mfe / entry_price * 10_000.0, mae / entry_price * 10_000.0)


def _compute_slippage_bps(
    first_fill_price: float | None,
    snapshot: PositionBboSnapshot | None,
) -> float:
    """Return ``|first_fill_price - first_mid| / first_mid * 10000``.

    Returns 0 if either side is missing — we'd rather under-report than
    surface a noisy figure based on guesswork. The BBO mid at first
    tick is a coarse stand-in for the operator's intended price; once
    the accept boundary stores the resolved intended mid on the order
    request (Phase 5), we'll switch this to use that value directly.
    """

    if (
        first_fill_price is None
        or snapshot is None
        or snapshot.tick_count == 0
        or snapshot.first_mid <= 0
    ):
        return 0.0
    return abs(first_fill_price - snapshot.first_mid) / snapshot.first_mid * 10_000.0


__all__ = ["OutcomeEmitter"]
