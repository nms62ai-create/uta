"""Phase 3e REST-snapshot translators for Binance USD-M Futures.

Companion to :mod:`trade_adapter.exchanges.binance_um.translators`
(which handles WS frames). These functions turn the
``/fapi/v2/positionRisk`` and ``/fapi/v2/account`` REST responses into
the same UTA event types so the position manager can:

1. **Bootstrap** the in-memory :class:`PositionStore` at start-up from
   the venue's authoritative snapshot.
2. **Reconcile** periodically by re-fetching and diffing against the
   live store — drift surfaces as a :class:`ReconcileDiff` event.

Same contract as :mod:`translators`:

* Pure functions, no I/O.
* Hedge-mode entries (``positionSide != "BOTH"``) are silently skipped
  (decision A0 — net-only). Producers in Hedge mode get no positions,
  not partial ones.
* Malformed rows raise :class:`ValueError` — silent skip would mask a
  real Binance API drift.
"""

from __future__ import annotations

from typing import Any

from ...types import (
    Direction,
    PositionState,
    PositionUpdate,
    Venue,
)

_VENUE = Venue.BINANCE_UM

_CONTEXT_POSITION_RISK = "binance_um positionRisk"
_CONTEXT_ACCOUNT = "binance_um account"


# ---------------------------------------------------------------------------
# Helpers (mirror those in translators.py; kept local so this file does
# not depend on the WS-event module)
# ---------------------------------------------------------------------------


def _required(row: dict[str, Any], key: str, *, context: str) -> Any:
    if key not in row:
        raise ValueError(f"{context}: missing required field {key!r}: {row!r}")
    return row[key]


def _float(value: Any, *, field: str, context: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"{context}: field {field!r} not convertible to float: {value!r}"
        ) from e


def _optional_float(value: Any) -> float | None:
    """Permissive float coercion — returns ``None`` for ``None`` or unparsable."""

    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    # Binance frequently sends "0" / "0.00000" for fields that are
    # logically absent (e.g. ``liquidationPrice`` on a flat position);
    # surface those as ``None`` so consumers can tell "no liquidation"
    # from "liquidation at zero".
    return out if out != 0 else None


# ---------------------------------------------------------------------------
# positionRisk → PositionUpdate
# ---------------------------------------------------------------------------


def position_risk_to_position_update(
    row: dict[str, Any], *, ts: float
) -> PositionUpdate | None:
    """Convert one ``/fapi/v2/positionRisk`` row into a :class:`PositionUpdate`.

    Returns ``None`` for Hedge-mode entries (``positionSide`` is
    ``"LONG"`` or ``"SHORT"`` instead of ``"BOTH"``) — decision A0.

    Flat rows (``positionAmt == 0``) still emit an update with
    ``qty == 0`` and ``state == IDLE`` so the position store can record
    "we have seen this symbol and it's currently flat" rather than
    "never traded".
    """

    position_side = _required(row, "positionSide", context=_CONTEXT_POSITION_RISK)
    if str(position_side).upper() != "BOTH":
        return None

    symbol = str(_required(row, "symbol", context=_CONTEXT_POSITION_RISK))
    position_amt = _float(
        _required(row, "positionAmt", context=_CONTEXT_POSITION_RISK),
        field="positionAmt",
        context=_CONTEXT_POSITION_RISK,
    )
    entry_price = _float(
        _required(row, "entryPrice", context=_CONTEXT_POSITION_RISK),
        field="entryPrice",
        context=_CONTEXT_POSITION_RISK,
    )

    if position_amt > 0:
        direction = Direction.LONG
        qty = position_amt
        state = PositionState.OPEN
    elif position_amt < 0:
        direction = Direction.SHORT
        qty = -position_amt
        state = PositionState.OPEN
    else:
        direction = Direction.LONG  # arbitrary — qty=0 makes it unobservable
        qty = 0.0
        state = PositionState.IDLE

    return PositionUpdate(
        venue=_VENUE,
        symbol=symbol,
        direction=direction,
        qty=qty,
        entry_price=entry_price,
        state=state,
        liquidation_price=_optional_float(row.get("liquidationPrice")),
        unrealized_pnl_usd=_optional_float(row.get("unRealizedProfit")),
        margin_used_usd=_optional_float(row.get("isolatedMargin")),
        ts=ts,
    )


def position_risk_to_position_updates(
    rows: list[dict[str, Any]], *, ts: float
) -> list[PositionUpdate]:
    """Bulk variant — silently drops Hedge-mode rows (decision A0)."""

    out: list[PositionUpdate] = []
    for row in rows:
        update = position_risk_to_position_update(row, ts=ts)
        if update is not None:
            out.append(update)
    return out


# ---------------------------------------------------------------------------
# account → equity (totalWalletBalance, in USD)
# ---------------------------------------------------------------------------


def account_info_to_equity_usd(info: dict[str, Any]) -> float:
    """Extract total USD equity from a ``/fapi/v2/account`` response.

    Uses ``totalWalletBalance`` (the spec's canonical "equity" reading).
    For futures on USDT-margined contracts, this number is already in
    USD (≈ USDT). If the field is missing or malformed, raises
    :class:`ValueError` so the caller can decide whether to retry the
    REST call rather than silently treating the account as empty.
    """

    raw = _required(info, "totalWalletBalance", context=_CONTEXT_ACCOUNT)
    return _float(raw, field="totalWalletBalance", context=_CONTEXT_ACCOUNT)


__all__ = [
    "account_info_to_equity_usd",
    "position_risk_to_position_update",
    "position_risk_to_position_updates",
]
