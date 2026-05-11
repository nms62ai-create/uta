"""Phase 3f: pre-submit risk gate.

Sits between the signal_router's accept-plan formation and the actual
:meth:`ExchangeAdapter.submit_order` call. The gate is purely a *check*:
it never mutates positions or sends to the venue. If it says no, the
router rejects the signal with one of two reason codes:

* :class:`RejectionReason.EMERGENCY_LIMIT_EXCEEDED` — a configured
  per-symbol qty cap, notional cap, daily-loss cap, or the manual kill
  switch.
* :class:`RejectionReason.RATE_LIMITED` — reserved for future rate
  limits (not implemented in 3f MVP).

Pieces
------

* :class:`RiskConfig` — frozen producer-supplied limits. Each cap is
  optional; ``None`` means "no limit on that axis".
* :class:`RiskState` — mutable runtime accounting (daily realized PnL,
  kill-switch flag). The position_manager calls
  :meth:`RiskState.record_realized_pnl` whenever a position closes; the
  embedded API exposes :meth:`RiskState.trip_kill_switch` /
  :meth:`reset_kill_switch` as the manual emergency-stop surface.
* :class:`RiskDecision` — the result of one :meth:`RiskGate.evaluate`
  call. ``allowed=True`` means proceed; otherwise the
  ``rejection_reason`` and human-readable ``detail`` go straight into
  the :class:`SignalAck`.
* :class:`RiskGate` — pulls config + state + position_provider together
  and answers a single yes/no per signal.

Daily PnL accounting
--------------------

Realised PnL is bucketed by UTC date — the day key flips at
``00:00 UTC``. A producer wanting a different timezone passes a custom
``clock`` to :class:`RiskState`; PnL bucketing follows whatever the
clock returns. The gate compares the day's cumulative PnL against the
configured cap and trips the kill switch automatically when it crosses
``-max_daily_loss_usd``. Once tripped, every subsequent
:meth:`evaluate` call denies until :meth:`reset_kill_switch` is called.

This module is venue-agnostic. Bybit reuses it unchanged in Phase 4.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from ..types import (
    Intent,
    RejectionReason,
    UniversalSignal,
    Venue,
)
from .protocols import PositionProvider

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """Producer-supplied risk caps.

    Each field is checked independently. ``None`` means "no limit on
    that axis". An empty mapping means "no limit on any symbol" — to
    actually deny all symbols, supply explicit zero caps.

    Attributes:
        max_position_qty_per_symbol: After accepting this signal, the
            ``(venue, symbol)`` net position qty must not exceed this
            cap. If the symbol is absent from the mapping, no cap
            applies. Qty is the absolute value of the post-fill
            position size; direction does not matter.
        max_notional_usd_per_symbol: Same as above but in USD —
            ``target_qty * reference_price`` of the new contribution
            (not the post-fill total) must not exceed this cap.
        max_daily_loss_usd: Cumulative realised loss for the UTC
            calendar day must stay strictly above ``-max_daily_loss_usd``.
            When the threshold is crossed the kill switch trips
            automatically.
    """

    max_position_qty_per_symbol: dict[tuple[Venue, str], float] | None = None
    max_notional_usd_per_symbol: float | None = None
    max_daily_loss_usd: float | None = None


# ---------------------------------------------------------------------------
# Runtime state (mutable)
# ---------------------------------------------------------------------------


def _default_clock() -> float:
    return time.time()


@dataclass(slots=True)
class RiskState:
    """Mutable accounting for the risk gate.

    Daily PnL is bucketed by UTC date derived from ``clock()``. The
    kill switch flag is sticky — once tripped (manually or by daily
    loss), only :meth:`reset_kill_switch` clears it.
    """

    clock: Callable[[], float] = field(default=_default_clock)
    _daily_realized_pnl: dict[tuple[Venue, str], float] = field(default_factory=dict)
    _kill_switch_tripped: bool = False
    _kill_switch_reason: str | None = None

    # -- daily PnL accounting -----------------------------------------------

    def record_realized_pnl(self, venue: Venue, pnl_usd: float) -> None:
        """Add ``pnl_usd`` to the current UTC day's bucket for ``venue``."""

        key = (venue, self._current_date())
        self._daily_realized_pnl[key] = (
            self._daily_realized_pnl.get(key, 0.0) + pnl_usd
        )

    def daily_realized_pnl(self, venue: Venue) -> float:
        """Cumulative realized PnL for ``venue`` on the current UTC date."""

        return self._daily_realized_pnl.get((venue, self._current_date()), 0.0)

    # -- kill switch --------------------------------------------------------

    def trip_kill_switch(self, reason: str) -> None:
        """Set the kill switch. Idempotent — first reason wins."""

        if not self._kill_switch_tripped:
            self._kill_switch_tripped = True
            self._kill_switch_reason = reason
            _log.warning("kill switch tripped: %s", reason)

    def reset_kill_switch(self) -> None:
        """Clear the kill switch and its reason."""

        if self._kill_switch_tripped:
            _log.info(
                "kill switch reset (was: %s)", self._kill_switch_reason
            )
        self._kill_switch_tripped = False
        self._kill_switch_reason = None

    @property
    def kill_switch_tripped(self) -> bool:
        return self._kill_switch_tripped

    @property
    def kill_switch_reason(self) -> str | None:
        return self._kill_switch_reason

    # -- internal -----------------------------------------------------------

    def _current_date(self) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime(self.clock()))


# ---------------------------------------------------------------------------
# Decision wrapper
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """Result of :meth:`RiskGate.evaluate`."""

    allowed: bool
    rejection_reason: RejectionReason | None = None
    detail: str | None = None

    @classmethod
    def allow(cls) -> RiskDecision:
        return cls(allowed=True)

    @classmethod
    def deny(
        cls, reason: RejectionReason, detail: str
    ) -> RiskDecision:
        return cls(allowed=False, rejection_reason=reason, detail=detail)


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


class RiskGate:
    """Pre-submit checks against :class:`RiskConfig` and :class:`RiskState`.

    Stateless w.r.t. the signal — the gate reads through
    :attr:`position_provider` and :attr:`state` on every call so config
    edits / position updates take effect immediately.
    """

    def __init__(
        self,
        *,
        config: RiskConfig,
        state: RiskState,
        position_provider: PositionProvider,
    ) -> None:
        self._config = config
        self._state = state
        self._position_provider = position_provider

    @property
    def config(self) -> RiskConfig:
        return self._config

    @property
    def state(self) -> RiskState:
        return self._state

    def evaluate(
        self,
        signal: UniversalSignal,
        *,
        target_qty: float,
        reference_price: float,
    ) -> RiskDecision:
        """Run all configured checks against ``signal`` + ``target_qty``.

        The signal_router has already resolved sizing into a concrete
        ``target_qty`` and a ``reference_price``; the gate just needs to
        ask: does executing this signal still fit inside the producer's
        limits?
        """

        # Kill switch is sticky. Daily-loss check below may also trip
        # it on this same call.
        if self._state.kill_switch_tripped:
            return RiskDecision.deny(
                RejectionReason.EMERGENCY_LIMIT_EXCEEDED,
                f"kill_switch: {self._state.kill_switch_reason or 'unspecified'}",
            )

        # Daily-loss cap: trip the kill switch and deny if we've
        # already breached. We do this *before* committing to a new
        # position so the breach itself ages out the next day.
        max_loss = self._config.max_daily_loss_usd
        if max_loss is not None:
            pnl = self._state.daily_realized_pnl(signal.venue)
            if pnl <= -abs(max_loss):
                self._state.trip_kill_switch(
                    f"daily_loss: {pnl:.2f} USD on {signal.venue.value}"
                )
                return RiskDecision.deny(
                    RejectionReason.EMERGENCY_LIMIT_EXCEEDED,
                    f"daily_loss_exceeded: pnl={pnl:.2f} cap={-abs(max_loss):.2f}",
                )

        # Per-symbol notional cap on the new contribution.
        max_notional = self._config.max_notional_usd_per_symbol
        if max_notional is not None:
            notional = target_qty * reference_price
            if notional > max_notional:
                return RiskDecision.deny(
                    RejectionReason.EMERGENCY_LIMIT_EXCEEDED,
                    f"notional_cap: notional={notional:.2f} cap={max_notional:.2f}",
                )

        # Per-symbol qty cap on the post-fill net position.
        per_symbol = self._config.max_position_qty_per_symbol
        if per_symbol is not None:
            cap = per_symbol.get((signal.venue, signal.symbol))
            if cap is not None:
                post_fill_qty = _projected_post_fill_qty(
                    signal=signal,
                    target_qty=target_qty,
                    position_provider=self._position_provider,
                )
                if post_fill_qty > cap:
                    return RiskDecision.deny(
                        RejectionReason.EMERGENCY_LIMIT_EXCEEDED,
                        f"position_cap: post_qty={post_fill_qty:.8f} cap={cap:.8f}",
                    )

        return RiskDecision.allow()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _projected_post_fill_qty(
    *,
    signal: UniversalSignal,
    target_qty: float,
    position_provider: PositionProvider,
) -> float:
    """Approximate post-fill absolute position qty assuming ``target_qty`` fills.

    For Phase 3f's MVP (OPEN intent only), the math is straightforward:
    a new OPEN on top of a (possibly tracked-but-flat) position simply
    contributes ``target_qty``. ADD / REDUCE / CLOSE / REVERSE intents
    will require richer accounting once Phase 3e's intent resolver
    accepts them — they are denied earlier in the accept path today.
    """

    existing = position_provider.get_position(signal.venue, signal.symbol)
    if existing is None or existing.qty == 0:
        return target_qty

    # Same-direction OPEN on top of an open position is rejected by the
    # signal_router's intent resolver, but defensively project: a
    # same-direction add increases qty, opposite-direction reduces it.
    if signal.intent is Intent.OPEN:
        # Router rejects this case; project additively to be safe.
        if existing.direction is signal.direction:
            return existing.qty + target_qty
        return abs(existing.qty - target_qty)

    return target_qty


__all__ = [
    "RiskConfig",
    "RiskDecision",
    "RiskGate",
    "RiskState",
]
