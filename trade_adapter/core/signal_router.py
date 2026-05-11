"""Phase 3d: :class:`SignalRouter` — ``UniversalSignal`` → orders on the venue.

The router is the accept boundary of the adapter. It orchestrates the
synchronous path that turns a producer's :class:`UniversalSignal` into
one or more :class:`OrderRequest` objects on the venue:

1. **Dedupe** via :class:`IdempotencyCache` (D.6) — same ``signal_id``
   replays the original :class:`SignalAck` with ``duplicate=True``.
2. **Schema sanity** — ``ttl_seconds > 0``, ``symbol`` non-empty.
3. **Intent resolution** against :class:`PositionProvider`. Phase 3d
   MVP supports :class:`Intent.OPEN` only: ``OPEN`` requires no existing
   open position; ``ADD`` / ``REDUCE`` / ``CLOSE`` / ``REVERSE`` reject
   with :class:`RejectionReason.INVALID_INTENT` until Phase 3e wires
   in real resolution.
4. **Reference price** from :class:`MarketDataProvider` — sizing and
   stop math both anchor here. Missing / non-positive → reject as
   :class:`RejectionReason.UNKNOWN_SYMBOL`.
5. **Stop prices** via :func:`compute_protective_price` (Phase 3c).
   SL is computed first so :class:`RiskBased` sizing can reuse it.
6. **Target qty** via :func:`compute_target_qty` (Phase 3c). The router
   pre-checks the :class:`RiskBased` + ``sl is None`` case so it can
   surface :class:`RejectionReason.SIZING_REQUIRES_SL` instead of a
   generic schema error.
7. **Submit** — entry order plus optional SL / TP orders, dispatched
   in parallel via :func:`asyncio.gather`. SL / TP use
   ``close_position=True`` so they auto-cancel when the position is
   flat (Binance semantics; Bybit's mirror is in Phase 4).
8. **Publish** ``SIGNAL_RECEIVED`` on :class:`EventBus` (best-effort —
   the bus is allowed to be ``None`` for tests).
9. **Cache** the :class:`SignalAck` so a retry replays the same
   response.

Any exception the adapter raises during submit propagates to the caller
— signal_router does *not* mask venue errors as rejections, because the
caller needs the original error to decide whether to retry. The accept
path itself never raises: every schema / sizing / stop / intent error
becomes a :class:`SignalAck` with the corresponding
:class:`RejectionReason`.

This module is async but otherwise pure: no clock side-effects (clock
is injected), no global state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from ..bus.event_bus import EventBus
from ..serialization import signal_ack_to_wire, signal_to_wire
from ..storage.idempotency import IdempotencyCache
from ..types import (
    Direction,
    EventType,
    Intent,
    OrderRequest,
    OrderSide,
    OrderType,
    RejectionReason,
    RiskBased,
    SignalAck,
    StopSpec,
    TimeInForce,
    UniversalSignal,
)
from .protocols import (
    EquityProvider,
    ExchangeAdapter,
    MarketDataProvider,
    PositionProvider,
)
from .risk import RiskDecision, RiskGate
from .sizing import SizingError, compute_target_qty
from .stops import StopError, compute_protective_price

_log = logging.getLogger(__name__)


def _default_clock() -> float:
    return time.time()


@dataclass(frozen=True, slots=True)
class _AcceptPlan:
    """Resolved per-signal plan after accept-phase checks pass."""

    qty: float
    sl_price: float | None
    tp_price: float | None
    entry_side: OrderSide
    exit_side: OrderSide


class SignalRouter:
    """Orchestrates the accept path. See module docstring."""

    def __init__(
        self,
        *,
        adapter: ExchangeAdapter,
        idempotency: IdempotencyCache,
        market_data: MarketDataProvider,
        position_provider: PositionProvider,
        equity_provider: EquityProvider | None = None,
        event_bus: EventBus | None = None,
        risk_gate: RiskGate | None = None,
        clock: Callable[[], float] = _default_clock,
        submit_timeout_s: float | None = None,
    ) -> None:
        self._adapter = adapter
        self._idempotency = idempotency
        self._market_data = market_data
        self._position_provider = position_provider
        self._equity_provider = equity_provider
        self._event_bus = event_bus
        self._risk_gate = risk_gate
        self._clock = clock
        self._submit_timeout_s = submit_timeout_s

    async def submit_signal(self, signal: UniversalSignal) -> SignalAck:
        """Validate, plan, submit, and return a synchronous :class:`SignalAck`.

        Raises whatever the adapter raises if the actual venue submit
        fails. Schema / sizing / stop / intent errors are caught and
        returned as a non-accepted :class:`SignalAck`.
        """

        cached_json = await self._idempotency.get(signal.signal_id)
        if cached_json is not None:
            return _ack_from_cache_json(cached_json)

        ack = await self._run_accept_path(signal)

        await self._idempotency.set(
            signal.signal_id, json.dumps(signal_ack_to_wire(ack))
        )
        self._publish_signal_received(signal, ack)
        return ack

    async def _run_accept_path(self, signal: UniversalSignal) -> SignalAck:
        # --- Schema sanity --------------------------------------------------
        if signal.ttl_seconds <= 0:
            return self._reject(signal, RejectionReason.SCHEMA)
        if not signal.symbol:
            return self._reject(signal, RejectionReason.UNKNOWN_SYMBOL)

        # --- Intent resolution ---------------------------------------------
        existing = self._position_provider.get_position(
            signal.venue, signal.symbol
        )
        if signal.intent is Intent.OPEN:
            if existing is not None and existing.qty > 0:
                return self._reject(signal, RejectionReason.INVALID_INTENT)
        else:
            # ADD / REDUCE / CLOSE / REVERSE are deferred to Phase 3e
            # (position_manager); the spec marks these as part of the
            # intent resolver but they require live position state to
            # validate. Reject explicitly so producers see a deterministic
            # rejection instead of a silent accept.
            return self._reject(signal, RejectionReason.INVALID_INTENT)

        # --- Pre-check: RiskBased sizing needs a stop ----------------------
        if isinstance(signal.sizing, RiskBased) and signal.sl is None:
            return self._reject(signal, RejectionReason.SIZING_REQUIRES_SL)

        # --- Reference price -----------------------------------------------
        reference_price = self._market_data.get_reference_price(
            signal.venue, signal.symbol
        )
        if reference_price is None or reference_price <= 0:
            return self._reject(signal, RejectionReason.UNKNOWN_SYMBOL)

        # --- Stop prices (sl first so RiskBased sizing can use it) ---------
        try:
            sl_price = _compute_optional_stop(
                signal.sl,
                entry_price=reference_price,
                direction=signal.direction,
                kind="sl",
            )
            tp_price = _compute_optional_stop(
                signal.tp,
                entry_price=reference_price,
                direction=signal.direction,
                kind="tp",
            )
        except StopError as exc:
            _log.warning(
                "stop calc failed for signal=%s: %s", signal.signal_id, exc
            )
            return self._reject(signal, RejectionReason.SCHEMA)

        # --- Target qty -----------------------------------------------------
        equity_usd: float | None = None
        if self._equity_provider is not None:
            equity_usd = self._equity_provider.get_equity_usd(signal.venue)
        try:
            qty = compute_target_qty(
                signal.sizing,
                reference_price=reference_price,
                equity_usd=equity_usd,
                sl_price=sl_price,
            )
        except SizingError as exc:
            _log.warning(
                "sizing failed for signal=%s: %s", signal.signal_id, exc
            )
            return self._reject(signal, RejectionReason.SCHEMA)

        plan = _AcceptPlan(
            qty=qty,
            sl_price=sl_price,
            tp_price=tp_price,
            entry_side=(
                OrderSide.BUY
                if signal.direction is Direction.LONG
                else OrderSide.SELL
            ),
            exit_side=(
                OrderSide.SELL
                if signal.direction is Direction.LONG
                else OrderSide.BUY
            ),
        )

        # --- Risk gate (kill switch / daily loss / per-symbol caps) -------
        if self._risk_gate is not None:
            decision: RiskDecision = self._risk_gate.evaluate(
                signal,
                target_qty=plan.qty,
                reference_price=reference_price,
            )
            if not decision.allowed:
                assert decision.rejection_reason is not None
                _log.info(
                    "risk gate denied signal=%s reason=%s detail=%s",
                    signal.signal_id,
                    decision.rejection_reason.value,
                    decision.detail,
                )
                return self._reject(signal, decision.rejection_reason)

        # --- Submit (raises propagate; idempotency NOT cached on raise) ----
        await self._submit_orders(signal, plan)

        return SignalAck(
            signal_id=signal.signal_id,
            accepted=True,
            duplicate=False,
            rejection_reason=None,
            ts=self._clock(),
            correlation_id=signal.correlation_id,
        )

    async def _submit_orders(
        self, signal: UniversalSignal, plan: _AcceptPlan
    ) -> None:
        """Dispatch the entry order plus optional SL / TP in parallel."""

        coros = [self._adapter.submit_order(
            _entry_request(signal, plan), timeout_s=self._submit_timeout_s,
        )]
        if plan.sl_price is not None:
            coros.append(
                self._adapter.submit_order(
                    _stop_request(
                        signal,
                        plan,
                        order_type=OrderType.STOP_MARKET,
                        stop_price=plan.sl_price,
                        leg="sl",
                    ),
                    timeout_s=self._submit_timeout_s,
                )
            )
        if plan.tp_price is not None:
            coros.append(
                self._adapter.submit_order(
                    _stop_request(
                        signal,
                        plan,
                        order_type=OrderType.TAKE_PROFIT_MARKET,
                        stop_price=plan.tp_price,
                        leg="tp",
                    ),
                    timeout_s=self._submit_timeout_s,
                )
            )
        await asyncio.gather(*coros)

    def _reject(
        self, signal: UniversalSignal, reason: RejectionReason
    ) -> SignalAck:
        return SignalAck(
            signal_id=signal.signal_id,
            accepted=False,
            duplicate=False,
            rejection_reason=reason,
            ts=self._clock(),
            correlation_id=signal.correlation_id,
        )

    def _publish_signal_received(
        self, signal: UniversalSignal, ack: SignalAck
    ) -> None:
        if self._event_bus is None:
            return
        payload = {
            "signal": signal_to_wire(signal),
            "ack": signal_ack_to_wire(ack),
        }
        self._event_bus.publish(EventType.SIGNAL_RECEIVED.value, payload)


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _coid(signal_id: str, leg: str) -> str:
    """Per-leg ``client_order_id`` from ``signal_id``.

    Binance allows up to 36 chars. We take the first 24 chars of
    ``signal_id`` (enough for any reasonable producer ID and leaving
    room for the ``-entry`` / ``-sl`` / ``-tp`` suffix) and append the
    leg name. Deterministic — a retry with the same ``signal_id`` lands
    on the same ``client_order_id`` so the venue's own idempotency
    catches duplicates if ours misses.
    """

    return f"{signal_id[:24]}-{leg}"


def _entry_request(
    signal: UniversalSignal, plan: _AcceptPlan
) -> OrderRequest:
    return OrderRequest(
        client_order_id=_coid(signal.signal_id, "entry"),
        venue=signal.venue,
        symbol=signal.symbol,
        side=plan.entry_side,
        order_type=OrderType.MARKET,
        qty=plan.qty,
        price=None,
        stop_price=None,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        close_position=False,
        signal_id=signal.signal_id,
        correlation_id=signal.correlation_id,
    )


def _stop_request(
    signal: UniversalSignal,
    plan: _AcceptPlan,
    *,
    order_type: OrderType,
    stop_price: float,
    leg: Literal["sl", "tp"],
) -> OrderRequest:
    return OrderRequest(
        client_order_id=_coid(signal.signal_id, leg),
        venue=signal.venue,
        symbol=signal.symbol,
        side=plan.exit_side,
        order_type=order_type,
        qty=plan.qty,
        price=None,
        stop_price=stop_price,
        time_in_force=TimeInForce.GTC,
        reduce_only=False,
        close_position=True,
        signal_id=signal.signal_id,
        correlation_id=signal.correlation_id,
    )


def _compute_optional_stop(
    spec: StopSpec | None,
    *,
    entry_price: float,
    direction: Direction,
    kind: Literal["sl", "tp"],
) -> float | None:
    if spec is None:
        return None
    return compute_protective_price(
        spec, entry_price=entry_price, direction=direction, kind=kind,
    )


def _ack_from_cache_json(raw: str) -> SignalAck:
    """Reconstitute a cached :class:`SignalAck` from its JSON wire form.

    Always flips ``duplicate`` to ``True`` — the caller is asking about
    a ``signal_id`` we already responded to, regardless of whether the
    original response was accept or reject.
    """

    data = json.loads(raw)
    reason_str = data.get("rejection_reason")
    return SignalAck(
        signal_id=data["signal_id"],
        accepted=bool(data["accepted"]),
        duplicate=True,
        rejection_reason=(
            RejectionReason(reason_str) if reason_str else None
        ),
        ts=float(data["ts"]),
        correlation_id=data.get("correlation_id"),
    )


__all__ = ["SignalRouter"]
