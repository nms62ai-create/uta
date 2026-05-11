"""Phase 3g: embedded :class:`TradeAdapter` public API (decision A3).

The single, stable surface that producers — heatmap-sdk, channel bots,
manual-trading UIs, anything else — use to drive UTA. Composes the
Phase 3 pieces (venue adapter, signal router, position manager, risk
gate, event bus) behind a small lifecycle (``start`` / ``close``),
synchronous reads, and a single submission method that returns a typed
:class:`SignalAck`. Producers never reach into the lower layers
directly.

Lifecycle
---------

* :meth:`start` brings the venue adapter online (which in turn starts
  its WS-trade / user-stream / market-stream clients) and then runs
  the :class:`PositionManager` bootstrap. Idempotent.
* :meth:`close` tears everything down in reverse order. Errors during
  individual close steps are logged but never re-raised, mirroring
  :class:`BinanceUmAdapter.close` semantics — one faulty close should
  not strand the others.

Construction
------------

Explicit dependency injection. The caller hands in pre-built
components; a companion ``from_binance_um`` factory wiring the Binance
stack from a :class:`Config` is intentionally deferred until Phase 4
(when we have a second venue to factor against). Until then producers
either wire the components themselves or use the recipe in
``docs/SIGNAL_PROTOCOL.md``.

Threading / concurrency
-----------------------

All public methods are async, single-event-loop. ``submit_signal`` is
re-entrant — the underlying signal router serialises per-signal-id and
the event bus drop-oldest policy means a slow consumer can never stall
the accept path.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .bus.event_bus import EventBus, Subscription
from .core.position_manager import PositionManager
from .core.protocols import ExchangeAdapter
from .core.risk import RiskGate, RiskState
from .core.signal_router import SignalRouter
from .types import EventType, Position, SignalAck, UniversalSignal, Venue

_log = logging.getLogger(__name__)


class TradeAdapterError(Exception):
    """Base class for :class:`TradeAdapter` errors."""


class TradeAdapterClosed(TradeAdapterError):
    """Operation attempted on a closed adapter."""


class TradeAdapterNotStarted(TradeAdapterError):
    """Operation attempted on an adapter that hasn't been started."""


class TradeAdapter:
    """One-stop public surface over the Phase 3 stack.

    See module docstring for the full contract.
    """

    def __init__(
        self,
        *,
        venue: Venue,
        exchange_adapter: ExchangeAdapter,
        signal_router: SignalRouter,
        event_bus: EventBus,
        position_manager: PositionManager | None = None,
        risk_state: RiskState | None = None,
        risk_gate: RiskGate | None = None,
    ) -> None:
        self._venue = venue
        self._exchange_adapter = exchange_adapter
        self._signal_router = signal_router
        self._event_bus = event_bus
        self._position_manager = position_manager
        self._risk_state = risk_state
        self._risk_gate = risk_gate

        self._started = False
        self._closed = False
        self._start_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def is_started(self) -> bool:
        return self._started and not self._closed

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        """Bring the stack online. Idempotent.

        Order of operations matters: venue first (REST + WS-trade +
        user-stream live) then the position manager (which hits REST
        for its bootstrap and needs the venue's listenKey rotation
        running so live events don't get lost during the bootstrap
        window).
        """

        if self._closed:
            raise TradeAdapterClosed("adapter is closed")
        async with self._start_lock:
            if self._started:
                return
            await self._call_lifecycle(self._exchange_adapter, "start")
            if self._position_manager is not None:
                try:
                    await self._position_manager.start()
                except Exception:
                    # Roll back the venue start so the caller can
                    # rebuild cleanly. ``close`` is idempotent and
                    # best-effort.
                    await self._call_lifecycle(
                        self._exchange_adapter, "close"
                    )
                    raise
            self._started = True
            _log.info("TradeAdapter started venue=%s", self._venue.value)

    async def close(self) -> None:
        """Tear everything down in reverse order. Idempotent.

        Errors during individual component closes are logged but never
        re-raised so a single faulty teardown doesn't strand the
        others.
        """

        if self._closed:
            return
        self._closed = True
        if self._position_manager is not None:
            try:
                await self._position_manager.stop()
            except Exception as e:  # pragma: no cover - defensive
                _log.warning("position_manager stop error: %s", e)
        try:
            await self._call_lifecycle(self._exchange_adapter, "close")
        except Exception as e:  # pragma: no cover - defensive
            _log.warning("exchange_adapter close error: %s", e)
        _log.info("TradeAdapter closed venue=%s", self._venue.value)

    # ------------------------------------------------------------------
    # Submission surface
    # ------------------------------------------------------------------

    async def submit_signal(self, signal: UniversalSignal) -> SignalAck:
        """Validate and submit a :class:`UniversalSignal`.

        Returns the :class:`SignalAck` produced by the signal router.
        Venue-side errors raised during the actual order submit
        propagate unchanged so the caller can decide whether to retry.
        """

        self._ensure_running()
        return await self._signal_router.submit_signal(signal)

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        """Cancel ``client_order_id`` on ``symbol`` via the venue adapter."""

        self._ensure_running()
        await self._exchange_adapter.cancel_order(
            symbol, client_order_id, timeout_s=timeout_s
        )

    # ------------------------------------------------------------------
    # Event subscription
    # ------------------------------------------------------------------

    def subscribe(
        self,
        event_type: EventType | str,
        *,
        queue_size: int = 256,
    ) -> Subscription:
        """Subscribe to an :class:`EventType` topic on the embedded bus.

        Accepts either an :class:`EventType` member or the raw string
        topic name; consumers that want every topic should subscribe
        to each explicitly (the bus has no wildcards by design — see
        :class:`EventBus`).
        """

        topic = (
            event_type.value if isinstance(event_type, EventType) else event_type
        )
        return self._event_bus.subscribe(topic, queue_size=queue_size)

    # ------------------------------------------------------------------
    # Read-only introspection
    # ------------------------------------------------------------------

    def get_position(self, venue: Venue, symbol: str) -> Position | None:
        """Look up the current stored position for ``(venue, symbol)``."""

        if self._position_manager is None:
            return None
        return self._position_manager.store.get_position(venue, symbol)

    def get_equity_usd(self, venue: Venue) -> float | None:
        """Return the last-known equity for ``venue`` in USD."""

        if self._position_manager is None:
            return None
        return self._position_manager.store.get_equity_usd(venue)

    def open_positions(self) -> list[Position]:
        """All positions in the store with ``qty > 0``."""

        if self._position_manager is None:
            return []
        return self._position_manager.store.open_positions()

    # ------------------------------------------------------------------
    # Kill switch surface
    # ------------------------------------------------------------------

    def trip_kill_switch(self, reason: str) -> None:
        """Trip the risk gate's kill switch. No-op if no risk_state is wired."""

        if self._risk_state is None:
            _log.warning(
                "trip_kill_switch called but no risk_state is wired"
            )
            return
        self._risk_state.trip_kill_switch(reason)

    def reset_kill_switch(self) -> None:
        """Clear the kill switch. No-op if no risk_state is wired."""

        if self._risk_state is None:
            return
        self._risk_state.reset_kill_switch()

    @property
    def kill_switch_tripped(self) -> bool:
        if self._risk_state is None:
            return False
        return self._risk_state.kill_switch_tripped

    # ------------------------------------------------------------------
    # Accessors (advanced — escape hatches for consumers that need
    # more than the curated surface)
    # ------------------------------------------------------------------

    @property
    def venue(self) -> Venue:
        return self._venue

    @property
    def event_bus(self) -> EventBus:
        """Direct access to the embedded bus."""

        return self._event_bus

    @property
    def position_manager(self) -> PositionManager | None:
        return self._position_manager

    @property
    def risk_state(self) -> RiskState | None:
        return self._risk_state

    @property
    def risk_gate(self) -> RiskGate | None:
        return self._risk_gate

    @property
    def exchange_adapter(self) -> ExchangeAdapter:
        return self._exchange_adapter

    @property
    def signal_router(self) -> SignalRouter:
        return self._signal_router

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> TradeAdapter:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_running(self) -> None:
        if self._closed:
            raise TradeAdapterClosed("adapter is closed")
        if not self._started:
            raise TradeAdapterNotStarted(
                "adapter not started; call start() first"
            )

    @staticmethod
    async def _call_lifecycle(target: Any, method_name: str) -> None:
        method = getattr(target, method_name, None)
        if method is None:
            return
        await method()


__all__ = [
    "TradeAdapter",
    "TradeAdapterClosed",
    "TradeAdapterError",
    "TradeAdapterNotStarted",
]
