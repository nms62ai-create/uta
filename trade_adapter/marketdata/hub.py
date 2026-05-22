"""Coalescing market-data hub (A.1).

The hub multiplexes ``N`` local subscribers onto **at most one** upstream
subscription per ``(venue, symbol, kind)`` triple (decision A16, spec
prohibition C.11). The wire-level transport is owned by the per-venue
:class:`MarketDataStreamProvider`; the hub only does demand counting and
fan-out.

Lifecycle of one stream:

1. First consumer calls :meth:`MarketDataHub.subscribe_book` etc.
2. Refcount for ``(venue, symbol, kind)`` goes 0 → 1; the hub awaits the
   provider's ``subscribe_stream`` once.
3. Every frame the provider hands us is fanned out non-blockingly to
   every active subscriber's bounded queue using drop-oldest semantics
   (same policy as :class:`~trade_adapter.bus.event_bus.EventBus`).
4. The Nth consumer joins / leaves — refcount goes up / down, no
   upstream churn.
5. Last consumer closes; refcount 1 → 0; the hub awaits the provider's
   ``unsubscribe_stream``.

Single-event-loop only. Concurrent subscribe / close are serialised per
``(venue, symbol, kind)`` via an internal lock so we don't race the
provider during the 0 ↔ 1 transitions.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from ..types import BBOUpdate, BookUpdate, TradePrint, Venue
from .types import MarketDataStreamProvider, StreamKind

_log = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 256


@dataclass(slots=True)
class MarketDataSubscription:
    """Per-consumer view onto a coalesced ``(venue, symbol, kind)`` stream.

    Mirrors :class:`~trade_adapter.bus.event_bus.Subscription` so async
    iterator usage is identical (``async for ev in sub: ...``). The hub
    keeps a strong reference until :meth:`close` is called.
    """

    venue: Venue
    symbol: str
    kind: StreamKind
    queue: asyncio.Queue[Any]
    dropped_count: int = 0
    _closed: bool = False
    _hub: MarketDataHub | None = field(default=None, repr=False)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Any]:
        while True:
            if self._closed and self.queue.empty():
                return
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=0.1)
            except TimeoutError:
                if self._closed:
                    return
                continue
            yield event

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._hub is not None:
            await self._hub._release(self)
            self._hub = None

    async def __aenter__(self) -> MarketDataSubscription:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


@dataclass(slots=True)
class _StreamState:
    """Hub-side bookkeeping for one ``(venue, symbol, kind)`` triple."""

    subscribers: list[MarketDataSubscription] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    upstream_active: bool = False


class MarketDataHubError(Exception):
    """Base class for hub-level errors."""


class MarketDataHubUnknownVenue(MarketDataHubError):
    """No provider registered for the requested venue."""


class MarketDataHubClosed(MarketDataHubError):
    """Operation attempted on a closed hub."""


class MarketDataHub:
    """Coalescing fan-out over per-venue :class:`MarketDataStreamProvider` s.

    See module docstring. The hub owns the providers' lifecycle when
    :meth:`start` / :meth:`close` are called; if the caller manages
    provider lifecycle separately, construct the hub with already-started
    providers and call only :meth:`close` (which is safe to repeat).
    """

    def __init__(
        self,
        *,
        providers: Mapping[Venue, MarketDataStreamProvider],
        default_queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> None:
        self._providers = dict(providers)
        self._default_queue_size = default_queue_size
        self._streams: dict[tuple[Venue, str, StreamKind], _StreamState] = {}
        self._started = False
        self._closed = False
        self._lifecycle_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start every registered provider. Idempotent."""

        if self._closed:
            raise MarketDataHubClosed("hub is closed")
        async with self._lifecycle_lock:
            if self._started:
                return
            for provider in self._providers.values():
                await provider.start()
            self._started = True
            _log.info(
                "market-data hub started venues=%s",
                sorted(v.value for v in self._providers),
            )

    async def close(self) -> None:
        """Close every subscription, then every provider. Idempotent."""

        if self._closed:
            return
        self._closed = True
        # Drop every active subscriber's queue reference; downstream
        # iterators wake on the timeout path and observe _closed=True.
        for state in self._streams.values():
            for sub in state.subscribers:
                sub._closed = True
                sub._hub = None
            state.subscribers.clear()
        self._streams.clear()
        for venue, provider in self._providers.items():
            try:
                await provider.close()
            except Exception as e:  # pragma: no cover - defensive
                _log.warning(
                    "market-data hub error closing provider %s: %s",
                    venue.value,
                    e,
                )
        _log.info("market-data hub closed")

    # ------------------------------------------------------------------
    # Subscription surface
    # ------------------------------------------------------------------

    async def subscribe_book(
        self,
        venue: Venue,
        symbol: str,
        *,
        queue_size: int | None = None,
    ) -> MarketDataSubscription:
        """Subscribe to top-N order book snapshots for ``(venue, symbol)``."""

        return await self._subscribe(venue, symbol, StreamKind.BOOK, queue_size)

    async def subscribe_trades(
        self,
        venue: Venue,
        symbol: str,
        *,
        queue_size: int | None = None,
    ) -> MarketDataSubscription:
        """Subscribe to taker-print tape for ``(venue, symbol)``."""

        return await self._subscribe(venue, symbol, StreamKind.TRADES, queue_size)

    async def subscribe_bbo(
        self,
        venue: Venue,
        symbol: str,
        *,
        queue_size: int | None = None,
    ) -> MarketDataSubscription:
        """Subscribe to best-bid / best-ask updates for ``(venue, symbol)``."""

        return await self._subscribe(venue, symbol, StreamKind.BBO, queue_size)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _subscribe(
        self,
        venue: Venue,
        symbol: str,
        kind: StreamKind,
        queue_size: int | None,
    ) -> MarketDataSubscription:
        if self._closed:
            raise MarketDataHubClosed("hub is closed")
        if venue not in self._providers:
            raise MarketDataHubUnknownVenue(
                f"no market-data provider registered for venue={venue.value}"
            )

        key = (venue, symbol, kind)
        state = self._streams.setdefault(key, _StreamState())

        sub = MarketDataSubscription(
            venue=venue,
            symbol=symbol,
            kind=kind,
            queue=asyncio.Queue(maxsize=queue_size or self._default_queue_size),
            _hub=self,
        )

        async with state.lock:
            # Register the subscriber *before* awaiting the upstream
            # subscribe so a frame that arrives between provider
            # ack and lock release still finds our queue.
            state.subscribers.append(sub)
            if not state.upstream_active:
                provider = self._providers[venue]
                try:
                    await provider.subscribe_stream(
                        symbol, kind, self._build_callback(key)
                    )
                except Exception:
                    state.subscribers.remove(sub)
                    if not state.subscribers and not state.upstream_active:
                        self._streams.pop(key, None)
                    raise
                state.upstream_active = True
                _log.info(
                    "market-data hub upstream open venue=%s symbol=%s kind=%s",
                    venue.value,
                    symbol,
                    kind.value,
                )

        return sub

    async def _release(self, sub: MarketDataSubscription) -> None:
        """Called by :meth:`MarketDataSubscription.close`."""

        key = (sub.venue, sub.symbol, sub.kind)
        state = self._streams.get(key)
        if state is None:
            return
        async with state.lock:
            try:
                state.subscribers.remove(sub)
            except ValueError:
                pass
            if state.subscribers:
                return
            if state.upstream_active and not self._closed:
                provider = self._providers.get(sub.venue)
                if provider is not None:
                    try:
                        await provider.unsubscribe_stream(sub.symbol, sub.kind)
                    except Exception as e:  # pragma: no cover - defensive
                        _log.warning(
                            "market-data hub error unsubscribing %s/%s/%s: %s",
                            sub.venue.value,
                            sub.symbol,
                            sub.kind.value,
                            e,
                        )
                state.upstream_active = False
                _log.info(
                    "market-data hub upstream closed venue=%s symbol=%s kind=%s",
                    sub.venue.value,
                    sub.symbol,
                    sub.kind.value,
                )
            self._streams.pop(key, None)

    def _build_callback(
        self, key: tuple[Venue, str, StreamKind]
    ) -> Any:
        """Return a closure that fans events out to ``key``'s subscribers.

        Drop-oldest backpressure: if a subscriber's queue is full we
        discard its oldest pending event and enqueue the new one,
        incrementing :attr:`MarketDataSubscription.dropped_count`. The
        publish path never awaits.
        """

        def deliver(event: Any) -> None:
            state = self._streams.get(key)
            if state is None:
                return
            for sub in state.subscribers:
                if sub._closed:
                    continue
                q = sub.queue
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:  # pragma: no cover - race
                        pass
                    sub.dropped_count += 1
                    try:
                        q.put_nowait(event)
                    except asyncio.QueueFull:  # pragma: no cover - tiny race
                        sub.dropped_count += 1

        return deliver

    # ------------------------------------------------------------------
    # Introspection (mostly for tests)
    # ------------------------------------------------------------------

    def subscriber_count(
        self, venue: Venue, symbol: str, kind: StreamKind
    ) -> int:
        state = self._streams.get((venue, symbol, kind))
        if state is None:
            return 0
        return sum(1 for s in state.subscribers if not s._closed)

    def upstream_active(
        self, venue: Venue, symbol: str, kind: StreamKind
    ) -> bool:
        state = self._streams.get((venue, symbol, kind))
        return bool(state and state.upstream_active)


# Re-export the typed events for convenience.
__all__ = [
    "BBOUpdate",
    "BookUpdate",
    "MarketDataHub",
    "MarketDataHubClosed",
    "MarketDataHubError",
    "MarketDataHubUnknownVenue",
    "MarketDataSubscription",
    "TradePrint",
]
