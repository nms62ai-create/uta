"""In-process pub/sub event bus.

Implements decision **A20** (event-bus backpressure): every subscriber
gets a private bounded ``asyncio.Queue``. When the queue is full, the
oldest event is dropped and a per-subscriber ``dropped_count`` is
incremented. The publish path never awaits a slow subscriber — that
guarantees the trading hot path (``Fill`` → ``OrderUpdate`` →
``PositionUpdate``) cannot be stalled by, for example, a UI client
that has stopped reading.

Topology
--------
* Topics are strings. The producer publishes to a topic; subscribers
  subscribe to a topic and receive only events for that topic.
* No wildcards. The set of topics is small and fixed (one per
  ``EventType``); a subscriber that wants \"everything\" subscribes to
  each topic explicitly.
* No message ordering between topics. Within a single topic, delivery
  is FIFO modulo drops.
* Drop policy is *drop oldest*: when a publish would overflow a
  subscriber's queue, the oldest pending event is removed and the new
  event is enqueued. Counterpart metrics:
  ``uta_event_bus_drops_total`` (per spec A20) is exposed by reading
  ``Subscription.dropped_count``.

The bus does not own any threads — it is pure ``asyncio``. It is safe
to call ``publish`` from any coroutine on the same event loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

DEFAULT_QUEUE_SIZE = 256

_log = logging.getLogger(__name__)


@dataclass(slots=True)
class Subscription:
    """One subscriber's view onto a topic.

    Returned by :meth:`EventBus.subscribe`. Iterate it with
    ``async for event in subscription:`` to receive events. Call
    :meth:`close` (or use ``async with``) to release the bus's
    reference and stop receiving events.
    """

    topic: str
    queue: asyncio.Queue[Any]
    dropped_count: int = 0
    _closed: bool = False
    _bus: EventBus | None = field(default=None, repr=False)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Any]:
        while True:
            if self._closed and self.queue.empty():
                return
            try:
                # Wake periodically so a close() observed during a long
                # quiet period can terminate the iterator without a
                # producer-side poison pill.
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
        if self._bus is not None:
            self._bus._unsubscribe(self)
            self._bus = None

    async def __aenter__(self) -> Subscription:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


class EventBus:
    """Topic-based, bounded, drop-oldest pub/sub. See module docstring."""

    def __init__(self) -> None:
        self._subs: dict[str, list[Subscription]] = {}
        self._closed = False

    def subscribe(
        self,
        topic: str,
        *,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> Subscription:
        """Register a new subscriber for ``topic``.

        ``queue_size`` is the bound for that subscriber's private
        queue. Once full, the bus drops the oldest pending event for
        that subscriber when a new one arrives.
        """

        if self._closed:
            raise RuntimeError("EventBus is closed")
        if queue_size < 1:
            raise ValueError("queue_size must be >= 1")
        sub = Subscription(
            topic=topic,
            queue=asyncio.Queue(maxsize=queue_size),
            _bus=self,
        )
        self._subs.setdefault(topic, []).append(sub)
        return sub

    def publish(self, topic: str, event: Any) -> int:
        """Publish ``event`` to all subscribers of ``topic``.

        Non-blocking. Returns the number of subscribers that received
        the event without dropping. The publish path never awaits a
        slow subscriber.
        """

        delivered = 0
        subs = self._subs.get(topic)
        if not subs:
            return 0
        for sub in subs:
            if sub._closed:
                continue
            q = sub.queue
            try:
                q.put_nowait(event)
                delivered += 1
            except asyncio.QueueFull:
                # Drop oldest, keep new (FIFO drop policy, A20).
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - race
                    pass
                sub.dropped_count += 1
                try:
                    q.put_nowait(event)
                    delivered += 1
                except asyncio.QueueFull:  # pragma: no cover - tiny race
                    sub.dropped_count += 1
                _log.debug(
                    "event_bus drop-oldest topic=%s dropped_total=%d",
                    topic,
                    sub.dropped_count,
                )
        return delivered

    def subscriber_count(self, topic: str) -> int:
        return sum(1 for s in self._subs.get(topic, ()) if not s._closed)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for subs in list(self._subs.values()):
            for sub in list(subs):
                await sub.close()
        self._subs.clear()

    # Internal --------------------------------------------------------

    def _unsubscribe(self, sub: Subscription) -> None:
        subs = self._subs.get(sub.topic)
        if not subs:
            return
        try:
            subs.remove(sub)
        except ValueError:
            return
        if not subs:
            del self._subs[sub.topic]
