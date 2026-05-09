"""Tests for the in-process event bus (decision A20)."""

from __future__ import annotations

import asyncio

import pytest

from trade_adapter.bus.event_bus import EventBus

pytestmark = pytest.mark.asyncio


async def test_publish_no_subscribers_returns_zero() -> None:
    bus = EventBus()
    delivered = bus.publish("fill", {"x": 1})
    assert delivered == 0


async def test_single_subscriber_receives_in_order() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill", queue_size=4)
    for i in range(3):
        bus.publish("fill", i)
    received: list[int] = []

    async def reader() -> None:
        async for ev in sub:
            received.append(ev)
            if len(received) == 3:
                await sub.close()

    await asyncio.wait_for(reader(), timeout=2.0)
    assert received == [0, 1, 2]


async def test_drop_oldest_on_full_queue() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill", queue_size=2)
    for i in range(5):
        bus.publish("fill", i)
    # Queue size 2; we published 5; drops should be 3.
    assert sub.dropped_count == 3
    assert sub.queue.qsize() == 2

    received: list[int] = []

    async def reader() -> None:
        async for ev in sub:
            received.append(ev)
            if len(received) == 2:
                await sub.close()

    await asyncio.wait_for(reader(), timeout=2.0)
    # Drop-oldest: the *latest* events survive.
    assert received == [3, 4]


async def test_multiple_subscribers_independent_queues() -> None:
    bus = EventBus()
    fast = bus.subscribe("fill", queue_size=8)
    slow = bus.subscribe("fill", queue_size=2)

    for i in range(5):
        bus.publish("fill", i)

    assert slow.dropped_count == 3
    assert fast.dropped_count == 0
    assert fast.queue.qsize() == 5
    assert slow.queue.qsize() == 2


async def test_publish_to_other_topic_does_not_reach_subscriber() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill")
    bus.publish("order_update", {"x": 1})
    assert sub.queue.qsize() == 0


async def test_unsubscribe_via_close() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill")
    assert bus.subscriber_count("fill") == 1
    await sub.close()
    assert bus.subscriber_count("fill") == 0
    bus.publish("fill", "x")
    # closed subscriber must not receive new events.
    assert sub.queue.qsize() == 0


async def test_close_bus_closes_subscribers() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill")
    await bus.close()
    assert sub._closed is True
    with pytest.raises(RuntimeError):
        bus.subscribe("fill")


async def test_async_context_manager_closes_on_exit() -> None:
    bus = EventBus()
    sub = bus.subscribe("fill")
    async with sub:
        bus.publish("fill", 1)
    assert sub._closed is True


async def test_invalid_queue_size_rejected() -> None:
    bus = EventBus()
    with pytest.raises(ValueError):
        bus.subscribe("fill", queue_size=0)
