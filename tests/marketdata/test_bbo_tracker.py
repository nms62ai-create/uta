"""Tests for :class:`BboTracker` auto-subscription (A.1)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.marketdata import BboTracker, MarketDataHub, StreamKind
from trade_adapter.marketdata.types import StreamCallback
from trade_adapter.types import (
    BBOUpdate,
    Direction,
    EventType,
    PositionState,
    PositionUpdate,
    Venue,
)

pytestmark = pytest.mark.asyncio


@dataclass
class FakeProvider:
    venue: Venue = Venue.BINANCE_UM
    subscribed: dict[tuple[str, StreamKind], StreamCallback] = field(
        default_factory=dict
    )
    subscribe_calls: int = 0
    unsubscribe_calls: int = 0

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def subscribe_stream(
        self,
        symbol: str,
        kind: StreamKind,
        callback: StreamCallback,
    ) -> None:
        self.subscribe_calls += 1
        self.subscribed[(symbol, kind)] = callback

    async def unsubscribe_stream(
        self, symbol: str, kind: StreamKind
    ) -> None:
        self.unsubscribe_calls += 1
        self.subscribed.pop((symbol, kind), None)

    def push(self, symbol: str, kind: StreamKind, event: Any) -> None:
        cb = self.subscribed.get((symbol, kind))
        assert cb is not None
        cb(event)


def _position_update(
    *,
    symbol: str = "BTCUSDT",
    qty: float = 1.0,
    state: PositionState = PositionState.OPEN,
    venue: Venue = Venue.BINANCE_UM,
    direction: Direction = Direction.LONG,
    ts: float = 1700_000_000.0,
) -> PositionUpdate:
    return PositionUpdate(
        venue=venue,
        symbol=symbol,
        direction=direction,
        qty=qty,
        entry_price=50000.0,
        state=state,
        liquidation_price=None,
        unrealized_pnl_usd=None,
        margin_used_usd=None,
        ts=ts,
        signal_id=None,
    )


def _bbo_tick(
    *,
    bid: float = 99.0,
    ask: float = 101.0,
    symbol: str = "BTCUSDT",
    ts: float = 1700_000_010.0,
) -> BBOUpdate:
    return BBOUpdate(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        bid_price=bid,
        bid_qty=1.0,
        ask_price=ask,
        ask_qty=1.0,
        ts=ts,
    )


async def _wait_for(predicate, timeout: float = 2.0) -> None:
    """Poll-wait helper — the tracker reacts asynchronously."""

    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate():
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError("predicate never became true")
        await asyncio.sleep(0.01)


async def _make_stack() -> tuple[BboTracker, EventBus, FakeProvider, MarketDataHub]:
    bus = EventBus()
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    tracker = BboTracker(event_bus=bus, market_data_hub=hub)
    return tracker, bus, provider, hub


async def test_position_open_triggers_bbo_subscribe() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    try:
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=1.0),
        )
        await _wait_for(lambda: provider.subscribe_calls == 1)
        assert (("BTCUSDT", StreamKind.BBO) in provider.subscribed)
        assert tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT") is not None
    finally:
        await tracker.close()
        await hub.close()


async def test_position_close_triggers_bbo_unsubscribe() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    try:
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=1.0),
        )
        await _wait_for(lambda: provider.subscribe_calls == 1)

        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=0.0, state=PositionState.IDLE),
        )
        await _wait_for(lambda: provider.unsubscribe_calls == 1)
        assert tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT") is None
    finally:
        await tracker.close()
        await hub.close()


async def test_bbo_ticks_fold_into_snapshot() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    try:
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=1.0),
        )
        await _wait_for(lambda: provider.subscribe_calls == 1)

        provider.push(
            "BTCUSDT", StreamKind.BBO,
            _bbo_tick(bid=99.0, ask=101.0, ts=1700_000_010.0),
        )
        provider.push(
            "BTCUSDT", StreamKind.BBO,
            _bbo_tick(bid=95.0, ask=105.0, ts=1700_000_020.0),
        )
        provider.push(
            "BTCUSDT", StreamKind.BBO,
            _bbo_tick(bid=98.0, ask=102.0, ts=1700_000_030.0),
        )

        async def all_ticks_seen() -> bool:
            snap = tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT")
            return snap is not None and snap.tick_count == 3

        await _wait_for(lambda: asyncio.ensure_future(all_ticks_seen()))
        # Re-do the wait without coroutine wrap; _wait_for expects sync predicate.
        async def _poll() -> None:
            deadline = asyncio.get_event_loop().time() + 2.0
            while True:
                snap = tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT")
                if snap is not None and snap.tick_count == 3:
                    return
                if asyncio.get_event_loop().time() > deadline:
                    raise AssertionError("tick_count never reached 3")
                await asyncio.sleep(0.01)

        await _poll()
        snap = tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT")
        assert snap is not None
        assert snap.tick_count == 3
        assert snap.first_mid == pytest.approx(100.0)
        assert snap.last_mid == pytest.approx(100.0)
        assert snap.min_bid == pytest.approx(95.0)
        assert snap.max_ask == pytest.approx(105.0)
        assert snap.last_ts == pytest.approx(1700_000_030.0)
    finally:
        await tracker.close()
        await hub.close()


async def test_two_positions_two_subscriptions() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    try:
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(symbol="BTCUSDT", qty=1.0),
        )
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(symbol="ETHUSDT", qty=2.0),
        )
        await _wait_for(lambda: provider.subscribe_calls == 2)
        assert ("BTCUSDT", StreamKind.BBO) in provider.subscribed
        assert ("ETHUSDT", StreamKind.BBO) in provider.subscribed
    finally:
        await tracker.close()
        await hub.close()


async def test_idle_position_does_not_trigger_subscribe() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    try:
        # Flat position arrives — should NOT auto-subscribe.
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=0.0, state=PositionState.IDLE),
        )
        # Give the supervisor a chance to process.
        await asyncio.sleep(0.1)
        assert provider.subscribe_calls == 0
    finally:
        await tracker.close()
        await hub.close()


async def test_close_cancels_consumer_tasks_and_unsubscribes() -> None:
    tracker, bus, provider, hub = await _make_stack()
    await tracker.start()
    bus.publish(
        EventType.POSITION_UPDATE.value, _position_update(qty=1.0)
    )
    await _wait_for(lambda: provider.subscribe_calls == 1)

    await tracker.close()
    assert provider.unsubscribe_calls == 1
    await hub.close()


async def test_close_is_idempotent() -> None:
    tracker, _bus, _provider, hub = await _make_stack()
    await tracker.start()
    await tracker.close()
    await tracker.close()  # no exception
    await hub.close()
