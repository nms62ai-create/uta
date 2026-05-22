"""Tests for :class:`MarketDataHub` coalescing semantics (A.1)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.marketdata import (
    MarketDataHub,
    StreamKind,
)
from trade_adapter.marketdata.hub import (
    MarketDataHubClosed,
    MarketDataHubUnknownVenue,
)
from trade_adapter.marketdata.types import StreamCallback
from trade_adapter.types import BBOUpdate, BookLevel, BookUpdate, TradePrint, Venue

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class FakeProvider:
    """In-memory :class:`MarketDataStreamProvider` that records calls.

    Each call to :meth:`subscribe_stream` registers the callback so the
    test can invoke it directly to simulate inbound frames — that's
    exactly the contract the hub depends on.
    """

    venue: Venue = Venue.BINANCE_UM
    start_calls: int = 0
    close_calls: int = 0
    subscribed: dict[tuple[str, StreamKind], StreamCallback] = field(
        default_factory=dict
    )
    unsubscribed: list[tuple[str, StreamKind]] = field(default_factory=list)
    subscribe_calls: int = 0
    unsubscribe_calls: int = 0
    subscribe_error: BaseException | None = None

    async def start(self) -> None:
        self.start_calls += 1

    async def close(self) -> None:
        self.close_calls += 1

    async def subscribe_stream(
        self,
        symbol: str,
        kind: StreamKind,
        callback: StreamCallback,
    ) -> None:
        self.subscribe_calls += 1
        if self.subscribe_error is not None:
            raise self.subscribe_error
        self.subscribed[(symbol, kind)] = callback

    async def unsubscribe_stream(
        self, symbol: str, kind: StreamKind
    ) -> None:
        self.unsubscribe_calls += 1
        self.unsubscribed.append((symbol, kind))
        self.subscribed.pop((symbol, kind), None)

    def push(self, symbol: str, kind: StreamKind, event: Any) -> None:
        """Test helper: invoke the registered callback with ``event``."""

        callback = self.subscribed.get((symbol, kind))
        assert callback is not None, (
            f"no callback registered for ({symbol}, {kind})"
        )
        callback(event)


def _book(symbol: str = "BTCUSDT", sequence: int = 1) -> BookUpdate:
    return BookUpdate(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        bids=(BookLevel(price=99.0, qty=1.0),),
        asks=(BookLevel(price=101.0, qty=1.0),),
        ts=1700_000_000.0,
        sequence=sequence,
    )


def _bbo(symbol: str = "BTCUSDT", ts: float = 1700_000_000.0) -> BBOUpdate:
    return BBOUpdate(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        bid_price=99.0,
        bid_qty=1.0,
        ask_price=101.0,
        ask_qty=1.0,
        ts=ts,
    )


def _trade(symbol: str = "BTCUSDT", trade_id: str = "1") -> TradePrint:
    from trade_adapter.types import OrderSide

    return TradePrint(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        price=100.0,
        qty=1.0,
        side=OrderSide.BUY,
        ts=1700_000_000.0,
        trade_id=trade_id,
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_calls_each_provider_once() -> None:
    p1 = FakeProvider(venue=Venue.BINANCE_UM)
    p2 = FakeProvider(venue=Venue.BYBIT_LINEAR)
    hub = MarketDataHub(
        providers={Venue.BINANCE_UM: p1, Venue.BYBIT_LINEAR: p2}
    )

    await hub.start()
    await hub.start()  # idempotent
    try:
        assert p1.start_calls == 1
        assert p2.start_calls == 1
    finally:
        await hub.close()


async def test_close_closes_each_provider() -> None:
    p1 = FakeProvider(venue=Venue.BINANCE_UM)
    p2 = FakeProvider(venue=Venue.BYBIT_LINEAR)
    hub = MarketDataHub(
        providers={Venue.BINANCE_UM: p1, Venue.BYBIT_LINEAR: p2}
    )
    await hub.start()
    await hub.close()
    await hub.close()  # idempotent

    assert p1.close_calls == 1
    assert p2.close_calls == 1


async def test_subscribe_after_close_raises() -> None:
    hub = MarketDataHub(providers={Venue.BINANCE_UM: FakeProvider()})
    await hub.start()
    await hub.close()
    with pytest.raises(MarketDataHubClosed):
        await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")


async def test_unknown_venue_raises() -> None:
    hub = MarketDataHub(providers={Venue.BINANCE_UM: FakeProvider()})
    await hub.start()
    try:
        with pytest.raises(MarketDataHubUnknownVenue):
            await hub.subscribe_book(Venue.BYBIT_LINEAR, "BTCUSDT")
    finally:
        await hub.close()


# ---------------------------------------------------------------------------
# Coalescing
# ---------------------------------------------------------------------------


async def test_single_subscriber_opens_upstream_once() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        sub = await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
        assert provider.subscribe_calls == 1
        assert hub.upstream_active(Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK)
        assert hub.subscriber_count(Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK) == 1

        provider.push("BTCUSDT", StreamKind.BOOK, _book())
        ev = await asyncio.wait_for(sub.queue.get(), timeout=1.0)
        assert isinstance(ev, BookUpdate)

        await sub.close()
        assert provider.unsubscribe_calls == 1
        assert not hub.upstream_active(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        )
    finally:
        await hub.close()


async def test_three_subscribers_one_upstream() -> None:
    """A16 / C.11: N local subscribers → 1 upstream WS subscription."""

    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        subs = [
            await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
            for _ in range(3)
        ]
        assert provider.subscribe_calls == 1  # coalesced
        assert hub.subscriber_count(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        ) == 3

        provider.push("BTCUSDT", StreamKind.BOOK, _book(sequence=7))
        # Each subscriber receives a copy.
        for sub in subs:
            ev = await asyncio.wait_for(sub.queue.get(), timeout=1.0)
            assert isinstance(ev, BookUpdate)
            assert ev.sequence == 7

        # Closing two doesn't tear down the upstream.
        await subs[0].close()
        await subs[1].close()
        assert provider.unsubscribe_calls == 0
        assert hub.upstream_active(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        )

        # Closing the last one does.
        await subs[2].close()
        assert provider.unsubscribe_calls == 1
        assert not hub.upstream_active(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        )
    finally:
        await hub.close()


async def test_different_kinds_independent_upstreams() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        sub_book = await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
        sub_bbo = await hub.subscribe_bbo(Venue.BINANCE_UM, "BTCUSDT")
        sub_trades = await hub.subscribe_trades(Venue.BINANCE_UM, "BTCUSDT")

        assert provider.subscribe_calls == 3
        provider.push("BTCUSDT", StreamKind.BOOK, _book())
        provider.push("BTCUSDT", StreamKind.BBO, _bbo())
        provider.push("BTCUSDT", StreamKind.TRADES, _trade())

        b = await asyncio.wait_for(sub_book.queue.get(), timeout=1.0)
        assert isinstance(b, BookUpdate)
        q = await asyncio.wait_for(sub_bbo.queue.get(), timeout=1.0)
        assert isinstance(q, BBOUpdate)
        t = await asyncio.wait_for(sub_trades.queue.get(), timeout=1.0)
        assert isinstance(t, TradePrint)

        await sub_book.close()
        await sub_bbo.close()
        await sub_trades.close()
        assert provider.unsubscribe_calls == 3
    finally:
        await hub.close()


async def test_different_symbols_independent_upstreams() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        sub_btc = await hub.subscribe_bbo(Venue.BINANCE_UM, "BTCUSDT")
        sub_eth = await hub.subscribe_bbo(Venue.BINANCE_UM, "ETHUSDT")

        assert provider.subscribe_calls == 2

        provider.push("BTCUSDT", StreamKind.BBO, _bbo("BTCUSDT"))
        provider.push("ETHUSDT", StreamKind.BBO, _bbo("ETHUSDT"))

        b = await asyncio.wait_for(sub_btc.queue.get(), timeout=1.0)
        e = await asyncio.wait_for(sub_eth.queue.get(), timeout=1.0)
        assert b.symbol == "BTCUSDT"
        assert e.symbol == "ETHUSDT"

        # Cross-symbol no leak.
        assert sub_btc.queue.empty()
        assert sub_eth.queue.empty()
    finally:
        await sub_btc.close()
        await sub_eth.close()
        await hub.close()


async def test_multi_venue_isolation() -> None:
    p_binance = FakeProvider(venue=Venue.BINANCE_UM)
    p_bybit = FakeProvider(venue=Venue.BYBIT_LINEAR)
    hub = MarketDataHub(
        providers={
            Venue.BINANCE_UM: p_binance,
            Venue.BYBIT_LINEAR: p_bybit,
        }
    )
    await hub.start()
    try:
        sub_b = await hub.subscribe_bbo(Venue.BINANCE_UM, "BTCUSDT")
        sub_y = await hub.subscribe_bbo(Venue.BYBIT_LINEAR, "BTCUSDT")

        assert p_binance.subscribe_calls == 1
        assert p_bybit.subscribe_calls == 1

        p_binance.push("BTCUSDT", StreamKind.BBO, _bbo())
        ev = await asyncio.wait_for(sub_b.queue.get(), timeout=1.0)
        assert ev.venue is Venue.BINANCE_UM
        assert sub_y.queue.empty()
    finally:
        await sub_b.close()
        await sub_y.close()
        await hub.close()


# ---------------------------------------------------------------------------
# Backpressure
# ---------------------------------------------------------------------------


async def test_drop_oldest_when_queue_full() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        sub = await hub.subscribe_book(
            Venue.BINANCE_UM, "BTCUSDT", queue_size=2
        )
        for i in range(5):
            provider.push("BTCUSDT", StreamKind.BOOK, _book(sequence=i))

        # 5 published, queue size 2 → 3 dropped, latest 2 retained.
        assert sub.dropped_count == 3
        assert sub.queue.qsize() == 2
        ev1 = sub.queue.get_nowait()
        ev2 = sub.queue.get_nowait()
        assert ev1.sequence == 3
        assert ev2.sequence == 4
    finally:
        await sub.close()
        await hub.close()


async def test_slow_subscriber_does_not_block_fast_one() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        slow = await hub.subscribe_book(
            Venue.BINANCE_UM, "BTCUSDT", queue_size=1
        )
        fast = await hub.subscribe_book(
            Venue.BINANCE_UM, "BTCUSDT", queue_size=10
        )
        for i in range(5):
            provider.push("BTCUSDT", StreamKind.BOOK, _book(sequence=i))

        assert slow.dropped_count == 4
        assert fast.dropped_count == 0
        assert fast.queue.qsize() == 5
    finally:
        await slow.close()
        await fast.close()
        await hub.close()


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


async def test_upstream_subscribe_failure_rolls_back_refcount() -> None:
    provider = FakeProvider(subscribe_error=RuntimeError("WS down"))
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        with pytest.raises(RuntimeError, match="WS down"):
            await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")

        # Refcount went back to 0; a retry must hit the provider again.
        assert hub.subscriber_count(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        ) == 0
        assert not hub.upstream_active(
            Venue.BINANCE_UM, "BTCUSDT", StreamKind.BOOK
        )

        provider.subscribe_error = None
        sub = await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
        assert provider.subscribe_calls == 2
        await sub.close()
    finally:
        await hub.close()


async def test_hub_close_cancels_active_subscribers() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    sub = await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
    await hub.close()
    assert sub._closed is True


async def test_async_iterator_yields_events() -> None:
    provider = FakeProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    try:
        sub = await hub.subscribe_book(Venue.BINANCE_UM, "BTCUSDT")
        provider.push("BTCUSDT", StreamKind.BOOK, _book(sequence=1))
        provider.push("BTCUSDT", StreamKind.BOOK, _book(sequence=2))

        received: list[BookUpdate] = []

        async def reader() -> None:
            async for ev in sub:
                received.append(ev)
                if len(received) == 2:
                    await sub.close()

        await asyncio.wait_for(reader(), timeout=2.0)
        assert [ev.sequence for ev in received] == [1, 2]
    finally:
        await hub.close()
