"""Tests for :class:`BinanceMarketDataStreamProvider` translation glue (A.1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.ws.market_dynamic import (
    StreamFrameHandler,
)
from trade_adapter.marketdata.binance_um import (
    BinanceMarketDataStreamProvider,
    _stream_name,
)
from trade_adapter.marketdata.types import StreamKind
from trade_adapter.types import (
    BBOUpdate,
    BookUpdate,
    OrderSide,
    TradePrint,
    Venue,
)

pytestmark = pytest.mark.asyncio


@dataclass
class _FakeStream:
    """Stand-in for :class:`BinanceDynamicMarketStream`.

    Records subscribe/unsubscribe calls and lets the test invoke the
    registered handler with synthetic Binance frames to verify the
    translator wiring.
    """

    started: bool = False
    closed: bool = False
    handlers: dict[str, StreamFrameHandler] = field(default_factory=dict)
    unsubscribed: list[str] = field(default_factory=list)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def subscribe(
        self, stream: str, handler: StreamFrameHandler
    ) -> None:
        self.handlers[stream] = handler

    async def unsubscribe(self, stream: str) -> None:
        self.unsubscribed.append(stream)
        self.handlers.pop(stream, None)


async def test_stream_name_mapping() -> None:
    assert _stream_name("BTCUSDT", StreamKind.BOOK) == "btcusdt@depth20@100ms"
    assert _stream_name("ETHusdt", StreamKind.TRADES) == "ethusdt@aggTrade"
    assert _stream_name("xrpusdt", StreamKind.BBO) == "xrpusdt@bookTicker"


async def test_provider_routes_book_ticker_to_bbo_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = BinanceMarketDataStreamProvider()

    fake = _FakeStream()
    monkeypatch.setattr(
        "trade_adapter.marketdata.binance_um.BinanceDynamicMarketStream",
        lambda **_: fake,
    )

    await provider.start()
    assert provider.venue is Venue.BINANCE_UM
    assert fake.started

    received: list[Any] = []

    def callback(event: Any) -> None:
        received.append(event)

    await provider.subscribe_stream("BTCUSDT", StreamKind.BBO, callback)
    handler = fake.handlers["btcusdt@bookTicker"]

    raw_frame = {
        "e": "bookTicker",
        "s": "BTCUSDT",
        "b": "99.10",
        "B": "2.5",
        "a": "99.20",
        "A": "3.5",
        "T": 1700_000_001_000,
    }
    await handler(raw_frame)

    assert len(received) == 1
    ev = received[0]
    assert isinstance(ev, BBOUpdate)
    assert ev.bid_price == pytest.approx(99.10)
    assert ev.ask_price == pytest.approx(99.20)
    assert ev.venue is Venue.BINANCE_UM
    assert ev.symbol == "BTCUSDT"

    await provider.close()
    assert fake.closed


async def test_provider_routes_agg_trade_to_trade_print(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = BinanceMarketDataStreamProvider()
    fake = _FakeStream()
    monkeypatch.setattr(
        "trade_adapter.marketdata.binance_um.BinanceDynamicMarketStream",
        lambda **_: fake,
    )
    await provider.start()

    received: list[Any] = []
    await provider.subscribe_stream(
        "BTCUSDT", StreamKind.TRADES, received.append
    )

    handler = fake.handlers["btcusdt@aggTrade"]
    await handler(
        {
            "e": "aggTrade",
            "s": "BTCUSDT",
            "p": "50100.5",
            "q": "0.1",
            "m": False,
            "a": 42,
            "T": 1700_000_002_000,
        }
    )

    assert len(received) == 1
    assert isinstance(received[0], TradePrint)
    assert received[0].price == pytest.approx(50100.5)
    assert received[0].side is OrderSide.BUY  # m=False ⇒ taker BUY
    assert received[0].trade_id == "42"

    await provider.close()


async def test_provider_routes_depth_snapshot_to_book_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = BinanceMarketDataStreamProvider()
    fake = _FakeStream()
    monkeypatch.setattr(
        "trade_adapter.marketdata.binance_um.BinanceDynamicMarketStream",
        lambda **_: fake,
    )
    await provider.start()

    received: list[Any] = []
    await provider.subscribe_stream(
        "BTCUSDT", StreamKind.BOOK, received.append
    )

    handler = fake.handlers["btcusdt@depth20@100ms"]
    await handler(
        {
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "u": 1234,
            "b": [["99.10", "2.0"], ["99.05", "1.0"]],
            "a": [["99.20", "3.0"]],
            "T": 1700_000_003_000,
        }
    )

    assert len(received) == 1
    assert isinstance(received[0], BookUpdate)
    assert received[0].sequence == 1234
    assert len(received[0].bids) == 2
    assert received[0].bids[0].price == pytest.approx(99.10)

    await provider.close()


async def test_provider_unsubscribe_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = BinanceMarketDataStreamProvider()
    fake = _FakeStream()
    monkeypatch.setattr(
        "trade_adapter.marketdata.binance_um.BinanceDynamicMarketStream",
        lambda **_: fake,
    )
    await provider.start()
    await provider.subscribe_stream(
        "BTCUSDT", StreamKind.BBO, lambda _: None
    )
    await provider.unsubscribe_stream("BTCUSDT", StreamKind.BBO)
    assert fake.unsubscribed == ["btcusdt@bookTicker"]
    await provider.close()


async def test_provider_malformed_frame_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Translator-level ValueError must be logged + dropped, not raised."""

    provider = BinanceMarketDataStreamProvider()
    fake = _FakeStream()
    monkeypatch.setattr(
        "trade_adapter.marketdata.binance_um.BinanceDynamicMarketStream",
        lambda **_: fake,
    )
    await provider.start()

    received: list[Any] = []
    await provider.subscribe_stream(
        "BTCUSDT", StreamKind.BBO, received.append
    )

    handler = fake.handlers["btcusdt@bookTicker"]
    # Missing required fields - translator raises ValueError; we expect
    # the provider's wrapper to swallow it.
    await handler({"e": "bookTicker", "s": "BTCUSDT"})
    assert received == []

    await provider.close()
