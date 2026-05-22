"""Tests for :class:`BinanceDynamicMarketStream` (A.1).

The dynamic stream wraps :class:`WsStreamTransport` with runtime
``SUBSCRIBE`` / ``UNSUBSCRIBE`` semantics. We exercise the public
surface (subscribe / unsubscribe / dispatch) directly against a
hand-built fake transport — connecting to a real WS server is the job
of the testnet smoke test, not unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.ws.market_dynamic import (
    BinanceDynamicMarketStream,
)

pytestmark = pytest.mark.asyncio


@dataclass
class _FakeWs:
    """Minimal stand-in for ``websockets.asyncio.client.ClientConnection``."""

    sent: list[str] = field(default_factory=list)

    async def send(self, payload: str) -> None:
        self.sent.append(payload)


@dataclass
class _FakeTransport:
    """Stub :class:`WsStreamTransport` — we inject this on the client."""

    is_connected: bool = True
    started: bool = False
    closed: bool = False
    _ws: _FakeWs = field(default_factory=_FakeWs)

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def wait_connected(self, *, timeout: float | None = None) -> None:
        return None


async def _attach_fake_transport(
    stream: BinanceDynamicMarketStream,
) -> _FakeTransport:
    """Swap in a fake transport so we don't open a real WS."""

    fake = _FakeTransport()
    await stream.start()
    # Replace whatever the real start() built with our fake. We keep
    # the started flag true so subscribe/unsubscribe proceed.
    object.__setattr__(stream, "_transport", fake)
    return fake


async def test_subscribe_sends_subscribe_envelope() -> None:
    stream = BinanceDynamicMarketStream()
    fake = await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@bookTicker", handler)
    assert len(fake._ws.sent) == 1
    import json

    envelope = json.loads(fake._ws.sent[0])
    assert envelope["method"] == "SUBSCRIBE"
    # Binance is case-insensitive on stream names; the client
    # normalizes to lowercase so the handler map stays stable.
    assert envelope["params"] == ["btcusdt@bookticker"]
    assert isinstance(envelope["id"], int)
    await stream.close()


async def test_subscribe_idempotent_does_not_resend() -> None:
    stream = BinanceDynamicMarketStream()
    fake = await _attach_fake_transport(stream)

    async def handler1(payload: dict[str, Any]) -> None:
        return None

    async def handler2(payload: dict[str, Any]) -> None:
        return None

    await stream.subscribe("btcusdt@bookTicker", handler1)
    await stream.subscribe("btcusdt@bookTicker", handler2)  # replaces handler
    # Second subscribe should NOT have sent a duplicate SUBSCRIBE
    # envelope.
    assert len(fake._ws.sent) == 1
    await stream.close()


async def test_unsubscribe_sends_unsubscribe_envelope() -> None:
    stream = BinanceDynamicMarketStream()
    fake = await _attach_fake_transport(stream)

    async def handler(payload: dict[str, Any]) -> None:
        return None

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream.unsubscribe("btcusdt@bookTicker")
    assert len(fake._ws.sent) == 2
    import json

    envelope = json.loads(fake._ws.sent[1])
    assert envelope["method"] == "UNSUBSCRIBE"
    assert envelope["params"] == ["btcusdt@bookticker"]
    await stream.close()


async def test_unsubscribe_unknown_is_noop() -> None:
    stream = BinanceDynamicMarketStream()
    fake = await _attach_fake_transport(stream)
    await stream.unsubscribe("ethusdt@aggTrade")
    assert fake._ws.sent == []
    await stream.close()


async def test_dispatch_routes_combined_stream_frame() -> None:
    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream._dispatch(
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "e": "bookTicker",
                "s": "BTCUSDT",
                "b": "99",
                "B": "1",
                "a": "100",
                "A": "1",
                "T": 1,
            },
        }
    )
    assert len(received) == 1
    assert received[0]["e"] == "bookTicker"
    await stream.close()


async def test_dispatch_routes_bare_book_ticker_frame_by_derivation() -> None:
    """Bare ``/ws`` endpoint pushes frames without ``stream`` envelope."""

    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream._dispatch(
        {
            "e": "bookTicker",
            "s": "BTCUSDT",
            "b": "99",
            "B": "1",
            "a": "100",
            "A": "1",
            "T": 1,
        }
    )
    assert len(received) == 1
    assert received[0]["e"] == "bookTicker"
    await stream.close()


async def test_dispatch_routes_bare_agg_trade_frame_by_derivation() -> None:
    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@aggTrade", handler)
    await stream._dispatch(
        {"e": "aggTrade", "s": "BTCUSDT", "p": "1", "q": "1", "m": True, "a": 1, "T": 1}
    )
    assert len(received) == 1
    await stream.close()


async def test_dispatch_drops_subscription_response() -> None:
    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream._dispatch({"result": None, "id": 1})  # SUBSCRIBE ack
    assert received == []
    await stream.close()


async def test_dispatch_drops_frame_after_unsubscribe() -> None:
    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream.unsubscribe("btcusdt@bookTicker")
    await stream._dispatch(
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "e": "bookTicker",
                "s": "BTCUSDT",
                "b": "99",
                "B": "1",
                "a": "100",
                "A": "1",
                "T": 1,
            },
        }
    )
    assert received == []
    await stream.close()


async def test_dispatch_unroutable_frame_is_dropped() -> None:
    """Depth frames on bare /ws aren't routable without combined envelope."""

    stream = BinanceDynamicMarketStream()
    await _attach_fake_transport(stream)

    received: list[dict[str, Any]] = []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await stream.subscribe("btcusdt@depth20@100ms", handler)
    # bare depthUpdate frame; can't reconstruct depth20@100ms from "e"
    await stream._dispatch(
        {"e": "depthUpdate", "s": "BTCUSDT", "u": 1, "b": [], "a": []}
    )
    assert received == []
    await stream.close()


async def test_close_releases_handlers_and_transport() -> None:
    stream = BinanceDynamicMarketStream()
    fake = await _attach_fake_transport(stream)

    async def handler(payload: dict[str, Any]) -> None:
        return None

    await stream.subscribe("btcusdt@bookTicker", handler)
    await stream.close()
    assert fake.closed is True
    # Subsequent subscribe after close raises
    with pytest.raises(RuntimeError):
        await stream.subscribe("btcusdt@bookTicker", handler)


async def test_subscribe_before_start_raises() -> None:
    stream = BinanceDynamicMarketStream()

    async def handler(payload: dict[str, Any]) -> None:
        return None

    with pytest.raises(RuntimeError):
        await stream.subscribe("btcusdt@bookTicker", handler)
