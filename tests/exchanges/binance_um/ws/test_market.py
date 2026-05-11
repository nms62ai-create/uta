"""Tests for the combined market-data stream client."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trade_adapter.exchanges.binance_um.ws.market import (
    MarketStreamClient,
    build_combined_stream_url,
)

ServerHandler = Callable[[ServerConnection], Awaitable[None]]


@asynccontextmanager
async def _serve_path(handler: ServerHandler) -> AsyncIterator[str]:
    """Start a WS server and return the ``ws://host:port`` (no path)."""
    server = await serve(handler, "127.0.0.1", 0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        yield f"ws://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


def test_build_combined_stream_url_basic() -> None:
    url = build_combined_stream_url(
        "wss://fstream.binance.com",
        ["btcusdt@aggTrade", "ETHUSDT@bookTicker"],
    )
    assert (
        url
        == "wss://fstream.binance.com/stream?streams=btcusdt@aggtrade/ethusdt@bookticker"
    )


def test_build_combined_stream_url_strips_trailing_slash() -> None:
    url = build_combined_stream_url(
        "wss://fstream.binance.com/", ["btcusdt@aggTrade"]
    )
    assert url == "wss://fstream.binance.com/stream?streams=btcusdt@aggtrade"


def test_build_combined_stream_url_rejects_empty() -> None:
    with pytest.raises(ValueError, match="at least one stream"):
        build_combined_stream_url("wss://x", [])


def test_build_combined_stream_url_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        build_combined_stream_url(
            "wss://x", ["btcusdt@aggTrade", "BTCUSDT@aggTrade"]
        )


def test_build_combined_stream_url_preserves_order() -> None:
    url = build_combined_stream_url(
        "wss://x", ["c@aggTrade", "a@aggTrade", "b@aggTrade"]
    )
    assert url.endswith("?streams=c@aggtrade/a@aggtrade/b@aggtrade")


@pytest.mark.asyncio
async def test_market_client_routes_frames_by_stream_name() -> None:
    """Each frame's ``data`` is delivered to the handler keyed by ``stream``."""
    agg_received: list[dict[str, Any]] = []
    book_received: list[dict[str, Any]] = []

    async def on_agg(data: dict[str, Any]) -> None:
        agg_received.append(data)

    async def on_book(data: dict[str, Any]) -> None:
        book_received.append(data)

    async def server(ws: ServerConnection) -> None:
        await ws.send(
            json.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {"e": "aggTrade", "p": "65000.00", "q": "0.001"},
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "stream": "btcusdt@bookTicker",
                    "data": {"u": 1, "s": "BTCUSDT", "b": "65000", "a": "65001"},
                }
            )
        )
        await ws.send(
            json.dumps(
                {
                    "stream": "btcusdt@aggTrade",
                    "data": {"e": "aggTrade", "p": "65001.00", "q": "0.002"},
                }
            )
        )
        await asyncio.sleep(0.1)

    async with _serve_path(server) as base_url:
        client = MarketStreamClient(
            streams=["btcusdt@aggTrade", "btcusdt@bookTicker"],
            handlers={
                "btcusdt@aggTrade": on_agg,
                "btcusdt@bookTicker": on_book,
            },
            base_url=base_url,
            transport_config_overrides={
                "connect_timeout_s": 1.0,
                "backoff_base_s": 0.01,
                "backoff_max_s": 0.05,
                "backoff_jitter": 0.0,
                "ping_interval_s": None,
                "ping_timeout_s": None,
            },
        )
        await client.start()
        try:
            await client.wait_connected()
            for _ in range(50):
                if len(agg_received) >= 2 and len(book_received) >= 1:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert [d["p"] for d in agg_received] == ["65000.00", "65001.00"]
    assert book_received == [{"u": 1, "s": "BTCUSDT", "b": "65000", "a": "65001"}]


@pytest.mark.asyncio
async def test_market_client_handler_lookup_is_case_insensitive() -> None:
    received: list[dict[str, Any]] = []

    async def handler(data: dict[str, Any]) -> None:
        received.append(data)

    async def server(ws: ServerConnection) -> None:
        # Server sends mixed-case stream name; client registered lower-case.
        await ws.send(
            json.dumps(
                {"stream": "BTCUSDT@aggTrade", "data": {"p": "1.0"}}
            )
        )
        await asyncio.sleep(0.1)

    async with _serve_path(server) as base_url:
        client = MarketStreamClient(
            streams=["btcusdt@aggTrade"],
            handlers={"btcusdt@aggtrade": handler},  # all lower
            base_url=base_url,
            transport_config_overrides={
                "connect_timeout_s": 1.0,
                "backoff_base_s": 0.01,
                "backoff_max_s": 0.05,
                "backoff_jitter": 0.0,
                "ping_interval_s": None,
                "ping_timeout_s": None,
            },
        )
        await client.start()
        try:
            await client.wait_connected()
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert received == [{"p": "1.0"}]


@pytest.mark.asyncio
async def test_market_client_drops_frames_with_unknown_stream() -> None:
    received: list[dict[str, Any]] = []

    async def handler(data: dict[str, Any]) -> None:
        received.append(data)

    async def server(ws: ServerConnection) -> None:
        await ws.send(
            json.dumps({"stream": "ethusdt@aggTrade", "data": {"p": "ignored"}})
        )
        await ws.send(
            json.dumps({"stream": "btcusdt@aggTrade", "data": {"p": "kept"}})
        )
        await asyncio.sleep(0.1)

    async with _serve_path(server) as base_url:
        client = MarketStreamClient(
            streams=["btcusdt@aggTrade"],
            handlers={"btcusdt@aggTrade": handler},
            base_url=base_url,
            transport_config_overrides={
                "connect_timeout_s": 1.0,
                "backoff_base_s": 0.01,
                "backoff_max_s": 0.05,
                "backoff_jitter": 0.0,
                "ping_interval_s": None,
                "ping_timeout_s": None,
            },
        )
        await client.start()
        try:
            await client.wait_connected()
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert received == [{"p": "kept"}]


@pytest.mark.asyncio
async def test_market_client_drops_non_combined_frames() -> None:
    """Direct (non-combined) frames have no ``stream`` envelope; ignore them."""
    received: list[dict[str, Any]] = []

    async def handler(data: dict[str, Any]) -> None:
        received.append(data)

    async def server(ws: ServerConnection) -> None:
        # Direct frame: just the data, no envelope.
        await ws.send(json.dumps({"e": "aggTrade", "p": "1.0"}))
        # Frame with non-string stream — also dropped.
        await ws.send(json.dumps({"stream": 42, "data": {"p": "x"}}))
        # Frame with non-object data — also dropped.
        await ws.send(json.dumps({"stream": "btcusdt@aggTrade", "data": [1, 2]}))
        # Real combined frame.
        await ws.send(
            json.dumps({"stream": "btcusdt@aggTrade", "data": {"p": "real"}})
        )
        await asyncio.sleep(0.1)

    async with _serve_path(server) as base_url:
        client = MarketStreamClient(
            streams=["btcusdt@aggTrade"],
            handlers={"btcusdt@aggTrade": handler},
            base_url=base_url,
            transport_config_overrides={
                "connect_timeout_s": 1.0,
                "backoff_base_s": 0.01,
                "backoff_max_s": 0.05,
                "backoff_jitter": 0.0,
                "ping_interval_s": None,
                "ping_timeout_s": None,
            },
        )
        await client.start()
        try:
            await client.wait_connected()
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert received == [{"p": "real"}]


@pytest.mark.asyncio
async def test_market_client_rejects_empty_streams_or_handlers() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    with pytest.raises(ValueError, match="at least one stream"):
        MarketStreamClient(streams=[], handlers={"x": handler})
    with pytest.raises(ValueError, match="at least one handler"):
        MarketStreamClient(streams=["x"], handlers={})


@pytest.mark.asyncio
async def test_market_client_double_start_raises() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(0.5)

    async with _serve_path(server) as base_url:
        client = MarketStreamClient(
            streams=["btcusdt@aggTrade"],
            handlers={"btcusdt@aggTrade": handler},
            base_url=base_url,
            transport_config_overrides={
                "connect_timeout_s": 1.0,
                "backoff_base_s": 0.01,
                "backoff_max_s": 0.05,
                "backoff_jitter": 0.0,
                "ping_interval_s": None,
                "ping_timeout_s": None,
            },
        )
        await client.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await client.start()
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_market_client_close_before_start_is_noop() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    client = MarketStreamClient(
        streams=["btcusdt@aggTrade"],
        handlers={"btcusdt@aggTrade": handler},
    )
    await client.close()  # must not raise
    assert client.is_connected is False


@pytest.mark.asyncio
async def test_market_client_wait_connected_before_start_raises() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    client = MarketStreamClient(
        streams=["btcusdt@aggTrade"],
        handlers={"btcusdt@aggTrade": handler},
    )
    with pytest.raises(RuntimeError, match="start"):
        await client.wait_connected()
