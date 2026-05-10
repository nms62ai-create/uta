"""Tests for the push-only WS stream transport.

In-process ``websockets.asyncio.server`` instances substitute for
Binance's combined-stream / user-data endpoints. No live network.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trade_adapter.exchanges.binance_um.ws.stream import (
    WsStreamClosed,
    WsStreamConfig,
    WsStreamTransport,
)

ServerHandler = Callable[[ServerConnection], Awaitable[None]]


@asynccontextmanager
async def _serve(handler: ServerHandler) -> AsyncIterator[str]:
    server = await serve(handler, "127.0.0.1", 0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        yield f"ws://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


def _config(url: str, **overrides: Any) -> WsStreamConfig:
    base = {
        "url": url,
        "connect_timeout_s": 1.0,
        "backoff_base_s": 0.01,
        "backoff_max_s": 0.05,
        "backoff_jitter": 0.0,
        "ping_interval_s": None,
        "ping_timeout_s": None,
    }
    base.update(overrides)
    return WsStreamConfig(**base)


@pytest.mark.asyncio
async def test_dispatches_each_frame_to_handler() -> None:
    received: list[dict[str, Any]] = []

    async def handler(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"a": 1}))
        await ws.send(json.dumps({"a": 2}))
        await ws.send(json.dumps({"a": 3}))
        await asyncio.sleep(0.1)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        await transport.start()
        try:
            await transport.wait_connected()
            for _ in range(50):
                if len(received) >= 3:
                    break
                await asyncio.sleep(0.02)
        finally:
            await transport.close()

    assert received == [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.mark.asyncio
async def test_drops_malformed_and_non_object_frames() -> None:
    received: list[dict[str, Any]] = []

    async def handler(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send("not-json")
        await ws.send(json.dumps([1, 2, 3]))
        await ws.send(json.dumps("scalar"))
        await ws.send(json.dumps({"good": True}))
        await asyncio.sleep(0.1)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        await transport.start()
        try:
            await transport.wait_connected()
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await transport.close()

    assert received == [{"good": True}]


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_supervisor() -> None:
    received: list[dict[str, Any]] = []

    async def handler(frame: dict[str, Any]) -> None:
        if frame.get("explode"):
            raise RuntimeError("boom")
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"explode": True}))
        await ws.send(json.dumps({"a": 1}))
        await ws.send(json.dumps({"a": 2}))
        await asyncio.sleep(0.1)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        await transport.start()
        try:
            await transport.wait_connected()
            for _ in range(50):
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.02)
        finally:
            await transport.close()

    assert received == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_reconnect_after_server_drops() -> None:
    received: list[dict[str, Any]] = []
    drop_first = {"value": True}

    async def handler(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        if drop_first["value"]:
            drop_first["value"] = False
            await ws.close(code=1011, reason="test drop")
            return
        await ws.send(json.dumps({"after": "reconnect"}))
        await asyncio.sleep(0.5)

    async with _serve(server) as url:
        transport = WsStreamTransport(
            config=_config(url, connect_timeout_s=2.0), on_message=handler
        )
        await transport.start()
        try:
            for _ in range(100):
                if {"after": "reconnect"} in received:
                    break
                await asyncio.sleep(0.05)
        finally:
            await transport.close()

    assert {"after": "reconnect"} in received


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(0.5)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        await transport.start()
        await transport.close()
        await transport.close()


@pytest.mark.asyncio
async def test_double_start_raises() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(0.5)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        await transport.start()
        try:
            with pytest.raises(RuntimeError):
                await transport.start()
        finally:
            await transport.close()


@pytest.mark.asyncio
async def test_start_after_close_raises() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    transport = WsStreamTransport(
        config=_config("ws://127.0.0.1:1"), on_message=handler
    )
    await transport.close()
    with pytest.raises(WsStreamClosed):
        await transport.start()


@pytest.mark.asyncio
async def test_wait_connected_times_out_when_server_unreachable() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    transport = WsStreamTransport(
        config=_config("ws://127.0.0.1:1", connect_timeout_s=0.05),
        on_message=handler,
    )
    await transport.start()
    try:
        with pytest.raises(asyncio.TimeoutError):
            await transport.wait_connected(timeout=0.05)
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_is_connected_reflects_state() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(0.5)

    async with _serve(server) as url:
        transport = WsStreamTransport(config=_config(url), on_message=handler)
        assert transport.is_connected is False
        await transport.start()
        await transport.wait_connected()
        assert transport.is_connected is True
        await transport.close()
        assert transport.is_connected is False


@pytest.mark.asyncio
async def test_compute_backoff_is_exponential_then_capped() -> None:
    async def handler(_: dict[str, Any]) -> None:
        pass

    transport = WsStreamTransport(
        config=_config(
            "ws://127.0.0.1:1",
            backoff_base_s=0.1,
            backoff_max_s=0.4,
            backoff_jitter=0.0,
        ),
        on_message=handler,
    )
    delays = [transport._compute_backoff(n) for n in range(1, 8)]
    assert delays[0] == pytest.approx(0.1)
    assert delays[1] == pytest.approx(0.2)
    assert delays[2] == pytest.approx(0.4)
    assert all(d == pytest.approx(0.4) for d in delays[3:])
