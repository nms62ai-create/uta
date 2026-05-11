"""Tests for the Binance USD-M USER_DATA_STREAM client.

In-process ``websockets`` server substitutes for
``wss://fstream.binance.com/ws/<listenKey>``. A small ``FakeRest`` stub
substitutes for :class:`BinanceRestClient` — the client only needs the
three listenKey methods, declared as a Protocol in
:mod:`trade_adapter.exchanges.binance_um.ws.user_stream`.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trade_adapter.exchanges.binance_um.ws.user_stream import (
    UserDataStreamClient,
    UserDataStreamClosed,
    UserDataStreamConfig,
)

ServerHandler = Callable[[ServerConnection], Awaitable[None]]


@asynccontextmanager
async def _serve(handler: ServerHandler) -> AsyncIterator[str]:
    """Spin up a WS server and return the ``ws://host:port`` (no path)."""
    server = await serve(handler, "127.0.0.1", 0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        yield f"ws://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


def _fast_config(stream_base_url: str, **overrides: Any) -> UserDataStreamConfig:
    """Build a :class:`UserDataStreamConfig` with timing tuned for tests."""
    base: dict[str, Any] = {
        "stream_base_url": stream_base_url,
        "keepalive_interval_s": 10.0,  # default: don't fire during a fast test
        "rotate_backoff_base_s": 0.01,
        "rotate_backoff_max_s": 0.05,
        "rotate_backoff_jitter": 0.0,
        "transport_config_overrides": {
            "connect_timeout_s": 1.0,
            "backoff_base_s": 0.01,
            "backoff_max_s": 0.05,
            "backoff_jitter": 0.0,
            "ping_interval_s": None,
            "ping_timeout_s": None,
        },
    }
    base.update(overrides)
    return UserDataStreamConfig(**base)


@dataclass
class FakeRest:
    """In-memory implementation of the listenKey REST endpoints."""

    keys: list[str] = field(default_factory=lambda: ["lk-aaa", "lk-bbb", "lk-ccc"])
    keepalive_failures: int = 0  # how many keepalive calls should raise
    start_failures: int = 0  # how many start calls should raise before succeeding
    starts: int = 0
    keepalives: int = 0
    closes: int = 0

    async def start_user_data_stream(self) -> str:
        self.starts += 1
        if self.start_failures > 0:
            self.start_failures -= 1
            raise RuntimeError("transient REST POST failure")
        if not self.keys:
            raise RuntimeError("no more keys configured")
        return self.keys.pop(0)

    async def keepalive_user_data_stream(self) -> None:
        self.keepalives += 1
        if self.keepalive_failures > 0:
            self.keepalive_failures -= 1
            raise RuntimeError("transient REST PUT failure")

    async def close_user_data_stream(self) -> None:
        self.closes += 1


def _path_of(ws: ServerConnection) -> str:
    """Return the request path of an incoming WS connection.

    The exact attribute path differs slightly across websockets versions;
    we accept either ``ws.request.path`` (12+) or ``ws.path`` (legacy).
    """
    request = getattr(ws, "request", None)
    if request is not None and hasattr(request, "path"):
        return str(request.path)
    return str(getattr(ws, "path", ""))


@pytest.mark.asyncio
async def test_routes_events_by_e_field() -> None:
    order_events: list[dict[str, Any]] = []
    account_events: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        order_events.append(frame)

    async def on_account(frame: dict[str, Any]) -> None:
        account_events.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(
            json.dumps({"e": "ORDER_TRADE_UPDATE", "T": 1, "o": {"i": 1}})
        )
        await ws.send(
            json.dumps({"e": "ACCOUNT_UPDATE", "T": 2, "a": {"P": []}})
        )
        await ws.send(
            json.dumps({"e": "ORDER_TRADE_UPDATE", "T": 3, "o": {"i": 2}})
        )
        await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={
                "ORDER_TRADE_UPDATE": on_order,
                "ACCOUNT_UPDATE": on_account,
            },
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            await client.wait_connected(timeout=2.0)
            for _ in range(100):
                if len(order_events) >= 2 and len(account_events) >= 1:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert [e["o"]["i"] for e in order_events] == [1, 2]
    assert account_events[0]["a"] == {"P": []}
    # close() should have called the DELETE endpoint exactly once.
    assert rest.closes == 1
    # And exactly one POST listenKey acquired (no rotations).
    assert rest.starts == 1


@pytest.mark.asyncio
async def test_url_includes_current_listen_key() -> None:
    captured_paths: list[str] = []

    async def server(ws: ServerConnection) -> None:
        captured_paths.append(_path_of(ws))
        await ws.send(json.dumps({"e": "ACCOUNT_UPDATE"}))
        await asyncio.sleep(0.5)

    received: list[dict[str, Any]] = []

    async def on_account(frame: dict[str, Any]) -> None:
        received.append(frame)

    async with _serve(server) as base_url:
        rest = FakeRest(keys=["lk-firstkey"])
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ACCOUNT_UPDATE": on_account},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            await client.wait_connected(timeout=2.0)
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert captured_paths == ["/ws/lk-firstkey"]
    assert client.current_listen_key == "lk-firstkey"


@pytest.mark.asyncio
async def test_listen_key_expired_triggers_rotation() -> None:
    """``listenKeyExpired`` must drop the WS, fetch a new key, reconnect."""
    captured_paths: list[str] = []
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        path = _path_of(ws)
        captured_paths.append(path)
        if path.endswith("lk-first"):
            # First session: send one event, then expire.
            await ws.send(
                json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": "first"}})
            )
            await asyncio.sleep(0.05)
            await ws.send(json.dumps({"e": "listenKeyExpired"}))
            await asyncio.sleep(1.0)
        elif path.endswith("lk-second"):
            # Second session: send another event.
            await ws.send(
                json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": "second"}})
            )
            await asyncio.sleep(1.0)
        else:
            await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest(keys=["lk-first", "lk-second"])
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            for _ in range(200):
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await client.close()

    assert len(received) == 2, f"expected 2 events; got {received}"
    assert [e["o"]["i"] for e in received] == ["first", "second"]
    assert captured_paths == ["/ws/lk-first", "/ws/lk-second"]
    # Two POST /listenKey calls (one per rotation).
    assert rest.starts == 2
    # close() at the end => one DELETE.
    assert rest.closes == 1


@pytest.mark.asyncio
async def test_keepalive_failure_triggers_rotation() -> None:
    """A failing PUT /listenKey forces the supervisor to rotate to a new key."""
    captured_paths: list[str] = []
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        captured_paths.append(_path_of(ws))
        # Send one event so handlers fire; then keep the connection
        # open so the keepalive loop has a chance to run.
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}))
        await asyncio.sleep(2.0)

    async with _serve(server) as base_url:
        rest = FakeRest(
            keys=["lk-keep1", "lk-keep2"],
            keepalive_failures=1,
        )
        cfg = _fast_config(base_url, keepalive_interval_s=0.05)
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=cfg,
        )
        await client.start()
        try:
            for _ in range(200):
                if rest.starts >= 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await client.close()

    assert rest.starts >= 2
    assert rest.keepalives >= 1
    assert "/ws/lk-keep1" in captured_paths
    assert "/ws/lk-keep2" in captured_paths


@pytest.mark.asyncio
async def test_unknown_event_dropped_silently() -> None:
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"e": "MARGIN_CALL"}))
        await ws.send(json.dumps({"e": "ACCOUNT_CONFIG_UPDATE"}))
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}))
        await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert received == [{"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}]


@pytest.mark.asyncio
async def test_handler_exception_does_not_kill_supervisor() -> None:
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        if frame.get("o", {}).get("i") == "boom":
            raise RuntimeError("handler boom")
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": "boom"}}))
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}))
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 2}}))
        await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            for _ in range(100):
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert [e["o"]["i"] for e in received] == [1, 2]


@pytest.mark.asyncio
async def test_start_user_data_stream_failure_backs_off_and_retries() -> None:
    """Transient REST POST failures cause a back-off, not a hard exit."""
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}))
        await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest(keys=["lk-after-retries"], start_failures=2)
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            for _ in range(200):
                if received:
                    break
                await asyncio.sleep(0.05)
        finally:
            await client.close()

    assert received == [{"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}]
    # Two failed start attempts + one successful => starts >= 3
    assert rest.starts >= 3


@pytest.mark.asyncio
async def test_close_is_idempotent_and_runs_delete_once() -> None:
    async def on_x(frame: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(1.0)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"X": on_x},
            config=_fast_config(base_url),
        )
        await client.start()
        await client.wait_connected(timeout=2.0)
        await client.close()
        await client.close()

    assert rest.closes == 1


@pytest.mark.asyncio
async def test_double_start_raises() -> None:
    async def on_x(frame: dict[str, Any]) -> None:
        pass

    async def server(ws: ServerConnection) -> None:
        await asyncio.sleep(1.0)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"X": on_x},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                await client.start()
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_start_after_close_raises() -> None:
    async def on_x(frame: dict[str, Any]) -> None:
        pass

    rest = FakeRest()
    client = UserDataStreamClient(
        rest=rest,  # type: ignore[arg-type]
        handlers={"X": on_x},
        config=_fast_config("ws://127.0.0.1:1"),
    )
    await client.close()
    with pytest.raises(UserDataStreamClosed):
        await client.start()


@pytest.mark.asyncio
async def test_dispatch_drops_frames_without_e_field() -> None:
    received: list[dict[str, Any]] = []

    async def on_order(frame: dict[str, Any]) -> None:
        received.append(frame)

    async def server(ws: ServerConnection) -> None:
        # No 'e' field — must be dropped without crashing.
        await ws.send(json.dumps({"x": 1}))
        # 'e' present but non-string — also dropped.
        await ws.send(json.dumps({"e": 42}))
        # Real event passes through.
        await ws.send(json.dumps({"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}))
        await asyncio.sleep(0.5)

    async with _serve(server) as base_url:
        rest = FakeRest()
        client = UserDataStreamClient(
            rest=rest,  # type: ignore[arg-type]
            handlers={"ORDER_TRADE_UPDATE": on_order},
            config=_fast_config(base_url),
        )
        await client.start()
        try:
            for _ in range(100):
                if received:
                    break
                await asyncio.sleep(0.02)
        finally:
            await client.close()

    assert received == [{"e": "ORDER_TRADE_UPDATE", "o": {"i": 1}}]


@pytest.mark.asyncio
async def test_compute_rotate_backoff_is_exponential_then_capped() -> None:
    async def on_x(frame: dict[str, Any]) -> None:
        pass

    rest = FakeRest()
    client = UserDataStreamClient(
        rest=rest,  # type: ignore[arg-type]
        handlers={"X": on_x},
        config=UserDataStreamConfig(
            rotate_backoff_base_s=0.1,
            rotate_backoff_max_s=0.4,
            rotate_backoff_jitter=0.0,
        ),
    )
    delays = [client._compute_rotate_backoff(n) for n in range(1, 8)]
    assert delays[0] == pytest.approx(0.1)
    assert delays[1] == pytest.approx(0.2)
    assert delays[2] == pytest.approx(0.4)
    assert all(d == pytest.approx(0.4) for d in delays[3:])
