"""Tests for the Binance USD-M WS RPC transport.

All tests run against an in-process ``websockets.asyncio.server.serve``
so no live network access is required. The mock servers echo the
incoming envelope back with a configurable status / latency / drop
behaviour so we can exercise success, timeout, reconnect, and disconnect
paths deterministically.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trade_adapter.exchanges.binance_um.ws.transport import (
    WsRpcClient,
    WsRpcClosed,
    WsRpcConfig,
    WsRpcDisconnected,
    WsRpcTimeout,
)

ServerHandler = Callable[[ServerConnection], Awaitable[None]]


@asynccontextmanager
async def _serve(handler: ServerHandler) -> AsyncIterator[str]:
    """Start an in-process WS server bound to a random local port.

    Yields the ``ws://host:port`` URL the client should dial.
    """
    server = await serve(handler, "127.0.0.1", 0)
    try:
        sock = server.sockets[0]
        host, port = sock.getsockname()[:2]
        yield f"ws://{host}:{port}"
    finally:
        server.close()
        await server.wait_closed()


def _config(url: str, **overrides: Any) -> WsRpcConfig:
    """Build a :class:`WsRpcConfig` with test-friendly defaults."""
    base = {
        "url": url,
        "request_timeout_s": 1.0,
        "connect_timeout_s": 1.0,
        "backoff_base_s": 0.01,
        "backoff_max_s": 0.05,
        "backoff_jitter": 0.0,
        # Disable library-managed pings; tests are short-lived.
        "ping_interval_s": None,
        "ping_timeout_s": None,
    }
    base.update(overrides)
    return WsRpcConfig(**base)


async def _echo_ok(ws: ServerConnection) -> None:
    """Echo each request as ``{id, status: 200, result: <params>}``."""
    async for raw in ws:
        msg = json.loads(raw)
        await ws.send(
            json.dumps(
                {
                    "id": msg["id"],
                    "status": 200,
                    "result": msg.get("params", {}),
                }
            )
        )


@pytest.mark.asyncio
async def test_request_roundtrip_returns_result() -> None:
    async with _serve(_echo_ok) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        try:
            resp = await client.request("ping", {"hello": "world"})
        finally:
            await client.close()

    assert resp["status"] == 200
    assert resp["result"] == {"hello": "world"}


@pytest.mark.asyncio
async def test_concurrent_requests_get_distinct_responses() -> None:
    """Out-of-order responses are routed by id."""

    async def slow_echo(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            # Sleep proportional to params.delay to force out-of-order replies.
            delay = float(msg.get("params", {}).get("delay", 0.0))
            await asyncio.sleep(delay)
            await ws.send(
                json.dumps(
                    {"id": msg["id"], "status": 200, "result": msg["params"]}
                )
            )

    async with _serve(slow_echo) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        try:
            results = await asyncio.gather(
                client.request("p", {"i": 1, "delay": 0.05}),
                client.request("p", {"i": 2, "delay": 0.0}),
                client.request("p", {"i": 3, "delay": 0.02}),
            )
        finally:
            await client.close()

    assert [r["result"]["i"] for r in results] == [1, 2, 3]


@pytest.mark.asyncio
async def test_request_id_uses_injected_factory() -> None:
    sent_ids: list[str] = []

    async def capture(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            sent_ids.append(msg["id"])
            await ws.send(json.dumps({"id": msg["id"], "status": 200, "result": {}}))

    counter = 0

    def factory() -> str:
        nonlocal counter
        counter += 1
        return f"req-{counter}"

    async with _serve(capture) as url:
        client = WsRpcClient(config=_config(url), id_factory=factory)
        await client.start()
        try:
            await client.request("a")
            await client.request("b")
        finally:
            await client.close()

    assert sent_ids == ["req-1", "req-2"]


@pytest.mark.asyncio
async def test_request_timeout_when_server_silent() -> None:
    async def silent(ws: ServerConnection) -> None:
        async for _ in ws:
            await asyncio.sleep(10)  # never reply

    async with _serve(silent) as url:
        client = WsRpcClient(config=_config(url, request_timeout_s=0.05))
        await client.start()
        try:
            with pytest.raises(WsRpcTimeout):
                await client.request("ping")
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_request_per_call_timeout_overrides_config() -> None:
    async def silent(ws: ServerConnection) -> None:
        async for _ in ws:
            await asyncio.sleep(10)

    async with _serve(silent) as url:
        client = WsRpcClient(config=_config(url, request_timeout_s=10.0))
        await client.start()
        try:
            with pytest.raises(WsRpcTimeout):
                await client.request("ping", timeout=0.05)
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_close_rejects_pending_requests() -> None:
    async def silent(ws: ServerConnection) -> None:
        async for _ in ws:
            await asyncio.sleep(10)

    async with _serve(silent) as url:
        client = WsRpcClient(config=_config(url, request_timeout_s=10.0))
        await client.start()

        async def issue() -> Any:
            return await client.request("ping")

        task = asyncio.create_task(issue())
        # Give the request a moment to be sent.
        await asyncio.sleep(0.05)
        await client.close()
        with pytest.raises((WsRpcClosed, WsRpcDisconnected)):
            await task


@pytest.mark.asyncio
async def test_request_after_close_raises() -> None:
    async with _serve(_echo_ok) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        await client.close()
        with pytest.raises(WsRpcClosed):
            await client.request("ping")


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    async with _serve(_echo_ok) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        await client.close()
        await client.close()


@pytest.mark.asyncio
async def test_double_start_raises() -> None:
    async with _serve(_echo_ok) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        try:
            with pytest.raises(RuntimeError):
                await client.start()
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_request_before_start_raises() -> None:
    client = WsRpcClient(config=_config("ws://127.0.0.1:1"))
    with pytest.raises(RuntimeError):
        await client.request("ping")


@pytest.mark.asyncio
async def test_reconnect_after_server_drops_connection() -> None:
    """Server drops the first connection mid-flight; client reconnects and
    the second request goes through.
    """
    drop_first = {"value": True}

    async def drop_then_echo(ws: ServerConnection) -> None:
        if drop_first["value"]:
            drop_first["value"] = False
            await ws.close(code=1011, reason="server-side test drop")
            return
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(
                json.dumps({"id": msg["id"], "status": 200, "result": msg["params"]})
            )

    async with _serve(drop_then_echo) as url:
        client = WsRpcClient(
            config=_config(url, request_timeout_s=2.0, connect_timeout_s=2.0)
        )
        await client.start()
        try:
            # First request races the server's close — either it completes
            # before the close lands, or it raises WsRpcDisconnected.
            try:
                await client.request("first", {"x": 1})
            except WsRpcDisconnected:
                pass

            # Second attempt: wait briefly for the supervisor to reconnect,
            # then succeed against the now-echoing server.
            async def attempt() -> dict[str, Any]:
                last_exc: Exception | None = None
                for _ in range(50):
                    try:
                        return await client.request("second", {"x": 2})
                    except WsRpcDisconnected as e:
                        last_exc = e
                        await asyncio.sleep(0.05)
                raise AssertionError(
                    f"reconnect never succeeded; last exc={last_exc}"
                )

            resp = await attempt()
        finally:
            await client.close()

    assert resp["status"] == 200
    assert resp["result"] == {"x": 2}


@pytest.mark.asyncio
async def test_dispatch_drops_malformed_and_idless_frames() -> None:
    """Malformed JSON / non-object / id-less frames must not crash the
    receive loop or affect other in-flight requests.
    """

    async def noisy(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            # Send a malformed frame, an id-less frame, an array frame, and
            # then the real reply. The client must ignore the first three.
            await ws.send("not-json")
            await ws.send(json.dumps({"status": 200, "result": "no-id"}))
            await ws.send(json.dumps([1, 2, 3]))
            await ws.send(
                json.dumps({"id": msg["id"], "status": 200, "result": msg["params"]})
            )

    async with _serve(noisy) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        try:
            resp = await client.request("ping", {"x": 7})
        finally:
            await client.close()

    assert resp["result"] == {"x": 7}


@pytest.mark.asyncio
async def test_is_connected_reflects_state() -> None:
    async with _serve(_echo_ok) as url:
        client = WsRpcClient(config=_config(url))
        assert client.is_connected is False
        await client.start()
        # Wait briefly for the supervisor to establish the socket.
        for _ in range(50):
            if client.is_connected:
                break
            await asyncio.sleep(0.02)
        assert client.is_connected is True
        await client.close()
        assert client.is_connected is False


@pytest.mark.asyncio
async def test_compute_backoff_is_bounded_and_monotonic_without_jitter() -> None:
    client = WsRpcClient(
        config=_config(
            "ws://127.0.0.1:1",
            backoff_base_s=0.1,
            backoff_max_s=0.4,
            backoff_jitter=0.0,
        )
    )
    delays = [client._compute_backoff(n) for n in range(1, 8)]
    # 0.1, 0.2, 0.4, 0.4, 0.4, 0.4, 0.4 — exponential then capped.
    assert delays[0] == pytest.approx(0.1)
    assert delays[1] == pytest.approx(0.2)
    assert delays[2] == pytest.approx(0.4)
    assert all(d == pytest.approx(0.4) for d in delays[3:])


@pytest.mark.asyncio
async def test_envelope_omits_params_when_none() -> None:
    """Binance WS-API accepts envelopes without ``params`` for
    parameter-less methods; we should not send ``"params": null``.
    """
    captured: list[dict[str, Any]] = []

    async def capture(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            captured.append(msg)
            await ws.send(json.dumps({"id": msg["id"], "status": 200, "result": {}}))

    async with _serve(capture) as url:
        client = WsRpcClient(config=_config(url))
        await client.start()
        try:
            await client.request("server.time")
        finally:
            await client.close()

    assert captured[0] == {"id": captured[0]["id"], "method": "server.time"}
    assert "params" not in captured[0]
