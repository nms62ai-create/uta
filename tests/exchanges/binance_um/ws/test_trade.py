"""Tests for the Binance USD-M WS-API signed-RPC client.

In-process ``websockets`` server validates signatures using the same
:mod:`auth` module the client uses, so the test suite exercises the full
sign-and-verify roundtrip without touching the real Binance endpoint.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from trade_adapter.exchanges.binance_um.auth import (
    canonical_query_string,
    sign_hmac,
)
from trade_adapter.exchanges.binance_um.ws.trade import (
    BinanceWsApiError,
    BinanceWsProtocolError,
    BinanceWsTradeClient,
)
from trade_adapter.exchanges.binance_um.ws.transport import (
    WsRpcClient,
    WsRpcConfig,
)

API_KEY = "test-api-key"
API_SECRET = "test-api-secret"

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


def _config(url: str) -> WsRpcConfig:
    return WsRpcConfig(
        url=url,
        request_timeout_s=1.0,
        connect_timeout_s=1.0,
        backoff_base_s=0.01,
        backoff_max_s=0.05,
        backoff_jitter=0.0,
        ping_interval_s=None,
        ping_timeout_s=None,
    )


@asynccontextmanager
async def _trade_client(
    handler: ServerHandler,
    *,
    timestamp: int = 1_700_000_000_000,
) -> AsyncIterator[BinanceWsTradeClient]:
    """Spin up a server, build a client, ensure cleanup."""
    async with _serve(handler) as url:
        rpc = WsRpcClient(config=_config(url))
        client = BinanceWsTradeClient(
            rpc=rpc,
            api_key=API_KEY,
            api_secret=API_SECRET,
            timestamp_fn=lambda: timestamp,
            own_rpc=True,
        )
        await client.start()
        try:
            yield client
        finally:
            await client.close()


def _ok(rid: Any, result: Any) -> str:
    return json.dumps({"id": rid, "status": 200, "result": result})


def _err(rid: Any, status: int, code: int, msg: str) -> str:
    return json.dumps(
        {"id": rid, "status": status, "error": {"code": code, "msg": msg}}
    )


@pytest.mark.asyncio
async def test_ping_unwraps_empty_result() -> None:
    """``ping`` is unsigned; result is ``{}`` per Binance docs."""

    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            assert msg["method"] == "ping"
            assert "params" not in msg, "ping should not carry params"
            await ws.send(_ok(msg["id"], {}))

    async with _trade_client(server) as client:
        result = await client.ping()
    assert result == {}


@pytest.mark.asyncio
async def test_server_time_returns_int() -> None:
    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(_ok(msg["id"], {"serverTime": 1700000123456}))

    async with _trade_client(server) as client:
        ts = await client.server_time()
    assert ts == 1700000123456


@pytest.mark.asyncio
async def test_signed_call_injects_apikey_timestamp_recvwindow_and_signature() -> None:
    """``call_signed`` must add the four required fields with the right values
    and a signature that matches an independent recomputation.
    """
    captured: list[dict[str, Any]] = []

    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            captured.append(msg)
            await ws.send(_ok(msg["id"], {"echo": msg.get("params", {})}))

    async with _trade_client(server, timestamp=1700000000000) as client:
        result = await client.call_signed(
            "order.place",
            {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": 0.001},
        )

    assert len(captured) == 1
    sent = captured[0]
    assert sent["method"] == "order.place"
    sent_params = sent["params"]
    assert sent_params["apiKey"] == API_KEY
    assert sent_params["timestamp"] == 1700000000000
    assert sent_params["recvWindow"] == 5000
    assert "signature" in sent_params

    # Recompute signature independently and verify match.
    unsigned = {k: v for k, v in sent_params.items() if k != "signature"}
    expected_sig = sign_hmac(API_SECRET, canonical_query_string(unsigned))
    assert sent_params["signature"] == expected_sig

    assert result["echo"]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_server_validates_signature_via_real_auth_module() -> None:
    """End-to-end: server uses the same ``auth`` module to verify; if the
    client signs incorrectly, the server returns an error envelope.
    """

    async def verifying_server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            params = msg.get("params", {})
            sig = params.pop("signature", None)
            if sig is None:
                await ws.send(_err(msg["id"], 400, -1102, "missing signature"))
                continue
            expected = sign_hmac(API_SECRET, canonical_query_string(params))
            if sig != expected:
                await ws.send(_err(msg["id"], 401, -1022, "invalid signature"))
                continue
            await ws.send(_ok(msg["id"], {"orderId": 12345}))

    async with _trade_client(verifying_server, timestamp=1700000000000) as client:
        result = await client.place_order({"symbol": "BTCUSDT", "side": "BUY"})
    assert result == {"orderId": 12345}


@pytest.mark.asyncio
async def test_signed_call_without_credentials_raises_before_send() -> None:
    """Calling a signed method on a client without keys must fail fast,
    not silently send an unsigned request the server will reject.
    """

    async def never_called(ws: ServerConnection) -> None:
        async for _ in ws:
            raise AssertionError("server should not receive any frames")

    async with _serve(never_called) as url:
        rpc = WsRpcClient(config=_config(url))
        client = BinanceWsTradeClient(rpc=rpc, own_rpc=True)
        await client.start()
        try:
            with pytest.raises(RuntimeError, match="api_key and api_secret"):
                await client.call_signed("order.place")
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_error_envelope_raises_binance_ws_api_error() -> None:
    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(
                _err(msg["id"], 400, -1102, "Mandatory parameter 'symbol' was not sent")
            )

    async with _trade_client(server) as client:
        with pytest.raises(BinanceWsApiError) as excinfo:
            await client.call_signed("order.place", {"side": "BUY"})

    err = excinfo.value
    assert err.status == 400
    assert err.code == -1102
    assert "symbol" in err.message


@pytest.mark.asyncio
async def test_protocol_error_when_neither_result_nor_error() -> None:
    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            # Status 200 but no result and no error — malformed.
            await ws.send(json.dumps({"id": msg["id"], "status": 200}))

    async with _trade_client(server) as client:
        with pytest.raises(BinanceWsProtocolError):
            await client.call_unsigned("weird.method")


@pytest.mark.asyncio
async def test_protocol_error_when_error_field_is_not_object() -> None:
    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(json.dumps({"id": msg["id"], "status": 400, "error": "bad"}))

    async with _trade_client(server) as client:
        with pytest.raises(BinanceWsProtocolError):
            await client.call_signed("order.place")


@pytest.mark.asyncio
async def test_non_dict_result_is_wrapped() -> None:
    """Some methods may return a list or scalar; we wrap it so callers
    always get a dict.
    """

    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(_ok(msg["id"], [1, 2, 3]))

    async with _trade_client(server) as client:
        out = await client.call_unsigned("listy.method")
    assert out == {"result": [1, 2, 3]}


@pytest.mark.asyncio
async def test_caller_supplied_timestamp_is_preserved() -> None:
    """If the caller put a ``timestamp`` in params, we don't overwrite it."""
    captured: list[dict[str, Any]] = []

    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            captured.append(msg)
            await ws.send(_ok(msg["id"], {}))

    async with _trade_client(server, timestamp=1700000000000) as client:
        await client.call_signed("order.place", {"timestamp": 1234567890123})

    assert captured[0]["params"]["timestamp"] == 1234567890123


@pytest.mark.asyncio
async def test_close_does_not_close_shared_rpc_when_not_owned() -> None:
    """If ``own_rpc=False``, ``client.close()`` leaves the transport open."""

    async def server(ws: ServerConnection) -> None:
        async for raw in ws:
            msg = json.loads(raw)
            await ws.send(_ok(msg["id"], {}))

    async with _serve(server) as url:
        rpc = WsRpcClient(config=_config(url))
        client = BinanceWsTradeClient(
            rpc=rpc,
            api_key=API_KEY,
            api_secret=API_SECRET,
            timestamp_fn=lambda: 1700000000000,
            own_rpc=False,
        )
        await client.start()
        await client.ping()
        await client.close()
        try:
            # rpc must still be usable.
            assert rpc.is_connected
            await rpc.request("ping")
        finally:
            await rpc.close()
