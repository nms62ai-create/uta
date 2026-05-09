"""Tests for the Binance USD-M signed REST client.

Uses ``respx`` to mock ``httpx`` transport so no live network is touched.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from trade_adapter.exchanges.binance_um.auth import sign_hmac
from trade_adapter.exchanges.binance_um.rest import (
    DEFAULT_BASE_URL,
    BinanceApiError,
    BinanceHttpError,
    BinanceRestClient,
)


def _frozen_clock(ts_ms: int = 1_700_000_000_000):
    return lambda: ts_ms


@pytest.fixture
def client() -> BinanceRestClient:
    return BinanceRestClient(
        api_key="ak",
        api_secret="sk",
        base_url=DEFAULT_BASE_URL,
        recv_window_ms=5000,
        timestamp_fn=_frozen_clock(),
    )


@respx.mock
async def test_get_unsigned_returns_parsed_json(client: BinanceRestClient) -> None:
    route = respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/exchangeInfo").mock(
        return_value=httpx.Response(200, json={"symbols": [{"symbol": "BTCUSDT"}]})
    )
    out = await client.fetch_exchange_info()
    await client.aclose()
    assert route.called
    assert out == {"symbols": [{"symbol": "BTCUSDT"}]}


@respx.mock
async def test_unsigned_request_does_not_send_signature(client: BinanceRestClient) -> None:
    route = respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/time").mock(
        return_value=httpx.Response(200, json={"serverTime": 1_700_000_000_000})
    )
    ts = await client.fetch_server_time()
    await client.aclose()
    assert ts == 1_700_000_000_000
    assert "signature" not in route.calls.last.request.url.params


@respx.mock
async def test_get_signed_appends_signature_and_api_key(client: BinanceRestClient) -> None:
    captured: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["headers"] = dict(request.headers)
        return httpx.Response(200, json=[{"symbol": "BTCUSDT", "positionAmt": "0"}])

    respx.get(f"{DEFAULT_BASE_URL}/fapi/v2/positionRisk").mock(side_effect=_capture)

    out = await client.fetch_positions()
    await client.aclose()
    assert out == [{"symbol": "BTCUSDT", "positionAmt": "0"}]

    # Verify the request included the signed query.
    assert "signature=" in captured["url"]
    assert "timestamp=1700000000000" in captured["url"]
    assert "recvWindow=5000" in captured["url"]
    assert captured["headers"].get("x-mbx-apikey") == "ak"

    # And the signature is exactly HMAC-SHA256 over the canonical form.
    expected_sig = sign_hmac("sk", "recvWindow=5000&timestamp=1700000000000")
    assert f"signature={expected_sig}" in captured["url"]


@respx.mock
async def test_get_signed_with_business_params(client: BinanceRestClient) -> None:
    captured: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json=[])

    respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/openOrders").mock(side_effect=_capture)

    await client.fetch_open_orders(symbol="BTCUSDT")
    await client.aclose()
    assert "symbol=BTCUSDT" in captured["url"]
    assert "signature=" in captured["url"]


@respx.mock
async def test_post_signed_sends_form_body(client: BinanceRestClient) -> None:
    captured: dict = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode("ascii")
        captured["content_type"] = request.headers.get("content-type")
        return httpx.Response(200, json={"orderId": 1})

    respx.post(f"{DEFAULT_BASE_URL}/fapi/v1/order").mock(side_effect=_capture)

    out = await client.post_signed(
        "/fapi/v1/order",
        {"symbol": "BTCUSDT", "side": "BUY", "type": "MARKET", "quantity": "0.001"},
    )
    await client.aclose()
    assert out == {"orderId": 1}
    # Body has signed query, URL has no query.
    assert captured["url"].endswith("/fapi/v1/order")
    assert "signature=" in captured["body"]
    assert "symbol=BTCUSDT" in captured["body"]
    assert captured["content_type"] == "application/x-www-form-urlencoded"


@respx.mock
async def test_api_error_raised_on_4xx_with_binance_envelope(client: BinanceRestClient) -> None:
    respx.get(f"{DEFAULT_BASE_URL}/fapi/v2/positionRisk").mock(
        return_value=httpx.Response(400, json={"code": -1021, "msg": "Timestamp ahead"})
    )
    with pytest.raises(BinanceApiError) as ei:
        await client.fetch_positions()
    await client.aclose()
    assert ei.value.code == -1021
    assert ei.value.msg == "Timestamp ahead"
    assert ei.value.status_code == 400


@respx.mock
async def test_http_error_raised_on_5xx_text_body(client: BinanceRestClient) -> None:
    respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/time").mock(
        return_value=httpx.Response(503, text="upstream unavailable")
    )
    with pytest.raises(BinanceHttpError) as ei:
        await client.fetch_server_time()
    await client.aclose()
    assert ei.value.status_code == 503


@respx.mock
async def test_http_error_raised_on_transport_failure() -> None:
    transport_error = httpx.ConnectError("dns failure")
    respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/time").mock(side_effect=transport_error)
    client = BinanceRestClient(
        api_key="",
        api_secret="",
        timestamp_fn=_frozen_clock(),
    )
    with pytest.raises(BinanceHttpError):
        await client.fetch_server_time()
    await client.aclose()


async def test_signed_request_without_secret_raises() -> None:
    client = BinanceRestClient(api_key="ak", api_secret="", timestamp_fn=_frozen_clock())
    with pytest.raises(Exception):  # noqa: B017 - BinanceRestError or RuntimeError
        await client.fetch_positions()
    await client.aclose()


async def test_closed_client_rejects_further_requests(client: BinanceRestClient) -> None:
    await client.aclose()
    with pytest.raises(RuntimeError):
        await client.fetch_server_time()


async def test_aclose_idempotent(client: BinanceRestClient) -> None:
    await client.aclose()
    await client.aclose()  # second call must not raise


@respx.mock
async def test_async_context_manager_closes_client() -> None:
    respx.get(f"{DEFAULT_BASE_URL}/fapi/v1/time").mock(
        return_value=httpx.Response(200, json={"serverTime": 1})
    )
    async with BinanceRestClient(api_key="", api_secret="", timestamp_fn=_frozen_clock()) as c:
        assert await c.fetch_server_time() == 1
    # After exit, further requests must fail.
    with pytest.raises(RuntimeError):
        await c.fetch_server_time()
