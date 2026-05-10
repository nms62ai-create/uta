"""Signed REST client for Binance USD-M Futures.

REST is **bootstrap, reconciliation, and explicit fallback only**
(decision A3b). The hot trading path is WebSocket-API; this module
exists for endpoints that don't have a WS equivalent (e.g.
``GET /fapi/v1/exchangeInfo``) or for emergency fallback if the
WS-trade socket is unavailable.

Public surface
    :class:`BinanceRestError`        — all error types share this base
    :class:`BinanceApiError`         — exchange returned ``-NNNN`` code
    :class:`BinanceHttpError`        — HTTP transport / 5xx
    :class:`BinanceRestClient`       — async client wrapping ``httpx``
        ``get_unsigned(path, params)``
        ``get_signed(path, params)``
        ``post_signed(path, params)``
        ``put_signed(path, params)``
        ``delete_signed(path, params)``

Auth
    Signed endpoints require ``timestamp`` (server-time milliseconds)
    and accept ``recvWindow``. The client injects both — the caller
    passes business params only. ``timestamp`` comes from the injected
    clock so tests are deterministic.

Concurrency
    Multiple coroutines may share one ``BinanceRestClient`` safely;
    ``httpx.AsyncClient`` is thread- and task-safe for concurrent
    requests on the same client.

Out of scope
    - Rate-limit token bucket (deferred to Phase 2c when WS-trade
      tokens are reasoned about uniformly).
    - Ed25519 signing (Phase 2c if user opts in).

The listen-key REST endpoints (``POST/PUT/DELETE /fapi/v1/listenKey``)
are exposed here as convenience methods because they reuse the same
``X-MBX-APIKEY`` header authentication path. The lifecycle orchestrator
that *uses* them — keepalive cadence, listenKey rotation, WS reconnect
on expiry — lives in :mod:`binance_um.ws.user_stream`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

from .auth import sign_query, url_encoded_query_string

_log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://fapi.binance.com"
TESTNET_BASE_URL = "https://testnet.binancefuture.com"
DEFAULT_TIMEOUT_S = 10.0
DEFAULT_RECV_WINDOW_MS = 5000


class BinanceRestError(Exception):
    """Base for all REST errors raised by this client."""


class BinanceHttpError(BinanceRestError):
    """Transport-level failure (DNS, timeout, 5xx, malformed body)."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class BinanceApiError(BinanceRestError):
    """Exchange returned ``{"code": -NNNN, "msg": "..."}`` for a request."""

    def __init__(self, code: int, msg: str, *, status_code: int | None = None) -> None:
        super().__init__(f"binance error {code}: {msg}")
        self.code = code
        self.msg = msg
        self.status_code = status_code


# Type alias for an injectable millisecond clock; defaults to wall time.
TimestampFn = Callable[[], int]


def _default_timestamp_ms() -> int:
    return int(time.time() * 1000)


class BinanceRestClient:
    """Signed REST client for Binance USD-M Futures.

    Parameters
    ----------
    api_key, api_secret:
        From the encrypted keystore (decision A2).
    base_url:
        ``DEFAULT_BASE_URL`` (mainnet) or ``TESTNET_BASE_URL``.
    recv_window_ms:
        Default ``recvWindow`` for signed requests; can be overridden
        per-call via ``params``.
    timestamp_fn:
        Callable returning the request timestamp in milliseconds.
        Injected so tests can run with a frozen clock.
    transport:
        Optional ``httpx.AsyncBaseTransport`` for tests (e.g.
        ``respx.MockTransport``).
    """

    def __init__(
        self,
        *,
        api_key: str = "",
        api_secret: str = "",
        base_url: str = DEFAULT_BASE_URL,
        recv_window_ms: int = DEFAULT_RECV_WINDOW_MS,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        timestamp_fn: TimestampFn | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url.rstrip("/")
        self._recv_window_ms = int(recv_window_ms)
        self._timestamp_fn = timestamp_fn or _default_timestamp_ms
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout_s,
            transport=transport,
            headers={"X-MBX-APIKEY": api_key} if api_key else {},
        )
        self._closed = False

    @property
    def base_url(self) -> str:
        return self._base_url

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._client.aclose()

    async def __aenter__(self) -> BinanceRestClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    # ---- Public request helpers ----

    async def get_unsigned(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("GET", path, params or {}, signed=False)

    async def get_signed(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("GET", path, params or {}, signed=True)

    async def post_signed(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("POST", path, params or {}, signed=True)

    async def put_signed(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("PUT", path, params or {}, signed=True)

    async def delete_signed(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        return await self._request("DELETE", path, params or {}, signed=True)

    # ---- Internal ----

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        *,
        signed: bool,
    ) -> Any:
        if self._closed:
            raise RuntimeError("BinanceRestClient is closed")

        if signed:
            if not self._api_secret:
                raise BinanceRestError("api_secret is required for signed requests")
            params = dict(params)
            params.setdefault("timestamp", self._timestamp_fn())
            params.setdefault("recvWindow", self._recv_window_ms)
            query = sign_query(self._api_secret, params)
        else:
            query = url_encoded_query_string(params) if params else ""

        send: Callable[[], Awaitable[httpx.Response]]
        if method in ("GET", "DELETE"):
            full = f"{path}?{query}" if query else path
            send = lambda: self._client.request(method, full)  # noqa: E731
        else:
            # Binance accepts POST/PUT params either as query string or
            # x-www-form-urlencoded body; the spec recommends body. We
            # follow the spec.
            content = query.encode("ascii") if query else b""
            headers = (
                {"Content-Type": "application/x-www-form-urlencoded"} if content else {}
            )
            send = lambda: self._client.request(  # noqa: E731
                method, path, content=content, headers=headers
            )

        try:
            resp = await send()
        except httpx.HTTPError as e:
            raise BinanceHttpError(f"{method} {path} transport failure: {e}") from e

        return _decode_response(resp)

    # ---- Convenience: high-level bootstrap calls ----

    async def fetch_exchange_info(self) -> dict[str, Any]:
        """``GET /fapi/v1/exchangeInfo`` — symbol filters + precision."""

        return await self.get_unsigned("/fapi/v1/exchangeInfo")

    async def fetch_server_time(self) -> int:
        """``GET /fapi/v1/time`` — returns the server-time ms."""

        payload = await self.get_unsigned("/fapi/v1/time")
        return int(payload["serverTime"])

    async def fetch_positions(self) -> list[dict[str, Any]]:
        """``GET /fapi/v2/positionRisk`` — used for bootstrap + reconciliation."""

        return await self.get_signed("/fapi/v2/positionRisk")

    async def fetch_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """``GET /fapi/v1/openOrders`` — used for bootstrap + reconciliation."""

        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        return await self.get_signed("/fapi/v1/openOrders", params)

    async def fetch_account(self) -> dict[str, Any]:
        """``GET /fapi/v2/account`` — balances, margin info."""

        return await self.get_signed("/fapi/v2/account")

    # ---- User data stream (listenKey) endpoints ----
    #
    # These three endpoints authenticate via the ``X-MBX-APIKEY`` header
    # only — no HMAC signature, no timestamp, no recvWindow. The
    # orchestrator that calls them — :class:`..ws.user_stream.UserDataStreamClient` —
    # owns keepalive cadence and listenKey rotation; this class only
    # exposes them as raw HTTP calls.

    async def start_user_data_stream(self) -> str:
        """``POST /fapi/v1/listenKey`` — open a USER_DATA_STREAM.

        Returns the ``listenKey`` string. The key is valid for ~60
        minutes; callers should ``keepalive_user_data_stream`` every
        ~30 minutes to extend it.
        """
        payload = await self._request(
            "POST", "/fapi/v1/listenKey", {}, signed=False
        )
        if not isinstance(payload, dict) or "listenKey" not in payload:
            raise BinanceHttpError(
                f"unexpected listenKey response: {payload!r}"
            )
        return str(payload["listenKey"])

    async def keepalive_user_data_stream(self) -> None:
        """``PUT /fapi/v1/listenKey`` — extend the current listenKey."""
        await self._request("PUT", "/fapi/v1/listenKey", {}, signed=False)

    async def close_user_data_stream(self) -> None:
        """``DELETE /fapi/v1/listenKey`` — close the current listenKey."""
        await self._request("DELETE", "/fapi/v1/listenKey", {}, signed=False)


def _decode_response(resp: httpx.Response) -> Any:
    """Map an ``httpx.Response`` to a parsed body or a typed exception."""

    if resp.is_success:
        try:
            return resp.json()
        except ValueError as e:
            raise BinanceHttpError(
                f"non-JSON response (status={resp.status_code})",
                status_code=resp.status_code,
            ) from e

    # On failure, try to extract Binance's ``{"code": -NNNN, "msg": ...}``.
    body: Any = None
    try:
        body = resp.json()
    except ValueError:
        body = None

    if isinstance(body, dict) and "code" in body and "msg" in body:
        raise BinanceApiError(
            int(body["code"]),
            str(body["msg"]),
            status_code=resp.status_code,
        )

    raise BinanceHttpError(
        f"HTTP {resp.status_code}: {resp.text[:200]}",
        status_code=resp.status_code,
    )


__all__ = [
    "DEFAULT_BASE_URL",
    "TESTNET_BASE_URL",
    "BinanceApiError",
    "BinanceHttpError",
    "BinanceRestClient",
    "BinanceRestError",
    "TimestampFn",
]
