"""Binance USD-M Futures WS-API signed-RPC client.

Thin layer on top of :class:`..ws.transport.WsRpcClient` that handles the
two Binance-specific concerns the transport doesn't know about:

1.  **Signing.** Signed methods (``order.place``, ``order.cancel``,
    ``account.status``, ...) require ``apiKey`` / ``timestamp`` /
    ``recvWindow`` / ``signature`` injected into ``params``. We compute
    and inject those here using :mod:`..auth`.
2.  **Envelope parsing.** Binance replies with
    ``{id, status, result, rateLimits}`` on success and
    ``{id, status, error: {code, msg}, rateLimits}`` on failure. The
    transport routes by ``id``; we unwrap ``result`` or raise
    :class:`BinanceWsApiError`.

This module deliberately stops at envelope unwrapping. It does **not**
build order params from a :class:`UniversalSignal` — that's the signal
router's job in Phase 3, where ``SymbolRegistry`` rounding, SL/TP
attachment, and `client_order_id` generation live.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..auth import sign_params
from ..ws.transport import WsRpcClient, WsRpcError

DEFAULT_RECV_WINDOW_MS = 5000


def _default_timestamp_ms() -> int:
    return int(time.time() * 1000)


TimestampFn = Callable[[], int]


class BinanceWsTradeError(WsRpcError):
    """Base class for WS-trade-specific errors (extends :class:`WsRpcError`)."""


class BinanceWsApiError(BinanceWsTradeError):
    """Server returned a non-2xx WS-API status with an ``error`` envelope.

    Attributes:
        status: HTTP-like status from the WS-API response (e.g. 400, 418).
        code: Binance error code (e.g. ``-1102``). 0 if missing.
        message: Binance error message (``msg`` field). Empty if missing.
    """

    def __init__(self, status: int, code: int, message: str) -> None:
        super().__init__(f"binance ws-api error status={status} code={code}: {message}")
        self.status = status
        self.code = code
        self.message = message


class BinanceWsProtocolError(BinanceWsTradeError):
    """Server returned a malformed envelope (e.g. missing ``status`` and ``result``)."""


@dataclass(slots=True)
class BinanceWsTradeClient:
    """Sign-and-call wrapper around :class:`WsRpcClient` for Binance WS-API.

    Construction does **not** start the underlying transport — call
    :meth:`start` explicitly so the lifecycle is symmetric with
    :meth:`close`. ``rpc`` is owned by the caller; we don't close it
    on :meth:`close` to allow sharing one transport across multiple
    higher-level clients (trade + future user-data muxing).

    Set ``own_rpc=True`` if you'd rather hand off ownership and have
    :meth:`close` close the transport too.
    """

    rpc: WsRpcClient
    api_key: str = ""
    api_secret: str = ""
    recv_window_ms: int = DEFAULT_RECV_WINDOW_MS
    timestamp_fn: TimestampFn = field(default=_default_timestamp_ms)
    own_rpc: bool = False

    async def start(self) -> None:
        """Start the underlying transport if it isn't running."""
        await self.rpc.start()

    async def close(self) -> None:
        """Close the underlying transport iff this client owns it."""
        if self.own_rpc:
            await self.rpc.close()

    async def call_unsigned(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send an unsigned WS-API request and return the unwrapped ``result``."""
        response = await self.rpc.request(method, params, timeout=timeout)
        return self._unwrap(response)

    async def call_signed(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send a signed WS-API request and return the unwrapped ``result``.

        Auto-injects ``apiKey``, ``timestamp``, ``recvWindow`` (if not
        already present), then signs via :func:`auth.sign_params`. Raises
        :class:`RuntimeError` if ``api_key`` / ``api_secret`` are unset —
        signed methods can't proceed without them and silently sending an
        unsigned request would be worse than failing fast.
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(
                f"signed call '{method}' requires api_key and api_secret"
            )
        signed = self._sign(params or {})
        response = await self.rpc.request(method, signed, timeout=timeout)
        return self._unwrap(response)

    async def ping(self, *, timeout: float | None = None) -> dict[str, Any]:
        """Convenience: call the unsigned ``ping`` method."""
        return await self.call_unsigned("ping", timeout=timeout)

    async def server_time(self, *, timeout: float | None = None) -> int:
        """Convenience: call ``time`` and return ``serverTime`` as an int."""
        result = await self.call_unsigned("time", timeout=timeout)
        return int(result["serverTime"])

    async def account_status(self, *, timeout: float | None = None) -> dict[str, Any]:
        """Convenience: call signed ``account.status``."""
        return await self.call_signed("account.status", timeout=timeout)

    async def place_order(
        self, params: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Send a signed ``order.place`` request with the given ``params``.

        Caller is responsible for building ``params`` correctly (symbol /
        side / type / quantity / price / etc.) — Phase 3 owns the mapping
        from :class:`UniversalSignal` to these fields.
        """
        return await self.call_signed("order.place", params, timeout=timeout)

    async def cancel_order(
        self, params: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Send a signed ``order.cancel`` request."""
        return await self.call_signed("order.cancel", params, timeout=timeout)

    async def query_order(
        self, params: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Send a signed ``order.status`` request."""
        return await self.call_signed("order.status", params, timeout=timeout)

    def _sign(self, params: dict[str, Any]) -> dict[str, Any]:
        """Build a signed copy of ``params``.

        Inserts ``apiKey``, ``timestamp``, ``recvWindow`` if absent, then
        signs in-place via :func:`auth.sign_params`. Existing values are
        preserved so a caller can override any of them per call (e.g. to
        replay a request with a frozen timestamp).
        """
        merged: dict[str, Any] = dict(params)
        merged.setdefault("apiKey", self.api_key)
        merged.setdefault("timestamp", self.timestamp_fn())
        merged.setdefault("recvWindow", self.recv_window_ms)
        return sign_params(self.api_secret, merged)

    @staticmethod
    def _unwrap(response: dict[str, Any]) -> dict[str, Any]:
        """Unwrap Binance's success envelope or raise on error.

        Success envelope: ``{id, status: 200, result: ..., rateLimits}``.
        Error envelope:   ``{id, status: 4xx/5xx, error: {code, msg}, rateLimits}``.

        The status code ``200`` is treated as success; any other value
        with an ``error`` field raises :class:`BinanceWsApiError`. A
        missing both ``result`` and ``error`` is a protocol violation —
        raise :class:`BinanceWsProtocolError` so the caller doesn't get
        ``None`` back without knowing why.
        """
        status_raw = response.get("status")
        try:
            status = int(status_raw) if status_raw is not None else 0
        except (TypeError, ValueError):
            status = 0

        error = response.get("error")
        if error is not None:
            if not isinstance(error, dict):
                raise BinanceWsProtocolError(
                    f"non-object 'error' field: {error!r}"
                )
            code_raw = error.get("code", 0)
            try:
                code = int(code_raw)
            except (TypeError, ValueError):
                code = 0
            message = str(error.get("msg", ""))
            raise BinanceWsApiError(status=status, code=code, message=message)

        if "result" not in response:
            raise BinanceWsProtocolError(
                f"response missing both 'result' and 'error': {response!r}"
            )
        result = response["result"]
        if not isinstance(result, dict):
            # Some methods (e.g. ping) reply with empty {} as result; some
            # could reply with a list. Wrap non-dict results in a stable
            # shape so callers always get a dict.
            return {"result": result}
        return result


__all__ = [
    "DEFAULT_RECV_WINDOW_MS",
    "BinanceWsApiError",
    "BinanceWsProtocolError",
    "BinanceWsTradeClient",
    "BinanceWsTradeError",
    "TimestampFn",
]
