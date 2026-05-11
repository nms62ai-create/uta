"""Bottom-layer WebSocket RPC transport for Binance USD-M Futures.

This module owns the connection lifecycle for Binance's WS-API endpoint.
It handles the boring plumbing — connect, ping/pong (delegated to the
``websockets`` library's built-in keepalive), reconnect with exponential
backoff + jitter, request-id correlation, and pending-request cancellation
on disconnect.

It deliberately knows nothing about:

* HMAC signing (:mod:`..auth` owns that — caller signs ``params`` before
  passing them in)
* Binance's success / error envelope (``status`` / ``error`` / ``code``)
  — the higher trade / user clients in Phase 2c interpret responses
* Stream subscriptions (push frames without ``id``) — those will live on
  a separate transport class because they have no request-response
  semantics

The envelope shape ``{id, method, params}`` -> ``{id, status, ...}`` *is*
Binance-specific. Bybit's WS-trade uses ``{op, args, req_id}`` — close
enough that we'll likely share the supervisor / backoff loop later, but
not so close that pretending to be vendor-agnostic now buys anything.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection, connect

logger = logging.getLogger(__name__)

# Default endpoints. Callers can override via WsRpcConfig.url for testing.
MAINNET_WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
TESTNET_WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"


class WsRpcError(Exception):
    """Base class for WS RPC errors."""


class WsRpcTimeout(WsRpcError):
    """Raised when a request does not get a response within its timeout."""


class WsRpcDisconnected(WsRpcError):
    """Raised when a request is in flight and the connection drops."""


class WsRpcClosed(WsRpcError):
    """Raised when a request is attempted on a closed client."""


SleepFn = Callable[[float], Awaitable[None]]
IdFactoryFn = Callable[[], str]


def _default_id_factory() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class WsRpcConfig:
    """Tunables for :class:`WsRpcClient`.

    Defaults are picked for production use against Binance USD-M; tests
    override timeouts and backoff to keep the suite fast.
    """

    url: str
    request_timeout_s: float = 10.0
    connect_timeout_s: float = 10.0
    backoff_base_s: float = 0.5
    backoff_max_s: float = 30.0
    backoff_jitter: float = 0.2  # ±20% multiplicative jitter
    # Binance WS-API frames are tiny (<10 KiB typical); 16 MiB is far over
    # what's needed but matches the websockets library default and avoids
    # rejecting any oversized rate-limit payload.
    max_size: int = 2**24
    # ping interval is library-managed; expose it for tests that need to
    # verify keepalive behaviour deterministically.
    ping_interval_s: float | None = 20.0
    ping_timeout_s: float | None = 20.0


@dataclass(slots=True)
class _PendingRequest:
    rid: str
    method: str
    future: asyncio.Future[dict[str, Any]]


@dataclass(slots=True)
class WsRpcClient:
    """Reconnecting WS-API client with id-correlated request/response.

    Lifecycle:

    * Construct with a :class:`WsRpcConfig`.
    * ``await client.start()`` — spawns the supervisor task that connects,
      reads, and reconnects on failure.
    * ``await client.request(method, params)`` — sends a single envelope
      and awaits the matching response.
    * ``await client.close()`` — stops the supervisor, rejects pending
      requests, closes the underlying socket.

    Instances are intended to be long-lived (one per upstream endpoint).
    Concurrent ``request()`` calls from multiple tasks are safe — each
    gets a unique id and its own future.
    """

    config: WsRpcConfig
    id_factory: IdFactoryFn = field(default=_default_id_factory)
    sleep: SleepFn = field(default=asyncio.sleep)
    _pending: dict[str, _PendingRequest] = field(default_factory=dict, init=False)
    _ws: ClientConnection | None = field(default=None, init=False)
    _connected: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _supervisor: asyncio.Task[None] | None = field(default=None, init=False)
    _closed: bool = field(default=False, init=False)
    _connect_attempts: int = field(default=0, init=False)

    @property
    def is_connected(self) -> bool:
        """Whether the underlying socket is currently open and dispatching."""
        return self._connected.is_set() and self._ws is not None

    async def start(self) -> None:
        """Begin the connect / reconnect supervisor task.

        Idempotent: calling ``start()`` twice raises :class:`RuntimeError`.
        """
        if self._closed:
            raise WsRpcClosed("client closed")
        if self._supervisor is not None:
            raise RuntimeError("WsRpcClient already started")
        self._supervisor = asyncio.create_task(
            self._supervise(), name="binance-um-ws-rpc-supervisor"
        )

    async def close(self) -> None:
        """Stop the supervisor, reject pending requests, close the socket.

        Safe to call multiple times. After ``close()`` the client cannot be
        restarted — construct a new instance.
        """
        if self._closed:
            return
        self._closed = True
        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except (asyncio.CancelledError, Exception):
                pass
            self._supervisor = None
        self._reject_pending(WsRpcClosed("client closed"))
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        self._connected.clear()

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send a WS-API request and await its response.

        ``params`` is sent verbatim — caller is responsible for adding
        ``apiKey`` / ``timestamp`` / ``signature`` for signed methods.

        Raises:
            WsRpcClosed: if the client was closed.
            WsRpcDisconnected: if the connection drops before a response
                arrives, or if no connection can be established within
                ``connect_timeout_s``.
            WsRpcTimeout: if the server does not reply within
                ``timeout`` (or ``config.request_timeout_s`` if not given).
        """
        if self._closed:
            raise WsRpcClosed("client closed")
        if self._supervisor is None:
            raise RuntimeError("WsRpcClient.start() not called")

        if not self._connected.is_set():
            try:
                await asyncio.wait_for(
                    self._connected.wait(), timeout=self.config.connect_timeout_s
                )
            except TimeoutError as e:
                raise WsRpcDisconnected(
                    f"not connected within {self.config.connect_timeout_s}s"
                ) from e

        ws = self._ws
        if ws is None:
            raise WsRpcDisconnected("connection lost before send")

        rid = str(self.id_factory())
        envelope: dict[str, Any] = {"id": rid, "method": method}
        if params is not None:
            envelope["params"] = params

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[rid] = _PendingRequest(rid=rid, method=method, future=fut)

        try:
            await ws.send(json.dumps(envelope, separators=(",", ":")))
        except Exception as e:
            self._pending.pop(rid, None)
            raise WsRpcDisconnected(f"send failed: {e}") from e

        wait_timeout = timeout if timeout is not None else self.config.request_timeout_s
        try:
            return await asyncio.wait_for(fut, timeout=wait_timeout)
        except TimeoutError as e:
            self._pending.pop(rid, None)
            raise WsRpcTimeout(
                f"no response for id={rid} method={method} within {wait_timeout}s"
            ) from e

    async def _supervise(self) -> None:
        """Maintain the WS connection, reconnecting on failure with backoff.

        Loop invariants:

        * Exits cleanly when ``self._closed`` is True (after :meth:`close`).
        * On successful connection: reset attempt counter, set
          ``_connected``, dispatch frames until disconnect.
        * On disconnect: reject all pending requests, increment attempt
          counter, sleep with exponential backoff + jitter, reconnect.
        """
        while not self._closed:
            try:
                async with connect(
                    self.config.url,
                    max_size=self.config.max_size,
                    open_timeout=self.config.connect_timeout_s,
                    ping_interval=self.config.ping_interval_s,
                    ping_timeout=self.config.ping_timeout_s,
                ) as ws:
                    self._ws = ws
                    self._connect_attempts = 0
                    self._connected.set()
                    logger.info("ws-rpc connected url=%s", self.config.url)
                    async for raw in ws:
                        self._dispatch(raw)
            except asyncio.CancelledError:
                raise
            except (TimeoutError, websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning("ws-rpc disconnected: %s", e)
            except Exception as e:  # pragma: no cover - defensive catch-all
                logger.exception("ws-rpc unexpected error: %s", e)
            finally:
                self._ws = None
                self._connected.clear()
                self._reject_pending(WsRpcDisconnected("connection lost"))

            if self._closed:
                return

            self._connect_attempts += 1
            delay = self._compute_backoff(self._connect_attempts)
            logger.info(
                "ws-rpc reconnecting in %.2fs (attempt=%d)",
                delay,
                self._connect_attempts,
            )
            try:
                await self.sleep(delay)
            except asyncio.CancelledError:
                raise

    def _dispatch(self, raw: str | bytes) -> None:
        """Parse a frame and resolve the matching pending future.

        Drops malformed frames and frames without an ``id`` field — the
        WS-API endpoint can occasionally emit ``error`` envelopes with
        no id (e.g. global rate-limit notifications). Those go to the
        log, not to a request future.
        """
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning("ws-rpc dropped non-utf8 frame")
                return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("ws-rpc dropped malformed json frame: %r", raw[:200])
            return
        if not isinstance(payload, dict):
            logger.warning("ws-rpc dropped non-object frame: %r", raw[:200])
            return
        rid_raw = payload.get("id")
        if rid_raw is None:
            logger.debug("ws-rpc id-less frame: %r", payload)
            return
        rid = str(rid_raw)
        pending = self._pending.pop(rid, None)
        if pending is None:
            logger.debug("ws-rpc no waiter for id=%s", rid)
            return
        if pending.future.done():
            return
        pending.future.set_result(payload)

    def _reject_pending(self, exc: BaseException) -> None:
        for pending in list(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(exc)
        self._pending.clear()

    def _compute_backoff(self, attempt: int) -> float:
        base = min(
            self.config.backoff_base_s * (2 ** (attempt - 1)),
            self.config.backoff_max_s,
        )
        jitter = self.config.backoff_jitter
        if jitter > 0:
            base *= 1.0 + random.uniform(-jitter, jitter)
        return max(0.0, base)


__all__ = [
    "MAINNET_WS_API_URL",
    "TESTNET_WS_API_URL",
    "WsRpcClient",
    "WsRpcClosed",
    "WsRpcConfig",
    "WsRpcDisconnected",
    "WsRpcError",
    "WsRpcTimeout",
]
