"""Push-only WebSocket transport for Binance USD-M Futures market &
user-data streams.

This module is the stream-side counterpart to :mod:`.transport`:

* :mod:`.transport` (Phase 2b) handles the WS-API endpoint where every
  message is a request/response pair correlated by ``id``.
* :mod:`.stream` (this module, Phase 2c-2) handles the
  ``/stream?streams=...`` and ``/ws/<listenKey>`` endpoints where
  frames are server-pushed with no id correlation.

Both share the same connect / reconnect / backoff pattern but the
top-level message loop differs enough that a common base class would
be shape-only. We duplicate the supervisor instead — small enough.

What this transport is NOT:

* Not Binance-stream-specific. The combined-stream URL (``?streams=...``)
  is built by :class:`.market.MarketStreamClient` on top.
* Not a frame router. ``on_message`` receives the raw parsed dict;
  routing by ``stream`` field (combined streams) or ``e`` field
  (user data) is the caller's job.
* Not a subscription manager. Runtime ``SUBSCRIBE`` / ``UNSUBSCRIBE``
  on the bare ``/ws`` endpoint is out of scope for v1 — pass the full
  stream set in the URL at construction time.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection, connect

logger = logging.getLogger(__name__)

# Binance USD-M Futures stream endpoints. Callers can override via
# WsStreamConfig.url for testing / mainnet vs testnet.
MAINNET_STREAM_BASE_URL = "wss://fstream.binance.com"
TESTNET_STREAM_BASE_URL = "wss://stream.binancefuture.com"


SleepFn = Callable[[float], Awaitable[None]]
MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]


class WsStreamError(Exception):
    """Base class for stream transport errors."""


class WsStreamClosed(WsStreamError):
    """Raised when an operation is attempted on a closed stream."""


@dataclass(frozen=True, slots=True)
class WsStreamConfig:
    """Tunables for :class:`WsStreamTransport`.

    Defaults are picked for production use against Binance USD-M; tests
    override aggressively to keep the suite fast.
    """

    url: str
    connect_timeout_s: float = 10.0
    backoff_base_s: float = 0.5
    backoff_max_s: float = 30.0
    backoff_jitter: float = 0.2
    # Binance combined-stream payloads are typically <100 KiB; 16 MiB
    # matches the websockets default and tolerates any oversized burst.
    max_size: int = 2**24
    ping_interval_s: float | None = 20.0
    ping_timeout_s: float | None = 20.0


@dataclass(slots=True)
class WsStreamTransport:
    """Reconnecting WS connection that pushes every parsed JSON frame to
    a single ``on_message`` callback.

    Lifecycle mirrors :class:`..transport.WsRpcClient`:

    * ``await transport.start()`` spawns the supervisor task that
      connects, reads frames, dispatches them, and reconnects on failure.
    * ``await transport.close()`` stops the supervisor and closes the
      socket. Idempotent.

    The handler is invoked with the parsed dict for every frame that:

    * decoded as UTF-8
    * parsed as JSON
    * was a JSON object (not array, scalar, etc.)

    Anything else is dropped with a debug-level log message — Binance
    occasionally emits text-mode keepalives or rate-limit notices that
    aren't useful to higher layers.

    The handler MUST be async. Synchronous callbacks should be wrapped
    in a small ``async def`` shim. Exceptions raised by the handler are
    logged but don't kill the supervisor — the next frame is dispatched
    normally.
    """

    config: WsStreamConfig
    on_message: MessageHandler
    sleep: SleepFn = field(default=asyncio.sleep)
    _ws: ClientConnection | None = field(default=None, init=False)
    _connected: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _supervisor: asyncio.Task[None] | None = field(default=None, init=False)
    _closed: bool = field(default=False, init=False)
    _connect_attempts: int = field(default=0, init=False)

    @property
    def is_connected(self) -> bool:
        """Whether the underlying socket is currently open."""
        return self._connected.is_set() and self._ws is not None

    async def start(self) -> None:
        """Begin the connect / reconnect supervisor task.

        Idempotent-by-error: a second :meth:`start` call raises
        :class:`RuntimeError` to surface the bug rather than silently
        no-op.
        """
        if self._closed:
            raise WsStreamClosed("transport closed")
        if self._supervisor is not None:
            raise RuntimeError("WsStreamTransport already started")
        self._supervisor = asyncio.create_task(
            self._supervise(), name="binance-um-ws-stream-supervisor"
        )

    async def close(self) -> None:
        """Stop the supervisor and close the socket. Safe to call twice."""
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
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        self._connected.clear()

    async def wait_connected(self, *, timeout: float | None = None) -> None:
        """Block until the underlying socket is open.

        Useful for tests and for higher layers that want to confirm the
        stream is live before reporting "ready" to consumers.
        """
        if self._closed:
            raise WsStreamClosed("transport closed")
        wait_timeout = timeout if timeout is not None else self.config.connect_timeout_s
        await asyncio.wait_for(self._connected.wait(), timeout=wait_timeout)

    async def _supervise(self) -> None:
        """Maintain the WS connection, reconnecting on failure with backoff."""
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
                    logger.info("ws-stream connected url=%s", self.config.url)
                    async for raw in ws:
                        await self._dispatch(raw)
            except asyncio.CancelledError:
                raise
            except (
                websockets.ConnectionClosed,
                ConnectionError,
                OSError,
                TimeoutError,
            ) as e:
                logger.warning("ws-stream disconnected: %s", e)
            except Exception as e:  # pragma: no cover - defensive catch-all
                logger.exception("ws-stream unexpected error: %s", e)
            finally:
                self._ws = None
                self._connected.clear()

            if self._closed:
                return

            self._connect_attempts += 1
            delay = self._compute_backoff(self._connect_attempts)
            logger.info(
                "ws-stream reconnecting in %.2fs (attempt=%d)",
                delay,
                self._connect_attempts,
            )
            try:
                await self.sleep(delay)
            except asyncio.CancelledError:
                raise

    async def _dispatch(self, raw: str | bytes) -> None:
        """Parse a frame and call ``on_message`` with the dict.

        Drops malformed frames. Catches handler exceptions so one bad
        frame can't tear down the supervisor.
        """
        if isinstance(raw, bytes):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning("ws-stream dropped non-utf8 frame")
                return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("ws-stream dropped malformed json frame: %r", raw[:200])
            return
        if not isinstance(payload, dict):
            logger.debug("ws-stream dropped non-object frame: %r", raw[:200])
            return
        try:
            await self.on_message(payload)
        except Exception:
            logger.exception("ws-stream handler raised; supervising continues")

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
    "MAINNET_STREAM_BASE_URL",
    "TESTNET_STREAM_BASE_URL",
    "MessageHandler",
    "WsStreamClosed",
    "WsStreamConfig",
    "WsStreamError",
    "WsStreamTransport",
]
