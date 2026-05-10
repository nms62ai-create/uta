"""Binance USD-M Futures USER_DATA_STREAM client.

USER_DATA_STREAM is a push-only WS endpoint authenticated by a
short-lived ``listenKey`` rather than per-frame HMAC. Lifecycle:

1.  ``POST /fapi/v1/listenKey`` returns a key valid for ~60 minutes.
2.  WS connects to ``wss://fstream.binance.com/ws/<listenKey>``.
3.  Server pushes ``ORDER_TRADE_UPDATE``, ``ACCOUNT_UPDATE``,
    ``MARGIN_CALL``, ``ACCOUNT_CONFIG_UPDATE``, and ``listenKeyExpired``
    events on that connection.
4.  Client must ``PUT /fapi/v1/listenKey`` every ~30 minutes to extend.
5.  If the key expires (or the server sends ``listenKeyExpired``), the
    WS connection is dropped. Client must acquire a new listenKey,
    rebuild the URL, and reconnect.
6.  On graceful close, ``DELETE /fapi/v1/listenKey`` releases the key.

This module owns all five concerns. The bottom-layer
:class:`..stream.WsStreamTransport` handles per-listenKey connect /
read / reconnect inside a session; the supervisor here owns the
listenKey lifecycle around it (acquire, keepalive, rotate on expiry).

Routing
    Each frame is a Binance event object with an ``e`` field naming the
    event type. We dispatch the whole event dict (not a sub-field) to a
    handler keyed by ``e``. ``listenKeyExpired`` is intercepted before
    user handlers are invoked — that event is a lifecycle signal, not a
    business event.

Out of scope
    Translating raw event dicts into UTA event types
    (``OrderUpdate`` / ``PositionUpdate`` / ``AccountUpdate``) is Phase 3.
    This client stops at delivering the raw Binance event dict to
    per-event callbacks.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

from .stream import (
    MAINNET_STREAM_BASE_URL,
    WsStreamConfig,
    WsStreamTransport,
)

if TYPE_CHECKING:  # pragma: no cover
    from ..rest import BinanceRestClient

logger = logging.getLogger(__name__)

DEFAULT_KEEPALIVE_INTERVAL_S: float = 30 * 60  # Binance recommends ≤30 min
LISTEN_KEY_EXPIRED_EVENT = "listenKeyExpired"

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]
SleepFn = Callable[[float], Awaitable[None]]


class _ListenKeyManager(Protocol):
    """Subset of :class:`BinanceRestClient` used by the stream client.

    Declared as a Protocol so tests can substitute a fake without
    spinning up a full ``httpx`` mock.
    """

    async def start_user_data_stream(self) -> str: ...
    async def keepalive_user_data_stream(self) -> None: ...
    async def close_user_data_stream(self) -> None: ...


class UserDataStreamError(Exception):
    """Base class for user-data stream errors."""


class UserDataStreamClosed(UserDataStreamError):
    """Raised when an operation is attempted on a closed client."""


@dataclass(frozen=True, slots=True)
class UserDataStreamConfig:
    """Tunables for :class:`UserDataStreamClient`.

    Defaults are picked for production against Binance USD-M; tests
    override aggressively to keep the suite fast.
    """

    stream_base_url: str = MAINNET_STREAM_BASE_URL
    keepalive_interval_s: float = DEFAULT_KEEPALIVE_INTERVAL_S
    rotate_backoff_base_s: float = 1.0
    rotate_backoff_max_s: float = 30.0
    rotate_backoff_jitter: float = 0.2
    transport_config_overrides: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class UserDataStreamClient:
    """Lifecycle manager for Binance USD-M USER_DATA_STREAM.

    Owns three coordinated tasks while running:

    * **Supervisor** — top-level loop that acquires a listenKey, builds
      the per-key WS transport, runs it, and on listenKey-level failure
      (expiry / keepalive failure) recycles to a new key with backoff.
    * **Transport** — :class:`WsStreamTransport` configured with the
      listenKey-specific URL. Handles intra-session connect / reconnect /
      backoff for transient socket failures.
    * **Keepalive** — periodic ``PUT /fapi/v1/listenKey``. On failure or
      if the server sends ``listenKeyExpired``, the supervisor rotates.

    Construction does not start the lifecycle — call :meth:`start`.
    """

    rest: BinanceRestClient
    handlers: Mapping[str, EventHandler]
    config: UserDataStreamConfig = field(default_factory=UserDataStreamConfig)
    sleep: SleepFn = field(default=asyncio.sleep)
    _supervisor: asyncio.Task[None] | None = field(default=None, init=False)
    _transport: WsStreamTransport | None = field(default=None, init=False)
    _keepalive_task: asyncio.Task[None] | None = field(default=None, init=False)
    _rotate_event: asyncio.Event = field(
        default_factory=asyncio.Event, init=False
    )
    _connected: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _closed: bool = field(default=False, init=False)
    _rotate_attempts: int = field(default=0, init=False)
    _current_listen_key: str | None = field(default=None, init=False)
    _handlers_normalised: dict[str, EventHandler] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        # Event names are case-sensitive on the wire (e.g. ORDER_TRADE_UPDATE,
        # listenKeyExpired) but we accept any case from the caller and
        # normalise the lookup. Per Binance docs the keys come in mixed
        # case; we match exactly against the ``e`` field but build the
        # lookup table verbatim from the user.
        self._handlers_normalised = dict(self.handlers)

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def current_listen_key(self) -> str | None:
        """Most recently acquired listenKey; ``None`` before first session."""
        return self._current_listen_key

    async def start(self) -> None:
        """Spawn the supervisor task. Idempotent-by-error."""
        if self._closed:
            raise UserDataStreamClosed("client closed")
        if self._supervisor is not None:
            raise RuntimeError("UserDataStreamClient already started")
        self._supervisor = asyncio.create_task(
            self._supervise(), name="binance-um-user-data-stream-supervisor"
        )

    async def close(self) -> None:
        """Stop the supervisor + transport + keepalive. Idempotent.

        Best-effort calls ``DELETE /fapi/v1/listenKey`` for the current
        key on the way out so the server can release it immediately.
        Errors from the DELETE are logged and swallowed — close should
        not raise.
        """
        if self._closed:
            return
        self._closed = True
        # Wake the supervisor so it exits its inner wait promptly.
        self._rotate_event.set()
        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except (asyncio.CancelledError, Exception):
                pass
            self._supervisor = None
        await self._teardown_session(delete_listen_key=True)

    async def wait_connected(self, *, timeout: float | None = None) -> None:
        """Block until the underlying socket is open."""
        if self._closed:
            raise UserDataStreamClosed("client closed")
        await asyncio.wait_for(self._connected.wait(), timeout=timeout)

    async def _supervise(self) -> None:
        """Top-level loop: acquire / run / rotate."""
        while not self._closed:
            try:
                await self._run_one_session()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive catch-all
                logger.exception("user-data-stream supervisor session crashed")

            if self._closed:
                return

            self._rotate_attempts += 1
            delay = self._compute_rotate_backoff(self._rotate_attempts)
            logger.info(
                "user-data-stream rotating listenKey in %.2fs (attempt=%d)",
                delay,
                self._rotate_attempts,
            )
            try:
                await self.sleep(delay)
            except asyncio.CancelledError:
                raise

    async def _run_one_session(self) -> None:
        """Acquire a listenKey, run the transport + keepalive until rotation."""
        try:
            listen_key = await self.rest.start_user_data_stream()
        except Exception as e:
            logger.warning(
                "user-data-stream listenKey acquisition failed: %s", e
            )
            return

        self._current_listen_key = listen_key
        url = self._build_url(listen_key)
        logger.info(
            "user-data-stream session start listenKey=%s***",
            _mask_key(listen_key),
        )

        config_kwargs: dict[str, Any] = {"url": url}
        config_kwargs.update(self.config.transport_config_overrides)
        transport_config = WsStreamConfig(**config_kwargs)
        transport = WsStreamTransport(
            config=transport_config, on_message=self._dispatch
        )
        self._transport = transport
        self._rotate_event = asyncio.Event()

        # Wire the transport's internal `connected` event into the
        # outward-facing one. The transport sets/clears its own; we
        # mirror it with a small watcher so callers of wait_connected()
        # see the union state.
        connected_watcher = asyncio.create_task(
            self._mirror_connected(transport),
            name="binance-um-user-data-stream-connected-mirror",
        )

        keepalive_task = asyncio.create_task(
            self._keepalive_loop(),
            name="binance-um-user-data-stream-keepalive",
        )
        self._keepalive_task = keepalive_task

        try:
            await transport.start()
            await self._rotate_event.wait()
        finally:
            connected_watcher.cancel()
            try:
                await connected_watcher
            except (asyncio.CancelledError, Exception):
                pass
            await self._teardown_session(delete_listen_key=False)

    async def _mirror_connected(self, transport: WsStreamTransport) -> None:
        """Mirror the transport's internal connected state to ``self._connected``.

        ``WsStreamTransport`` exposes ``is_connected`` as a property and
        ``wait_connected`` as a one-shot wait. To track the up/down
        transitions across reconnects we poll lightly. The poll loop
        terminates when cancelled or the supervisor moves on to a new
        session.
        """
        try:
            while True:
                if transport.is_connected:
                    self._connected.set()
                else:
                    self._connected.clear()
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            self._connected.clear()
            raise

    async def _keepalive_loop(self) -> None:
        """Periodically ``PUT /fapi/v1/listenKey``; trigger rotation on failure."""
        try:
            while not self._closed:
                try:
                    await self.sleep(self.config.keepalive_interval_s)
                except asyncio.CancelledError:
                    raise
                if self._closed or self._rotate_event.is_set():
                    return
                try:
                    await self.rest.keepalive_user_data_stream()
                    logger.debug("user-data-stream keepalive ok")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(
                        "user-data-stream keepalive failed: %s; rotating", e
                    )
                    self._rotate_event.set()
                    return
        except asyncio.CancelledError:
            return

    async def _teardown_session(self, *, delete_listen_key: bool) -> None:
        """Tear down keepalive + transport for the current session.

        ``delete_listen_key=True`` is set only by :meth:`close` so the
        DELETE happens once on shutdown. During mid-supervise rotation
        we let the now-dead key fall away naturally — best-effort
        DELETE on a dead key is wasted, and the new key from the next
        ``start_user_data_stream`` invalidates the old one anyway.
        """
        keepalive = self._keepalive_task
        self._keepalive_task = None
        if keepalive is not None:
            keepalive.cancel()
            try:
                await keepalive
            except (asyncio.CancelledError, Exception):
                pass

        transport = self._transport
        self._transport = None
        if transport is not None:
            await transport.close()

        self._connected.clear()

        if delete_listen_key and self._current_listen_key is not None:
            try:
                await self.rest.close_user_data_stream()
            except Exception as e:
                logger.warning(
                    "user-data-stream DELETE listenKey failed: %s", e
                )
        # Reset for the next session. _current_listen_key stays set so
        # callers can still inspect the most-recent key after close().

    def _build_url(self, listen_key: str) -> str:
        base = self.config.stream_base_url.rstrip("/")
        return f"{base}/ws/{listen_key}"

    async def _dispatch(self, frame: dict[str, Any]) -> None:
        """Route an event frame to its handler by ``e`` field.

        ``listenKeyExpired`` is intercepted as a lifecycle signal —
        user handlers don't see it. All other events pass through.
        """
        event_type = frame.get("e")
        if not isinstance(event_type, str):
            logger.debug(
                "user-data-stream dropped frame without 'e' field: %r", frame
            )
            return
        if event_type == LISTEN_KEY_EXPIRED_EVENT:
            logger.info(
                "user-data-stream listenKeyExpired received; rotating"
            )
            # Reset rotate_attempts because this is the *expected* path,
            # not a failure — we don't want to back off as if we've been
            # losing connections.
            self._rotate_attempts = 0
            self._rotate_event.set()
            return
        handler = self._handlers_normalised.get(event_type)
        if handler is None:
            logger.debug(
                "user-data-stream no handler for e=%s (registered=%s)",
                event_type,
                sorted(self._handlers_normalised.keys()),
            )
            return
        await handler(frame)

    def _compute_rotate_backoff(self, attempt: int) -> float:
        base = min(
            self.config.rotate_backoff_base_s * (2 ** (attempt - 1)),
            self.config.rotate_backoff_max_s,
        )
        jitter = self.config.rotate_backoff_jitter
        if jitter > 0:
            base *= 1.0 + random.uniform(-jitter, jitter)
        return max(0.0, base)


def _mask_key(key: str) -> str:
    """Return a short, log-safe prefix of the listenKey."""
    if not key:
        return ""
    return key[:6]


__all__ = [
    "DEFAULT_KEEPALIVE_INTERVAL_S",
    "LISTEN_KEY_EXPIRED_EVENT",
    "EventHandler",
    "UserDataStreamClient",
    "UserDataStreamClosed",
    "UserDataStreamConfig",
    "UserDataStreamError",
]
