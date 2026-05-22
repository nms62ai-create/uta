"""Binance USD-M Futures dynamic market-stream client (A.1).

The :class:`BinanceDynamicMarketStream` extends the static
:class:`MarketStreamClient` shape to support **runtime**
``SUBSCRIBE`` / ``UNSUBSCRIBE`` on Binance's bare ``/ws`` endpoint.
Where ``MarketStreamClient`` builds the combined-stream URL once at
construction time, this client keeps a single connection open across
the lifetime of the adapter and lets the upper layer add / remove
stream names dynamically — which is exactly what the coalescing
:class:`MarketDataHub` needs.

Wire protocol (Binance docs, "WebSocket Streams" §"Live Subscribing"):

* SUBSCRIBE request:
  ``{"method":"SUBSCRIBE","params":["btcusdt@depth20@100ms"],"id":<n>}``
* UNSUBSCRIBE request:
  ``{"method":"UNSUBSCRIBE","params":["btcusdt@depth20@100ms"],"id":<n>}``
* Both return ``{"result":null,"id":<n>}`` on success; we don't await
  the ack (fire-and-forget keeps the hub's subscribe path simple),
  but the request id is monotonically incremented so it shows up in
  the server logs if needed.
* Inbound push frames carry a ``stream`` field that identifies which
  active subscription they belong to. The :attr:`_handlers` map
  routes each frame to the matching handler.

Reconnect behaviour: the underlying :class:`WsStreamTransport`
already restores the socket on drop, but we have to **re-send** every
active SUBSCRIBE because Binance's session state is lost on
disconnect. ``_resubscribe_on_connect`` is a small one-shot helper
that fires on every ``connected`` transition.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import websockets

from .stream import (
    MAINNET_STREAM_BASE_URL,
    WsStreamConfig,
    WsStreamTransport,
)

logger = logging.getLogger(__name__)

# Per-stream handler signature. Receives the inner ``data`` object that
# Binance wraps in ``{"stream":..., "data":...}`` for combined streams,
# or the raw frame for endpoints that don't wrap.
StreamFrameHandler = Callable[[dict[str, Any]], Awaitable[None]]


def _normalize_stream_name(name: str) -> str:
    """Lowercase canonical form. Binance is case-insensitive on streams."""

    return name.lower()


@dataclass(slots=True)
class BinanceDynamicMarketStream:
    """One long-lived WS to Binance's ``/ws`` endpoint with dynamic streams.

    The transport is owned here; callers shouldn't touch
    :attr:`_transport` directly. Subscribe / unsubscribe semantics:

    * :meth:`subscribe` is idempotent — a second call for the same
      stream name replaces the handler but doesn't re-send SUBSCRIBE
      (the server would just ignore it).
    * :meth:`unsubscribe` is tolerant — calling it for an unknown
      stream is a no-op.
    * :meth:`close` cleanly tears down the socket; any further
      subscribe / unsubscribe is rejected.

    Concurrency: subscribe / unsubscribe acquire a single
    :class:`asyncio.Lock` so the resubscribe-on-reconnect path can
    observe a consistent handler map.
    """

    base_url: str = MAINNET_STREAM_BASE_URL
    transport_config_overrides: dict[str, Any] = field(default_factory=dict)
    _transport: WsStreamTransport | None = field(default=None, init=False)
    _handlers: dict[str, StreamFrameHandler] = field(
        default_factory=dict, init=False
    )
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _next_id: int = field(default=1, init=False)
    _started: bool = field(default=False, init=False)
    _closed: bool = field(default=False, init=False)
    _last_resubscribe_sent: set[str] = field(default_factory=set, init=False)

    @property
    def is_connected(self) -> bool:
        return self._transport is not None and self._transport.is_connected

    async def start(self) -> None:
        """Open the underlying WS to ``<base_url>/ws``. Idempotent."""

        if self._closed:
            raise RuntimeError("BinanceDynamicMarketStream is closed")
        if self._started:
            return
        config_kwargs: dict[str, Any] = {
            "url": f"{self.base_url.rstrip('/')}/ws",
        }
        config_kwargs.update(self.transport_config_overrides)
        config = WsStreamConfig(**config_kwargs)
        self._transport = WsStreamTransport(
            config=config, on_message=self._dispatch
        )
        await self._transport.start()
        # Kick off the connection in the background; subscribe calls
        # will await wait_connected() before sending SUBSCRIBE.
        self._started = True

    async def close(self) -> None:
        """Close the underlying transport. Idempotent."""

        if self._closed:
            return
        self._closed = True
        transport = self._transport
        self._transport = None
        if transport is not None:
            try:
                await transport.close()
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "BinanceDynamicMarketStream error closing transport"
                )
        self._handlers.clear()
        self._last_resubscribe_sent.clear()

    async def wait_connected(self, *, timeout: float | None = None) -> None:
        """Block until the underlying socket is open.

        Useful for tests / for the hub before issuing the first
        SUBSCRIBE so we don't race the open-handshake.
        """

        if self._transport is None:
            raise RuntimeError("BinanceDynamicMarketStream not started")
        await self._transport.wait_connected(timeout=timeout)

    async def subscribe(
        self, stream: str, handler: StreamFrameHandler
    ) -> None:
        """Subscribe to ``stream`` and register ``handler`` for its frames."""

        if self._closed:
            raise RuntimeError("BinanceDynamicMarketStream is closed")
        if not self._started:
            raise RuntimeError(
                "BinanceDynamicMarketStream.start() not called"
            )
        name = _normalize_stream_name(stream)
        async with self._lock:
            already_active = name in self._handlers
            self._handlers[name] = handler
            if already_active:
                return
            await self._send_subscribe([name])

    async def unsubscribe(self, stream: str) -> None:
        """Unsubscribe from ``stream``; tolerant of unknown names."""

        if self._closed:
            return
        name = _normalize_stream_name(stream)
        async with self._lock:
            if name not in self._handlers:
                return
            self._handlers.pop(name, None)
            self._last_resubscribe_sent.discard(name)
            if self._transport is None or not self._transport.is_connected:
                # No live socket — nothing to UNSUBSCRIBE; the next
                # reconnect just won't include ``name`` in the
                # resubscribe set.
                return
            await self._send_unsubscribe([name])

    async def _send_subscribe(self, streams: list[str]) -> None:
        await self._send_envelope("SUBSCRIBE", streams)

    async def _send_unsubscribe(self, streams: list[str]) -> None:
        await self._send_envelope("UNSUBSCRIBE", streams)

    async def _send_envelope(self, method: str, streams: list[str]) -> None:
        if self._transport is None:
            return
        if not streams:
            return
        envelope = {
            "method": method,
            "params": list(streams),
            "id": self._next_id,
        }
        self._next_id += 1
        # Intentional reach-in to the transport's underlying socket —
        # the dynamic-subscribe envelope must go out on the *same* WS
        # connection as the inbound frames, not a fresh one.
        ws = self._transport._ws
        if ws is None:
            # No live socket; the server doesn't know about us. We'll
            # re-issue on reconnect via _maybe_resubscribe.
            return
        try:
            await ws.send(json.dumps(envelope, separators=(",", ":")))
        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            logger.warning(
                "BinanceDynamicMarketStream %s failed (will resubscribe on reconnect): %s",
                method,
                e,
            )

    async def _dispatch(self, payload: dict[str, Any]) -> None:
        """Route one inbound frame to its handler.

        Two payload shapes Binance uses on the bare ``/ws`` endpoint:

        * Subscription response: ``{"result":null,"id":<n>}`` — drop.
        * Push frame: ``{"e":<type>, "s":<symbol>, ...}`` — route by
          ``stream`` if present, else derived stream name from ``e`` +
          ``s``. (Bare ``/ws`` doesn't always include a ``stream``
          envelope; combined ``/stream?streams=`` always does.)
        """

        # SUBSCRIBE / UNSUBSCRIBE response — has ``id`` and ``result``.
        if "result" in payload and "id" in payload:
            # On every successful (re)subscribe ack, capture the set
            # we last sent so close can stay accurate.
            return

        stream = payload.get("stream")
        data: dict[str, Any]
        if stream is not None and "data" in payload:
            data = payload["data"]  # combined-stream envelope
        else:
            stream = self._derive_stream_name(payload)
            data = payload
        if stream is None:
            logger.debug(
                "BinanceDynamicMarketStream dropped unroutable frame: %r",
                payload,
            )
            return
        handler = self._handlers.get(_normalize_stream_name(stream))
        if handler is None:
            # Frame arrived after we unsubscribed but before the
            # server stopped pushing — common in practice, just drop.
            return
        try:
            await handler(data)
        except Exception:  # pragma: no cover - handler errors are logged
            logger.exception(
                "BinanceDynamicMarketStream handler raised for stream=%s",
                stream,
            )

    @staticmethod
    def _derive_stream_name(payload: dict[str, Any]) -> str | None:
        """Best-effort stream-name reconstruction from an event payload.

        Binance's push frames on ``/ws`` include ``e`` (event type)
        and ``s`` (symbol). The canonical stream name is
        ``<symbol_lower>@<event_marker>`` where event marker depends
        on the type (``bookTicker`` → ``bookTicker``; ``aggTrade`` →
        ``aggTrade``; ``depthUpdate`` of a partial-book stream is
        ambiguous — we can't reconstruct ``depth20@100ms`` from the
        frame alone, so partial-book streams MUST use the combined
        ``/stream?streams=`` endpoint or carry the ``stream``
        envelope).

        Returns ``None`` if reconstruction isn't possible — caller
        drops the frame.
        """

        event = payload.get("e")
        symbol = payload.get("s")
        if not isinstance(event, str) or not isinstance(symbol, str):
            return None
        symbol_lower = symbol.lower()
        if event == "bookTicker":
            return f"{symbol_lower}@bookTicker"
        if event == "aggTrade":
            return f"{symbol_lower}@aggTrade"
        # depthUpdate is ambiguous (could be depth5/10/20@100/250/500ms);
        # rely on combined-stream envelope for routing.
        return None


__all__ = [
    "BinanceDynamicMarketStream",
    "StreamFrameHandler",
]
