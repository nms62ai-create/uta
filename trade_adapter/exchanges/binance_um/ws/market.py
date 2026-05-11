"""Binance USD-M Futures combined market-data stream client.

Sits on top of :class:`.stream.WsStreamTransport`. Handles the two
combined-stream-specific concerns:

1.  **URL construction.** Binance's combined-stream endpoint expects
    ``wss://fstream.binance.com/stream?streams=name1/name2/name3``. We
    build that URL once at construction time from the requested stream
    set; no runtime ``SUBSCRIBE`` messages are needed.
2.  **Frame routing.** Each frame is shaped
    ``{"stream": "btcusdt@aggTrade", "data": {...}}``. We dispatch the
    inner ``data`` dict to a per-stream handler keyed by stream name,
    case-insensitive.

This client is the lowest layer that knows the wire shape of Binance
streams. It does **not** translate ``data`` into UTA event types
(``BookUpdate`` / ``TradePrint`` / ``BBOUpdate``) — that translation
belongs to the venue adapter in Phase 3, where the per-stream callback
will read raw Binance fields and emit the canonical UTA event onto the
event bus.

What this client does NOT do:

* Add or remove streams at runtime. v1 ships a fixed stream set per
  client instance. To change the stream set, ``close()`` and create a
  new instance.
* User data (``USER_DATA_STREAM`` listenKey lifecycle, ``ORDER_TRADE_UPDATE``
  / ``ACCOUNT_UPDATE``). That's Phase 2c-3, which reuses
  :class:`WsStreamTransport` but with a different URL pattern and
  routing key (``e`` field instead of ``stream`` field).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from .stream import (
    MAINNET_STREAM_BASE_URL,
    WsStreamConfig,
    WsStreamTransport,
)

logger = logging.getLogger(__name__)

StreamHandler = Callable[[dict[str, Any]], Awaitable[None]]


def _normalize_stream_name(name: str) -> str:
    """Stream names are case-insensitive on Binance — canonicalise to lower."""
    return name.lower()


def build_combined_stream_url(base_url: str, streams: list[str]) -> str:
    """Build a Binance combined-stream URL.

    ``base_url`` is ``wss://fstream.binance.com`` (mainnet) or the
    testnet equivalent. ``streams`` is a list of canonical stream
    names like ``btcusdt@depth20@100ms``. Order is preserved so tests
    and logs can assert on the URL deterministically.

    Raises:
        ValueError: if ``streams`` is empty (Binance rejects it) or
            contains duplicates after lower-casing.
    """
    if not streams:
        raise ValueError("at least one stream is required")
    normalised: list[str] = []
    seen: set[str] = set()
    for s in streams:
        n = _normalize_stream_name(s)
        if n in seen:
            raise ValueError(f"duplicate stream after lower-casing: {s!r}")
        seen.add(n)
        normalised.append(n)
    base = base_url.rstrip("/")
    return f"{base}/stream?streams={'/'.join(normalised)}"


@dataclass(slots=True)
class MarketStreamClient:
    """Combined-stream client for Binance USD-M Futures market data.

    Construct with the set of streams you want and a mapping from stream
    name to async handler. The client owns its underlying
    :class:`WsStreamTransport` (start / close are forwarded).

    Streams not present in the handler map are silently ignored; one
    common pattern is to register a single ``catch_all`` handler under
    every stream name. (We don't add an explicit ``catch_all`` field to
    avoid scope creep — Phase 3's venue adapter wires per-stream
    callbacks anyway.)
    """

    streams: list[str]
    handlers: Mapping[str, StreamHandler]
    base_url: str = MAINNET_STREAM_BASE_URL
    transport_config_overrides: dict[str, Any] = field(default_factory=dict)
    _transport: WsStreamTransport | None = field(default=None, init=False)
    _handlers_normalised: dict[str, StreamHandler] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        if not self.streams:
            raise ValueError("at least one stream is required")
        if not self.handlers:
            raise ValueError("at least one handler is required")
        self._handlers_normalised = {
            _normalize_stream_name(k): v for k, v in self.handlers.items()
        }

    @property
    def is_connected(self) -> bool:
        return self._transport is not None and self._transport.is_connected

    async def start(self) -> None:
        """Spin up the underlying :class:`WsStreamTransport`."""
        if self._transport is not None:
            raise RuntimeError("MarketStreamClient already started")
        url = build_combined_stream_url(self.base_url, self.streams)
        config_kwargs: dict[str, Any] = {"url": url}
        config_kwargs.update(self.transport_config_overrides)
        config = WsStreamConfig(**config_kwargs)
        self._transport = WsStreamTransport(
            config=config, on_message=self._dispatch
        )
        await self._transport.start()

    async def close(self) -> None:
        """Shut down the transport. Idempotent."""
        if self._transport is None:
            return
        await self._transport.close()
        self._transport = None

    async def wait_connected(self, *, timeout: float | None = None) -> None:
        if self._transport is None:
            raise RuntimeError("MarketStreamClient.start() not called")
        await self._transport.wait_connected(timeout=timeout)

    async def _dispatch(self, frame: dict[str, Any]) -> None:
        """Route ``frame['data']`` to the handler for ``frame['stream']``.

        Combined-stream frames are always shaped ``{stream, data}``.
        Direct (single-stream) frames have no envelope — those should
        not appear on this client because we always use the combined
        endpoint, but we tolerate them by dropping with a debug log.
        """
        stream_name = frame.get("stream")
        if not isinstance(stream_name, str):
            logger.debug("market-stream dropped non-combined frame: %r", frame)
            return
        data = frame.get("data")
        if not isinstance(data, dict):
            logger.debug(
                "market-stream dropped frame with non-object data: %r", frame
            )
            return
        handler = self._handlers_normalised.get(_normalize_stream_name(stream_name))
        if handler is None:
            logger.debug(
                "market-stream no handler for stream=%s (registered=%s)",
                stream_name,
                sorted(self._handlers_normalised.keys()),
            )
            return
        await handler(data)


__all__ = [
    "MarketStreamClient",
    "StreamHandler",
    "build_combined_stream_url",
]
