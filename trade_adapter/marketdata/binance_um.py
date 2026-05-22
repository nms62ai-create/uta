"""Binance USD-M Futures :class:`MarketDataStreamProvider` (A.1).

Glue layer that turns the venue-neutral ``(symbol, kind)`` subscription
shape into Binance's per-symbol stream-name vocabulary, and translates
inbound wire frames into the UTA typed events that the hub fans out.

Mapping (``symbol`` is lower-cased on the wire):

* :attr:`StreamKind.BOOK`   → ``<symbol>@depth20@100ms`` (top-20 partial
  book, 100 ms refresh — fastest Binance offers for the partial-book
  feed).
* :attr:`StreamKind.TRADES` → ``<symbol>@aggTrade``.
* :attr:`StreamKind.BBO`    → ``<symbol>@bookTicker``.

We deliberately pick ``depth20@100ms`` for BOOK (not ``depth5``): A22
will require enough levels to compute realistic MFE / MAE bands during
the position's lifetime, and 20 levels at 100 ms is the sweet spot for
the bandwidth budget on a single host. Callers that want different
granularity can swap this map without touching the hub.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..exchanges.binance_um.translators import (
    agg_trade_to_trade_print,
    book_ticker_to_bbo_update,
    depth_snapshot_to_book_update,
)
from ..exchanges.binance_um.ws.market_dynamic import (
    BinanceDynamicMarketStream,
)
from ..exchanges.binance_um.ws.stream import MAINNET_STREAM_BASE_URL
from ..types import Venue
from .types import StreamCallback, StreamKind

_log = logging.getLogger(__name__)


_KIND_TO_SUFFIX: dict[StreamKind, str] = {
    StreamKind.BOOK: "depth20@100ms",
    StreamKind.TRADES: "aggTrade",
    StreamKind.BBO: "bookTicker",
}


def _stream_name(symbol: str, kind: StreamKind) -> str:
    """Build the canonical Binance stream name for ``(symbol, kind)``."""

    return f"{symbol.lower()}@{_KIND_TO_SUFFIX[kind]}"


@dataclass(slots=True)
class BinanceMarketDataStreamProvider:
    """Adapts :class:`BinanceDynamicMarketStream` to the hub Protocol.

    Owns the underlying dynamic stream (one persistent WS to
    ``<base>/ws`` with runtime SUBSCRIBE / UNSUBSCRIBE). The provider
    is responsible for the wire-frame → typed-event translation —
    each handler is a tiny closure that calls into the existing pure
    translators in :mod:`...exchanges.binance_um.translators`.
    """

    base_url: str = MAINNET_STREAM_BASE_URL
    transport_config_overrides: dict[str, Any] = field(default_factory=dict)
    venue: Venue = field(default=Venue.BINANCE_UM, init=False)
    _stream: BinanceDynamicMarketStream | None = field(default=None, init=False)
    _started: bool = field(default=False, init=False)
    _closed: bool = field(default=False, init=False)

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("BinanceMarketDataStreamProvider is closed")
        if self._started:
            return
        self._stream = BinanceDynamicMarketStream(
            base_url=self.base_url,
            transport_config_overrides=self.transport_config_overrides,
        )
        await self._stream.start()
        self._started = True

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        stream = self._stream
        self._stream = None
        if stream is not None:
            try:
                await stream.close()
            except Exception:  # pragma: no cover - defensive
                _log.exception(
                    "BinanceMarketDataStreamProvider error closing stream"
                )

    async def subscribe_stream(
        self,
        symbol: str,
        kind: StreamKind,
        callback: StreamCallback,
    ) -> None:
        if self._closed or self._stream is None:
            raise RuntimeError(
                "BinanceMarketDataStreamProvider not started"
            )
        name = _stream_name(symbol, kind)
        handler = _make_frame_handler(kind, callback)
        await self._stream.subscribe(name, handler)

    async def unsubscribe_stream(self, symbol: str, kind: StreamKind) -> None:
        if self._closed or self._stream is None:
            return
        name = _stream_name(symbol, kind)
        await self._stream.unsubscribe(name)


def _make_frame_handler(
    kind: StreamKind, callback: StreamCallback
) -> Any:
    """Build the wire-frame → typed-event → callback closure for ``kind``."""

    if kind is StreamKind.BOOK:
        async def on_book(frame: dict[str, Any]) -> None:
            try:
                event = depth_snapshot_to_book_update(frame)
            except ValueError as e:
                _log.warning(
                    "BinanceMarketDataStreamProvider book translator error: %s",
                    e,
                )
                return
            callback(event)
        return on_book

    if kind is StreamKind.TRADES:
        async def on_trade(frame: dict[str, Any]) -> None:
            try:
                event = agg_trade_to_trade_print(frame)
            except ValueError as e:
                _log.warning(
                    "BinanceMarketDataStreamProvider trade translator error: %s",
                    e,
                )
                return
            callback(event)
        return on_trade

    if kind is StreamKind.BBO:
        async def on_bbo(frame: dict[str, Any]) -> None:
            try:
                event = book_ticker_to_bbo_update(frame)
            except ValueError as e:
                _log.warning(
                    "BinanceMarketDataStreamProvider bbo translator error: %s",
                    e,
                )
                return
            callback(event)
        return on_bbo

    raise ValueError(f"unsupported stream kind: {kind!r}")


__all__ = ["BinanceMarketDataStreamProvider"]
