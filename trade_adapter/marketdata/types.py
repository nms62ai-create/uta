"""Protocols and enums for the market-data passthrough layer."""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from ..types import Venue


class StreamKind(StrEnum):
    """The three market-data stream kinds A16 promises to passthrough."""

    BOOK = "book"
    TRADES = "trades"
    BBO = "bbo"


# Callback type: the hub hands a "deliver this event to all current
# subscribers of (venue, symbol, kind)" closure to the provider.
StreamCallback = Callable[[Any], None]


@runtime_checkable
class MarketDataStreamProvider(Protocol):
    """Venue-side surface that :class:`MarketDataHub` consumes.

    One instance per venue (Binance USD-M, Bybit Linear, …). The
    provider owns the upstream WS connection and routes frames to the
    callback registered for each ``(symbol, kind)``.

    Implementations MUST be idempotent on ``start`` / ``close`` and
    MUST be safe to call ``subscribe_stream`` / ``unsubscribe_stream``
    concurrently from the hub's event loop — the hub serialises per
    ``(venue, symbol, kind)`` but may interleave across different
    keys.
    """

    venue: Venue

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def subscribe_stream(
        self,
        symbol: str,
        kind: StreamKind,
        callback: StreamCallback,
    ) -> None:
        """Begin pushing typed events for ``(symbol, kind)`` to ``callback``.

        The provider is responsible for the wire-level decoding (raw
        Binance frame → :class:`BookUpdate` / :class:`TradePrint` /
        :class:`BBOUpdate`). Exceptions raised here propagate to the
        hub caller; the hub leaves no refcount in place if the
        upstream subscribe failed.
        """
        ...

    async def unsubscribe_stream(self, symbol: str, kind: StreamKind) -> None:
        """Tear down the upstream subscription for ``(symbol, kind)``.

        Tolerant of double-unsubscribe (just no-op) — that lets the
        hub call this from close() unconditionally.
        """
        ...


__all__ = [
    "MarketDataStreamProvider",
    "StreamCallback",
    "StreamKind",
]
