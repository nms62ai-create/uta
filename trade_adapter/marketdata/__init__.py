"""Coalescing market-data layer (A.1).

Exposes ``subscribe_book`` / ``subscribe_trades`` / ``subscribe_bbo`` to
external consumers via :class:`TradeAdapter`. Maintains at most one
upstream WebSocket subscription per ``(venue, symbol, kind)`` regardless
of how many local subscribers exist — covered by spec prohibition
``C.11`` and decision ``A16``.

Layout:

* :class:`MarketDataStreamProvider` — venue-side Protocol the hub
  consumes. Each venue implements one (Binance USD-M, Bybit Linear …).
* :class:`MarketDataHub` — venue-neutral fan-out + refcounted upstream
  lifecycle.
* :class:`MarketDataSubscription` — per-consumer view, mirrors the
  :class:`~trade_adapter.bus.event_bus.Subscription` shape so the
  async-iterator contract is identical.
* :class:`BboTracker` — auto-subscribes to BBO for every open position
  so A.2 (``OutcomeReport`` MFE/MAE) has the tick stream it needs.
"""

from __future__ import annotations

from .bbo_tracker import BboTracker, PositionBboSnapshot
from .hub import MarketDataHub, MarketDataSubscription
from .types import MarketDataStreamProvider, StreamKind

__all__ = [
    "BboTracker",
    "MarketDataHub",
    "MarketDataStreamProvider",
    "MarketDataSubscription",
    "PositionBboSnapshot",
    "StreamKind",
]
