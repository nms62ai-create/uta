"""heatmap-sdk producer integration.

Reference bridge that lets heatmap-sdk (or any AdaptiveAnalyticsSDK
consumer) drive UTA's embedded :class:`TradeAdapter` as its order
execution backend.

The bridge is intentionally split into three independent pieces so a
consumer can pick only what it needs:

* :class:`AutotradeSettings` + :func:`build_universal_signal` —
  pure-function translator from an ``adaptive_sdk.Signal``-shaped input
  to UTA's :class:`UniversalSignal`. No state, no I/O.
* :class:`HeatmapAutoTrader` — UI-controlled lifecycle around the
  translator. Holds the "current autotrade settings" (symbol, notional,
  SL%, TP%, min-confidence), an enable flag, and a single entry point
  ``on_adaptive_signal(signal)`` that the consumer calls from inside
  its existing per-trade callback.
* :class:`HeatmapEventBroadcaster` — translates UTA event-bus payloads
  into the JSON shape heatmap-sdk already broadcasts over its browser
  WebSocket (``{"type": "order_status", ...}`` etc.). Drop-in
  replacement for the manual broadcasts heatmap-sdk currently issues
  from ``LiveHeatmapService._evaluate_exit`` / position polling.

The producer does not need to install UTA's adaptive_sdk; the bridge
uses structural (Protocol) typing for its inputs so it is import-cycle-
free in either direction.

See ``INTEGRATION.md`` in this package for the exact wiring recipe.
"""

from .autotrader import (
    AutotradeSettings,
    HeatmapAutoTrader,
    InvalidAutotradeSettings,
)
from .event_broadcaster import HeatmapEventBroadcaster
from .translator import (
    AdaptiveSignal,
    build_universal_signal,
    direction_from_exhaustion,
)

__all__ = [
    "AdaptiveSignal",
    "AutotradeSettings",
    "HeatmapAutoTrader",
    "HeatmapEventBroadcaster",
    "InvalidAutotradeSettings",
    "build_universal_signal",
    "direction_from_exhaustion",
]
