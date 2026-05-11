"""Translate UTA event-bus payloads into heatmap-sdk's existing
browser-WebSocket JSON shape.

Why a translator rather than direct subscription
------------------------------------------------

heatmap-sdk's frontend already speaks a small dialect of JSON over its
``/ws`` endpoint: ``{"type": "order_status", ...}``,
``{"type": "position", ...}``, etc. We translate UTA events to that
shape so the browser code does not have to change — only the *source*
of those messages does (from heatmap-sdk's own ``OrderExecutor`` and
position-poll loop to UTA's event bus).

This module is intentionally I/O-free. It takes a UTA event dataclass
and returns a ``dict`` ready to be passed to
``WebSocket.send_json(...)``. The wiring that actually subscribes to
the bus and forwards messages lives in :class:`HeatmapEventBroadcaster`,
which is also kept small so a consumer can inline it or replace it.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from trade_adapter.bus.event_bus import Subscription
from trade_adapter.embedded import TradeAdapter
from trade_adapter.types import (
    AlertEvent,
    EventType,
    Fill,
    OrderUpdate,
    OutcomeReport,
    PositionUpdate,
)

_log = logging.getLogger(__name__)

WsSender = Callable[[dict[str, Any]], Awaitable[None]]
"""Async callable a consumer hands in to receive translated payloads.

In heatmap-sdk this is a thin wrapper around ``LiveHeatmapService.broadcast``
so the message reaches every connected browser. In tests it can be a
simple ``async def sink(payload): captured.append(payload)``.
"""


def order_update_to_payload(event: OrderUpdate) -> dict[str, Any]:
    """heatmap-sdk-compatible ``order_status`` payload from an
    :class:`OrderUpdate`.

    Mirrors the existing frontend contract (see ``ws_session.py`` /
    ``_evaluate_exit``: ``{"type": "order_status", "status": ...,
    "clientOrderId": ...}``) and adds the symbol + filled qty so the
    UI can render entry fills (not just close fills).
    """

    return {
        "type": "order_status",
        "status": event.status.value,
        "clientOrderId": event.client_order_id,
        "exchangeOrderId": event.exchange_order_id,
        "symbol": event.symbol,
        "filled_qty": event.filled_qty,
        "avg_fill_price": event.avg_fill_price,
        "signal_id": event.signal_id,
        "correlation_id": event.correlation_id,
        "rejection_reason": event.rejection_reason,
        "ts": event.ts,
        "venue": event.venue.value,
    }


def fill_to_payload(event: Fill) -> dict[str, Any]:
    """heatmap-sdk-compatible ``fill`` payload.

    Not used by the current heatmap-sdk frontend, but trivial to ship
    so an updated UI can render per-fill prints next to its own
    trades panel.
    """

    return {
        "type": "fill",
        "fill_id": event.fill_id,
        "clientOrderId": event.client_order_id,
        "exchangeOrderId": event.exchange_order_id,
        "symbol": event.symbol,
        "side": event.side.value,
        "qty": event.qty,
        "price": event.price,
        "fee_usd": event.fee_usd,
        "is_maker": event.is_maker,
        "ts": event.ts,
        "venue": event.venue.value,
        "signal_id": event.signal_id,
        "correlation_id": event.correlation_id,
    }


def position_update_to_payload(event: PositionUpdate) -> dict[str, Any]:
    """heatmap-sdk-compatible ``position`` payload from a
    :class:`PositionUpdate`.

    Field names match heatmap-sdk's current
    ``_broadcast_assistant_snapshot`` output so the frontend renders
    unchanged. ``side`` is derived from ``direction`` (heatmap-sdk's
    ``PositionState.side`` returns ``"LONG"`` / ``"SHORT"`` / ``"FLAT"``).
    """

    side = event.direction.value if event.qty != 0.0 else "FLAT"
    return {
        "type": "position",
        "symbol": event.symbol,
        "side": side,
        "quantity": event.qty,
        "entry_price": event.entry_price,
        "unrealized_pnl": event.unrealized_pnl_usd,
        "opened_at_ms": int(event.ts * 1000.0) if event.ts else None,
        "state": event.state.value,
        "venue": event.venue.value,
        "signal_id": event.signal_id,
        "correlation_id": event.correlation_id,
    }


def outcome_to_payload(event: OutcomeReport) -> dict[str, Any]:
    """heatmap-sdk-compatible ``outcome`` payload for the
    after-trade summary."""

    return {
        "type": "outcome",
        "signal_id": event.signal_id,
        "symbol": event.symbol,
        "direction": event.direction.value,
        "entry_price": event.entry_price,
        "exit_price": event.exit_price,
        "qty": event.qty,
        "realized_pnl_usd": event.realized_pnl_usd,
        "fees_usd": event.fees_usd,
        "slippage_bps": event.slippage_bps,
        "holding_time_s": event.holding_time_s,
        "mfe_bps": event.mfe_bps,
        "mae_bps": event.mae_bps,
        "close_reason": event.close_reason.value,
        "opened_at": event.opened_at,
        "closed_at": event.closed_at,
        "venue": event.venue.value,
        "correlation_id": event.correlation_id,
    }


def alert_to_payload(event: AlertEvent) -> dict[str, Any]:
    """heatmap-sdk-compatible ``alert`` payload."""

    return {
        "type": "alert",
        "severity": event.severity,
        "code": event.code,
        "message": event.message,
        "ts": event.ts,
        "venue": event.venue.value if event.venue is not None else None,
        "symbol": event.symbol,
    }


def to_ws_payload(event: Any) -> dict[str, Any] | None:
    """Dispatch ``event`` to the right per-type translator.

    Returns ``None`` for event types we deliberately don't surface to
    the UI (e.g. raw ``BookUpdate`` / ``TradePrint`` — those go through
    a different channel in heatmap-sdk).
    """

    if isinstance(event, OrderUpdate):
        return order_update_to_payload(event)
    if isinstance(event, Fill):
        return fill_to_payload(event)
    if isinstance(event, PositionUpdate):
        return position_update_to_payload(event)
    if isinstance(event, OutcomeReport):
        return outcome_to_payload(event)
    if isinstance(event, AlertEvent):
        return alert_to_payload(event)
    return None


_DEFAULT_TOPICS: tuple[EventType, ...] = (
    EventType.ORDER_UPDATE,
    EventType.FILL,
    EventType.POSITION_UPDATE,
    EventType.OUTCOME_REPORT,
    EventType.ALERT,
)


class HeatmapEventBroadcaster:
    """Subscribe to UTA's bus, translate, fan out to the browser.

    Usage::

        broadcaster = HeatmapEventBroadcaster(
            adapter=trade_adapter,
            send=live_heatmap_service.broadcast,
        )
        broadcaster.start()
        ...
        await broadcaster.stop()

    The broadcaster owns one ``asyncio.Task`` per topic so a slow
    consumer of one topic cannot stall the others. Each task is a
    plain ``async for event in subscription`` loop; UTA's bus already
    enforces drop-oldest backpressure (see :class:`EventBus`), so the
    broadcaster does not need its own queue.
    """

    def __init__(
        self,
        *,
        adapter: TradeAdapter,
        send: WsSender,
        topics: tuple[EventType, ...] = _DEFAULT_TOPICS,
        queue_size: int = 256,
    ) -> None:
        self._adapter = adapter
        self._send = send
        self._topics = topics
        self._queue_size = queue_size
        self._subscriptions: list[Subscription] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Subscribe to every configured topic and start fan-out."""

        if self._running:
            return
        for topic in self._topics:
            sub = self._adapter.subscribe(topic, queue_size=self._queue_size)
            self._subscriptions.append(sub)
            task = asyncio.create_task(
                self._forward(sub),
                name=f"heatmap-broadcast-{topic.value}",
            )
            self._tasks.append(task)
        self._running = True

    async def stop(self) -> None:
        """Close every subscription and await every fan-out task."""

        if not self._running:
            return
        self._running = False
        for sub in self._subscriptions:
            await sub.close()
        for task in self._tasks:
            if not task.done():
                task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception) as e:
                if not isinstance(e, asyncio.CancelledError):
                    _log.warning(
                        "broadcaster task %s errored: %s", task.get_name(), e
                    )
        self._subscriptions.clear()
        self._tasks.clear()

    async def _forward(self, subscription: Subscription) -> None:
        async for event in subscription:
            payload = to_ws_payload(event)
            if payload is None:
                continue
            try:
                await self._send(payload)
            except Exception as e:
                # We never want one failed send to kill the whole
                # forward loop; heatmap-sdk's own ``broadcast`` already
                # eats per-socket failures. Log and keep going.
                _log.warning(
                    "broadcaster send error on topic=%s: %s",
                    subscription.topic,
                    e,
                )


__all__ = [
    "HeatmapEventBroadcaster",
    "WsSender",
    "alert_to_payload",
    "fill_to_payload",
    "order_update_to_payload",
    "outcome_to_payload",
    "position_update_to_payload",
    "to_ws_payload",
]
