"""Tests for :class:`HeatmapEventBroadcaster` and the per-event
translators that produce heatmap-sdk-compatible WS payloads.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.integrations.heatmap_sdk.event_broadcaster import (
    HeatmapEventBroadcaster,
    alert_to_payload,
    fill_to_payload,
    order_update_to_payload,
    outcome_to_payload,
    position_update_to_payload,
    to_ws_payload,
)
from trade_adapter.types import (
    AlertEvent,
    CloseReason,
    Direction,
    EventType,
    Fill,
    OrderSide,
    OrderStatus,
    OrderUpdate,
    OutcomeReport,
    PositionState,
    PositionUpdate,
    Venue,
)

# ---------------------------------------------------------------------------
# Per-event translators
# ---------------------------------------------------------------------------


def _order_update() -> OrderUpdate:
    return OrderUpdate(
        client_order_id="hm-1",
        exchange_order_id="ex-1",
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
        status=OrderStatus.FILLED,
        filled_qty=0.001,
        avg_fill_price=50_000.0,
        ts=1_700_000_000.0,
        signal_id="sig-1",
        correlation_id="sig-1",
    )


def _fill() -> Fill:
    return Fill(
        fill_id="f-1",
        client_order_id="hm-1",
        exchange_order_id="ex-1",
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        qty=0.001,
        price=50_000.0,
        fee_usd=0.05,
        is_maker=False,
        ts=1_700_000_000.0,
        signal_id="sig-1",
        correlation_id="sig-1",
    )


def _position_update(*, qty: float = 0.001) -> PositionUpdate:
    return PositionUpdate(
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        qty=qty,
        entry_price=50_000.0,
        state=PositionState.OPEN if qty != 0.0 else PositionState.IDLE,
        liquidation_price=None,
        unrealized_pnl_usd=12.5,
        margin_used_usd=None,
        ts=1_700_000_000.0,
        signal_id="sig-1",
        correlation_id="sig-1",
    )


def _outcome() -> OutcomeReport:
    return OutcomeReport(
        signal_id="sig-1",
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        entry_price=50_000.0,
        exit_price=50_500.0,
        qty=0.001,
        realized_pnl_usd=0.5,
        fees_usd=0.10,
        slippage_bps=1.0,
        holding_time_s=42.0,
        mfe_bps=12.0,
        mae_bps=4.0,
        close_reason=CloseReason.TP,
        opened_at=1_700_000_000.0,
        closed_at=1_700_000_042.0,
        correlation_id="sig-1",
    )


def _alert() -> AlertEvent:
    return AlertEvent(
        severity="warning",
        code="RATE_LIMIT",
        message="ws-trade rate-limited",
        ts=1_700_000_000.0,
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
    )


def test_order_update_payload_contract() -> None:
    payload = order_update_to_payload(_order_update())

    assert payload["type"] == "order_status"
    assert payload["status"] == "FILLED"
    assert payload["clientOrderId"] == "hm-1"
    assert payload["exchangeOrderId"] == "ex-1"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["filled_qty"] == 0.001
    assert payload["avg_fill_price"] == 50_000.0
    assert payload["signal_id"] == "sig-1"
    assert payload["correlation_id"] == "sig-1"
    assert payload["venue"] == "binance_um"


def test_fill_payload_contract() -> None:
    payload = fill_to_payload(_fill())

    assert payload["type"] == "fill"
    assert payload["fill_id"] == "f-1"
    assert payload["side"] == "BUY"
    assert payload["qty"] == 0.001
    assert payload["price"] == 50_000.0
    assert payload["fee_usd"] == 0.05
    assert payload["is_maker"] is False


def test_position_update_payload_open_side() -> None:
    payload = position_update_to_payload(_position_update(qty=0.001))

    assert payload["type"] == "position"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["side"] == "LONG"
    assert payload["quantity"] == 0.001
    assert payload["entry_price"] == 50_000.0
    assert payload["unrealized_pnl"] == 12.5
    assert payload["state"] == "OPEN"
    assert payload["venue"] == "binance_um"


def test_position_update_payload_flat_side_when_qty_zero() -> None:
    payload = position_update_to_payload(_position_update(qty=0.0))

    assert payload["side"] == "FLAT"
    assert payload["quantity"] == 0.0


def test_outcome_payload_contract() -> None:
    payload = outcome_to_payload(_outcome())

    assert payload["type"] == "outcome"
    assert payload["signal_id"] == "sig-1"
    assert payload["direction"] == "LONG"
    assert payload["entry_price"] == 50_000.0
    assert payload["exit_price"] == 50_500.0
    assert payload["realized_pnl_usd"] == 0.5
    assert payload["close_reason"] == "tp"
    assert payload["venue"] == "binance_um"


def test_alert_payload_contract() -> None:
    payload = alert_to_payload(_alert())

    assert payload["type"] == "alert"
    assert payload["severity"] == "warning"
    assert payload["code"] == "RATE_LIMIT"
    assert payload["message"] == "ws-trade rate-limited"
    assert payload["venue"] == "binance_um"
    assert payload["symbol"] == "BTCUSDT"


def test_alert_payload_handles_missing_venue() -> None:
    alert = AlertEvent(
        severity="info",
        code="CLOCK_DRIFT",
        message="local clock drifted",
        ts=1_700_000_000.0,
    )

    payload = alert_to_payload(alert)

    assert payload["venue"] is None
    assert payload["symbol"] is None


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def test_dispatcher_handles_known_types() -> None:
    assert to_ws_payload(_order_update())["type"] == "order_status"
    assert to_ws_payload(_fill())["type"] == "fill"
    assert to_ws_payload(_position_update())["type"] == "position"
    assert to_ws_payload(_outcome())["type"] == "outcome"
    assert to_ws_payload(_alert())["type"] == "alert"


def test_dispatcher_returns_none_for_unknown_type() -> None:
    assert to_ws_payload({"not": "an event"}) is None
    assert to_ws_payload(object()) is None


# ---------------------------------------------------------------------------
# Live broadcaster
# ---------------------------------------------------------------------------


class _StubAdapter:
    """Minimal stand-in for :class:`TradeAdapter` exposing only what
    the broadcaster touches: ``subscribe(EventType, queue_size=...)``."""

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus

    def subscribe(self, event_type: EventType, *, queue_size: int = 256):
        return self._bus.subscribe(event_type.value, queue_size=queue_size)


async def _drain(captured: list[Any], expected: int, timeout_s: float = 1.0) -> None:
    """Wait until ``captured`` holds at least ``expected`` items."""

    deadline = asyncio.get_event_loop().time() + timeout_s
    while len(captured) < expected:
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"timed out waiting for {expected} broadcasts; have {len(captured)}"
            )
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_broadcaster_forwards_order_update_to_send() -> None:
    bus = EventBus()
    captured: list[dict[str, Any]] = []

    async def send(payload: dict[str, Any]) -> None:
        captured.append(payload)

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    broadcaster.start()
    assert broadcaster.is_running is True

    try:
        bus.publish(EventType.ORDER_UPDATE.value, _order_update())
        await _drain(captured, 1)
    finally:
        await broadcaster.stop()

    assert captured[0]["type"] == "order_status"
    assert broadcaster.is_running is False


@pytest.mark.asyncio
async def test_broadcaster_forwards_position_update() -> None:
    bus = EventBus()
    captured: list[dict[str, Any]] = []

    async def send(payload: dict[str, Any]) -> None:
        captured.append(payload)

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    broadcaster.start()
    try:
        bus.publish(EventType.POSITION_UPDATE.value, _position_update())
        await _drain(captured, 1)
    finally:
        await broadcaster.stop()

    assert captured[0]["type"] == "position"
    assert captured[0]["side"] == "LONG"


@pytest.mark.asyncio
async def test_broadcaster_send_errors_do_not_stop_loop() -> None:
    bus = EventBus()
    captured: list[dict[str, Any]] = []
    raised = 0

    async def send(payload: dict[str, Any]) -> None:
        nonlocal raised
        # First call raises, subsequent calls succeed.
        if raised == 0:
            raised += 1
            raise RuntimeError("simulated socket error")
        captured.append(payload)

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    broadcaster.start()
    try:
        bus.publish(EventType.ORDER_UPDATE.value, _order_update())
        bus.publish(EventType.ORDER_UPDATE.value, _order_update())
        await _drain(captured, 1)
    finally:
        await broadcaster.stop()

    # First publish raised inside ``send`` and was swallowed.
    # Second publish made it through.
    assert raised == 1
    assert len(captured) >= 1


@pytest.mark.asyncio
async def test_broadcaster_double_start_is_idempotent() -> None:
    bus = EventBus()

    async def send(payload: dict[str, Any]) -> None:
        return None

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    broadcaster.start()
    n_subs_after_first_start = bus.subscriber_count(EventType.ORDER_UPDATE.value)
    broadcaster.start()
    n_subs_after_second_start = bus.subscriber_count(EventType.ORDER_UPDATE.value)
    await broadcaster.stop()

    assert n_subs_after_first_start == n_subs_after_second_start
    assert bus.subscriber_count(EventType.ORDER_UPDATE.value) == 0


@pytest.mark.asyncio
async def test_broadcaster_stop_without_start_is_safe() -> None:
    bus = EventBus()

    async def send(payload: dict[str, Any]) -> None:
        return None

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    # Must not raise.
    await broadcaster.stop()
    assert broadcaster.is_running is False


@pytest.mark.asyncio
async def test_broadcaster_subscribes_to_default_topics() -> None:
    bus = EventBus()

    async def send(payload: dict[str, Any]) -> None:
        return None

    broadcaster = HeatmapEventBroadcaster(
        adapter=_StubAdapter(bus),  # type: ignore[arg-type]
        send=send,
    )
    broadcaster.start()
    try:
        for topic in (
            EventType.ORDER_UPDATE,
            EventType.FILL,
            EventType.POSITION_UPDATE,
            EventType.OUTCOME_REPORT,
            EventType.ALERT,
        ):
            assert bus.subscriber_count(topic.value) == 1
    finally:
        await broadcaster.stop()
