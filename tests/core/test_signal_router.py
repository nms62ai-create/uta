"""Tests for :class:`trade_adapter.core.signal_router.SignalRouter`.

The router composes pure-function modules (sizing + stops) with three
provider Protocols (market data, equity, positions) and an
:class:`ExchangeAdapter` Protocol. Each test instantiates the in-memory
fakes below and asserts one slice of the accept-path contract.

Where possible, asserts target the externally observable outputs:

* the :class:`SignalAck` returned to the caller,
* what :class:`OrderRequest` objects landed on the fake adapter,
* what events were published on the event bus,
* what the idempotency cache stored.

This keeps the tests as a wire-format contract on the router rather
than a re-implementation of its internals.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.protocols import NullPositionProvider
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    AbsolutePrice,
    BpsFromEntry,
    Direction,
    EventType,
    FixedQty,
    Intent,
    NotionalUsd,
    OrderAck,
    OrderRequest,
    OrderSide,
    OrderType,
    PctEquity,
    Position,
    PositionState,
    RejectionReason,
    RiskBased,
    UniversalSignal,
    Venue,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# In-memory fakes for the four Protocol surfaces.
# ---------------------------------------------------------------------------


@dataclass
class FakeAdapter:
    """Stub :class:`ExchangeAdapter`. Records every submit / cancel."""

    submitted: list[OrderRequest] = field(default_factory=list)
    canceled: list[tuple[str, str]] = field(default_factory=list)
    submit_error: Exception | None = None
    accepted_at: float = 1700_000_000.0

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:
        self.submitted.append(req)
        if self.submit_error is not None:
            raise self.submit_error
        return OrderAck(
            client_order_id=req.client_order_id,
            exchange_order_id=f"ex-{len(self.submitted)}",
            venue=req.venue,
            symbol=req.symbol,
            accepted_at=self.accepted_at,
        )

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        self.canceled.append((symbol, client_order_id))


@dataclass
class FakeMarketData:
    prices: dict[tuple[Venue, str], float] = field(default_factory=dict)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self.prices.get((venue, symbol))


@dataclass
class FakeEquity:
    equities: dict[Venue, float] = field(default_factory=dict)

    def get_equity_usd(self, venue: Venue) -> float | None:
        return self.equities.get(venue)


@dataclass
class FakePositions:
    positions: dict[tuple[Venue, str], Position] = field(default_factory=dict)

    def get_position(self, venue: Venue, symbol: str) -> Position | None:
        return self.positions.get((venue, symbol))


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _signal(
    *,
    signal_id: str = "sig-1",
    venue: Venue = Venue.BINANCE_UM,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    intent: Intent = Intent.OPEN,
    sizing=None,
    sl=None,
    tp=None,
    ttl_seconds: float = 5.0,
    correlation_id: str | None = "corr-1",
) -> UniversalSignal:
    return UniversalSignal(
        signal_id=signal_id,
        source="test",
        symbol=symbol,
        venue=venue,
        direction=direction,
        intent=intent,
        sizing=sizing if sizing is not None else FixedQty(qty=0.5),
        sl=sl,
        tp=tp,
        ttl_seconds=ttl_seconds,
        correlation_id=correlation_id,
    )


async def _make_router(
    dao: SqliteDAO,
    *,
    adapter: FakeAdapter | None = None,
    market_data: FakeMarketData | None = None,
    positions: FakePositions | None = None,
    equity: FakeEquity | None = None,
    event_bus: EventBus | None = None,
    clock_value: float = 1700_000_000.0,
) -> tuple[
    SignalRouter,
    FakeAdapter,
    FakeMarketData,
    FakePositions,
    EventBus,
    IdempotencyCache,
]:
    cache = IdempotencyCache(dao, ttl_s=3600.0)
    adapter = adapter or FakeAdapter()
    market_data = market_data or FakeMarketData(
        prices={(Venue.BINANCE_UM, "BTCUSDT"): 50000.0}
    )
    positions = positions or FakePositions()
    bus = event_bus or EventBus()
    router = SignalRouter(
        adapter=adapter,
        idempotency=cache,
        market_data=market_data,
        position_provider=positions,
        equity_provider=equity,
        event_bus=bus,
        clock=lambda: clock_value,
    )
    return router, adapter, market_data, positions, bus, cache


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_open_long_fixed_qty_submits_market_entry(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(sizing=FixedQty(qty=0.5))

    ack = await router.submit_signal(sig)

    assert ack.accepted is True
    assert ack.duplicate is False
    assert ack.rejection_reason is None
    assert ack.correlation_id == "corr-1"
    assert len(adapter.submitted) == 1
    entry = adapter.submitted[0]
    assert entry.order_type is OrderType.MARKET
    assert entry.side is OrderSide.BUY
    assert entry.qty == 0.5
    assert entry.venue is Venue.BINANCE_UM
    assert entry.symbol == "BTCUSDT"
    assert entry.signal_id == "sig-1"
    assert entry.correlation_id == "corr-1"
    assert entry.client_order_id == "sig-1-entry"
    assert entry.close_position is False
    assert entry.reduce_only is False


async def test_open_short_fixed_qty_submits_sell_entry(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(direction=Direction.SHORT, sizing=FixedQty(qty=0.5))

    await router.submit_signal(sig)

    assert adapter.submitted[0].side is OrderSide.SELL


async def test_notional_usd_divides_by_reference_price(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(sizing=NotionalUsd(notional_usd=500.0))

    await router.submit_signal(sig)

    assert adapter.submitted[0].qty == pytest.approx(0.01)  # 500 / 50000


async def test_pct_equity_uses_equity_provider(dao: SqliteDAO) -> None:
    equity = FakeEquity(equities={Venue.BINANCE_UM: 10_000.0})
    router, adapter, *_ = await _make_router(dao, equity=equity)
    sig = _signal(sizing=PctEquity(pct=2.0))

    await router.submit_signal(sig)

    # 2% of 10_000 = 200; 200 / 50_000 = 0.004
    assert adapter.submitted[0].qty == pytest.approx(0.004)


async def test_pct_equity_without_provider_rejects(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)  # equity=None
    sig = _signal(sizing=PctEquity(pct=2.0))

    ack = await router.submit_signal(sig)

    assert ack.accepted is False
    assert ack.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


async def test_pct_equity_with_missing_equity_rejects(dao: SqliteDAO) -> None:
    """``equity_provider`` returns ``None`` (no snapshot yet) -> SCHEMA."""

    equity = FakeEquity()  # no entry for BINANCE_UM
    router, adapter, *_ = await _make_router(dao, equity=equity)
    sig = _signal(sizing=PctEquity(pct=2.0))

    ack = await router.submit_signal(sig)

    assert ack.accepted is False
    assert ack.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


# ---------------------------------------------------------------------------
# SL / TP submission (parallel orders)
# ---------------------------------------------------------------------------


async def test_signal_with_sl_submits_stop_market(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(sl=BpsFromEntry(bps=50))  # 0.5% SL on LONG -> below entry

    await router.submit_signal(sig)

    legs = {o.order_type: o for o in adapter.submitted}
    assert OrderType.MARKET in legs
    assert OrderType.STOP_MARKET in legs
    sl = legs[OrderType.STOP_MARKET]
    assert sl.side is OrderSide.SELL  # close LONG
    assert sl.close_position is True
    assert sl.client_order_id == "sig-1-sl"
    assert sl.stop_price == pytest.approx(50000.0 * (1 - 0.005))


async def test_signal_with_tp_submits_take_profit_market(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(tp=BpsFromEntry(bps=100))  # 1% TP on LONG -> above entry

    await router.submit_signal(sig)

    legs = {o.order_type: o for o in adapter.submitted}
    assert OrderType.TAKE_PROFIT_MARKET in legs
    tp = legs[OrderType.TAKE_PROFIT_MARKET]
    assert tp.side is OrderSide.SELL
    assert tp.close_position is True
    assert tp.client_order_id == "sig-1-tp"
    assert tp.stop_price == pytest.approx(50000.0 * 1.01)


async def test_signal_with_sl_and_tp_submits_three_legs(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(sl=BpsFromEntry(bps=50), tp=BpsFromEntry(bps=100))

    await router.submit_signal(sig)

    types = {o.order_type for o in adapter.submitted}
    assert types == {
        OrderType.MARKET,
        OrderType.STOP_MARKET,
        OrderType.TAKE_PROFIT_MARKET,
    }


async def test_short_signal_sl_uses_buy_side(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(direction=Direction.SHORT, sl=BpsFromEntry(bps=50))

    await router.submit_signal(sig)

    sl = next(o for o in adapter.submitted if o.order_type is OrderType.STOP_MARKET)
    assert sl.side is OrderSide.BUY
    assert sl.stop_price == pytest.approx(50000.0 * (1 + 0.005))


# ---------------------------------------------------------------------------
# RiskBased sizing — needs sl_price
# ---------------------------------------------------------------------------


async def test_risk_based_uses_sl_distance(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    # Entry 50000, SL = 1% below = 49500, distance = 500
    # risk_usd=50 -> qty = 50 / 500 = 0.1
    sig = _signal(
        sizing=RiskBased(risk_usd=50.0),
        sl=BpsFromEntry(bps=100),
    )

    await router.submit_signal(sig)

    entry = next(o for o in adapter.submitted if o.order_type is OrderType.MARKET)
    assert entry.qty == pytest.approx(0.1)


async def test_risk_based_without_sl_rejects_with_sizing_requires_sl(
    dao: SqliteDAO,
) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(sizing=RiskBased(risk_usd=50.0), sl=None)

    ack = await router.submit_signal(sig)

    assert ack.accepted is False
    assert ack.rejection_reason is RejectionReason.SIZING_REQUIRES_SL
    assert adapter.submitted == []


# ---------------------------------------------------------------------------
# Schema / intent rejections
# ---------------------------------------------------------------------------


async def test_ttl_zero_rejects_schema(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(ttl_seconds=0.0)

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


async def test_negative_ttl_rejects_schema(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(ttl_seconds=-1.0)

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


async def test_empty_symbol_rejects_unknown_symbol(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(symbol="")

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.UNKNOWN_SYMBOL
    assert adapter.submitted == []


async def test_missing_reference_price_rejects(dao: SqliteDAO) -> None:
    market = FakeMarketData()  # no entry
    router, adapter, *_ = await _make_router(dao, market_data=market)
    sig = _signal()

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.UNKNOWN_SYMBOL
    assert adapter.submitted == []


async def test_zero_reference_price_rejects(dao: SqliteDAO) -> None:
    market = FakeMarketData(prices={(Venue.BINANCE_UM, "BTCUSDT"): 0.0})
    router, adapter, *_ = await _make_router(dao, market_data=market)
    sig = _signal()

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.UNKNOWN_SYMBOL
    assert adapter.submitted == []


async def test_open_with_existing_position_rejects_invalid_intent(
    dao: SqliteDAO,
) -> None:
    positions = FakePositions(
        positions={
            (Venue.BINANCE_UM, "BTCUSDT"): Position(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                direction=Direction.LONG,
                qty=0.1,
                entry_price=50000.0,
                state=PositionState.OPEN,
                liquidation_price=None,
                unrealized_pnl_usd=None,
                margin_used_usd=None,
                opened_at=None,
            )
        }
    )
    router, adapter, *_ = await _make_router(dao, positions=positions)
    sig = _signal(intent=Intent.OPEN)

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.INVALID_INTENT
    assert adapter.submitted == []


async def test_open_with_flat_position_proceeds(dao: SqliteDAO) -> None:
    """``qty=0`` means tracked-but-flat — still openable."""

    positions = FakePositions(
        positions={
            (Venue.BINANCE_UM, "BTCUSDT"): Position(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                direction=Direction.LONG,
                qty=0.0,
                entry_price=0.0,
                state=PositionState.IDLE,
                liquidation_price=None,
                unrealized_pnl_usd=None,
                margin_used_usd=None,
                opened_at=None,
            )
        }
    )
    router, adapter, *_ = await _make_router(dao, positions=positions)
    sig = _signal(intent=Intent.OPEN)

    ack = await router.submit_signal(sig)

    assert ack.accepted is True
    assert len(adapter.submitted) == 1


@pytest.mark.parametrize(
    "intent",
    [Intent.ADD, Intent.REDUCE, Intent.CLOSE, Intent.REVERSE],
)
async def test_non_open_intents_rejected_in_phase_3d(
    dao: SqliteDAO, intent: Intent
) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(intent=intent)

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.INVALID_INTENT
    assert adapter.submitted == []


# ---------------------------------------------------------------------------
# Stop-calc failures bubble up as SCHEMA
# ---------------------------------------------------------------------------


async def test_absolute_sl_on_wrong_side_rejects(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(
        direction=Direction.LONG,
        sl=AbsolutePrice(price=51000.0),  # above entry on LONG -> invalid
    )

    ack = await router.submit_signal(sig)

    assert ack.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


# ---------------------------------------------------------------------------
# Adapter failures propagate (no idempotency cache, no event)
# ---------------------------------------------------------------------------


async def test_adapter_error_propagates_and_is_not_cached(dao: SqliteDAO) -> None:
    adapter = FakeAdapter(submit_error=RuntimeError("venue boom"))
    router, _, _, _, bus, cache = await _make_router(dao, adapter=adapter)
    sig = _signal()

    with pytest.raises(RuntimeError, match="venue boom"):
        await router.submit_signal(sig)

    # Idempotency was NOT populated (caller can retry).
    assert await cache.get("sig-1") is None
    # No SIGNAL_RECEIVED event was published.
    assert bus.subscriber_count(EventType.SIGNAL_RECEIVED.value) == 0


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


async def test_replay_returns_duplicate_true(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal()

    first = await router.submit_signal(sig)
    second = await router.submit_signal(sig)

    assert first.accepted is True
    assert first.duplicate is False
    assert second.accepted is True
    assert second.duplicate is True
    # No additional orders on the second call.
    assert len(adapter.submitted) == 1


async def test_replay_of_rejection_preserves_reason(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(ttl_seconds=0.0)

    first = await router.submit_signal(sig)
    second = await router.submit_signal(sig)

    assert first.accepted is False
    assert first.duplicate is False
    assert first.rejection_reason is RejectionReason.SCHEMA
    assert second.accepted is False
    assert second.duplicate is True
    assert second.rejection_reason is RejectionReason.SCHEMA
    assert adapter.submitted == []


async def test_idempotency_stores_ack_json(dao: SqliteDAO) -> None:
    router, _, _, _, _, cache = await _make_router(dao)
    sig = _signal()

    await router.submit_signal(sig)
    raw = await cache.get("sig-1")
    assert raw is not None
    decoded = json.loads(raw)
    assert decoded["signal_id"] == "sig-1"
    assert decoded["accepted"] is True
    assert decoded["correlation_id"] == "corr-1"


# ---------------------------------------------------------------------------
# Event bus
# ---------------------------------------------------------------------------


async def test_signal_received_event_published_on_accept(dao: SqliteDAO) -> None:
    bus = EventBus()
    sub = bus.subscribe(EventType.SIGNAL_RECEIVED.value)
    router, *_ = await _make_router(dao, event_bus=bus)
    sig = _signal()

    await router.submit_signal(sig)

    event = sub.queue.get_nowait()
    assert event["signal"]["signal_id"] == "sig-1"
    assert event["ack"]["accepted"] is True


async def test_signal_received_event_published_on_reject(dao: SqliteDAO) -> None:
    bus = EventBus()
    sub = bus.subscribe(EventType.SIGNAL_RECEIVED.value)
    router, *_ = await _make_router(dao, event_bus=bus)
    sig = _signal(ttl_seconds=0.0)

    await router.submit_signal(sig)

    event = sub.queue.get_nowait()
    assert event["signal"]["signal_id"] == "sig-1"
    assert event["ack"]["accepted"] is False
    assert event["ack"]["rejection_reason"] == RejectionReason.SCHEMA.value


async def test_event_bus_optional(dao: SqliteDAO) -> None:
    """Router must work without an event bus (no exception)."""

    router, adapter, _, _, _, _ = await _make_router(dao, event_bus=None)
    # Override event_bus to None via direct construction.
    router = SignalRouter(
        adapter=adapter,
        idempotency=IdempotencyCache(dao, ttl_s=3600.0),
        market_data=FakeMarketData(
            prices={(Venue.BINANCE_UM, "BTCUSDT"): 50000.0}
        ),
        position_provider=NullPositionProvider(),
        event_bus=None,
    )
    ack = await router.submit_signal(_signal())
    assert ack.accepted is True


# ---------------------------------------------------------------------------
# Misc invariants
# ---------------------------------------------------------------------------


async def test_signal_ack_carries_correlation_id(dao: SqliteDAO) -> None:
    router, *_ = await _make_router(dao)
    sig = _signal(correlation_id="corr-xyz")

    ack = await router.submit_signal(sig)

    assert ack.correlation_id == "corr-xyz"


async def test_signal_ack_ts_uses_injected_clock(dao: SqliteDAO) -> None:
    router, *_ = await _make_router(dao, clock_value=42.0)
    sig = _signal()

    ack = await router.submit_signal(sig)

    assert ack.ts == 42.0


async def test_client_order_id_truncates_long_signal_id(dao: SqliteDAO) -> None:
    router, adapter, *_ = await _make_router(dao)
    sig = _signal(signal_id="x" * 64)

    await router.submit_signal(sig)

    coid = adapter.submitted[0].client_order_id
    assert coid.startswith("x" * 24)
    assert coid.endswith("-entry")
    assert len(coid) <= 36
