"""Tests for the embedded :class:`TradeAdapter` API (Phase 3g).

The adapter is a thin facade composing the Phase 3 stack:

* :class:`SignalRouter` for the accept path,
* :class:`PositionManager` for live state,
* :class:`RiskGate` + :class:`RiskState` for pre-submit risk,
* :class:`EventBus` as the public read-back surface,
* :class:`ExchangeAdapter` (here, a fake) as the venue side.

These tests pin the public contract — lifecycle ordering, idempotency,
read accessors, kill switch, event subscription — without reaching
into Binance specifics.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import (
    TradeAdapter,
    TradeAdapterClosed,
    TradeAdapterNotStarted,
)
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    Direction,
    EventType,
    FixedQty,
    Intent,
    OrderAck,
    OrderRequest,
    OrderType,
    PositionState,
    PositionUpdate,
    RejectionReason,
    UniversalSignal,
    Venue,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class FakeExchangeAdapter:
    """Stub :class:`ExchangeAdapter` that records start/close + orders."""

    submitted: list[OrderRequest] = field(default_factory=list)
    canceled: list[tuple[str, str]] = field(default_factory=list)
    start_calls: int = 0
    close_calls: int = 0
    start_error: BaseException | None = None
    cancel_error: BaseException | None = None
    submit_error: BaseException | None = None

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    async def close(self) -> None:
        self.close_calls += 1

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
            accepted_at=1700_000_000.0,
        )

    async def cancel_order(
        self,
        symbol: str,
        client_order_id: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        self.canceled.append((symbol, client_order_id))
        if self.cancel_error is not None:
            raise self.cancel_error


@dataclass
class FakeSnapshotProvider:
    """Stub :class:`VenueSnapshotProvider` for :class:`PositionManager`."""

    venue: Venue = Venue.BINANCE_UM
    positions: list[PositionUpdate] = field(default_factory=list)
    equity_usd: float = 10_000.0
    bootstrap_calls: int = 0
    equity_calls: int = 0
    bootstrap_error: BaseException | None = None

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        self.bootstrap_calls += 1
        if self.bootstrap_error is not None:
            raise self.bootstrap_error
        return list(self.positions)

    async def fetch_equity_snapshot(self) -> float:
        self.equity_calls += 1
        return self.equity_usd


@dataclass
class FakeMarketData:
    prices: dict[tuple[Venue, str], float] = field(default_factory=dict)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self.prices.get((venue, symbol))


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


async def _make_stack(
    dao: SqliteDAO,
    *,
    venue: Venue = Venue.BINANCE_UM,
    exchange_adapter: FakeExchangeAdapter | None = None,
    snapshot_provider: FakeSnapshotProvider | None = None,
    market_data: FakeMarketData | None = None,
    risk_config: RiskConfig | None = None,
    use_position_manager: bool = True,
    use_risk: bool = True,
) -> tuple[
    TradeAdapter,
    FakeExchangeAdapter,
    FakeSnapshotProvider,
    EventBus,
    PositionManager | None,
    RiskState | None,
]:
    bus = EventBus()
    adapter = exchange_adapter or FakeExchangeAdapter()
    snap = snapshot_provider or FakeSnapshotProvider(venue=venue)
    md = market_data or FakeMarketData(
        prices={(venue, "BTCUSDT"): 50000.0}
    )

    store = PositionStore()
    mgr: PositionManager | None = None
    if use_position_manager:
        mgr = PositionManager(
            venue=venue,
            store=store,
            snapshot_provider=snap,
            event_bus=bus,
            reconcile_interval_s=0,  # one-shot bootstrap only
        )

    risk_state: RiskState | None = None
    risk_gate: RiskGate | None = None
    if use_risk:
        risk_state = RiskState()
        risk_gate = RiskGate(
            config=risk_config or RiskConfig(),
            state=risk_state,
            position_provider=store,
        )

    cache = IdempotencyCache(dao, ttl_s=3600.0)
    router = SignalRouter(
        adapter=adapter,
        idempotency=cache,
        market_data=md,
        position_provider=store,
        equity_provider=store,
        event_bus=bus,
        risk_gate=risk_gate,
        clock=lambda: 1700_000_000.0,
    )

    ta = TradeAdapter(
        venue=venue,
        exchange_adapter=adapter,
        signal_router=router,
        event_bus=bus,
        position_manager=mgr,
        risk_state=risk_state,
        risk_gate=risk_gate,
    )

    return ta, adapter, snap, bus, mgr, risk_state


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_start_brings_exchange_and_manager_online(dao: SqliteDAO) -> None:
    ta, adapter, snap, _, mgr, _ = await _make_stack(dao)

    await ta.start()
    try:
        assert ta.is_started is True
        assert ta.is_closed is False
        assert adapter.start_calls == 1
        assert snap.bootstrap_calls == 1
        assert snap.equity_calls == 1
        assert mgr is not None
    finally:
        await ta.close()


async def test_start_is_idempotent(dao: SqliteDAO) -> None:
    ta, adapter, snap, *_ = await _make_stack(dao)

    await ta.start()
    await ta.start()  # no-op
    try:
        assert adapter.start_calls == 1
        assert snap.bootstrap_calls == 1
    finally:
        await ta.close()


async def test_close_is_idempotent(dao: SqliteDAO) -> None:
    ta, adapter, *_ = await _make_stack(dao)

    await ta.start()
    await ta.close()
    await ta.close()  # no-op

    assert adapter.close_calls == 1


async def test_close_swallows_individual_component_errors(
    dao: SqliteDAO,
) -> None:
    """A faulty teardown step must not strand the other steps."""

    ta, adapter, *_ = await _make_stack(dao)
    await ta.start()

    # Force the exchange adapter's close to raise. Position manager's
    # stop runs first, then exchange adapter's close — both should be
    # invoked, and neither error reaches the caller.
    async def boom() -> None:
        raise RuntimeError("close failure")

    adapter.close = boom  # type: ignore[assignment]

    await ta.close()  # does not raise


async def test_start_after_close_raises(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    await ta.close()

    with pytest.raises(TradeAdapterClosed):
        await ta.start()


async def test_async_context_manager_starts_and_closes(dao: SqliteDAO) -> None:
    ta, adapter, *_ = await _make_stack(dao)

    async with ta:
        assert ta.is_started is True
        assert adapter.start_calls == 1
    assert adapter.close_calls == 1
    assert ta.is_closed is True


async def test_start_rolls_back_exchange_on_manager_bootstrap_failure(
    dao: SqliteDAO,
) -> None:
    snap = FakeSnapshotProvider(bootstrap_error=RuntimeError("rest down"))
    ta, adapter, *_ = await _make_stack(dao, snapshot_provider=snap)

    with pytest.raises(RuntimeError, match="rest down"):
        await ta.start()

    # Exchange was started but rolled back so the user can rebuild.
    assert adapter.start_calls == 1
    assert adapter.close_calls == 1
    assert ta.is_started is False


# ---------------------------------------------------------------------------
# submit_signal / cancel_order
# ---------------------------------------------------------------------------


async def test_submit_signal_before_start_raises(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)

    with pytest.raises(TradeAdapterNotStarted):
        await ta.submit_signal(_signal())


async def test_submit_signal_after_close_raises(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    await ta.close()

    with pytest.raises(TradeAdapterClosed):
        await ta.submit_signal(_signal())


async def test_submit_signal_routes_to_signal_router(dao: SqliteDAO) -> None:
    ta, adapter, *_ = await _make_stack(dao)
    await ta.start()
    try:
        ack = await ta.submit_signal(_signal())
        assert ack.accepted is True
        assert ack.correlation_id == "corr-1"
        assert len(adapter.submitted) == 1
        assert adapter.submitted[0].order_type == OrderType.MARKET
        assert adapter.submitted[0].qty == pytest.approx(0.5)
    finally:
        await ta.close()


async def test_submit_signal_returns_reject_when_risk_gate_trips(
    dao: SqliteDAO,
) -> None:
    ta, adapter, *_rest = await _make_stack(dao)
    await ta.start()
    try:
        ta.trip_kill_switch("manual stop")

        ack = await ta.submit_signal(_signal())

        assert ack.accepted is False
        assert (
            ack.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
        )
        # Order never reached the venue.
        assert len(adapter.submitted) == 0
    finally:
        await ta.close()


async def test_cancel_order_routes_to_exchange_adapter(dao: SqliteDAO) -> None:
    ta, adapter, *_ = await _make_stack(dao)
    await ta.start()
    try:
        await ta.cancel_order("BTCUSDT", "coid-1")
        assert adapter.canceled == [("BTCUSDT", "coid-1")]
    finally:
        await ta.close()


async def test_cancel_order_before_start_raises(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)

    with pytest.raises(TradeAdapterNotStarted):
        await ta.cancel_order("BTCUSDT", "coid-1")


async def test_cancel_order_after_close_raises(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    await ta.close()

    with pytest.raises(TradeAdapterClosed):
        await ta.cancel_order("BTCUSDT", "coid-1")


# ---------------------------------------------------------------------------
# Event subscription
# ---------------------------------------------------------------------------


async def test_subscribe_accepts_eventtype_enum(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    try:
        sub = ta.subscribe(EventType.SIGNAL_RECEIVED)
        try:
            ack = await ta.submit_signal(_signal())
            assert ack.accepted is True

            assert sub.queue.qsize() == 1
            payload = sub.queue.get_nowait()
            assert payload["signal"]["signal_id"] == "sig-1"
            assert payload["ack"]["accepted"] is True
        finally:
            await sub.close()
    finally:
        await ta.close()


async def test_subscribe_accepts_raw_topic_string(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    try:
        sub = ta.subscribe(EventType.SIGNAL_RECEIVED.value)
        try:
            await ta.submit_signal(_signal())
            assert sub.queue.qsize() == 1
        finally:
            await sub.close()
    finally:
        await ta.close()


# ---------------------------------------------------------------------------
# Read accessors
# ---------------------------------------------------------------------------


async def test_get_position_returns_bootstrapped_position(
    dao: SqliteDAO,
) -> None:
    snap = FakeSnapshotProvider(
        positions=[
            PositionUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                direction=Direction.LONG,
                qty=0.5,
                entry_price=50000.0,
                state=PositionState.OPEN,
                liquidation_price=None,
                unrealized_pnl_usd=12.34,
                margin_used_usd=None,
                ts=1700_000_000.0,
                signal_id=None,
            )
        ],
        equity_usd=12_345.0,
    )

    ta, *_ = await _make_stack(dao, snapshot_provider=snap)
    await ta.start()
    try:
        pos = ta.get_position(Venue.BINANCE_UM, "BTCUSDT")
        assert pos is not None
        assert pos.qty == pytest.approx(0.5)
        assert pos.direction is Direction.LONG
        assert ta.get_equity_usd(Venue.BINANCE_UM) == pytest.approx(12_345.0)
        assert [p.symbol for p in ta.open_positions()] == ["BTCUSDT"]
    finally:
        await ta.close()


async def test_get_position_returns_none_when_no_manager_wired(
    dao: SqliteDAO,
) -> None:
    ta, *_ = await _make_stack(dao, use_position_manager=False)
    await ta.start()
    try:
        assert ta.get_position(Venue.BINANCE_UM, "BTCUSDT") is None
        assert ta.get_equity_usd(Venue.BINANCE_UM) is None
        assert ta.open_positions() == []
    finally:
        await ta.close()


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------


async def test_trip_and_reset_kill_switch(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao)
    await ta.start()
    try:
        assert ta.kill_switch_tripped is False
        ta.trip_kill_switch("manual")
        assert ta.kill_switch_tripped is True
        ta.reset_kill_switch()
        assert ta.kill_switch_tripped is False
    finally:
        await ta.close()


async def test_kill_switch_no_op_without_risk_state(dao: SqliteDAO) -> None:
    ta, *_ = await _make_stack(dao, use_risk=False)
    await ta.start()
    try:
        ta.trip_kill_switch("manual")  # no-ops, no exception
        assert ta.kill_switch_tripped is False
        ta.reset_kill_switch()  # no-ops
    finally:
        await ta.close()


# ---------------------------------------------------------------------------
# Property accessors
# ---------------------------------------------------------------------------


async def test_property_accessors_return_underlying_components(
    dao: SqliteDAO,
) -> None:
    ta, adapter, _, bus, mgr, risk_state = await _make_stack(dao)

    assert ta.venue is Venue.BINANCE_UM
    assert ta.exchange_adapter is adapter
    assert ta.event_bus is bus
    assert ta.position_manager is mgr
    assert ta.risk_state is risk_state
    assert ta.risk_gate is not None
    assert ta.signal_router is not None


# ---------------------------------------------------------------------------
# Golden path
# ---------------------------------------------------------------------------


async def test_golden_path_open_then_close(dao: SqliteDAO) -> None:
    """End-to-end through the public surface: subscribe → submit → ack."""

    ta, adapter, *_ = await _make_stack(dao)
    await ta.start()
    try:
        sub = ta.subscribe(EventType.SIGNAL_RECEIVED)
        try:
            ack = await ta.submit_signal(_signal(signal_id="sig-golden"))
            assert ack.accepted is True

            payload = sub.queue.get_nowait()
            assert payload["signal"]["signal_id"] == "sig-golden"
            assert payload["ack"]["accepted"] is True

            # Cancel the entry via the public surface.
            entry_coid = adapter.submitted[0].client_order_id
            await ta.cancel_order("BTCUSDT", entry_coid)
            assert adapter.canceled == [("BTCUSDT", entry_coid)]
        finally:
            await sub.close()
    finally:
        await ta.close()
