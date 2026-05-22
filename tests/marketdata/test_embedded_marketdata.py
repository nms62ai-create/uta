"""End-to-end tests for the :class:`TradeAdapter` market-data surface (A.1)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter, TradeAdapterError
from trade_adapter.marketdata import BboTracker, MarketDataHub, StreamKind
from trade_adapter.marketdata.types import StreamCallback
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    BBOUpdate,
    BookLevel,
    BookUpdate,
    Direction,
    EventType,
    OrderAck,
    OrderRequest,
    OrderSide,
    PositionState,
    PositionUpdate,
    TradePrint,
    Venue,
)

pytestmark = pytest.mark.asyncio


@dataclass
class FakeExchangeAdapter:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:  # pragma: no cover - not exercised here
        return OrderAck(
            client_order_id=req.client_order_id,
            exchange_order_id="ex-1",
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
        return None


@dataclass
class FakeSnapshotProvider:
    venue: Venue = Venue.BINANCE_UM
    positions: list[PositionUpdate] = field(default_factory=list)
    equity_usd: float = 10_000.0

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        return list(self.positions)

    async def fetch_equity_snapshot(self) -> float:
        return self.equity_usd


@dataclass
class FakeMarketDataProvider:
    """Sync side: cached reference price for SignalRouter."""

    prices: dict[tuple[Venue, str], float] = field(default_factory=dict)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self.prices.get((venue, symbol))


@dataclass
class FakeStreamProvider:
    """Async side: hub provider that lets tests push synthetic ticks."""

    venue: Venue = Venue.BINANCE_UM
    subscribed: dict[tuple[str, StreamKind], StreamCallback] = field(
        default_factory=dict
    )

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def subscribe_stream(
        self,
        symbol: str,
        kind: StreamKind,
        callback: StreamCallback,
    ) -> None:
        self.subscribed[(symbol, kind)] = callback

    async def unsubscribe_stream(
        self, symbol: str, kind: StreamKind
    ) -> None:
        self.subscribed.pop((symbol, kind), None)

    def push(self, symbol: str, kind: StreamKind, event: Any) -> None:
        cb = self.subscribed[(symbol, kind)]
        cb(event)


async def _build_stack(
    dao: SqliteDAO,
    *,
    with_hub: bool = True,
    with_tracker: bool = False,
) -> tuple[TradeAdapter, FakeStreamProvider, MarketDataHub | None]:
    bus = EventBus()
    store = PositionStore()
    snap = FakeSnapshotProvider()
    manager = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        event_bus=bus,
        reconcile_interval_s=0,
    )
    risk_state = RiskState()
    risk_gate = RiskGate(
        config=__import__(
            "trade_adapter.core.risk", fromlist=["RiskConfig"]
        ).RiskConfig(),
        state=risk_state,
        position_provider=store,
    )
    cache = IdempotencyCache(dao, ttl_s=3600.0)
    router = SignalRouter(
        adapter=FakeExchangeAdapter(),
        idempotency=cache,
        market_data=FakeMarketDataProvider(
            prices={(Venue.BINANCE_UM, "BTCUSDT"): 50000.0}
        ),
        position_provider=store,
        equity_provider=store,
        event_bus=bus,
        risk_gate=risk_gate,
        clock=lambda: 1700_000_000.0,
    )

    provider: FakeStreamProvider | None = None
    hub: MarketDataHub | None = None
    tracker: BboTracker | None = None
    if with_hub:
        provider = FakeStreamProvider()
        hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    if with_tracker:
        assert hub is not None
        tracker = BboTracker(event_bus=bus, market_data_hub=hub)

    ta = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=FakeExchangeAdapter(),
        signal_router=router,
        event_bus=bus,
        position_manager=manager,
        risk_state=risk_state,
        risk_gate=risk_gate,
        market_data_hub=hub,
        auto_bbo_tracker=tracker,
    )
    return ta, provider or FakeStreamProvider(), hub


async def test_subscribe_book_returns_book_updates(dao: SqliteDAO) -> None:
    ta, provider, _hub = await _build_stack(dao, with_hub=True)
    await ta.start()
    try:
        sub = await ta.subscribe_book("BTCUSDT")
        provider.push(
            "BTCUSDT",
            StreamKind.BOOK,
            BookUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                bids=(BookLevel(price=99.0, qty=1.0),),
                asks=(BookLevel(price=101.0, qty=1.0),),
                ts=1700_000_000.0,
                sequence=1,
            ),
        )
        ev = await asyncio.wait_for(sub.queue.get(), timeout=1.0)
        assert isinstance(ev, BookUpdate)
        await sub.close()
    finally:
        await ta.close()


async def test_subscribe_trades_and_bbo_routes(dao: SqliteDAO) -> None:
    ta, provider, _ = await _build_stack(dao, with_hub=True)
    await ta.start()
    try:
        sub_t = await ta.subscribe_trades("BTCUSDT")
        sub_q = await ta.subscribe_bbo("BTCUSDT")
        provider.push(
            "BTCUSDT",
            StreamKind.TRADES,
            TradePrint(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                price=100.0,
                qty=1.0,
                side=OrderSide.BUY,
                ts=1700_000_000.0,
                trade_id="1",
            ),
        )
        provider.push(
            "BTCUSDT",
            StreamKind.BBO,
            BBOUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                bid_price=99.0,
                bid_qty=1.0,
                ask_price=101.0,
                ask_qty=1.0,
                ts=1700_000_001.0,
            ),
        )
        t = await asyncio.wait_for(sub_t.queue.get(), timeout=1.0)
        q = await asyncio.wait_for(sub_q.queue.get(), timeout=1.0)
        assert isinstance(t, TradePrint)
        assert isinstance(q, BBOUpdate)
        await sub_t.close()
        await sub_q.close()
    finally:
        await ta.close()


async def test_subscribe_without_hub_raises(dao: SqliteDAO) -> None:
    ta, _, _ = await _build_stack(dao, with_hub=False)
    await ta.start()
    try:
        with pytest.raises(TradeAdapterError):
            await ta.subscribe_book("BTCUSDT")
    finally:
        await ta.close()


async def test_auto_bbo_tracker_subscribes_on_position_open(
    dao: SqliteDAO,
) -> None:
    ta, provider, _ = await _build_stack(
        dao, with_hub=True, with_tracker=True
    )
    await ta.start()
    try:
        ta.event_bus.publish(
            EventType.POSITION_UPDATE.value,
            PositionUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                direction=Direction.LONG,
                qty=1.0,
                entry_price=50000.0,
                state=PositionState.OPEN,
                liquidation_price=None,
                unrealized_pnl_usd=None,
                margin_used_usd=None,
                ts=1700_000_000.0,
                signal_id=None,
            ),
        )
        # Wait for tracker to react.
        for _ in range(100):
            await asyncio.sleep(0.01)
            if ("BTCUSDT", StreamKind.BBO) in provider.subscribed:
                break

        assert ("BTCUSDT", StreamKind.BBO) in provider.subscribed
        provider.push(
            "BTCUSDT",
            StreamKind.BBO,
            BBOUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                bid_price=95.0,
                bid_qty=1.0,
                ask_price=105.0,
                ask_qty=1.0,
                ts=1700_000_010.0,
            ),
        )
        for _ in range(100):
            await asyncio.sleep(0.01)
            snap = ta.get_bbo_snapshot("BTCUSDT")
            if snap is not None and snap.tick_count >= 1:
                break

        snap = ta.get_bbo_snapshot("BTCUSDT")
        assert snap is not None
        assert snap.tick_count == 1
        assert snap.min_bid == pytest.approx(95.0)
        assert snap.max_ask == pytest.approx(105.0)
    finally:
        await ta.close()


async def test_get_bbo_snapshot_returns_none_without_tracker(
    dao: SqliteDAO,
) -> None:
    ta, _, _ = await _build_stack(dao, with_hub=True, with_tracker=False)
    await ta.start()
    try:
        assert ta.get_bbo_snapshot("BTCUSDT") is None
    finally:
        await ta.close()
