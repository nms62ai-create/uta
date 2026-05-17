"""Tests for :class:`HeatmapAutoTrader` — the UI-gated bridge from
adaptive signals to :class:`TradeAdapter.submit_signal`.

These tests exercise the *gate* logic (enable / confidence / dedupe /
position-already-open / kill switch) by handing the autotrader a real
:class:`TradeAdapter` wired to fake components. The downstream Phase 3
stack is covered by its own tests; here we only check that the bridge
either forwards a signal or correctly suppresses it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskConfig, RiskGate, RiskState
from trade_adapter.core.signal_router import SignalRouter
from trade_adapter.embedded import TradeAdapter
from trade_adapter.integrations.heatmap_sdk import (
    AutotradeSettings,
    HeatmapAutoTrader,
    InvalidAutotradeSettings,
)
from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO
from trade_adapter.types import (
    Direction,
    OrderAck,
    OrderRequest,
    PositionState,
    PositionUpdate,
    Venue,
)

# ---------------------------------------------------------------------------
# Adaptive-signal stubs (kept identical to test_translator.py so that
# heatmap-sdk's own dataclass shape is the contract we test against).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ExhaustionType:
    name: str


BUY_EXHAUSTION = _ExhaustionType("BUY_EXHAUSTION")
SELL_EXHAUSTION = _ExhaustionType("SELL_EXHAUSTION")


@dataclass(frozen=True)
class _FakeAdaptiveSignal:
    signal_id: str
    symbol: str
    timestamp: float
    exhaustion_type: _ExhaustionType
    confidence: float


def _signal(
    *,
    signal_id: str = "sig-1",
    confidence: float = 0.80,
    exhaustion: _ExhaustionType = SELL_EXHAUSTION,
) -> _FakeAdaptiveSignal:
    return _FakeAdaptiveSignal(
        signal_id=signal_id,
        symbol="BTCUSDT",
        timestamp=1_700_000_000.0,
        exhaustion_type=exhaustion,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Fake stack (kept compatible with tests/test_embedded.py).
# ---------------------------------------------------------------------------


@dataclass
class FakeExchangeAdapter:
    submitted: list[OrderRequest] = field(default_factory=list)
    canceled: list[tuple[str, str]] = field(default_factory=list)

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def submit_order(
        self, req: OrderRequest, *, timeout_s: float | None = None
    ) -> OrderAck:
        self.submitted.append(req)
        return OrderAck(
            client_order_id=req.client_order_id,
            exchange_order_id=f"ex-{len(self.submitted)}",
            venue=req.venue,
            symbol=req.symbol,
            accepted_at=1_700_000_000.0,
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
class FakeSnapshotProvider:
    venue: Venue = Venue.BINANCE_UM
    positions: list[PositionUpdate] = field(default_factory=list)
    equity_usd: float = 10_000.0

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        return list(self.positions)

    async def fetch_equity_snapshot(self) -> float:
        return self.equity_usd


@dataclass
class FakeMarketData:
    prices: dict[tuple[Venue, str], float] = field(default_factory=dict)

    def get_reference_price(self, venue: Venue, symbol: str) -> float | None:
        return self.prices.get((venue, symbol))


async def _make_adapter(
    dao: SqliteDAO,
) -> tuple[
    TradeAdapter,
    FakeExchangeAdapter,
    FakeSnapshotProvider,
    PositionStore,
    RiskState,
]:
    """Build a started-ready :class:`TradeAdapter` over fake components.

    Mirrors ``tests/test_embedded.py::_make_stack`` so that the
    autotrader gets exactly the same wiring real consumers will use.
    """

    bus = EventBus()
    store = PositionStore()
    snapshot = FakeSnapshotProvider(venue=Venue.BINANCE_UM)
    manager = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snapshot,
        event_bus=bus,
        reconcile_interval_s=0,
    )
    cache = IdempotencyCache(dao, ttl_s=3600.0)
    risk_state = RiskState()
    risk_gate = RiskGate(
        config=RiskConfig(),
        state=risk_state,
        position_provider=store,
    )
    exchange = FakeExchangeAdapter()
    market_data = FakeMarketData(prices={(Venue.BINANCE_UM, "BTCUSDT"): 50_000.0})
    router = SignalRouter(
        adapter=exchange,
        idempotency=cache,
        market_data=market_data,
        position_provider=store,
        equity_provider=store,
        event_bus=bus,
        risk_gate=risk_gate,
        clock=lambda: 1_700_000_000.0,
    )
    adapter = TradeAdapter(
        venue=Venue.BINANCE_UM,
        exchange_adapter=exchange,
        signal_router=router,
        event_bus=bus,
        position_manager=manager,
        risk_state=risk_state,
        risk_gate=risk_gate,
    )
    return adapter, exchange, snapshot, store, risk_state


def _settings(
    *,
    enabled: bool = True,
    min_confidence: float = 0.0,
    notional_usd: float = 100.0,
) -> AutotradeSettings:
    return AutotradeSettings(
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=notional_usd,
        sl_pct=1.0,
        tp_pct=2.0,
        min_confidence=min_confidence,
        ttl_seconds=5.0,
        enabled=enabled,
    )


# ---------------------------------------------------------------------------
# AutotradeSettings validation
# ---------------------------------------------------------------------------


def test_settings_rejects_empty_symbol() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="symbol must be non-empty"):
        AutotradeSettings(
            symbol="",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=None,
        )


def test_settings_rejects_non_positive_notional() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="notional_usd must be positive"):
        AutotradeSettings(
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=0.0,
            sl_pct=None,
            tp_pct=None,
        )


def test_settings_rejects_non_positive_sl() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="sl_pct must be positive"):
        AutotradeSettings(
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=0.0,
            tp_pct=None,
        )


def test_settings_rejects_non_positive_tp() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="tp_pct must be positive"):
        AutotradeSettings(
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=-0.1,
        )


def test_settings_rejects_min_confidence_out_of_range() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="min_confidence"):
        AutotradeSettings(
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=None,
            min_confidence=1.5,
        )


def test_settings_rejects_non_positive_ttl() -> None:
    with pytest.raises(InvalidAutotradeSettings, match="ttl_seconds must be positive"):
        AutotradeSettings(
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=None,
            ttl_seconds=0.0,
        )


def test_settings_with_no_stops_is_allowed() -> None:
    settings = AutotradeSettings(
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
    )
    assert settings.sl_pct is None
    assert settings.tp_pct is None


# ---------------------------------------------------------------------------
# AutoTrader enable / disable / update
# ---------------------------------------------------------------------------


async def test_autotrader_disabled_by_default(dao: SqliteDAO) -> None:
    adapter, _exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    assert trader.enabled is False
    assert trader.settings is None


async def test_enable_without_settings_raises(dao: SqliteDAO) -> None:
    adapter, _exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    with pytest.raises(InvalidAutotradeSettings, match="before update_settings"):
        trader.enable()


async def test_update_settings_replaces_settings(dao: SqliteDAO) -> None:
    adapter, _exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    trader.update_settings(_settings(enabled=False))
    assert trader.enabled is False

    trader.update_settings(_settings(enabled=True, notional_usd=200.0))
    assert trader.enabled is True
    assert trader.settings is not None
    assert trader.settings.notional_usd == 200.0


async def test_enable_then_disable_toggles_flag(dao: SqliteDAO) -> None:
    adapter, _exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    trader.update_settings(_settings(enabled=False))
    trader.enable()
    assert trader.enabled is True

    trader.disable()
    assert trader.enabled is False


async def test_disable_without_settings_is_noop(dao: SqliteDAO) -> None:
    adapter, _exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    trader.disable()
    assert trader.enabled is False


# ---------------------------------------------------------------------------
# on_adaptive_signal — gate behaviour
# ---------------------------------------------------------------------------


async def test_signal_ignored_when_no_settings_set(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter)

    async with adapter:
        ack = await trader.on_adaptive_signal(_signal())

    assert ack is None
    assert exch.submitted == []


async def test_signal_ignored_when_disabled(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter, settings=_settings(enabled=False))

    async with adapter:
        ack = await trader.on_adaptive_signal(_signal(confidence=0.99))

    assert ack is None
    assert exch.submitted == []


async def test_signal_ignored_below_min_confidence(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(
        adapter, settings=_settings(min_confidence=0.7)
    )

    async with adapter:
        ack = await trader.on_adaptive_signal(_signal(confidence=0.5))

    assert ack is None
    assert exch.submitted == []


async def test_signal_accepted_at_min_confidence_boundary(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(
        adapter, settings=_settings(min_confidence=0.7)
    )

    async with adapter:
        ack = await trader.on_adaptive_signal(_signal(confidence=0.7))

    assert ack is not None
    assert ack.accepted is True
    assert len(exch.submitted) >= 1


async def test_signal_forwarded_uses_settings_symbol_and_sizing(
    dao: SqliteDAO,
) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(
        adapter,
        settings=_settings(notional_usd=200.0),
    )

    async with adapter:
        # Submit a signal that has a different symbol than the user's
        # UI selection — the autotrader should use the *settings*
        # symbol, not the adaptive signal's own symbol.
        adaptive = _FakeAdaptiveSignal(
            signal_id="sig-other",
            symbol="OTHERPAIR",
            timestamp=1_700_000_000.0,
            exhaustion_type=SELL_EXHAUSTION,
            confidence=0.8,
        )
        ack = await trader.on_adaptive_signal(adaptive)

    assert ack is not None
    assert ack.accepted is True
    assert len(exch.submitted) >= 1
    entry = exch.submitted[0]
    assert entry.symbol == "BTCUSDT"


async def test_duplicate_signal_id_is_suppressed(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter, settings=_settings())

    async with adapter:
        ack1 = await trader.on_adaptive_signal(_signal(signal_id="dup-1"))
        before = len(exch.submitted)
        ack2 = await trader.on_adaptive_signal(_signal(signal_id="dup-1"))
        after = len(exch.submitted)

    assert ack1 is not None
    assert ack1.accepted is True
    assert ack2 is None
    assert before == after


async def test_kill_switch_blocks_signal(dao: SqliteDAO) -> None:
    adapter, exch, _snap, _store, risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter, settings=_settings())

    async with adapter:
        risk.trip_kill_switch("manual stop")
        ack = await trader.on_adaptive_signal(_signal())

    assert ack is None
    assert exch.submitted == []


async def test_open_position_blocks_signal(dao: SqliteDAO) -> None:
    adapter, exch, _snap, store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter, settings=_settings())

    async with adapter:
        # Seed an already-open position into the store.
        store.apply_position_update(
            PositionUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                direction=Direction.LONG,
                qty=0.001,
                entry_price=50_000.0,
                state=PositionState.OPEN,
                liquidation_price=None,
                unrealized_pnl_usd=None,
                margin_used_usd=None,
                ts=1_700_000_000.0,
            )
        )

        ack = await trader.on_adaptive_signal(_signal())

    assert ack is None
    assert exch.submitted == []


# ---------------------------------------------------------------------------
# on_adaptive_signal — concurrency
# ---------------------------------------------------------------------------


async def test_concurrent_signals_serialise_through_submit_lock(
    dao: SqliteDAO,
) -> None:
    """Two near-simultaneous distinct signals must not both pass the
    position guard. The first one opens a position; the second one
    sees it and is suppressed."""

    adapter, exch, _snap, store, _risk = await _make_adapter(dao)
    trader = HeatmapAutoTrader(adapter, settings=_settings())

    async with adapter:
        # Wrap submit_order to (a) record the position as OPEN on the
        # store so the second concurrent caller sees it after the
        # lock releases, and (b) yield to the loop so contention is
        # observable.
        original_submit = exch.submit_order

        async def submit_and_open(
            req: OrderRequest, *, timeout_s: float | None = None
        ) -> OrderAck:
            ack = await original_submit(req, timeout_s=timeout_s)
            store.apply_position_update(
                PositionUpdate(
                    venue=Venue.BINANCE_UM,
                    symbol=req.symbol,
                    direction=Direction.LONG,
                    qty=0.001,
                    entry_price=50_000.0,
                    state=PositionState.OPEN,
                    liquidation_price=None,
                    unrealized_pnl_usd=None,
                    margin_used_usd=None,
                    ts=1_700_000_000.0,
                )
            )
            await asyncio.sleep(0)
            return ack

        exch.submit_order = submit_and_open  # type: ignore[method-assign]

        sig_a = _signal(signal_id="conc-a")
        sig_b = _signal(signal_id="conc-b")

        results = await asyncio.gather(
            trader.on_adaptive_signal(sig_a),
            trader.on_adaptive_signal(sig_b),
        )

    accepted = [r for r in results if r is not None and r.accepted]
    suppressed = [r for r in results if r is None]
    # Exactly one of the two should win the entry; the other should
    # be silently suppressed once it sees the open position. Both
    # entries went through the same lock — UTA's accept path itself
    # never sees the second.
    assert len(accepted) == 1
    assert len(suppressed) == 1
    # Only one entry order on the exchange.
    entry_orders = [
        o for o in exch.submitted if not o.reduce_only and not o.close_position
    ]
    assert len(entry_orders) == 1
