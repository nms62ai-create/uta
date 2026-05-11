"""Tests for :class:`trade_adapter.core.position_manager.PositionManager`."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.types import (
    Direction,
    EventType,
    Fill,
    OrderSide,
    PositionState,
    PositionUpdate,
    Venue,
)

pytestmark = pytest.mark.asyncio


@dataclass
class FakeSnapshotProvider:
    """In-memory :class:`VenueSnapshotProvider` for tests."""

    venue: Venue = Venue.BINANCE_UM
    positions: list[PositionUpdate] = field(default_factory=list)
    equity_usd: float = 10_000.0
    fetch_position_calls: int = 0
    fetch_equity_calls: int = 0
    fetch_position_error: Exception | None = None
    fetch_equity_error: Exception | None = None

    async def fetch_position_snapshot(self) -> list[PositionUpdate]:
        self.fetch_position_calls += 1
        if self.fetch_position_error is not None:
            raise self.fetch_position_error
        return list(self.positions)

    async def fetch_equity_snapshot(self) -> float:
        self.fetch_equity_calls += 1
        if self.fetch_equity_error is not None:
            raise self.fetch_equity_error
        return self.equity_usd


def _update(
    *,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    qty: float = 0.5,
    entry_price: float = 50_000.0,
    state: PositionState = PositionState.OPEN,
    ts: float = 1700_000_000.0,
) -> PositionUpdate:
    return PositionUpdate(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        direction=direction,
        qty=qty,
        entry_price=entry_price,
        state=state,
        liquidation_price=None,
        unrealized_pnl_usd=None,
        margin_used_usd=None,
        ts=ts,
    )


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


async def test_start_bootstraps_positions_into_store() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(symbol="BTCUSDT"), _update(symbol="ETHUSDT", qty=2.0)],
        equity_usd=10_000.0,
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,  # disable loop for this test
    )

    await mgr.start()
    try:
        assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is not None
        assert store.get_position(Venue.BINANCE_UM, "ETHUSDT") is not None
        assert store.get_equity_usd(Venue.BINANCE_UM) == 10_000.0
        assert snap.fetch_position_calls == 1
        assert snap.fetch_equity_calls == 1
    finally:
        await mgr.stop()


async def test_start_with_empty_venue_records_zero_positions() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(positions=[], equity_usd=500.0)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )

    await mgr.start()
    try:
        assert store.iter_positions() == []
        assert store.get_equity_usd(Venue.BINANCE_UM) == 500.0
    finally:
        await mgr.stop()


async def test_start_propagates_fetch_error() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        fetch_position_error=RuntimeError("REST boom"),
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )

    with pytest.raises(RuntimeError, match="REST boom"):
        await mgr.start()


async def test_start_twice_rejected() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            await mgr.start()
    finally:
        await mgr.stop()


async def test_negative_reconcile_interval_rejected() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    with pytest.raises(ValueError, match=r"reconcile_interval_s must be >= 0"):
        PositionManager(
            venue=Venue.BINANCE_UM,
            store=store,
            snapshot_provider=snap,
            reconcile_interval_s=-1.0,
        )


# ---------------------------------------------------------------------------
# Event ingestion (post-bootstrap)
# ---------------------------------------------------------------------------


async def test_apply_position_update_writes_to_store() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        mgr.apply_position_update(_update(qty=0.7))
        p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
        assert p is not None
        assert p.qty == 0.7
    finally:
        await mgr.stop()


async def test_apply_equity_update_writes_to_store() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        mgr.apply_equity_update(9_500.0)
        assert store.get_equity_usd(Venue.BINANCE_UM) == 9_500.0
    finally:
        await mgr.stop()


async def test_apply_fill_increments_counter() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        mgr.apply_fill(
            Fill(
                fill_id="f-1",
                client_order_id="coid-1",
                exchange_order_id="ex-1",
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                side=OrderSide.BUY,
                qty=0.1,
                price=50_000.0,
                fee_usd=0.05,
                is_maker=False,
                ts=1.0,
            )
        )
        assert store.fill_count(Venue.BINANCE_UM, "BTCUSDT") == 1
    finally:
        await mgr.stop()


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


async def test_reconcile_no_drift_returns_empty() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        summary = await mgr.reconcile_once()
        assert summary.diffs == []
    finally:
        await mgr.stop()


async def test_reconcile_detects_qty_drift() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        # Bypass apply_position_update via the manager so we have a
        # drift to detect — the snapshot will still say qty=0.5.
        snap.positions = [_update(qty=0.7)]
        summary = await mgr.reconcile_once()
        assert len(summary.diffs) == 1
        diff = summary.diffs[0]
        assert diff.kind == "drift"
        assert diff.detail["stored_qty"] == repr(0.5)
        assert diff.detail["venue_qty"] == repr(0.7)
        # Store is now updated to truth.
        p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
        assert p is not None
        assert p.qty == 0.7
    finally:
        await mgr.stop()


async def test_reconcile_detects_missing_in_store() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(positions=[], equity_usd=10_000.0)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        # Venue now has a position we have never recorded locally.
        snap.positions = [_update(qty=0.5)]
        summary = await mgr.reconcile_once()
        kinds = {d.kind for d in summary.diffs}
        assert "missing_in_store" in kinds
        assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is not None
    finally:
        await mgr.stop()


async def test_reconcile_flat_position_appearing_silent() -> None:
    """A new flat position from the venue is applied without a diff event."""

    store = PositionStore()
    snap = FakeSnapshotProvider(positions=[], equity_usd=10_000.0)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.positions = [_update(qty=0.0, state=PositionState.IDLE)]
        summary = await mgr.reconcile_once()
        # Equity hasn't changed -> only position-related diffs are
        # candidates. A new flat position should NOT generate a diff.
        position_diffs = [d for d in summary.diffs if d.symbol == "BTCUSDT"]
        assert position_diffs == []
        # But it IS written to the store.
        assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is not None
    finally:
        await mgr.stop()


async def test_reconcile_detects_missing_on_venue() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.positions = []  # symbol disappeared on venue
        summary = await mgr.reconcile_once()
        kinds = {d.kind for d in summary.diffs}
        assert "missing_on_venue" in kinds
        # Store is purged.
        assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is None
    finally:
        await mgr.stop()


async def test_reconcile_detects_equity_drift() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(positions=[], equity_usd=10_000.0)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.equity_usd = 9_500.0
        summary = await mgr.reconcile_once()
        kinds = {d.kind for d in summary.diffs}
        assert "equity_drift" in kinds
        assert store.get_equity_usd(Venue.BINANCE_UM) == 9_500.0
    finally:
        await mgr.stop()


async def test_reconcile_detects_direction_flip() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(direction=Direction.LONG, qty=0.5)],
        equity_usd=10_000.0,
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.positions = [_update(direction=Direction.SHORT, qty=0.5)]
        summary = await mgr.reconcile_once()
        assert len(summary.diffs) == 1
        assert summary.diffs[0].kind == "drift"
    finally:
        await mgr.stop()


async def test_reconcile_publishes_to_event_bus() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    bus = EventBus()
    sub = bus.subscribe(EventType.RECONCILE_DIFF.value)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        event_bus=bus,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.positions = [_update(qty=0.7)]
        await mgr.reconcile_once()
        event = sub.queue.get_nowait()
        assert event["kind"] == "drift"
        assert event["symbol"] == "BTCUSDT"
    finally:
        await mgr.stop()


async def test_reconcile_skips_publish_when_no_bus() -> None:
    """Bus is optional — manager works without one."""

    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        event_bus=None,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        snap.positions = [_update(qty=0.7)]
        summary = await mgr.reconcile_once()
        # Drift still returned, just not published.
        assert len(summary.diffs) == 1
    finally:
        await mgr.stop()


# ---------------------------------------------------------------------------
# Reconcile loop
# ---------------------------------------------------------------------------


async def test_reconcile_loop_runs_periodically() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider(positions=[], equity_usd=10_000.0)
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0.01,  # tight loop
    )
    await mgr.start()
    try:
        # Wait long enough for the loop to fire at least twice past
        # bootstrap; bootstrap counts as call #1.
        for _ in range(50):
            if snap.fetch_position_calls >= 3:
                break
            await asyncio.sleep(0.01)
        assert snap.fetch_position_calls >= 3
    finally:
        await mgr.stop()


async def test_stop_cancels_reconcile_loop() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0.01,
    )
    await mgr.start()
    await mgr.stop()
    calls_after_stop = snap.fetch_position_calls
    await asyncio.sleep(0.05)
    # No further calls after stop.
    assert snap.fetch_position_calls == calls_after_stop


async def test_stop_without_start_is_safe() -> None:
    store = PositionStore()
    snap = FakeSnapshotProvider()
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.stop()  # should not raise


# ---------------------------------------------------------------------------
# Integration with sizing math (signal_router downstream)
# ---------------------------------------------------------------------------


async def test_store_satisfies_position_provider_after_bootstrap() -> None:
    """End-to-end smoke: store + manager + signal_router can be wired."""

    store = PositionStore()
    snap = FakeSnapshotProvider(
        positions=[_update(qty=0.5)], equity_usd=10_000.0
    )
    mgr = PositionManager(
        venue=Venue.BINANCE_UM,
        store=store,
        snapshot_provider=snap,
        reconcile_interval_s=0,
    )
    await mgr.start()
    try:
        # PositionProvider.get_position
        p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
        assert p is not None
        # EquityProvider.get_equity_usd
        assert store.get_equity_usd(Venue.BINANCE_UM) == 10_000.0
    finally:
        await mgr.stop()
