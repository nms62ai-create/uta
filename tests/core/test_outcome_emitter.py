"""Unit tests for :class:`OutcomeEmitter` (A.2)."""

from __future__ import annotations

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.outcome import OutcomeEmitter
from trade_adapter.marketdata.bbo_tracker import (
    BboTracker,
    PositionBboSnapshot,
)
from trade_adapter.types import (
    BBOUpdate,
    CloseReason,
    Direction,
    EventType,
    Fill,
    OrderSide,
    PositionState,
    PositionUpdate,
    Venue,
)


def _position_update(
    *,
    symbol: str = "BTCUSDT",
    venue: Venue = Venue.BINANCE_UM,
    direction: Direction = Direction.LONG,
    qty: float = 1.0,
    entry_price: float = 50000.0,
    state: PositionState = PositionState.OPEN,
    ts: float = 1700_000_000.0,
    signal_id: str | None = "sig-1",
    correlation_id: str | None = "corr-1",
) -> PositionUpdate:
    return PositionUpdate(
        venue=venue,
        symbol=symbol,
        direction=direction,
        qty=qty,
        entry_price=entry_price,
        state=state,
        liquidation_price=None,
        unrealized_pnl_usd=None,
        margin_used_usd=None,
        ts=ts,
        signal_id=signal_id,
        correlation_id=correlation_id,
    )


def _fill(
    *,
    side: OrderSide = OrderSide.BUY,
    qty: float = 1.0,
    price: float = 50000.0,
    fee_usd: float = 2.5,
    symbol: str = "BTCUSDT",
    venue: Venue = Venue.BINANCE_UM,
    ts: float = 1700_000_000.0,
    client_order_id: str = "sig-1-entry",
    signal_id: str | None = "sig-1",
) -> Fill:
    return Fill(
        fill_id="f1",
        client_order_id=client_order_id,
        exchange_order_id="ex1",
        venue=venue,
        symbol=symbol,
        side=side,
        qty=qty,
        price=price,
        fee_usd=fee_usd,
        is_maker=False,
        ts=ts,
        signal_id=signal_id,
        correlation_id=None,
    )


# ---------------------------------------------------------------------------
# Lifecycle basics
# ---------------------------------------------------------------------------


def test_open_then_close_emits_one_report() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    open_report = emitter.apply_position_update(
        _position_update(qty=1.0, ts=1000.0)
    )
    assert open_report is None
    assert emitter.is_tracking(Venue.BINANCE_UM, "BTCUSDT")

    closed_report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE, ts=2000.0)
    )
    assert closed_report is not None
    assert emitter.reports_emitted == 1
    assert emitter.last_report is closed_report
    assert not emitter.is_tracking(Venue.BINANCE_UM, "BTCUSDT")
    assert closed_report.signal_id == "sig-1"
    assert closed_report.correlation_id == "corr-1"
    assert closed_report.holding_time_s == pytest.approx(1000.0)
    assert closed_report.opened_at == pytest.approx(1000.0)
    assert closed_report.closed_at == pytest.approx(2000.0)


def test_flat_only_emits_nothing() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is None
    assert emitter.reports_emitted == 0


def test_intermediate_updates_do_not_emit() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    # qty grew → still open
    mid = emitter.apply_position_update(_position_update(qty=2.0))
    # qty shrank but still > 0 → still open
    mid2 = emitter.apply_position_update(_position_update(qty=0.5))
    assert mid is None
    assert mid2 is None
    assert emitter.reports_emitted == 0


# ---------------------------------------------------------------------------
# Entry-price refresh on adds (weighted avg)
# ---------------------------------------------------------------------------


def test_entry_price_refreshed_on_growing_position() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0, entry_price=50000.0))
    # Position adds; Binance updates ep to weighted avg
    emitter.apply_position_update(_position_update(qty=2.0, entry_price=49500.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE, ts=1700_000_100.0)
    )
    assert report is not None
    # entry_price should reflect the growth, not the first open
    assert report.entry_price == pytest.approx(49500.0)
    # max_qty captured
    assert report.qty == pytest.approx(2.0)


def test_entry_price_unchanged_on_reduction() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=2.0, entry_price=50000.0))
    # Reduction — ep should NOT change in real Binance, and our code shouldn't either
    emitter.apply_position_update(_position_update(qty=1.0, entry_price=99999.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.entry_price == pytest.approx(50000.0)
    assert report.qty == pytest.approx(2.0)


# ---------------------------------------------------------------------------
# Fees / realised PnL accounting
# ---------------------------------------------------------------------------


def test_fees_summed_across_fills() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(side=OrderSide.BUY, fee_usd=2.5))
    emitter.apply_fill(_fill(side=OrderSide.SELL, fee_usd=3.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.fees_usd == pytest.approx(5.5)


def test_realized_pnl_from_fills_when_no_venue_value() -> None:
    """Long round trip: BUY 1@100, SELL 1@110, fees=2.5+3.0=5.5 → +4.5"""

    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(side=OrderSide.BUY, qty=1.0, price=100.0, fee_usd=2.5))
    emitter.apply_fill(_fill(side=OrderSide.SELL, qty=1.0, price=110.0, fee_usd=3.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    # (110 - 100) - 5.5 = 4.5
    assert report.realized_pnl_usd == pytest.approx(4.5)


def test_realized_pnl_prefers_venue_reported_value() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(side=OrderSide.BUY, qty=1.0, price=100.0, fee_usd=2.5))
    emitter.apply_fill(_fill(side=OrderSide.SELL, qty=1.0, price=110.0, fee_usd=3.0))
    # Venue says PnL is 7.3 (e.g. funding adjustment) — that wins
    emitter.record_realized_pnl(Venue.BINANCE_UM, "BTCUSDT", 7.3)
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.realized_pnl_usd == pytest.approx(7.3)


def test_realized_pnl_accumulates_multiple_venue_values() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.record_realized_pnl(Venue.BINANCE_UM, "BTCUSDT", 2.0)
    emitter.record_realized_pnl(Venue.BINANCE_UM, "BTCUSDT", -0.5)
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.realized_pnl_usd == pytest.approx(1.5)


def test_record_pnl_before_open_is_dropped() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    # No lifetime — should be a no-op, not an exception
    emitter.record_realized_pnl(Venue.BINANCE_UM, "BTCUSDT", 5.0)
    # Now open + close; venue value was not captured (no error)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(side=OrderSide.BUY, fee_usd=1.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    # Falls back to fill cash: BUY 1@50000, no SELL → -50000 - 1 = -50001
    # The exact number is incidental — the point is the dropped pre-open
    # record was not retroactively included.
    assert report.realized_pnl_usd == pytest.approx(-50001.0)


# ---------------------------------------------------------------------------
# Close-reason inference from client_order_id
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("coid", "expected"),
    [
        ("sig-1-sl", CloseReason.SL),
        ("sig-1-tp", CloseReason.TP),
        ("sig-1-entry", CloseReason.SIGNAL),
        ("autoclose-abc", CloseReason.MANUAL),
        ("", CloseReason.MANUAL),
    ],
)
def test_close_reason_inferred_from_last_fill_coid(
    coid: str, expected: CloseReason
) -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(client_order_id="sig-1-entry"))
    if coid:
        emitter.apply_fill(_fill(client_order_id=coid, side=OrderSide.SELL))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    if coid == "":
        # No closing fill at all — coid stays at "sig-1-entry" (SIGNAL)
        assert report.close_reason is CloseReason.SIGNAL
    else:
        assert report.close_reason is expected


def test_close_reason_hint_overrides_inferred() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(client_order_id="sig-1-entry"))
    emitter.apply_fill(_fill(client_order_id="sig-1-tp", side=OrderSide.SELL))
    # Reconciler discovers the position was actually liquidated
    emitter.record_close_reason_hint(
        Venue.BINANCE_UM, "BTCUSDT", CloseReason.LIQUIDATION
    )
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.close_reason is CloseReason.LIQUIDATION


# ---------------------------------------------------------------------------
# MFE / MAE from BBO snapshot
# ---------------------------------------------------------------------------


class _StubTracker:
    """Minimal stand-in for :class:`BboTracker` — returns a canned snapshot."""

    def __init__(self, snapshot: PositionBboSnapshot | None) -> None:
        self._snapshot = snapshot

    def get_snapshot(self, venue: Venue, symbol: str) -> PositionBboSnapshot | None:
        return self._snapshot


def _snapshot(
    *,
    first_mid: float = 50000.0,
    min_bid: float = 49500.0,
    max_ask: float = 50500.0,
    tick_count: int = 5,
) -> PositionBboSnapshot:
    return PositionBboSnapshot(
        venue=Venue.BINANCE_UM,
        symbol="BTCUSDT",
        opened_at=1000.0,
        tick_count=tick_count,
        first_mid=first_mid,
        last_mid=first_mid,
        min_bid=min_bid,
        max_ask=max_ask,
        last_ts=2000.0,
    )


def test_mfe_mae_long_position() -> None:
    """LONG @ 50000; market ranged 49500..50500 → MFE/MAE both = 100 bps."""

    tracker = _StubTracker(_snapshot(first_mid=50000.0))
    emitter = OutcomeEmitter(
        venue=Venue.BINANCE_UM, bbo_tracker=tracker
    )
    emitter.apply_position_update(
        _position_update(direction=Direction.LONG, entry_price=50000.0, qty=1.0)
    )
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    # (50500 - 50000) / 50000 * 10000 = 100 bps
    assert report.mfe_bps == pytest.approx(100.0)
    assert report.mae_bps == pytest.approx(100.0)


def test_mfe_mae_short_position() -> None:
    """SHORT @ 50000; market ranged 49500..50500.

    Short MFE = downward excursion of the bid = 50000 - 49500 = 500 → 100 bps.
    Short MAE = upward excursion of the ask = 50500 - 50000 = 500 → 100 bps.
    """

    tracker = _StubTracker(_snapshot(first_mid=50000.0))
    emitter = OutcomeEmitter(
        venue=Venue.BINANCE_UM, bbo_tracker=tracker
    )
    emitter.apply_position_update(
        _position_update(direction=Direction.SHORT, entry_price=50000.0, qty=1.0)
    )
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE, direction=Direction.SHORT)
    )
    assert report is not None
    assert report.mfe_bps == pytest.approx(100.0)
    assert report.mae_bps == pytest.approx(100.0)


def test_mfe_mae_zero_when_no_ticks() -> None:
    tracker = _StubTracker(_snapshot(tick_count=0))
    emitter = OutcomeEmitter(
        venue=Venue.BINANCE_UM, bbo_tracker=tracker
    )
    emitter.apply_position_update(_position_update(qty=1.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.mfe_bps == 0.0
    assert report.mae_bps == 0.0


def test_mfe_mae_clamps_to_zero_on_adverse_envelope() -> None:
    """LONG @ 60000 but BBO envelope sat below entry (49500..50500)
    → MFE = 0 (no favourable excursion above entry), MAE > 0."""

    tracker = _StubTracker(_snapshot(first_mid=50000.0))
    emitter = OutcomeEmitter(
        venue=Venue.BINANCE_UM, bbo_tracker=tracker
    )
    emitter.apply_position_update(
        _position_update(direction=Direction.LONG, entry_price=60000.0, qty=1.0)
    )
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    # Ask peaked at 50500 < entry 60000 — MFE clamps to 0
    assert report.mfe_bps == 0.0
    # Bid dropped to 49500 below entry — MAE > 0
    assert report.mae_bps > 0.0


# ---------------------------------------------------------------------------
# Slippage
# ---------------------------------------------------------------------------


def test_slippage_uses_first_fill_vs_first_mid() -> None:
    tracker = _StubTracker(_snapshot(first_mid=50000.0))
    emitter = OutcomeEmitter(
        venue=Venue.BINANCE_UM, bbo_tracker=tracker
    )
    emitter.apply_position_update(_position_update(qty=1.0))
    # First fill at 50025 → slippage = 25/50000 * 10000 = 5 bps
    emitter.apply_fill(_fill(price=50025.0))
    emitter.apply_fill(
        _fill(price=50050.0, side=OrderSide.SELL, client_order_id="sig-1-tp")
    )
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.slippage_bps == pytest.approx(5.0)


def test_slippage_zero_without_snapshot() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(price=50025.0))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.slippage_bps == 0.0


# ---------------------------------------------------------------------------
# Bus publishing
# ---------------------------------------------------------------------------


def test_emit_publishes_outcome_report_on_bus() -> None:
    bus = EventBus()
    sub = bus.subscribe(EventType.OUTCOME_REPORT.value)
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM, event_bus=bus)
    emitter.apply_position_update(_position_update(qty=1.0, ts=1000.0))
    emitter.apply_fill(_fill(side=OrderSide.BUY, price=50000.0, fee_usd=1.0))
    emitter.apply_fill(
        _fill(
            side=OrderSide.SELL, price=50100.0, fee_usd=1.0,
            client_order_id="sig-1-tp",
        )
    )
    emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE, ts=2000.0)
    )
    assert sub.queue.qsize() == 1
    payload = sub.queue.get_nowait()
    assert payload["signal_id"] == "sig-1"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["venue"] == Venue.BINANCE_UM.value
    assert payload["close_reason"] == CloseReason.TP.value
    assert payload["holding_time_s"] == pytest.approx(1000.0)
    assert payload["fees_usd"] == pytest.approx(2.0)
    assert payload["realized_pnl_usd"] == pytest.approx(98.0)  # 100 - 2


def test_no_bus_still_records_last_report() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM, event_bus=None)
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert emitter.last_report is not None
    assert emitter.reports_emitted == 1


# ---------------------------------------------------------------------------
# Fills before lifetime open — defensive bookkeeping
# ---------------------------------------------------------------------------


def test_fill_before_open_is_counted_and_dropped() -> None:
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    emitter.apply_fill(_fill())  # arrives before any PositionUpdate
    assert emitter.fills_unmatched == 1
    # Now open + close normally; the lost fill is gone for accounting
    emitter.apply_position_update(_position_update(qty=1.0))
    emitter.apply_fill(_fill(side=OrderSide.SELL, client_order_id="sig-1-tp"))
    report = emitter.apply_position_update(
        _position_update(qty=0.0, state=PositionState.IDLE)
    )
    assert report is not None
    assert report.fees_usd == pytest.approx(2.5)  # only the post-open fill


# ---------------------------------------------------------------------------
# End-to-end with real BboTracker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_emitter_reads_live_snapshot_before_tracker_tears_down() -> None:
    """The user-data wiring pushes ``apply_position_update`` into the emitter
    *before* the bus publish that wakes :class:`BboTracker`. This test
    simulates that ordering: we put a snapshot in the tracker, push the
    flat update into the emitter, and verify it captured the snapshot
    even though the bus event hasn't fired yet."""

    from trade_adapter.marketdata.hub import MarketDataHub

    class _NopProvider:
        venue = Venue.BINANCE_UM

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def subscribe_stream(self, symbol, kind, callback):
            return None

        async def unsubscribe_stream(self, symbol, kind):
            return None

    bus = EventBus()
    provider = _NopProvider()
    hub = MarketDataHub(providers={Venue.BINANCE_UM: provider})
    await hub.start()
    tracker = BboTracker(event_bus=bus, market_data_hub=hub)
    await tracker.start()
    try:
        # Open + seed a fake snapshot directly on the tracker (we can
        # only do this once the tracker has already auto-subscribed).
        bus.publish(
            EventType.POSITION_UPDATE.value,
            _position_update(qty=1.0),
        )
        # Wait for the tracker to subscribe
        import asyncio
        for _ in range(50):
            if tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT") is not None:
                break
            await asyncio.sleep(0.01)
        snap = tracker.get_snapshot(Venue.BINANCE_UM, "BTCUSDT")
        assert snap is not None
        # Inject ticks via direct ingest (no provider in this test).
        snap.ingest(
            BBOUpdate(
                venue=Venue.BINANCE_UM,
                symbol="BTCUSDT",
                bid_price=49500.0,
                bid_qty=1.0,
                ask_price=50500.0,
                ask_qty=1.0,
                ts=1700_000_100.0,
            )
        )
        emitter = OutcomeEmitter(
            venue=Venue.BINANCE_UM, bbo_tracker=tracker
        )
        # Mimic user_handlers ordering: open the lifetime first (the
        # tracker had already been opened above; emitter needs its own
        # open event).
        emitter.apply_position_update(_position_update(qty=1.0))
        report = emitter.apply_position_update(
            _position_update(qty=0.0, state=PositionState.IDLE)
        )
        assert report is not None
        # Excursions should be non-zero — the snapshot was alive.
        assert report.mfe_bps > 0.0
        assert report.mae_bps > 0.0
    finally:
        await tracker.close()
        await hub.close()
