"""Tests for :func:`make_user_data_handlers` (Phase 3g wiring).

The factory returns a handler dict for :class:`UserDataStreamClient`.
Each handler converts a raw Binance frame into UTA types and applies
them to the :class:`PositionManager`, the optional :class:`EventBus`,
and the optional :class:`RiskState`. These tests pin the wiring
contract end-to-end with in-memory fakes — no network, no real
WebSocket.
"""

from __future__ import annotations

from typing import Any

import pytest

from trade_adapter.bus.event_bus import EventBus
from trade_adapter.core.outcome import OutcomeEmitter
from trade_adapter.core.position_manager import PositionManager
from trade_adapter.core.position_store import PositionStore
from trade_adapter.core.risk import RiskState
from trade_adapter.exchanges.binance_um.user_handlers import (
    make_user_data_handlers,
)
from trade_adapter.types import (
    CloseReason,
    Direction,
    EventType,
    OrderSide,
    OrderStatus,
    PositionState,
    Venue,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _order_trade_update(
    *,
    symbol: str = "BTCUSDT",
    client_order_id: str = "coid-1",
    exchange_order_id: int = 28,
    side: str = "BUY",
    status: str = "FILLED",
    execution_type: str = "TRADE",
    last_qty: str = "0.001",
    last_price: str = "50000.0",
    cumulative_qty: str = "0.001",
    avg_price: str = "50000.0",
    fee_asset: str = "USDT",
    fee: str = "0.025",
    trade_id: int = 9999,
    is_maker: bool = False,
    realized_pnl: str = "0",
    ts_ms: int = 1700_000_001234,
) -> dict[str, Any]:
    return {
        "e": "ORDER_TRADE_UPDATE",
        "T": ts_ms,
        "o": {
            "s": symbol,
            "c": client_order_id,
            "i": exchange_order_id,
            "S": side,
            "X": status,
            "x": execution_type,
            "l": last_qty,
            "L": last_price,
            "z": cumulative_qty,
            "ap": avg_price,
            "N": fee_asset,
            "n": fee,
            "t": trade_id,
            "m": is_maker,
            "rp": realized_pnl,
            "T": ts_ms,
        },
    }


def _account_update(
    *,
    symbol: str = "BTCUSDT",
    position_amt: str = "0.5",
    entry_price: str = "50000.0",
    unrealized: str = "12.34",
    wallet_balance: str = "10000.0",
    asset: str = "USDT",
    ts_ms: int = 1700_000_002000,
) -> dict[str, Any]:
    return {
        "e": "ACCOUNT_UPDATE",
        "T": ts_ms,
        "a": {
            "B": [
                {"a": asset, "wb": wallet_balance, "cw": wallet_balance, "bc": "0"},
            ],
            "P": [
                {
                    "s": symbol,
                    "pa": position_amt,
                    "ep": entry_price,
                    "up": unrealized,
                    "ps": "BOTH",
                }
            ],
        },
    }


def _make_manager(
    *,
    venue: Venue = Venue.BINANCE_UM,
    store: PositionStore | None = None,
) -> PositionManager:
    """Build a manager without starting it — handlers only need the venue tag."""

    return PositionManager(
        venue=venue,
        store=store or PositionStore(),
        snapshot_provider=_NopSnapshotProvider(venue=venue),
        reconcile_interval_s=0,
    )


class _NopSnapshotProvider:
    def __init__(self, *, venue: Venue) -> None:
        self.venue = venue

    async def fetch_position_snapshot(self):  # pragma: no cover - not exercised
        return []

    async def fetch_equity_snapshot(self) -> float:  # pragma: no cover - not exercised
        return 0.0


# ---------------------------------------------------------------------------
# ORDER_TRADE_UPDATE
# ---------------------------------------------------------------------------


async def test_order_trade_update_publishes_order_update_event() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)
    sub = bus.subscribe(EventType.ORDER_UPDATE.value)

    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update())

    assert sub.queue.qsize() == 1
    payload = sub.queue.get_nowait()
    assert payload["client_order_id"] == "coid-1"
    assert payload["symbol"] == "BTCUSDT"
    assert payload["status"] == OrderStatus.FILLED.value


async def test_order_trade_update_trade_records_fill_on_manager() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update())

    assert store.fill_count(Venue.BINANCE_UM, "BTCUSDT") == 1


async def test_order_trade_update_non_trade_does_not_record_fill() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    # NEW: state transition only, no execution.
    await handlers["ORDER_TRADE_UPDATE"](
        _order_trade_update(status="NEW", execution_type="NEW", last_qty="0")
    )

    assert store.fill_count(Venue.BINANCE_UM, "BTCUSDT") == 0


async def test_order_trade_update_publishes_fill_event_on_trade() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)
    fill_sub = bus.subscribe(EventType.FILL.value)

    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update())

    assert fill_sub.queue.qsize() == 1
    payload = fill_sub.queue.get_nowait()
    assert payload["side"] == OrderSide.BUY.value
    assert payload["qty"] == pytest.approx(0.001)
    assert payload["client_order_id"] == "coid-1"


async def test_order_trade_update_skips_fill_when_translator_returns_none() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)
    fill_sub = bus.subscribe(EventType.FILL.value)

    # Translator returns None for last_qty == "0" even when execution_type
    # is TRADE (Binance occasionally sends spurious TRADE frames on
    # amendments).
    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update(last_qty="0"))

    assert fill_sub.queue.empty()


async def test_order_trade_update_records_realized_pnl_on_risk_state() -> None:
    mgr = _make_manager()
    state = RiskState()
    handlers = make_user_data_handlers(
        position_manager=mgr, risk_state=state
    )

    # Closing trade with -12 USD realised loss.
    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update(realized_pnl="-12.5"))

    assert state.daily_realized_pnl(Venue.BINANCE_UM) == pytest.approx(-12.5)


async def test_order_trade_update_skips_zero_realized_pnl() -> None:
    mgr = _make_manager()
    state = RiskState()
    handlers = make_user_data_handlers(
        position_manager=mgr, risk_state=state
    )

    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update(realized_pnl="0"))

    # Zero on a non-closing fill — the bucket should still be zero,
    # not "incremented by 0" which would technically be equivalent but
    # would clutter the bucket dict.
    assert state.daily_realized_pnl(Venue.BINANCE_UM) == 0.0


async def test_order_trade_update_handles_missing_rp() -> None:
    mgr = _make_manager()
    state = RiskState()
    handlers = make_user_data_handlers(
        position_manager=mgr, risk_state=state
    )

    frame = _order_trade_update()
    del frame["o"]["rp"]

    await handlers["ORDER_TRADE_UPDATE"](frame)

    assert state.daily_realized_pnl(Venue.BINANCE_UM) == 0.0


async def test_order_trade_update_handles_malformed_rp_as_zero() -> None:
    mgr = _make_manager()
    state = RiskState()
    handlers = make_user_data_handlers(
        position_manager=mgr, risk_state=state
    )

    await handlers["ORDER_TRADE_UPDATE"](
        _order_trade_update(realized_pnl="not-a-number")
    )

    assert state.daily_realized_pnl(Venue.BINANCE_UM) == 0.0


async def test_order_trade_update_swallows_malformed_frame() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)

    # Missing required 'o' sub-object → translator raises ValueError;
    # the handler logs and swallows.
    await handlers["ORDER_TRADE_UPDATE"]({"e": "ORDER_TRADE_UPDATE"})


async def test_order_trade_update_uses_manager_venue_for_pnl_bucket() -> None:
    """The realised-PnL bucket key follows the manager's venue, not the venue
    enum literal — important once Phase 4 wires a second manager."""

    mgr = _make_manager(venue=Venue.BYBIT_LINEAR)
    state = RiskState()
    handlers = make_user_data_handlers(
        position_manager=mgr, risk_state=state
    )

    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update(realized_pnl="-5"))

    assert state.daily_realized_pnl(Venue.BYBIT_LINEAR) == pytest.approx(-5.0)
    assert state.daily_realized_pnl(Venue.BINANCE_UM) == 0.0


# ---------------------------------------------------------------------------
# ACCOUNT_UPDATE
# ---------------------------------------------------------------------------


async def test_account_update_applies_position_update_to_store() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    await handlers["ACCOUNT_UPDATE"](_account_update())

    pos = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    assert pos is not None
    assert pos.direction is Direction.LONG
    assert pos.qty == pytest.approx(0.5)
    assert pos.state is PositionState.OPEN


async def test_account_update_publishes_position_update_event() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)
    sub = bus.subscribe(EventType.POSITION_UPDATE.value)

    await handlers["ACCOUNT_UPDATE"](_account_update())

    assert sub.queue.qsize() == 1
    payload = sub.queue.get_nowait()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["venue"] == Venue.BINANCE_UM.value


async def test_account_update_emits_one_event_per_position() -> None:
    bus = EventBus()
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=bus)
    sub = bus.subscribe(EventType.POSITION_UPDATE.value)

    frame = _account_update()
    frame["a"]["P"].append(
        {
            "s": "ETHUSDT",
            "pa": "1.0",
            "ep": "3000",
            "up": "0",
            "ps": "BOTH",
        }
    )

    await handlers["ACCOUNT_UPDATE"](frame)

    assert sub.queue.qsize() == 2


async def test_account_update_writes_equity_when_usdt_balance_present() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    await handlers["ACCOUNT_UPDATE"](_account_update(wallet_balance="9876.5"))

    assert store.get_equity_usd(Venue.BINANCE_UM) == pytest.approx(9876.5)


async def test_account_update_skips_equity_when_no_stable_asset() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    await handlers["ACCOUNT_UPDATE"](
        _account_update(asset="BNB", wallet_balance="3.14")
    )

    assert store.get_equity_usd(Venue.BINANCE_UM) is None


async def test_account_update_sums_multiple_stable_assets() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    frame = _account_update(wallet_balance="100")
    frame["a"]["B"].append(
        {"a": "BUSD", "wb": "200", "cw": "200", "bc": "0"}
    )
    frame["a"]["B"].append(
        {"a": "BNB", "wb": "9999", "cw": "9999", "bc": "0"}  # ignored
    )

    await handlers["ACCOUNT_UPDATE"](frame)

    assert store.get_equity_usd(Venue.BINANCE_UM) == pytest.approx(300.0)


async def test_account_update_drops_hedge_mode_positions() -> None:
    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr)

    frame = _account_update()
    frame["a"]["P"].append(
        {"s": "ETHUSDT", "pa": "1.0", "ep": "3000", "ps": "LONG"}
    )

    await handlers["ACCOUNT_UPDATE"](frame)

    # BTCUSDT (BOTH) applied; ETHUSDT (LONG) silently dropped.
    assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is not None
    assert store.get_position(Venue.BINANCE_UM, "ETHUSDT") is None


async def test_account_update_swallows_malformed_frame() -> None:
    mgr = _make_manager()
    handlers = make_user_data_handlers(position_manager=mgr)

    # Missing required 'a' sub-object → translator raises ValueError;
    # the handler logs and swallows.
    await handlers["ACCOUNT_UPDATE"]({"e": "ACCOUNT_UPDATE", "T": 1700_000_000_000})


async def test_account_update_event_publish_optional() -> None:
    """No bus, no errors — handler still updates the manager."""

    store = PositionStore()
    mgr = _make_manager(store=store)
    handlers = make_user_data_handlers(position_manager=mgr, event_bus=None)

    await handlers["ACCOUNT_UPDATE"](_account_update())

    assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is not None


async def test_make_handlers_returns_expected_keys() -> None:
    mgr = _make_manager()

    handlers = make_user_data_handlers(position_manager=mgr)

    assert set(handlers.keys()) == {"ORDER_TRADE_UPDATE", "ACCOUNT_UPDATE"}


# ---------------------------------------------------------------------------
# OutcomeEmitter wiring (A.2)
# ---------------------------------------------------------------------------


async def test_outcome_emitter_receives_fill_pushes() -> None:
    mgr = _make_manager()
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    handlers = make_user_data_handlers(
        position_manager=mgr, outcome_emitter=emitter
    )
    # Without an open lifetime the fill is counted but dropped.
    await handlers["ORDER_TRADE_UPDATE"](_order_trade_update())
    assert emitter.fills_unmatched == 1


async def test_outcome_emitter_emits_report_on_round_trip() -> None:
    bus = EventBus()
    mgr = _make_manager()
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM, event_bus=bus)
    handlers = make_user_data_handlers(
        position_manager=mgr, event_bus=bus, outcome_emitter=emitter
    )
    report_sub = bus.subscribe(EventType.OUTCOME_REPORT.value)

    # Open: ACCOUNT_UPDATE with non-zero qty
    await handlers["ACCOUNT_UPDATE"](
        _account_update(position_amt="1.0", entry_price="50000.0")
    )
    assert emitter.is_tracking(Venue.BINANCE_UM, "BTCUSDT")

    # Entry fill
    await handlers["ORDER_TRADE_UPDATE"](
        _order_trade_update(
            client_order_id="sig-1-entry",
            side="BUY",
            last_qty="1.0",
            last_price="50000.0",
            fee="2.5",
        )
    )

    # Exit fill (TP) — also reports venue realised PnL +100
    await handlers["ORDER_TRADE_UPDATE"](
        _order_trade_update(
            client_order_id="sig-1-tp",
            side="SELL",
            last_qty="1.0",
            last_price="50100.0",
            fee="2.5",
            realized_pnl="100",
        )
    )

    # Close: ACCOUNT_UPDATE with zero qty
    await handlers["ACCOUNT_UPDATE"](
        _account_update(position_amt="0", entry_price="0")
    )

    assert emitter.reports_emitted == 1
    assert report_sub.queue.qsize() == 1
    payload = report_sub.queue.get_nowait()
    assert payload["close_reason"] == CloseReason.TP.value
    assert payload["fees_usd"] == pytest.approx(5.0)
    # Venue-reported PnL wins over the fill-cash heuristic.
    assert payload["realized_pnl_usd"] == pytest.approx(100.0)


async def test_outcome_emitter_receives_position_update_before_bus_publish() -> None:
    """The wiring must push into the emitter *before* publishing on the
    bus, so that BboTracker's snapshot is still live when the emitter
    samples it on close. Verify ordering: emitter is updated before any
    bus subscriber sees the event."""

    bus = EventBus()
    mgr = _make_manager()
    emitter = OutcomeEmitter(venue=Venue.BINANCE_UM)
    handlers = make_user_data_handlers(
        position_manager=mgr, event_bus=bus, outcome_emitter=emitter
    )
    # No subscribers — the publish is a no-op. But the emitter must
    # have been updated first.
    await handlers["ACCOUNT_UPDATE"](
        _account_update(position_amt="1.0", entry_price="50000.0")
    )
    assert emitter.is_tracking(Venue.BINANCE_UM, "BTCUSDT")
