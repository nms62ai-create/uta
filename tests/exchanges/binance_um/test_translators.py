"""Tests for Phase 3b wire-event translators.

Each Binance frame is hand-crafted from the public docs so the tests
double as a wire-format spec. If Binance changes a field name, exactly
one test should break — and the failing assertion will say which field
moved.
"""

from __future__ import annotations

from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.translators import (
    account_update_to_position_updates,
    agg_trade_to_trade_print,
    book_ticker_to_bbo_update,
    depth_snapshot_to_book_update,
    order_trade_update_to_fill,
    order_trade_update_to_order_update,
)
from trade_adapter.types import (
    Direction,
    OrderSide,
    OrderStatus,
    PositionState,
    Venue,
)

# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


def _agg_trade(
    *,
    symbol: str = "BTCUSDT",
    price: str = "65000.5",
    qty: str = "0.123",
    is_buyer_maker: bool = True,
    trade_time_ms: int = 1700000001234,
    agg_trade_id: int = 999_000_001,
) -> dict[str, Any]:
    return {
        "e": "aggTrade",
        "E": trade_time_ms,
        "s": symbol,
        "a": agg_trade_id,
        "p": price,
        "q": qty,
        "f": 100,
        "l": 105,
        "T": trade_time_ms,
        "m": is_buyer_maker,
    }


def test_agg_trade_buyer_maker_yields_sell_taker() -> None:
    """Binance ``m=True`` means buyer is maker, so the *taker* is the seller."""
    tp = agg_trade_to_trade_print(_agg_trade(is_buyer_maker=True))
    assert tp.venue is Venue.BINANCE_UM
    assert tp.symbol == "BTCUSDT"
    assert tp.price == pytest.approx(65000.5)
    assert tp.qty == pytest.approx(0.123)
    assert tp.side is OrderSide.SELL
    assert tp.ts == pytest.approx(1700000001.234)
    assert tp.trade_id == "999000001"


def test_agg_trade_seller_maker_yields_buy_taker() -> None:
    tp = agg_trade_to_trade_print(_agg_trade(is_buyer_maker=False))
    assert tp.side is OrderSide.BUY


def test_agg_trade_missing_field_raises() -> None:
    bad = _agg_trade()
    del bad["p"]
    with pytest.raises(ValueError, match="missing required field 'p'"):
        agg_trade_to_trade_print(bad)


def test_agg_trade_invalid_price_raises() -> None:
    bad = _agg_trade(price="not-a-number")
    with pytest.raises(ValueError, match="not convertible to float"):
        agg_trade_to_trade_print(bad)


def _book_ticker(
    *,
    symbol: str = "BTCUSDT",
    bid_price: str = "65000.0",
    bid_qty: str = "1.5",
    ask_price: str = "65000.1",
    ask_qty: str = "2.0",
    transaction_time_ms: int = 1700000005000,
) -> dict[str, Any]:
    return {
        "e": "bookTicker",
        "u": 400900217,
        "E": transaction_time_ms,
        "T": transaction_time_ms,
        "s": symbol,
        "b": bid_price,
        "B": bid_qty,
        "a": ask_price,
        "A": ask_qty,
    }


def test_book_ticker_to_bbo_update() -> None:
    bbo = book_ticker_to_bbo_update(_book_ticker())
    assert bbo.venue is Venue.BINANCE_UM
    assert bbo.symbol == "BTCUSDT"
    assert bbo.bid_price == pytest.approx(65000.0)
    assert bbo.bid_qty == pytest.approx(1.5)
    assert bbo.ask_price == pytest.approx(65000.1)
    assert bbo.ask_qty == pytest.approx(2.0)
    assert bbo.ts == pytest.approx(1700000005.0)


def test_book_ticker_falls_back_to_event_time() -> None:
    """When ``T`` is missing, ``E`` should be used as the timestamp."""

    raw = _book_ticker()
    del raw["T"]
    bbo = book_ticker_to_bbo_update(raw)
    assert bbo.ts == pytest.approx(1700000005.0)


def test_book_ticker_missing_both_timestamps_raises() -> None:
    raw = _book_ticker()
    del raw["T"]
    del raw["E"]
    with pytest.raises(ValueError, match="missing both 'T' and 'E'"):
        book_ticker_to_bbo_update(raw)


def _depth_snapshot(
    *,
    symbol: str = "BTCUSDT",
    bids: list[list[str]] | None = None,
    asks: list[list[str]] | None = None,
    transaction_time_ms: int = 1700000010000,
    update_id: int = 12345,
) -> dict[str, Any]:
    return {
        "e": "depthUpdate",
        "E": transaction_time_ms,
        "T": transaction_time_ms,
        "s": symbol,
        "U": update_id - 5,
        "u": update_id,
        "pu": update_id - 7,
        "b": bids if bids is not None else [["65000.0", "1.5"], ["64999.9", "0.5"]],
        "a": asks if asks is not None else [["65000.1", "2.0"], ["65000.2", "0.1"]],
    }


def test_depth_snapshot_to_book_update() -> None:
    bu = depth_snapshot_to_book_update(_depth_snapshot())
    assert bu.venue is Venue.BINANCE_UM
    assert bu.symbol == "BTCUSDT"
    assert len(bu.bids) == 2
    assert bu.bids[0].price == pytest.approx(65000.0)
    assert bu.bids[0].qty == pytest.approx(1.5)
    assert len(bu.asks) == 2
    assert bu.asks[1].price == pytest.approx(65000.2)
    assert bu.ts == pytest.approx(1700000010.0)
    assert bu.sequence == 12345


def test_depth_snapshot_with_empty_sides() -> None:
    bu = depth_snapshot_to_book_update(_depth_snapshot(bids=[], asks=[]))
    assert bu.bids == ()
    assert bu.asks == ()


def test_depth_snapshot_malformed_level_raises() -> None:
    bad = _depth_snapshot(bids=[["65000.0"]])  # missing qty
    with pytest.raises(ValueError, match="malformed book"):
        depth_snapshot_to_book_update(bad)


def test_depth_snapshot_non_list_side_raises() -> None:
    bad = _depth_snapshot()
    bad["b"] = "not-a-list"
    with pytest.raises(ValueError, match="expected list"):
        depth_snapshot_to_book_update(bad)


# ---------------------------------------------------------------------------
# Order events
# ---------------------------------------------------------------------------


def _order_trade_update(
    *,
    symbol: str = "BTCUSDT",
    client_order_id: str = "my-coid",
    order_id: int = 11111,
    side: str = "BUY",
    status: str = "NEW",
    execution_type: str = "NEW",
    last_qty: str = "0",
    cum_filled: str = "0",
    last_price: str = "0",
    avg_price: str = "0",
    trade_id: int = 0,
    commission: str = "0",
    commission_asset: str = "USDT",
    is_maker: bool = False,
    trade_time_ms: int = 1700000020000,
) -> dict[str, Any]:
    return {
        "e": "ORDER_TRADE_UPDATE",
        "E": trade_time_ms,
        "T": trade_time_ms,
        "o": {
            "s": symbol,
            "c": client_order_id,
            "S": side,
            "o": "MARKET",
            "f": "GTC",
            "q": "0.001",
            "p": "0",
            "ap": avg_price,
            "sp": "0",
            "x": execution_type,
            "X": status,
            "i": order_id,
            "l": last_qty,
            "z": cum_filled,
            "L": last_price,
            "N": commission_asset,
            "n": commission,
            "T": trade_time_ms,
            "t": trade_id,
            "b": "0",
            "a": "0",
            "m": is_maker,
            "R": False,
            "wt": "CONTRACT_PRICE",
            "ot": "MARKET",
            "ps": "BOTH",
            "cp": False,
            "rp": "0",
        },
    }


def test_order_trade_update_new_to_order_update() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(status="NEW"))
    assert ou.client_order_id == "my-coid"
    assert ou.exchange_order_id == "11111"
    assert ou.venue is Venue.BINANCE_UM
    assert ou.symbol == "BTCUSDT"
    assert ou.status is OrderStatus.ACK
    assert ou.filled_qty == 0.0
    assert ou.avg_fill_price is None
    assert ou.ts == pytest.approx(1700000020.0)


def test_order_trade_update_partially_filled() -> None:
    ou = order_trade_update_to_order_update(
        _order_trade_update(
            status="PARTIALLY_FILLED",
            execution_type="TRADE",
            last_qty="0.001",
            cum_filled="0.001",
            last_price="65000.0",
            avg_price="65000.0",
            trade_id=42,
        )
    )
    assert ou.status is OrderStatus.PARTIALLY_FILLED
    assert ou.filled_qty == pytest.approx(0.001)
    assert ou.avg_fill_price == pytest.approx(65000.0)


def test_order_trade_update_filled() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(status="FILLED"))
    assert ou.status is OrderStatus.FILLED


def test_order_trade_update_canceled_maps_to_double_l_cancelled() -> None:
    """Binance spells it 'CANCELED'; we normalise to ``OrderStatus.CANCELLED``."""

    ou = order_trade_update_to_order_update(_order_trade_update(status="CANCELED"))
    assert ou.status is OrderStatus.CANCELLED


def test_order_trade_update_rejected() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(status="REJECTED"))
    assert ou.status is OrderStatus.REJECTED


def test_order_trade_update_expired() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(status="EXPIRED"))
    assert ou.status is OrderStatus.EXPIRED


def test_order_trade_update_expired_in_match_normalises_to_expired() -> None:
    ou = order_trade_update_to_order_update(
        _order_trade_update(status="EXPIRED_IN_MATCH")
    )
    assert ou.status is OrderStatus.EXPIRED


def test_order_trade_update_unknown_status_falls_back_to_ambiguous() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(status="WHATEVER"))
    assert ou.status is OrderStatus.AMBIGUOUS


def test_order_trade_update_zero_avg_price_yields_none() -> None:
    ou = order_trade_update_to_order_update(_order_trade_update(avg_price="0"))
    assert ou.avg_fill_price is None


def test_order_trade_update_missing_required_field_raises() -> None:
    bad = _order_trade_update()
    del bad["o"]["c"]
    with pytest.raises(ValueError, match="missing required field 'c'"):
        order_trade_update_to_order_update(bad)


def test_order_trade_update_non_object_o_raises() -> None:
    bad: dict[str, Any] = {"e": "ORDER_TRADE_UPDATE", "o": "not-a-dict"}
    with pytest.raises(ValueError, match="'o' field is not an object"):
        order_trade_update_to_order_update(bad)


# ---------------------------------------------------------------------------
# Fill events
# ---------------------------------------------------------------------------


def test_fill_only_on_trade_execution_type() -> None:
    """``x="NEW"`` carries no fill, even with non-zero ``l``."""

    fill = order_trade_update_to_fill(
        _order_trade_update(execution_type="NEW", last_qty="0.1", trade_id=1)
    )
    assert fill is None


def test_fill_with_zero_last_qty_returns_none() -> None:
    """Sometimes Binance sends ``x="TRADE"`` with ``l="0"`` on amendments."""

    fill = order_trade_update_to_fill(
        _order_trade_update(execution_type="TRADE", last_qty="0", trade_id=42)
    )
    assert fill is None


def test_fill_with_zero_trade_id_returns_none() -> None:
    fill = order_trade_update_to_fill(
        _order_trade_update(execution_type="TRADE", last_qty="0.001", trade_id=0)
    )
    assert fill is None


def test_fill_translates_full_shape() -> None:
    fill = order_trade_update_to_fill(
        _order_trade_update(
            execution_type="TRADE",
            side="SELL",
            last_qty="0.001",
            last_price="65000.10",
            commission="0.026",
            commission_asset="USDT",
            trade_id=999,
            is_maker=True,
        )
    )
    assert fill is not None
    assert fill.fill_id == "999"
    assert fill.client_order_id == "my-coid"
    assert fill.exchange_order_id == "11111"
    assert fill.venue is Venue.BINANCE_UM
    assert fill.symbol == "BTCUSDT"
    assert fill.side is OrderSide.SELL
    assert fill.qty == pytest.approx(0.001)
    assert fill.price == pytest.approx(65000.10)
    assert fill.fee_usd == pytest.approx(0.026)
    assert fill.is_maker is True
    assert fill.ts == pytest.approx(1700000020.0)


def test_fill_non_usdt_fee_surfaces_as_zero() -> None:
    """BNB-denominated commission isn't FX-converted in v1 — it surfaces as 0."""

    fill = order_trade_update_to_fill(
        _order_trade_update(
            execution_type="TRADE",
            last_qty="0.001",
            last_price="100",
            commission="0.0001",
            commission_asset="BNB",
            trade_id=1,
        )
    )
    assert fill is not None
    assert fill.fee_usd == 0.0


# ---------------------------------------------------------------------------
# Account / position events
# ---------------------------------------------------------------------------


def _account_update(
    *,
    positions: list[dict[str, Any]] | None = None,
    ts_ms: int = 1700000030000,
) -> dict[str, Any]:
    return {
        "e": "ACCOUNT_UPDATE",
        "E": ts_ms,
        "T": ts_ms,
        "a": {
            "m": "ORDER",
            "B": [{"a": "USDT", "wb": "10000", "cw": "10000", "bc": "0"}],
            "P": positions
            if positions is not None
            else [
                {
                    "s": "BTCUSDT",
                    "pa": "0.001",
                    "ep": "65000",
                    "cr": "0",
                    "up": "1.50",
                    "mt": "isolated",
                    "iw": "65.0",
                    "ps": "BOTH",
                }
            ],
        },
    }


def test_account_update_long_position() -> None:
    out = account_update_to_position_updates(_account_update())
    assert len(out) == 1
    pu = out[0]
    assert pu.venue is Venue.BINANCE_UM
    assert pu.symbol == "BTCUSDT"
    assert pu.direction is Direction.LONG
    assert pu.qty == pytest.approx(0.001)
    assert pu.entry_price == pytest.approx(65000.0)
    assert pu.state is PositionState.OPEN
    assert pu.unrealized_pnl_usd == pytest.approx(1.50)
    assert pu.ts == pytest.approx(1700000030.0)


def test_account_update_short_position() -> None:
    out = account_update_to_position_updates(
        _account_update(
            positions=[
                {
                    "s": "ETHUSDT",
                    "pa": "-0.5",
                    "ep": "3000",
                    "up": "-2.0",
                    "ps": "BOTH",
                }
            ]
        )
    )
    assert len(out) == 1
    pu = out[0]
    assert pu.direction is Direction.SHORT
    assert pu.qty == pytest.approx(0.5)
    assert pu.state is PositionState.OPEN
    assert pu.unrealized_pnl_usd == pytest.approx(-2.0)


def test_account_update_flat_position_is_idle() -> None:
    out = account_update_to_position_updates(
        _account_update(
            positions=[
                {"s": "BTCUSDT", "pa": "0", "ep": "0", "up": "0", "ps": "BOTH"}
            ]
        )
    )
    assert len(out) == 1
    pu = out[0]
    assert pu.qty == 0.0
    assert pu.state is PositionState.IDLE


def test_account_update_multi_symbol() -> None:
    out = account_update_to_position_updates(
        _account_update(
            positions=[
                {"s": "BTCUSDT", "pa": "0.001", "ep": "65000", "up": "1", "ps": "BOTH"},
                {"s": "ETHUSDT", "pa": "-0.5", "ep": "3000", "up": "-1", "ps": "BOTH"},
                {"s": "SOLUSDT", "pa": "0", "ep": "0", "up": "0", "ps": "BOTH"},
            ]
        )
    )
    symbols = [p.symbol for p in out]
    assert symbols == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    assert out[0].direction is Direction.LONG
    assert out[1].direction is Direction.SHORT
    assert out[2].state is PositionState.IDLE


def test_account_update_skips_hedge_mode_entries() -> None:
    """``ps="LONG"`` / ``"SHORT"`` indicate Hedge mode — out of scope for v1."""

    out = account_update_to_position_updates(
        _account_update(
            positions=[
                {"s": "BTCUSDT", "pa": "0.001", "ep": "65000", "up": "1", "ps": "LONG"},
                {"s": "BTCUSDT", "pa": "0", "ep": "0", "up": "0", "ps": "SHORT"},
                {"s": "ETHUSDT", "pa": "0.5", "ep": "3000", "up": "0", "ps": "BOTH"},
            ]
        )
    )
    assert len(out) == 1
    assert out[0].symbol == "ETHUSDT"


def test_account_update_missing_unrealized_yields_none() -> None:
    out = account_update_to_position_updates(
        _account_update(
            positions=[
                {"s": "BTCUSDT", "pa": "0.001", "ep": "65000", "ps": "BOTH"}
            ]
        )
    )
    assert out[0].unrealized_pnl_usd is None


def test_account_update_empty_positions() -> None:
    out = account_update_to_position_updates(_account_update(positions=[]))
    assert out == []


def test_account_update_non_object_a_raises() -> None:
    bad: dict[str, Any] = {"e": "ACCOUNT_UPDATE", "T": 1, "a": "not-a-dict"}
    with pytest.raises(ValueError, match="'a' field is not an object"):
        account_update_to_position_updates(bad)


def test_account_update_non_list_positions_raises() -> None:
    bad: dict[str, Any] = {
        "e": "ACCOUNT_UPDATE",
        "T": 1,
        "a": {"m": "ORDER", "P": "not-a-list"},
    }
    with pytest.raises(ValueError, match=r"'a\.P' is not a list"):
        account_update_to_position_updates(bad)
