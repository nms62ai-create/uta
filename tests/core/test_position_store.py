"""Tests for :class:`trade_adapter.core.position_store.PositionStore`."""

from __future__ import annotations

import pytest

from trade_adapter.core.position_store import PositionStore
from trade_adapter.types import (
    Direction,
    Fill,
    OrderSide,
    PositionState,
    PositionUpdate,
    Venue,
)


def _update(
    *,
    venue: Venue = Venue.BINANCE_UM,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    qty: float = 0.5,
    entry_price: float = 50_000.0,
    state: PositionState = PositionState.OPEN,
    ts: float = 1700_000_000.0,
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
    )


def _fill(
    *, venue: Venue = Venue.BINANCE_UM, symbol: str = "BTCUSDT"
) -> Fill:
    return Fill(
        fill_id="f-1",
        client_order_id="coid-1",
        exchange_order_id="ex-1",
        venue=venue,
        symbol=symbol,
        side=OrderSide.BUY,
        qty=0.1,
        price=50_000.0,
        fee_usd=0.05,
        is_maker=False,
        ts=1700_000_000.0,
    )


# ---------------------------------------------------------------------------
# Position state
# ---------------------------------------------------------------------------


def test_empty_store_returns_none() -> None:
    store = PositionStore()
    assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is None
    assert store.iter_positions() == []
    assert store.open_positions() == []


def test_apply_position_update_writes_through() -> None:
    store = PositionStore()
    store.apply_position_update(_update(qty=0.5))

    p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    assert p is not None
    assert p.qty == 0.5
    assert p.direction is Direction.LONG
    assert p.state is PositionState.OPEN
    assert p.entry_price == 50_000.0


def test_apply_position_update_overwrites_previous() -> None:
    store = PositionStore()
    store.apply_position_update(_update(qty=0.5))
    store.apply_position_update(_update(qty=0.7, entry_price=51_000.0))

    p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    assert p is not None
    assert p.qty == 0.7
    assert p.entry_price == 51_000.0


def test_flat_position_retained_in_store() -> None:
    store = PositionStore()
    store.apply_position_update(_update(qty=0.0, state=PositionState.IDLE))

    p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    assert p is not None
    assert p.qty == 0.0
    assert p.state is PositionState.IDLE


def test_open_positions_filters_flat() -> None:
    store = PositionStore()
    store.apply_position_update(
        _update(symbol="BTCUSDT", qty=0.5, state=PositionState.OPEN)
    )
    store.apply_position_update(
        _update(symbol="ETHUSDT", qty=0.0, state=PositionState.IDLE)
    )

    opens = store.open_positions()
    assert len(opens) == 1
    assert opens[0].symbol == "BTCUSDT"


def test_remove_position_drops_from_store() -> None:
    store = PositionStore()
    store.apply_position_update(_update())
    store.remove_position(Venue.BINANCE_UM, "BTCUSDT")
    assert store.get_position(Venue.BINANCE_UM, "BTCUSDT") is None


def test_remove_nonexistent_position_is_noop() -> None:
    store = PositionStore()
    store.remove_position(Venue.BINANCE_UM, "NOPE")  # should not raise


def test_two_symbols_tracked_independently() -> None:
    store = PositionStore()
    store.apply_position_update(_update(symbol="BTCUSDT", qty=0.5))
    store.apply_position_update(_update(symbol="ETHUSDT", qty=2.0))

    btc = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    eth = store.get_position(Venue.BINANCE_UM, "ETHUSDT")
    assert btc is not None and btc.qty == 0.5
    assert eth is not None and eth.qty == 2.0


# ---------------------------------------------------------------------------
# Equity
# ---------------------------------------------------------------------------


def test_get_equity_returns_none_until_set() -> None:
    store = PositionStore()
    assert store.get_equity_usd(Venue.BINANCE_UM) is None


def test_apply_equity_update_overwrites() -> None:
    store = PositionStore()
    store.apply_equity_update(Venue.BINANCE_UM, 10_000.0)
    assert store.get_equity_usd(Venue.BINANCE_UM) == 10_000.0
    store.apply_equity_update(Venue.BINANCE_UM, 9_500.0)
    assert store.get_equity_usd(Venue.BINANCE_UM) == 9_500.0


def test_apply_equity_zero_allowed() -> None:
    """A completely drained account should still be representable."""

    store = PositionStore()
    store.apply_equity_update(Venue.BINANCE_UM, 0.0)
    assert store.get_equity_usd(Venue.BINANCE_UM) == 0.0


def test_apply_negative_equity_rejected() -> None:
    store = PositionStore()
    with pytest.raises(ValueError, match=r"equity_usd must be >= 0"):
        store.apply_equity_update(Venue.BINANCE_UM, -1.0)


# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------


def test_apply_fill_increments_counter() -> None:
    store = PositionStore()
    assert store.fill_count(Venue.BINANCE_UM, "BTCUSDT") == 0
    store.apply_fill(_fill())
    store.apply_fill(_fill())
    assert store.fill_count(Venue.BINANCE_UM, "BTCUSDT") == 2


def test_apply_fill_does_not_mutate_position() -> None:
    """The store relies on ACCOUNT_UPDATE for authoritative position state."""

    store = PositionStore()
    store.apply_position_update(_update(qty=0.5))
    store.apply_fill(_fill())

    p = store.get_position(Venue.BINANCE_UM, "BTCUSDT")
    assert p is not None
    assert p.qty == 0.5  # unchanged
