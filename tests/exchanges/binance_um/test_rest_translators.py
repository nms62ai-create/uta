"""Tests for the Phase 3e REST snapshot translators."""

from __future__ import annotations

import pytest

from trade_adapter.exchanges.binance_um.rest_translators import (
    account_info_to_equity_usd,
    position_risk_to_position_update,
    position_risk_to_position_updates,
)
from trade_adapter.types import Direction, PositionState, Venue


def _row(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "symbol": "BTCUSDT",
        "positionSide": "BOTH",
        "positionAmt": "0.500",
        "entryPrice": "50000.0",
        "liquidationPrice": "0",
        "unRealizedProfit": "0",
        "isolatedMargin": "0",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# position_risk_to_position_update
# ---------------------------------------------------------------------------


def test_long_position_translates() -> None:
    update = position_risk_to_position_update(_row(positionAmt="0.5"), ts=1.0)
    assert update is not None
    assert update.venue is Venue.BINANCE_UM
    assert update.symbol == "BTCUSDT"
    assert update.direction is Direction.LONG
    assert update.qty == 0.5
    assert update.state is PositionState.OPEN
    assert update.entry_price == 50_000.0
    assert update.ts == 1.0


def test_short_position_translates() -> None:
    update = position_risk_to_position_update(_row(positionAmt="-0.5"), ts=1.0)
    assert update is not None
    assert update.direction is Direction.SHORT
    assert update.qty == 0.5  # magnitude
    assert update.state is PositionState.OPEN


def test_flat_position_emits_idle() -> None:
    update = position_risk_to_position_update(_row(positionAmt="0"), ts=1.0)
    assert update is not None
    assert update.qty == 0.0
    assert update.state is PositionState.IDLE


def test_hedge_long_skipped() -> None:
    update = position_risk_to_position_update(
        _row(positionSide="LONG"), ts=1.0
    )
    assert update is None


def test_hedge_short_skipped() -> None:
    update = position_risk_to_position_update(
        _row(positionSide="SHORT"), ts=1.0
    )
    assert update is None


def test_position_side_case_insensitive() -> None:
    update = position_risk_to_position_update(
        _row(positionSide="both"), ts=1.0
    )
    assert update is not None


def test_liquidation_price_zero_becomes_none() -> None:
    """Binance sends '0' for missing liquidationPrice on flat positions."""

    update = position_risk_to_position_update(
        _row(positionAmt="0.5", liquidationPrice="0"), ts=1.0
    )
    assert update is not None
    assert update.liquidation_price is None


def test_liquidation_price_passes_through_when_set() -> None:
    update = position_risk_to_position_update(
        _row(positionAmt="0.5", liquidationPrice="45000.0"), ts=1.0
    )
    assert update is not None
    assert update.liquidation_price == 45_000.0


def test_unrealized_profit_passes_through() -> None:
    update = position_risk_to_position_update(
        _row(positionAmt="0.5", unRealizedProfit="123.45"), ts=1.0
    )
    assert update is not None
    assert update.unrealized_pnl_usd == 123.45


def test_isolated_margin_passes_through() -> None:
    update = position_risk_to_position_update(
        _row(positionAmt="0.5", isolatedMargin="1234.56"), ts=1.0
    )
    assert update is not None
    assert update.margin_used_usd == 1234.56


def test_missing_symbol_raises() -> None:
    row = _row()
    del row["symbol"]
    with pytest.raises(ValueError, match=r"missing required field 'symbol'"):
        position_risk_to_position_update(row, ts=1.0)


def test_missing_position_amt_raises() -> None:
    row = _row()
    del row["positionAmt"]
    with pytest.raises(ValueError, match=r"missing required field 'positionAmt'"):
        position_risk_to_position_update(row, ts=1.0)


def test_missing_entry_price_raises() -> None:
    row = _row()
    del row["entryPrice"]
    with pytest.raises(ValueError, match=r"missing required field 'entryPrice'"):
        position_risk_to_position_update(row, ts=1.0)


def test_missing_position_side_raises() -> None:
    row = _row()
    del row["positionSide"]
    with pytest.raises(ValueError, match=r"missing required field 'positionSide'"):
        position_risk_to_position_update(row, ts=1.0)


def test_malformed_position_amt_raises() -> None:
    with pytest.raises(ValueError, match=r"not convertible to float"):
        position_risk_to_position_update(
            _row(positionAmt="not-a-number"), ts=1.0
        )


# ---------------------------------------------------------------------------
# Bulk variant
# ---------------------------------------------------------------------------


def test_bulk_skips_hedge_rows() -> None:
    rows = [
        _row(symbol="BTCUSDT", positionAmt="0.5"),
        _row(symbol="ETHUSDT", positionSide="LONG", positionAmt="2.0"),
        _row(symbol="SOLUSDT", positionAmt="10.0"),
    ]
    updates = position_risk_to_position_updates(rows, ts=1.0)
    assert len(updates) == 2
    symbols = {u.symbol for u in updates}
    assert symbols == {"BTCUSDT", "SOLUSDT"}


def test_bulk_empty_input_returns_empty() -> None:
    assert position_risk_to_position_updates([], ts=1.0) == []


# ---------------------------------------------------------------------------
# account → equity
# ---------------------------------------------------------------------------


def test_account_info_to_equity_extracts_total_wallet_balance() -> None:
    info = {"totalWalletBalance": "1234.5678"}
    assert account_info_to_equity_usd(info) == 1234.5678


def test_account_info_to_equity_handles_integer_value() -> None:
    info = {"totalWalletBalance": 100}
    assert account_info_to_equity_usd(info) == 100.0


def test_account_info_to_equity_missing_field_raises() -> None:
    with pytest.raises(
        ValueError, match=r"missing required field 'totalWalletBalance'"
    ):
        account_info_to_equity_usd({})


def test_account_info_to_equity_malformed_value_raises() -> None:
    with pytest.raises(ValueError, match=r"not convertible to float"):
        account_info_to_equity_usd({"totalWalletBalance": "garbage"})
