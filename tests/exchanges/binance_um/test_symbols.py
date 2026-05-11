"""Tests for Binance USD-M exchangeInfo cache + tick/step rounding."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trade_adapter.exchanges.binance_um.symbols import (
    SymbolInfo,
    SymbolRegistry,
    precision_from_step,
    precision_to_step,
)


def _btc_symbol_payload() -> dict:
    """A trimmed BTCUSDT entry as it appears in exchangeInfo.symbols[]."""

    return {
        "symbol": "BTCUSDT",
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "pricePrecision": 1,
        "quantityPrecision": 3,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "0.10",
             "maxPrice": "1000000"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001",
             "maxQty": "10000"},
            {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001", "minQty": "0.001",
             "maxQty": "100"},
            {"filterType": "MIN_NOTIONAL", "notional": "100"},
        ],
    }


def _eth_symbol_payload() -> dict:
    return {
        "symbol": "ETHUSDT",
        "baseAsset": "ETH",
        "quoteAsset": "USDT",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "pricePrecision": 2,
        "quantityPrecision": 3,
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001",
             "maxQty": "10000"},
            {"filterType": "MIN_NOTIONAL", "minNotional": "20"},
        ],
    }


def test_load_from_exchange_info_count() -> None:
    reg = SymbolRegistry()
    n = reg.load_from_exchange_info({"symbols": [_btc_symbol_payload(), _eth_symbol_payload()]})
    assert n == 2
    assert "BTCUSDT" in reg
    assert "ETHUSDT" in reg
    assert len(reg) == 2


def test_load_replaces_cache() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    reg.load_from_exchange_info({"symbols": [_eth_symbol_payload()]})
    assert "BTCUSDT" not in reg
    assert "ETHUSDT" in reg


def test_get_returns_symbol_info() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    info = reg.get("BTCUSDT")
    assert isinstance(info, SymbolInfo)
    assert info.tick_size == Decimal("0.10")
    assert info.step_size == Decimal("0.001")
    assert info.min_notional == Decimal("100")
    assert info.market_step_size == Decimal("0.001")


def test_get_raises_on_unknown_symbol() -> None:
    reg = SymbolRegistry()
    with pytest.raises(KeyError):
        reg.get("NOPE")


def test_round_qty_floors_to_step() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    # step=0.001 — 0.0034 floors to 0.003.
    assert reg.round_qty("BTCUSDT", 0.0034) == Decimal("0.003")
    # Exactly on a step boundary stays put.
    assert reg.round_qty("BTCUSDT", 0.001) == Decimal("0.001")


def test_round_price_nearest() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    # tick=0.10 — 65432.17 rounds to 65432.20 (.17 -> .20 half-up).
    assert reg.round_price("BTCUSDT", 65432.17) == Decimal("65432.20")
    # Half-up: 65432.15 rounds to 65432.20.
    assert reg.round_price("BTCUSDT", 65432.15) == Decimal("65432.20")


def test_round_price_floor() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    assert reg.round_price_floor("BTCUSDT", 65432.17) == Decimal("65432.10")


def test_decimal_arithmetic_no_float_drift() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    # Add 0.001 a thousand times — float would drift, Decimal must not.
    cumulative = Decimal("0")
    step = Decimal("0.001")
    for _ in range(1000):
        cumulative += step
    assert reg.round_qty("BTCUSDT", cumulative) == Decimal("1.000")


def test_check_min_notional_below_threshold() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_btc_symbol_payload()]})
    # qty=0.001, price=50000 -> notional=50, below 100.
    assert reg.check_min_notional("BTCUSDT", 0.001, 50000) is False
    # qty=0.002, price=50000 -> notional=100, exactly threshold.
    assert reg.check_min_notional("BTCUSDT", 0.002, 50000) is True
    # qty=0.01, price=50000 -> notional=500, above threshold.
    assert reg.check_min_notional("BTCUSDT", 0.01, 50000) is True


def test_eth_uses_minnotional_alias() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_eth_symbol_payload()]})
    info = reg.get("ETHUSDT")
    assert info.min_notional == Decimal("20")


def test_market_lot_falls_back_to_lot_size() -> None:
    reg = SymbolRegistry()
    reg.load_from_exchange_info({"symbols": [_eth_symbol_payload()]})
    info = reg.get("ETHUSDT")
    # ETH payload has no MARKET_LOT_SIZE — falls back via round_market_qty.
    assert info.market_step_size is None
    assert reg.round_market_qty("ETHUSDT", 1.2345) == Decimal("1.234")


def test_precision_helpers_round_trip() -> None:
    assert precision_from_step(Decimal("0.001")) == 3
    assert precision_from_step(Decimal("0.10")) == 2  # internal exponent of '0.10' is -2
    assert precision_from_step(Decimal("1")) == 0
    assert precision_to_step(3) == Decimal("0.001")
    assert precision_to_step(0) == Decimal("1")


def test_missing_required_filter_raises() -> None:
    bad = {
        "symbol": "BAD",
        "baseAsset": "B",
        "quoteAsset": "Q",
        "contractType": "PERPETUAL",
        "status": "TRADING",
        "filters": [{"filterType": "MIN_NOTIONAL", "notional": "10"}],
    }
    reg = SymbolRegistry()
    with pytest.raises(ValueError):
        reg.load_from_exchange_info({"symbols": [bad]})
