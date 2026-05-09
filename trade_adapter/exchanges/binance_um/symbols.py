"""Binance USD-M exchange-info cache and tick/step rounding.

A ``SymbolRegistry`` is loaded once at startup from the
``GET /fapi/v1/exchangeInfo`` REST response and holds a frozen
``SymbolInfo`` per symbol with the filters needed to round prices /
quantities and to validate min-notional before placing an order.

The rounding helpers use :class:`decimal.Decimal` rather than ``float``
so that ``0.001`` step sizes don't drift after multiple operations.

This module is pure: it does **no** I/O. The REST fetch lives in
:mod:`trade_adapter.exchanges.binance_um.rest` and feeds the parsed
JSON into :meth:`SymbolRegistry.load_from_exchange_info`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any


@dataclass(slots=True, frozen=True)
class SymbolInfo:
    """A subset of Binance's per-symbol filter set, normalised."""

    symbol: str
    base_asset: str
    quote_asset: str
    contract_type: str
    status: str
    price_precision: int
    quantity_precision: int
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    market_step_size: Decimal | None = None
    market_min_qty: Decimal | None = None
    market_max_qty: Decimal | None = None


def _decimal(value: Any) -> Decimal:
    """``Decimal(str(value))`` so ``0.1`` doesn't introduce float noise."""

    return Decimal(str(value))


def _filter_by_type(filters: list[dict[str, Any]], filter_type: str) -> dict[str, Any] | None:
    for f in filters:
        if f.get("filterType") == filter_type:
            return f
    return None


def _parse_symbol(raw: dict[str, Any]) -> SymbolInfo:
    """Translate one entry from ``exchangeInfo.symbols`` into a ``SymbolInfo``."""

    filters = raw.get("filters") or []

    price_filter = _filter_by_type(filters, "PRICE_FILTER")
    lot_size = _filter_by_type(filters, "LOT_SIZE")
    if price_filter is None or lot_size is None:
        raise ValueError(f"symbol {raw.get('symbol')!r} missing required filter")

    market_lot_size = _filter_by_type(filters, "MARKET_LOT_SIZE")

    # MIN_NOTIONAL on USD-M futures historically used ``notional``;
    # some endpoints return ``minNotional``. Tolerate both, default to
    # zero only as a last resort.
    min_notional_filter = _filter_by_type(filters, "MIN_NOTIONAL") or {}
    min_notional_raw = (
        min_notional_filter.get("notional")
        or min_notional_filter.get("minNotional")
        or "0"
    )

    return SymbolInfo(
        symbol=str(raw["symbol"]),
        base_asset=str(raw.get("baseAsset", "")),
        quote_asset=str(raw.get("quoteAsset", "")),
        contract_type=str(raw.get("contractType", "")),
        status=str(raw.get("status", "")),
        price_precision=int(raw.get("pricePrecision", 0)),
        quantity_precision=int(raw.get("quantityPrecision", 0)),
        tick_size=_decimal(price_filter["tickSize"]),
        step_size=_decimal(lot_size["stepSize"]),
        min_qty=_decimal(lot_size["minQty"]),
        max_qty=_decimal(lot_size["maxQty"]),
        min_notional=_decimal(min_notional_raw),
        market_step_size=(
            _decimal(market_lot_size["stepSize"]) if market_lot_size is not None else None
        ),
        market_min_qty=(
            _decimal(market_lot_size["minQty"]) if market_lot_size is not None else None
        ),
        market_max_qty=(
            _decimal(market_lot_size["maxQty"]) if market_lot_size is not None else None
        ),
    )


@dataclass(slots=True)
class SymbolRegistry:
    """In-memory cache of :class:`SymbolInfo` keyed by symbol."""

    by_symbol: dict[str, SymbolInfo] = field(default_factory=dict)

    def load_from_exchange_info(self, payload: dict[str, Any]) -> int:
        """Replace the cache from a parsed ``exchangeInfo`` payload.

        Returns the number of symbols loaded.
        """

        symbols = payload.get("symbols") or []
        new_cache: dict[str, SymbolInfo] = {}
        for raw in symbols:
            info = _parse_symbol(raw)
            new_cache[info.symbol] = info
        self.by_symbol = new_cache
        return len(new_cache)

    def get(self, symbol: str) -> SymbolInfo:
        info = self.by_symbol.get(symbol)
        if info is None:
            raise KeyError(f"symbol {symbol!r} not in exchangeInfo cache")
        return info

    def __contains__(self, symbol: object) -> bool:
        return symbol in self.by_symbol

    def __len__(self) -> int:
        return len(self.by_symbol)

    def round_qty(self, symbol: str, qty: float | Decimal) -> Decimal:
        """Snap ``qty`` **down** to the symbol's ``stepSize``.

        Rounding down is the safer default for venue-bound quantities:
        we never accidentally exceed the requested size.
        """

        info = self.get(symbol)
        return _snap_floor(_decimal(qty), info.step_size)

    def round_market_qty(self, symbol: str, qty: float | Decimal) -> Decimal:
        """Snap ``qty`` to the ``MARKET_LOT_SIZE`` step (falls back to LOT_SIZE)."""

        info = self.get(symbol)
        step = info.market_step_size if info.market_step_size is not None else info.step_size
        return _snap_floor(_decimal(qty), step)

    def round_price(self, symbol: str, price: float | Decimal) -> Decimal:
        """Snap ``price`` to the nearest ``tickSize`` (banker-free, half-up)."""

        info = self.get(symbol)
        return _snap_nearest(_decimal(price), info.tick_size)

    def round_price_floor(self, symbol: str, price: float | Decimal) -> Decimal:
        info = self.get(symbol)
        return _snap_floor(_decimal(price), info.tick_size)

    def check_min_notional(self, symbol: str, qty: float | Decimal, price: float | Decimal) -> bool:
        """Return ``True`` iff ``qty * price >= minNotional`` for the symbol."""

        info = self.get(symbol)
        return (_decimal(qty) * _decimal(price)) >= info.min_notional


def _snap_floor(value: Decimal, step: Decimal) -> Decimal:
    """Snap ``value`` to ``step`` rounding **down**.

    Using ``Decimal`` arithmetic so e.g. step=Decimal('0.001') never
    accumulates float noise.
    """

    if step <= 0:
        return value
    n = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return (n * step).quantize(step)


def _snap_nearest(value: Decimal, step: Decimal) -> Decimal:
    """Snap ``value`` to the nearest multiple of ``step`` (half-up)."""

    if step <= 0:
        return value
    n = (value / step).to_integral_value(rounding=ROUND_HALF_UP)
    return (n * step).quantize(step)


def precision_from_step(step: Decimal) -> int:
    """Number of fractional digits represented by ``step`` (e.g. ``0.001`` → 3)."""

    if step == 0:
        return 0
    # Decimal exponent is negative for fractional, zero/positive otherwise.
    exp = step.as_tuple().exponent
    if isinstance(exp, int):
        return -exp if exp < 0 else 0
    # Special values like 'n', 'N', 'F' don't have a numeric precision.
    return 0


def precision_to_step(precision: int) -> Decimal:
    """Inverse of :func:`precision_from_step` for symmetry in tests."""

    if precision <= 0:
        return Decimal(1)
    return Decimal(1) / (Decimal(10) ** precision)


__all__ = [
    "SymbolInfo",
    "SymbolRegistry",
    "precision_from_step",
    "precision_to_step",
]
