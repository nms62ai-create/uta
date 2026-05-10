"""Tests for :func:`trade_adapter.core.stops.compute_protective_price`.

The stop calculator is pure: each test locks one slice of the
direction/kind sign matrix or one validation rule.
"""

from __future__ import annotations

import pytest

from trade_adapter.core.stops import StopError, compute_protective_price
from trade_adapter.types import (
    AbsolutePrice,
    AtrMultiple,
    BpsFromEntry,
    Direction,
    PctFromEntry,
)

# ---------------------------------------------------------------------------
# AbsolutePrice — direction invariant
# ---------------------------------------------------------------------------


def test_absolute_long_sl_must_be_below_entry() -> None:
    price = compute_protective_price(
        AbsolutePrice(price=49000.0),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="sl",
    )
    assert price == 49000.0


def test_absolute_long_tp_must_be_above_entry() -> None:
    price = compute_protective_price(
        AbsolutePrice(price=51000.0),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="tp",
    )
    assert price == 51000.0


def test_absolute_short_sl_must_be_above_entry() -> None:
    price = compute_protective_price(
        AbsolutePrice(price=51000.0),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="sl",
    )
    assert price == 51000.0


def test_absolute_short_tp_must_be_below_entry() -> None:
    price = compute_protective_price(
        AbsolutePrice(price=49000.0),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="tp",
    )
    assert price == 49000.0


def test_absolute_long_sl_above_entry_raises() -> None:
    """SL above entry on a LONG would trigger immediately."""

    with pytest.raises(StopError, match="sl for LONG requires price < entry"):
        compute_protective_price(
            AbsolutePrice(price=51000.0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_absolute_long_sl_at_entry_raises() -> None:
    """Equality is rejected too — a stop at exactly entry triggers on first tick."""

    with pytest.raises(StopError, match="sl for LONG requires price < entry"):
        compute_protective_price(
            AbsolutePrice(price=50000.0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_absolute_long_tp_below_entry_raises() -> None:
    with pytest.raises(StopError, match="tp for LONG requires price > entry"):
        compute_protective_price(
            AbsolutePrice(price=49000.0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="tp",
        )


def test_absolute_short_sl_below_entry_raises() -> None:
    with pytest.raises(StopError, match="sl for SHORT requires price > entry"):
        compute_protective_price(
            AbsolutePrice(price=49000.0),
            entry_price=50000.0,
            direction=Direction.SHORT,
            kind="sl",
        )


def test_absolute_short_tp_above_entry_raises() -> None:
    with pytest.raises(StopError, match="tp for SHORT requires price < entry"):
        compute_protective_price(
            AbsolutePrice(price=51000.0),
            entry_price=50000.0,
            direction=Direction.SHORT,
            kind="tp",
        )


def test_absolute_zero_price_raises() -> None:
    with pytest.raises(StopError, match=r"AbsolutePrice.price must be > 0"):
        compute_protective_price(
            AbsolutePrice(price=0.0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_absolute_negative_price_raises() -> None:
    with pytest.raises(StopError, match=r"AbsolutePrice.price must be > 0"):
        compute_protective_price(
            AbsolutePrice(price=-1.0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


# ---------------------------------------------------------------------------
# BpsFromEntry — sign matrix
# ---------------------------------------------------------------------------


def test_bps_long_sl_subtracts() -> None:
    # 50 bps = 0.5%, LONG SL = 50000 * 0.995 = 49750.
    price = compute_protective_price(
        BpsFromEntry(bps=50),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="sl",
    )
    assert price == pytest.approx(49750.0)


def test_bps_long_tp_adds() -> None:
    price = compute_protective_price(
        BpsFromEntry(bps=50),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="tp",
    )
    assert price == pytest.approx(50250.0)


def test_bps_short_sl_adds() -> None:
    price = compute_protective_price(
        BpsFromEntry(bps=50),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="sl",
    )
    assert price == pytest.approx(50250.0)


def test_bps_short_tp_subtracts() -> None:
    price = compute_protective_price(
        BpsFromEntry(bps=50),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="tp",
    )
    assert price == pytest.approx(49750.0)


def test_bps_rejects_zero() -> None:
    with pytest.raises(StopError, match=r"BpsFromEntry.bps must be > 0"):
        compute_protective_price(
            BpsFromEntry(bps=0),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_bps_rejects_negative() -> None:
    with pytest.raises(StopError, match=r"BpsFromEntry.bps must be > 0"):
        compute_protective_price(
            BpsFromEntry(bps=-5),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_bps_long_sl_above_100pct_raises() -> None:
    """A LONG SL more than 10000 bps below entry would underflow to <= 0."""

    with pytest.raises(StopError, match="non-positive price"):
        compute_protective_price(
            BpsFromEntry(bps=10000),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


# ---------------------------------------------------------------------------
# PctFromEntry — same matrix as BPS but with percentage units
# ---------------------------------------------------------------------------


def test_pct_long_sl_subtracts() -> None:
    # 2% LONG SL on entry 100 -> 98.
    price = compute_protective_price(
        PctFromEntry(pct=2.0),
        entry_price=100.0,
        direction=Direction.LONG,
        kind="sl",
    )
    assert price == pytest.approx(98.0)


def test_pct_long_tp_adds() -> None:
    price = compute_protective_price(
        PctFromEntry(pct=2.0),
        entry_price=100.0,
        direction=Direction.LONG,
        kind="tp",
    )
    assert price == pytest.approx(102.0)


def test_pct_short_sl_adds() -> None:
    price = compute_protective_price(
        PctFromEntry(pct=2.0),
        entry_price=100.0,
        direction=Direction.SHORT,
        kind="sl",
    )
    assert price == pytest.approx(102.0)


def test_pct_short_tp_subtracts() -> None:
    price = compute_protective_price(
        PctFromEntry(pct=2.0),
        entry_price=100.0,
        direction=Direction.SHORT,
        kind="tp",
    )
    assert price == pytest.approx(98.0)


def test_pct_rejects_zero() -> None:
    with pytest.raises(StopError, match=r"PctFromEntry.pct must be > 0"):
        compute_protective_price(
            PctFromEntry(pct=0.0),
            entry_price=100.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_pct_rejects_negative() -> None:
    with pytest.raises(StopError, match=r"PctFromEntry.pct must be > 0"):
        compute_protective_price(
            PctFromEntry(pct=-1.0),
            entry_price=100.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_pct_long_sl_above_100pct_raises() -> None:
    """A LONG SL ≥ 100% below entry is non-positive — reject."""

    with pytest.raises(StopError, match="non-positive price"):
        compute_protective_price(
            PctFromEntry(pct=120.0),
            entry_price=100.0,
            direction=Direction.LONG,
            kind="sl",
        )


# ---------------------------------------------------------------------------
# AtrMultiple — absolute (not relative) offset
# ---------------------------------------------------------------------------


def test_atr_long_sl_subtracts() -> None:
    # 1.5 ATR below entry on a LONG.
    price = compute_protective_price(
        AtrMultiple(multiple=1.5, atr_period_seconds=900),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="sl",
        atr=200.0,
    )
    assert price == pytest.approx(49700.0)


def test_atr_long_tp_adds() -> None:
    price = compute_protective_price(
        AtrMultiple(multiple=2.0, atr_period_seconds=900),
        entry_price=50000.0,
        direction=Direction.LONG,
        kind="tp",
        atr=200.0,
    )
    assert price == pytest.approx(50400.0)


def test_atr_short_sl_adds() -> None:
    price = compute_protective_price(
        AtrMultiple(multiple=1.5, atr_period_seconds=900),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="sl",
        atr=200.0,
    )
    assert price == pytest.approx(50300.0)


def test_atr_short_tp_subtracts() -> None:
    price = compute_protective_price(
        AtrMultiple(multiple=2.0, atr_period_seconds=900),
        entry_price=50000.0,
        direction=Direction.SHORT,
        kind="tp",
        atr=200.0,
    )
    assert price == pytest.approx(49600.0)


def test_atr_requires_atr() -> None:
    with pytest.raises(StopError, match="AtrMultiple stop requires atr"):
        compute_protective_price(
            AtrMultiple(multiple=1.5, atr_period_seconds=900),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_atr_rejects_zero_atr() -> None:
    with pytest.raises(StopError, match="atr must be > 0"):
        compute_protective_price(
            AtrMultiple(multiple=1.5, atr_period_seconds=900),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
            atr=0.0,
        )


def test_atr_rejects_negative_atr() -> None:
    with pytest.raises(StopError, match="atr must be > 0"):
        compute_protective_price(
            AtrMultiple(multiple=1.5, atr_period_seconds=900),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
            atr=-10.0,
        )


def test_atr_rejects_zero_multiple() -> None:
    with pytest.raises(StopError, match=r"AtrMultiple.multiple must be > 0"):
        compute_protective_price(
            AtrMultiple(multiple=0.0, atr_period_seconds=900),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
            atr=10.0,
        )


def test_atr_rejects_negative_multiple() -> None:
    with pytest.raises(StopError, match=r"AtrMultiple.multiple must be > 0"):
        compute_protective_price(
            AtrMultiple(multiple=-1.0, atr_period_seconds=900),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="sl",
            atr=10.0,
        )


def test_atr_long_sl_huge_offset_raises() -> None:
    """ATR * multiple > entry pushes the price ≤ 0 on a LONG SL — reject."""

    with pytest.raises(StopError, match="non-positive price"):
        compute_protective_price(
            AtrMultiple(multiple=10.0, atr_period_seconds=900),
            entry_price=100.0,
            direction=Direction.LONG,
            kind="sl",
            atr=20.0,
        )


# ---------------------------------------------------------------------------
# Shared invariants
# ---------------------------------------------------------------------------


def test_zero_entry_price_raises() -> None:
    with pytest.raises(StopError, match="entry_price must be > 0"):
        compute_protective_price(
            BpsFromEntry(bps=50),
            entry_price=0.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_negative_entry_price_raises() -> None:
    with pytest.raises(StopError, match="entry_price must be > 0"):
        compute_protective_price(
            BpsFromEntry(bps=50),
            entry_price=-50000.0,
            direction=Direction.LONG,
            kind="sl",
        )


def test_unknown_kind_raises() -> None:
    with pytest.raises(StopError, match="unknown stop kind"):
        compute_protective_price(
            BpsFromEntry(bps=50),
            entry_price=50000.0,
            direction=Direction.LONG,
            kind="trail",  # type: ignore[arg-type]
        )
