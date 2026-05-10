"""Tests for :func:`trade_adapter.core.sizing.compute_target_qty`.

The sizing engine is pure and venue-agnostic, so the tests don't need
async fixtures or transport fakes. Each test locks one slice of the
contract: a happy-path value, a missing-context rejection, or a
boundary (zero / negative input).
"""

from __future__ import annotations

import pytest

from trade_adapter.core.sizing import SizingError, compute_target_qty
from trade_adapter.types import FixedQty, NotionalUsd, PctEquity, RiskBased

# ---------------------------------------------------------------------------
# FixedQty
# ---------------------------------------------------------------------------


def test_fixed_qty_returns_qty_unchanged() -> None:
    qty = compute_target_qty(FixedQty(qty=0.5), reference_price=65000.0)
    assert qty == 0.5


def test_fixed_qty_rejects_zero() -> None:
    with pytest.raises(SizingError, match=r"FixedQty.qty must be > 0"):
        compute_target_qty(FixedQty(qty=0.0), reference_price=65000.0)


def test_fixed_qty_rejects_negative() -> None:
    with pytest.raises(SizingError, match=r"FixedQty.qty must be > 0"):
        compute_target_qty(FixedQty(qty=-0.1), reference_price=65000.0)


# ---------------------------------------------------------------------------
# NotionalUsd
# ---------------------------------------------------------------------------


def test_notional_usd_divides_by_reference_price() -> None:
    qty = compute_target_qty(
        NotionalUsd(notional_usd=500.0), reference_price=50000.0
    )
    assert qty == pytest.approx(0.01)


def test_notional_usd_rejects_zero_notional() -> None:
    with pytest.raises(SizingError, match=r"NotionalUsd.notional_usd must be > 0"):
        compute_target_qty(
            NotionalUsd(notional_usd=0.0), reference_price=50000.0
        )


def test_notional_usd_rejects_negative_notional() -> None:
    with pytest.raises(SizingError, match=r"NotionalUsd.notional_usd must be > 0"):
        compute_target_qty(
            NotionalUsd(notional_usd=-1.0), reference_price=50000.0
        )


def test_notional_usd_rejects_zero_reference_price() -> None:
    with pytest.raises(SizingError, match="reference_price must be > 0"):
        compute_target_qty(
            NotionalUsd(notional_usd=500.0), reference_price=0.0
        )


def test_notional_usd_rejects_negative_reference_price() -> None:
    with pytest.raises(SizingError, match="reference_price must be > 0"):
        compute_target_qty(
            NotionalUsd(notional_usd=500.0), reference_price=-50000.0
        )


# ---------------------------------------------------------------------------
# PctEquity
# ---------------------------------------------------------------------------


def test_pct_equity_scales_equity_then_divides() -> None:
    # 2% of $10_000 = $200; at price $40_000 -> qty 0.005.
    qty = compute_target_qty(
        PctEquity(pct=2.0),
        reference_price=40000.0,
        equity_usd=10_000.0,
    )
    assert qty == pytest.approx(0.005)


def test_pct_equity_requires_equity_usd() -> None:
    with pytest.raises(SizingError, match="PctEquity sizing requires equity_usd"):
        compute_target_qty(PctEquity(pct=2.0), reference_price=40000.0)


def test_pct_equity_rejects_zero_pct() -> None:
    with pytest.raises(SizingError, match=r"PctEquity.pct must be > 0"):
        compute_target_qty(
            PctEquity(pct=0.0), reference_price=40000.0, equity_usd=1000.0
        )


def test_pct_equity_rejects_negative_pct() -> None:
    with pytest.raises(SizingError, match=r"PctEquity.pct must be > 0"):
        compute_target_qty(
            PctEquity(pct=-1.0), reference_price=40000.0, equity_usd=1000.0
        )


def test_pct_equity_rejects_zero_equity() -> None:
    with pytest.raises(SizingError, match="equity_usd must be > 0"):
        compute_target_qty(
            PctEquity(pct=2.0), reference_price=40000.0, equity_usd=0.0
        )


def test_pct_equity_rejects_negative_equity() -> None:
    with pytest.raises(SizingError, match="equity_usd must be > 0"):
        compute_target_qty(
            PctEquity(pct=2.0), reference_price=40000.0, equity_usd=-100.0
        )


# ---------------------------------------------------------------------------
# RiskBased
# ---------------------------------------------------------------------------


def test_risk_based_divides_by_stop_distance_long() -> None:
    # Risk $100, entry 50000, SL 49000 -> distance 1000 -> qty 0.1.
    qty = compute_target_qty(
        RiskBased(risk_usd=100.0),
        reference_price=50000.0,
        sl_price=49000.0,
    )
    assert qty == pytest.approx(0.1)


def test_risk_based_divides_by_stop_distance_short() -> None:
    # Direction-agnostic: distance uses abs(), so SL above entry works too.
    qty = compute_target_qty(
        RiskBased(risk_usd=100.0),
        reference_price=50000.0,
        sl_price=51000.0,
    )
    assert qty == pytest.approx(0.1)


def test_risk_based_requires_sl_price() -> None:
    with pytest.raises(SizingError, match="RiskBased sizing requires sl_price"):
        compute_target_qty(
            RiskBased(risk_usd=100.0), reference_price=50000.0
        )


def test_risk_based_rejects_zero_risk_usd() -> None:
    with pytest.raises(SizingError, match=r"RiskBased.risk_usd must be > 0"):
        compute_target_qty(
            RiskBased(risk_usd=0.0),
            reference_price=50000.0,
            sl_price=49000.0,
        )


def test_risk_based_rejects_negative_risk_usd() -> None:
    with pytest.raises(SizingError, match=r"RiskBased.risk_usd must be > 0"):
        compute_target_qty(
            RiskBased(risk_usd=-5.0),
            reference_price=50000.0,
            sl_price=49000.0,
        )


def test_risk_based_rejects_zero_sl_price() -> None:
    with pytest.raises(SizingError, match="sl_price must be > 0"):
        compute_target_qty(
            RiskBased(risk_usd=100.0),
            reference_price=50000.0,
            sl_price=0.0,
        )


def test_risk_based_rejects_sl_equal_to_reference() -> None:
    """``risk_usd / 0`` would explode — surface as a clear error."""

    with pytest.raises(SizingError, match="sl_price != reference_price"):
        compute_target_qty(
            RiskBased(risk_usd=100.0),
            reference_price=50000.0,
            sl_price=50000.0,
        )
