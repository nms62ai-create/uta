"""Tests for the pure adaptive-signal → UniversalSignal translator."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from trade_adapter.integrations.heatmap_sdk.translator import (
    build_universal_signal,
    direction_from_exhaustion,
)
from trade_adapter.types import (
    Direction,
    Intent,
    NotionalUsd,
    PctFromEntry,
    Venue,
)

# ---------------------------------------------------------------------------
# Stand-in for ``adaptive_sdk.ExhaustionType`` / ``Signal``.
#
# We intentionally do not depend on heatmap-sdk being importable from tests
# — the bridge is structurally typed so any object with the right attrs is
# acceptable. These stubs mirror the heatmap-sdk dataclass shape exactly so
# the test doubles as a contract check.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ExhaustionType:
    name: str


BUY_EXHAUSTION = _ExhaustionType("BUY_EXHAUSTION")
SELL_EXHAUSTION = _ExhaustionType("SELL_EXHAUSTION")


@dataclass(frozen=True)
class _FakeAdaptiveSignal:
    signal_id: str
    symbol: str
    timestamp: float
    exhaustion_type: _ExhaustionType
    confidence: float


def _signal(
    *,
    exhaustion: _ExhaustionType = SELL_EXHAUSTION,
    signal_id: str = "sig-1",
    confidence: float = 0.80,
    timestamp: float = 1_700_000_000.0,
) -> _FakeAdaptiveSignal:
    return _FakeAdaptiveSignal(
        signal_id=signal_id,
        symbol="BTCUSDT",
        timestamp=timestamp,
        exhaustion_type=exhaustion,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# direction_from_exhaustion
# ---------------------------------------------------------------------------


def test_buy_exhaustion_maps_to_short() -> None:
    assert direction_from_exhaustion("BUY_EXHAUSTION") is Direction.SHORT


def test_sell_exhaustion_maps_to_long() -> None:
    assert direction_from_exhaustion("SELL_EXHAUSTION") is Direction.LONG


def test_direction_mapping_is_case_insensitive() -> None:
    assert direction_from_exhaustion("buy_exhaustion") is Direction.SHORT
    assert direction_from_exhaustion("sell_exhaustion") is Direction.LONG


def test_unknown_exhaustion_raises() -> None:
    with pytest.raises(ValueError, match="unknown exhaustion type"):
        direction_from_exhaustion("HOLD")


# ---------------------------------------------------------------------------
# build_universal_signal — happy path
# ---------------------------------------------------------------------------


def test_build_signal_uses_long_for_sell_exhaustion() -> None:
    adaptive = _signal(exhaustion=SELL_EXHAUSTION)

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=1.0,
        tp_pct=2.0,
        ttl_seconds=5.0,
    )

    assert universal.direction is Direction.LONG
    assert universal.intent is Intent.OPEN


def test_build_signal_uses_short_for_buy_exhaustion() -> None:
    adaptive = _signal(exhaustion=BUY_EXHAUSTION)

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=1.0,
        tp_pct=2.0,
        ttl_seconds=5.0,
    )

    assert universal.direction is Direction.SHORT


def test_build_signal_sizing_is_notional_usd() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=250.5,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert isinstance(universal.sizing, NotionalUsd)
    assert universal.sizing.notional_usd == 250.5


def test_build_signal_stops_use_pct_from_entry() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=0.5,
        tp_pct=1.5,
        ttl_seconds=5.0,
    )

    assert isinstance(universal.sl, PctFromEntry)
    assert universal.sl.pct == 0.5
    assert isinstance(universal.tp, PctFromEntry)
    assert universal.tp.pct == 1.5


def test_build_signal_omits_stops_when_pct_none() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert universal.sl is None
    assert universal.tp is None


def test_build_signal_uppercases_symbol() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="btcusdt",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert universal.symbol == "BTCUSDT"


def test_build_signal_correlation_and_signal_id_match_adaptive() -> None:
    adaptive = _signal(signal_id="adaptive-42")

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert universal.signal_id == "adaptive-42"
    assert universal.correlation_id == "adaptive-42"


def test_build_signal_metadata_carries_confidence_and_ts() -> None:
    adaptive = _signal(confidence=0.78, timestamp=1_700_000_001.5)

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert universal.metadata["producer"] == "heatmap_sdk"
    assert universal.metadata["confidence"] == "0.7800"
    assert universal.metadata["adaptive_signal_ts"] == "1700000001.500000"


def test_build_signal_source_defaults_to_heatmap_sdk() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
    )

    assert universal.source == "heatmap_sdk"


def test_build_signal_source_overridable() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=5.0,
        source="my-bot",
    )

    assert universal.source == "my-bot"


def test_build_signal_passes_through_ttl() -> None:
    adaptive = _signal()

    universal = build_universal_signal(
        adaptive,
        symbol="BTCUSDT",
        venue=Venue.BINANCE_UM,
        notional_usd=100.0,
        sl_pct=None,
        tp_pct=None,
        ttl_seconds=12.5,
    )

    assert universal.ttl_seconds == 12.5


# ---------------------------------------------------------------------------
# build_universal_signal — validation
# ---------------------------------------------------------------------------


def test_build_signal_rejects_non_positive_notional() -> None:
    adaptive = _signal()

    with pytest.raises(ValueError, match="notional_usd must be positive"):
        build_universal_signal(
            adaptive,
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=0.0,
            sl_pct=None,
            tp_pct=None,
            ttl_seconds=5.0,
        )


def test_build_signal_rejects_non_positive_sl() -> None:
    adaptive = _signal()

    with pytest.raises(ValueError, match="sl_pct must be positive"):
        build_universal_signal(
            adaptive,
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=0.0,
            tp_pct=None,
            ttl_seconds=5.0,
        )


def test_build_signal_rejects_non_positive_tp() -> None:
    adaptive = _signal()

    with pytest.raises(ValueError, match="tp_pct must be positive"):
        build_universal_signal(
            adaptive,
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=-1.0,
            ttl_seconds=5.0,
        )


def test_build_signal_rejects_non_positive_ttl() -> None:
    adaptive = _signal()

    with pytest.raises(ValueError, match="ttl_seconds must be positive"):
        build_universal_signal(
            adaptive,
            symbol="BTCUSDT",
            venue=Venue.BINANCE_UM,
            notional_usd=100.0,
            sl_pct=None,
            tp_pct=None,
            ttl_seconds=0.0,
        )
