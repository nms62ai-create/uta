"""Tests for :mod:`trade_adapter.core.risk`."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from trade_adapter.core.risk import (
    RiskConfig,
    RiskDecision,
    RiskGate,
    RiskState,
)
from trade_adapter.types import (
    Direction,
    FixedQty,
    Intent,
    Position,
    PositionState,
    RejectionReason,
    UniversalSignal,
    Venue,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class FakePositions:
    positions: dict[tuple[Venue, str], Position] = field(default_factory=dict)

    def get_position(self, venue: Venue, symbol: str) -> Position | None:
        return self.positions.get((venue, symbol))


def _signal(
    *,
    signal_id: str = "sig-1",
    venue: Venue = Venue.BINANCE_UM,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    intent: Intent = Intent.OPEN,
) -> UniversalSignal:
    return UniversalSignal(
        signal_id=signal_id,
        source="test",
        symbol=symbol,
        venue=venue,
        direction=direction,
        intent=intent,
        sizing=FixedQty(qty=0.5),
        sl=None,
        tp=None,
        ttl_seconds=5.0,
        correlation_id=None,
    )


def _position(
    *,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    qty: float = 0.5,
) -> Position:
    return Position(
        venue=Venue.BINANCE_UM,
        symbol=symbol,
        direction=direction,
        qty=qty,
        entry_price=50_000.0,
        state=PositionState.OPEN,
        liquidation_price=None,
        unrealized_pnl_usd=None,
        margin_used_usd=None,
        opened_at=None,
    )


# ---------------------------------------------------------------------------
# RiskDecision
# ---------------------------------------------------------------------------


def test_allow_is_allowed_with_no_reason() -> None:
    d = RiskDecision.allow()
    assert d.allowed is True
    assert d.rejection_reason is None
    assert d.detail is None


def test_deny_carries_reason_and_detail() -> None:
    d = RiskDecision.deny(RejectionReason.EMERGENCY_LIMIT_EXCEEDED, "over cap")
    assert d.allowed is False
    assert d.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
    assert d.detail == "over cap"


# ---------------------------------------------------------------------------
# RiskState
# ---------------------------------------------------------------------------


def test_state_starts_with_zero_pnl_and_no_kill() -> None:
    s = RiskState()
    assert s.daily_realized_pnl(Venue.BINANCE_UM) == 0.0
    assert s.kill_switch_tripped is False
    assert s.kill_switch_reason is None


def test_state_record_pnl_accumulates() -> None:
    s = RiskState(clock=lambda: 1700_000_000.0)
    s.record_realized_pnl(Venue.BINANCE_UM, -10.0)
    s.record_realized_pnl(Venue.BINANCE_UM, -5.0)
    assert s.daily_realized_pnl(Venue.BINANCE_UM) == -15.0


def test_state_pnl_is_per_venue() -> None:
    s = RiskState(clock=lambda: 1700_000_000.0)
    s.record_realized_pnl(Venue.BINANCE_UM, -10.0)
    s.record_realized_pnl(Venue.BYBIT_LINEAR, -5.0)
    assert s.daily_realized_pnl(Venue.BINANCE_UM) == -10.0
    assert s.daily_realized_pnl(Venue.BYBIT_LINEAR) == -5.0


def test_state_pnl_buckets_by_utc_day() -> None:
    # 2024-01-01 00:00:00 UTC
    clock_ts = [1704067200.0]
    s = RiskState(clock=lambda: clock_ts[0])
    s.record_realized_pnl(Venue.BINANCE_UM, -10.0)
    assert s.daily_realized_pnl(Venue.BINANCE_UM) == -10.0
    # Advance one full day.
    clock_ts[0] = 1704067200.0 + 24 * 3600
    # Different bucket — yesterday's loss is invisible.
    assert s.daily_realized_pnl(Venue.BINANCE_UM) == 0.0


def test_trip_and_reset_kill_switch() -> None:
    s = RiskState()
    s.trip_kill_switch("manual stop")
    assert s.kill_switch_tripped is True
    assert s.kill_switch_reason == "manual stop"
    s.reset_kill_switch()
    assert s.kill_switch_tripped is False
    assert s.kill_switch_reason is None


def test_trip_kill_switch_is_idempotent_first_reason_wins() -> None:
    s = RiskState()
    s.trip_kill_switch("first")
    s.trip_kill_switch("second")
    assert s.kill_switch_reason == "first"


# ---------------------------------------------------------------------------
# RiskGate — kill switch
# ---------------------------------------------------------------------------


def test_kill_switch_denies_everything() -> None:
    cfg = RiskConfig()
    state = RiskState()
    state.trip_kill_switch("emergency stop")
    gate = RiskGate(
        config=cfg, state=state, position_provider=FakePositions()
    )

    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)

    assert d.allowed is False
    assert d.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
    assert d.detail is not None and "emergency stop" in d.detail


def test_no_config_allows_everything() -> None:
    gate = RiskGate(
        config=RiskConfig(),
        state=RiskState(),
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=10.0, reference_price=50_000.0)
    assert d.allowed is True


# ---------------------------------------------------------------------------
# RiskGate — daily loss
# ---------------------------------------------------------------------------


def test_daily_loss_within_limit_allowed() -> None:
    state = RiskState(clock=lambda: 1700_000_000.0)
    state.record_realized_pnl(Venue.BINANCE_UM, -50.0)
    gate = RiskGate(
        config=RiskConfig(max_daily_loss_usd=100.0),
        state=state,
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is True


def test_daily_loss_at_limit_denies_and_trips_kill_switch() -> None:
    state = RiskState(clock=lambda: 1700_000_000.0)
    state.record_realized_pnl(Venue.BINANCE_UM, -100.0)
    gate = RiskGate(
        config=RiskConfig(max_daily_loss_usd=100.0),
        state=state,
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is False
    assert d.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
    assert state.kill_switch_tripped is True


def test_daily_loss_negative_value_in_config_treated_as_magnitude() -> None:
    """``max_daily_loss_usd=-50`` and ``50`` behave identically."""

    state = RiskState(clock=lambda: 1700_000_000.0)
    state.record_realized_pnl(Venue.BINANCE_UM, -60.0)
    gate = RiskGate(
        config=RiskConfig(max_daily_loss_usd=-50.0),
        state=state,
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is False


def test_daily_loss_resets_per_day() -> None:
    clock_ts = [1700_000_000.0]
    state = RiskState(clock=lambda: clock_ts[0])
    state.record_realized_pnl(Venue.BINANCE_UM, -200.0)
    gate = RiskGate(
        config=RiskConfig(max_daily_loss_usd=100.0),
        state=state,
        position_provider=FakePositions(),
    )
    # Today: denied
    assert gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0).allowed is False
    # Reset the kill switch (manual recovery)
    state.reset_kill_switch()
    # Next day: yesterday's PnL is invisible -> allowed
    clock_ts[0] += 24 * 3600
    assert gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0).allowed is True


# ---------------------------------------------------------------------------
# RiskGate — notional cap
# ---------------------------------------------------------------------------


def test_notional_cap_below_limit_allowed() -> None:
    gate = RiskGate(
        config=RiskConfig(max_notional_usd_per_symbol=30_000.0),
        state=RiskState(),
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    # 0.5 * 50_000 = 25_000 < 30_000
    assert d.allowed is True


def test_notional_cap_above_limit_denied() -> None:
    gate = RiskGate(
        config=RiskConfig(max_notional_usd_per_symbol=10_000.0),
        state=RiskState(),
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    # 0.5 * 50_000 = 25_000 > 10_000
    assert d.allowed is False
    assert d.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
    assert d.detail is not None and "notional_cap" in d.detail


def test_notional_at_limit_allowed() -> None:
    """Strict greater-than check — at-limit is OK."""

    gate = RiskGate(
        config=RiskConfig(max_notional_usd_per_symbol=25_000.0),
        state=RiskState(),
        position_provider=FakePositions(),
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is True


# ---------------------------------------------------------------------------
# RiskGate — per-symbol qty cap
# ---------------------------------------------------------------------------


def test_per_symbol_qty_cap_no_existing_position_allowed() -> None:
    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "BTCUSDT"): 1.0},
    )
    gate = RiskGate(
        config=cfg, state=RiskState(), position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is True


def test_per_symbol_qty_cap_exceeded_denied() -> None:
    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "BTCUSDT"): 0.4},
    )
    gate = RiskGate(
        config=cfg, state=RiskState(), position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is False
    assert d.rejection_reason is RejectionReason.EMERGENCY_LIMIT_EXCEEDED
    assert d.detail is not None and "position_cap" in d.detail


def test_per_symbol_qty_cap_at_limit_allowed() -> None:
    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "BTCUSDT"): 0.5},
    )
    gate = RiskGate(
        config=cfg, state=RiskState(), position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is True


def test_per_symbol_qty_cap_no_entry_for_symbol_allowed() -> None:
    """An empty mapping for one symbol means no cap for any other."""

    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "ETHUSDT"): 0.1},
    )
    gate = RiskGate(
        config=cfg, state=RiskState(), position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(symbol="BTCUSDT"), target_qty=10.0, reference_price=50_000.0)
    assert d.allowed is True


def test_per_symbol_qty_cap_projects_post_fill_for_flat_position() -> None:
    """Same-direction OPEN on a flat tracked symbol contributes ``target_qty``."""

    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "BTCUSDT"): 0.4},
    )
    positions = FakePositions(
        positions={
            (Venue.BINANCE_UM, "BTCUSDT"): _position(qty=0.0),
        }
    )
    gate = RiskGate(
        config=cfg, state=RiskState(), position_provider=positions
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    # post-fill qty = 0.5 (existing.qty=0, so projection == target_qty)
    assert d.allowed is False


# ---------------------------------------------------------------------------
# RiskGate — check ordering
# ---------------------------------------------------------------------------


def test_kill_switch_short_circuits_other_checks() -> None:
    """Kill switch is checked first — other checks never run."""

    cfg = RiskConfig(
        max_position_qty_per_symbol={(Venue.BINANCE_UM, "BTCUSDT"): 100.0},
        max_notional_usd_per_symbol=1_000_000.0,
        max_daily_loss_usd=1_000_000.0,
    )
    state = RiskState()
    state.trip_kill_switch("manual")
    gate = RiskGate(
        config=cfg, state=state, position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(), target_qty=0.5, reference_price=50_000.0)
    assert d.allowed is False
    assert d.detail is not None and "kill_switch" in d.detail


@pytest.mark.parametrize(
    "config_kwargs,target_qty,reference_price,expected_in_detail",
    [
        # daily_loss is checked before notional_cap.
        (
            {
                "max_daily_loss_usd": 100.0,
                "max_notional_usd_per_symbol": 10_000.0,
            },
            0.5,
            50_000.0,
            "daily_loss",
        ),
        # notional_cap is checked before position_cap.
        (
            {
                "max_notional_usd_per_symbol": 10_000.0,
                "max_position_qty_per_symbol": {
                    (Venue.BINANCE_UM, "BTCUSDT"): 0.4
                },
            },
            0.5,
            50_000.0,
            "notional_cap",
        ),
    ],
)
def test_check_ordering(
    config_kwargs: dict[str, object],
    target_qty: float,
    reference_price: float,
    expected_in_detail: str,
) -> None:
    state = RiskState(clock=lambda: 1700_000_000.0)
    state.record_realized_pnl(Venue.BINANCE_UM, -200.0)
    cfg = RiskConfig(**config_kwargs)  # type: ignore[arg-type]
    gate = RiskGate(
        config=cfg, state=state, position_provider=FakePositions()
    )
    d = gate.evaluate(_signal(), target_qty=target_qty, reference_price=reference_price)
    assert d.allowed is False
    assert d.detail is not None and expected_in_detail in d.detail
