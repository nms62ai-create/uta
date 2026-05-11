"""Tests for :class:`BinanceUmSnapshotProvider` (Phase 3g glue).

The provider is a tiny adapter from :class:`BinanceRestClient` to the
venue-agnostic :class:`VenueSnapshotProvider` Protocol; these tests
pin: (a) it calls the right REST endpoints, (b) it passes the clock
timestamp through to the translators, (c) translator errors propagate
unchanged, and (d) Hedge-mode rows are silently dropped (decision A0
inherited from :func:`position_risk_to_position_updates`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from trade_adapter.exchanges.binance_um.snapshot import (
    BinanceUmSnapshotProvider,
)
from trade_adapter.types import Direction, PositionState, Venue

pytestmark = pytest.mark.asyncio


@dataclass
class FakeRest:
    """In-memory stub for the subset of :class:`BinanceRestClient` used."""

    positions_response: list[dict[str, Any]] = field(default_factory=list)
    account_response: dict[str, Any] = field(
        default_factory=lambda: {"totalWalletBalance": "10000.00"}
    )
    positions_calls: int = 0
    account_calls: int = 0
    positions_exc: BaseException | None = None
    account_exc: BaseException | None = None

    async def fetch_positions(self) -> list[dict[str, Any]]:
        self.positions_calls += 1
        if self.positions_exc is not None:
            raise self.positions_exc
        return list(self.positions_response)

    async def fetch_account(self) -> dict[str, Any]:
        self.account_calls += 1
        if self.account_exc is not None:
            raise self.account_exc
        return dict(self.account_response)


def _net_row(
    *,
    symbol: str = "BTCUSDT",
    position_amt: str = "0.5",
    entry_price: str = "50000.0",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "positionSide": "BOTH",
        "positionAmt": position_amt,
        "entryPrice": entry_price,
        "unRealizedProfit": "12.34",
        "isolatedMargin": "100.0",
        "liquidationPrice": "0",
    }


# ---------------------------------------------------------------------------
# Position snapshot
# ---------------------------------------------------------------------------


async def test_fetch_position_snapshot_translates_rows_with_clock_ts() -> None:
    rest = FakeRest(positions_response=[_net_row(), _net_row(symbol="ETHUSDT")])
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1700_000_000.0)

    snapshot = await provider.fetch_position_snapshot()

    assert rest.positions_calls == 1
    assert [s.symbol for s in snapshot] == ["BTCUSDT", "ETHUSDT"]
    assert all(s.venue is Venue.BINANCE_UM for s in snapshot)
    assert all(s.ts == 1700_000_000.0 for s in snapshot)


async def test_fetch_position_snapshot_emits_long_for_positive_amt() -> None:
    rest = FakeRest(positions_response=[_net_row(position_amt="0.5")])
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1.0)

    [snap] = await provider.fetch_position_snapshot()

    assert snap.direction is Direction.LONG
    assert snap.qty == 0.5
    assert snap.state is PositionState.OPEN


async def test_fetch_position_snapshot_emits_short_for_negative_amt() -> None:
    rest = FakeRest(positions_response=[_net_row(position_amt="-0.5")])
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1.0)

    [snap] = await provider.fetch_position_snapshot()

    assert snap.direction is Direction.SHORT
    assert snap.qty == 0.5
    assert snap.state is PositionState.OPEN


async def test_fetch_position_snapshot_emits_idle_for_zero_amt() -> None:
    rest = FakeRest(positions_response=[_net_row(position_amt="0")])
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1.0)

    [snap] = await provider.fetch_position_snapshot()

    assert snap.qty == 0.0
    assert snap.state is PositionState.IDLE


async def test_fetch_position_snapshot_drops_hedge_mode_rows() -> None:
    rest = FakeRest(
        positions_response=[
            _net_row(),
            {**_net_row(symbol="ETHUSDT"), "positionSide": "LONG"},
            {**_net_row(symbol="SOLUSDT"), "positionSide": "SHORT"},
        ]
    )
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1.0)

    snapshot = await provider.fetch_position_snapshot()

    assert [s.symbol for s in snapshot] == ["BTCUSDT"]


async def test_fetch_position_snapshot_propagates_translator_error() -> None:
    rest = FakeRest(
        positions_response=[
            {"symbol": "BTCUSDT", "positionSide": "BOTH"},  # missing positionAmt
        ]
    )
    provider = BinanceUmSnapshotProvider(rest, clock=lambda: 1.0)

    with pytest.raises(ValueError):
        await provider.fetch_position_snapshot()


async def test_fetch_position_snapshot_propagates_rest_error() -> None:
    rest = FakeRest(positions_exc=RuntimeError("boom"))
    provider = BinanceUmSnapshotProvider(rest)

    with pytest.raises(RuntimeError, match="boom"):
        await provider.fetch_position_snapshot()


# ---------------------------------------------------------------------------
# Equity snapshot
# ---------------------------------------------------------------------------


async def test_fetch_equity_snapshot_returns_total_wallet_balance() -> None:
    rest = FakeRest(account_response={"totalWalletBalance": "12345.67"})
    provider = BinanceUmSnapshotProvider(rest)

    equity = await provider.fetch_equity_snapshot()

    assert equity == pytest.approx(12345.67)
    assert rest.account_calls == 1


async def test_fetch_equity_snapshot_missing_field_raises_value_error() -> None:
    rest = FakeRest(account_response={"unrelated": "0"})
    provider = BinanceUmSnapshotProvider(rest)

    with pytest.raises(ValueError):
        await provider.fetch_equity_snapshot()


async def test_fetch_equity_snapshot_propagates_rest_error() -> None:
    rest = FakeRest(account_exc=RuntimeError("net down"))
    provider = BinanceUmSnapshotProvider(rest)

    with pytest.raises(RuntimeError, match="net down"):
        await provider.fetch_equity_snapshot()


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


async def test_provider_carries_venue_tag() -> None:
    rest = FakeRest()
    provider = BinanceUmSnapshotProvider(rest)

    assert provider.venue is Venue.BINANCE_UM
