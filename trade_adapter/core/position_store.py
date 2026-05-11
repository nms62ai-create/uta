"""Phase 3e: in-memory store of positions + equity, indexed by ``(venue, symbol)`` and ``venue``.

The store is a pure data structure — no I/O, no clock, no async. It owns
the authoritative *local* mirror of venue state for the adapter; the
position_manager keeps it in sync with the venue via REST bootstrap +
user-data-stream events.

Externally, the store satisfies both :class:`PositionProvider` and
:class:`EquityProvider` (structural Protocols defined in
:mod:`trade_adapter.core.protocols`), so the signal_router can read
through it directly without an intermediate adapter.

Update model
------------
* :class:`PositionUpdate` events are write-through:
  :meth:`apply_position_update` derives a :class:`Position` snapshot and
  stores it under ``(venue, symbol)``. Flat positions (``qty == 0``)
  are *kept* in the store rather than deleted, so reconciliation can
  distinguish "never seen this symbol" (``None``) from "have seen it,
  it's currently flat" (``qty == 0``).
* :class:`Fill` events are recorded by count but do not mutate
  positions — the venue's ``ACCOUNT_UPDATE`` is the authoritative
  post-fill snapshot, and applying a fill on top of it would double-count.
  The counter is exposed for observability and reconciliation tests.
* Equity is set wholesale via :meth:`apply_equity_update`. The
  position_manager calls this after each REST snapshot and after each
  user-data event that carries a wallet-balance update.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from ..types import (
    Fill,
    Position,
    PositionUpdate,
    Venue,
)

_log = logging.getLogger(__name__)


@dataclass(slots=True)
class PositionStore:
    """In-memory mirror of venue position state."""

    _positions: dict[tuple[Venue, str], Position] = field(default_factory=dict)
    _equities: dict[Venue, float] = field(default_factory=dict)
    _fill_count: dict[tuple[Venue, str], int] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # PositionProvider surface
    # ------------------------------------------------------------------

    def get_position(self, venue: Venue, symbol: str) -> Position | None:
        return self._positions.get((venue, symbol))

    # ------------------------------------------------------------------
    # EquityProvider surface
    # ------------------------------------------------------------------

    def get_equity_usd(self, venue: Venue) -> float | None:
        return self._equities.get(venue)

    # ------------------------------------------------------------------
    # Mutators (called by position_manager)
    # ------------------------------------------------------------------

    def apply_position_update(self, update: PositionUpdate) -> None:
        """Write a :class:`PositionUpdate` to the store.

        Keeps flat positions (``qty == 0``) so reconciliation can tell
        "never traded" from "traded and now flat".
        """

        self._positions[(update.venue, update.symbol)] = Position(
            venue=update.venue,
            symbol=update.symbol,
            direction=update.direction,
            qty=update.qty,
            entry_price=update.entry_price,
            state=update.state,
            liquidation_price=update.liquidation_price,
            unrealized_pnl_usd=update.unrealized_pnl_usd,
            margin_used_usd=update.margin_used_usd,
            opened_at=None,
        )

    def apply_equity_update(self, venue: Venue, equity_usd: float) -> None:
        """Set total equity in USD for ``venue``."""

        if equity_usd < 0:
            raise ValueError(f"equity_usd must be >= 0, got {equity_usd}")
        self._equities[venue] = equity_usd

    def apply_fill(self, fill: Fill) -> None:
        """Record a fill for observability.

        Does *not* mutate the corresponding position: the venue's
        ACCOUNT_UPDATE is the authoritative post-fill snapshot, and
        adding the fill's delta would double-count once that snapshot
        arrives.
        """

        key = (fill.venue, fill.symbol)
        self._fill_count[key] = self._fill_count.get(key, 0) + 1

    # ------------------------------------------------------------------
    # Introspection (used by reconciliation + tests)
    # ------------------------------------------------------------------

    def fill_count(self, venue: Venue, symbol: str) -> int:
        return self._fill_count.get((venue, symbol), 0)

    def iter_positions(self) -> list[Position]:
        """All positions currently in the store, flat or not."""

        return list(self._positions.values())

    def open_positions(self) -> list[Position]:
        """Subset of :meth:`iter_positions` with ``qty > 0``."""

        return [p for p in self._positions.values() if p.qty > 0]

    def remove_position(self, venue: Venue, symbol: str) -> None:
        """Drop a position from the store entirely.

        Used by reconciliation when the venue says a symbol no longer
        exists at all (rare — Binance keeps flat positions in
        ``positionRisk`` indefinitely, so this is mostly defensive).
        """

        self._positions.pop((venue, symbol), None)


__all__ = ["PositionStore"]
