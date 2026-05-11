"""Phase 3e: :class:`PositionManager` — REST bootstrap + reconcile + event ingestion.

Sits between the venue (REST + user-data WS stream) and the in-memory
:class:`PositionStore`. Its job is to keep the store's view of the world
consistent with the venue's, and to surface any drift it spots as a
:class:`ReconcileDiff` event on the bus.

Three responsibilities
----------------------

1. **Bootstrap** at :meth:`start`: pulls the current position list and
   total equity from :class:`VenueSnapshotProvider` (a thin Protocol that
   each venue adapter implements over its own REST + translators) and
   writes them into the store. No partial bootstrap — both fetches
   complete or the manager refuses to start.

2. **Event ingestion**: the venue adapter (Phase 3g wires it up) calls
   :meth:`apply_position_update` / :meth:`apply_fill` /
   :meth:`apply_equity_update` for each user-data-stream event. These are
   the hot path; they don't touch the network and don't await anything
   beyond the store's in-memory write.

3. **Reconciliation**: a background coroutine re-runs the bootstrap
   every :attr:`reconcile_interval_s` seconds, diffs the snapshot
   against the store, and (a) overwrites the store with the venue's
   truth, (b) publishes one :class:`ReconcileDiff` per drift. The
   periodic sweep covers the user-data-stream gap-or-drop case where a
   missed event would otherwise leave the store stale forever.

The manager is venue-agnostic — every Binance-specific bit lives behind
the :class:`VenueSnapshotProvider` Protocol. Bybit will reuse this class
unchanged in Phase 4.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..bus.event_bus import EventBus
from ..serialization import reconcile_diff_to_wire
from ..types import (
    EventType,
    Fill,
    Position,
    PositionUpdate,
    ReconcileDiff,
    Venue,
)
from .position_store import PositionStore
from .protocols import VenueSnapshotProvider

_log = logging.getLogger(__name__)


# Drift threshold for entry-price comparison. Floats round-trip through
# Binance JSON with sub-cent jitter; anything inside this is considered
# the "same" entry.
_ENTRY_PRICE_EPS = 1e-8
_EQUITY_EPS = 1e-6


@dataclass(slots=True)
class _ReconcileSummary:
    """Result of one reconcile pass. Returned by :meth:`reconcile_once`."""

    diffs: list[ReconcileDiff]


class PositionManager:
    """See module docstring."""

    def __init__(
        self,
        *,
        venue: Venue,
        store: PositionStore,
        snapshot_provider: VenueSnapshotProvider,
        event_bus: EventBus | None = None,
        reconcile_interval_s: float = 30.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if reconcile_interval_s < 0:
            raise ValueError("reconcile_interval_s must be >= 0")
        self._venue = venue
        self._store = store
        self._snapshot_provider = snapshot_provider
        self._event_bus = event_bus
        self._reconcile_interval_s = reconcile_interval_s
        self._clock = clock

        self._started = False
        self._stopping = asyncio.Event()
        self._reconcile_task: asyncio.Task[None] | None = None

    @property
    def venue(self) -> Venue:
        return self._venue

    @property
    def store(self) -> PositionStore:
        return self._store

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Run REST bootstrap, then (if interval > 0) start the reconcile loop."""

        if self._started:
            raise RuntimeError("PositionManager already started")
        await self._bootstrap()
        self._started = True
        self._stopping.clear()
        if self._reconcile_interval_s > 0:
            self._reconcile_task = asyncio.create_task(
                self._reconcile_loop(), name="position-manager-reconcile"
            )

    async def stop(self) -> None:
        """Stop the reconcile loop. Safe to call before :meth:`start`."""

        self._stopping.set()
        task = self._reconcile_task
        self._reconcile_task = None
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except TimeoutError:  # pragma: no cover - defensive
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._started = False

    # ------------------------------------------------------------------
    # Event ingestion (called by Phase 3g wiring)
    # ------------------------------------------------------------------

    def apply_position_update(self, update: PositionUpdate) -> None:
        """Apply a translated :class:`PositionUpdate` to the store."""

        self._store.apply_position_update(update)

    def apply_fill(self, fill: Fill) -> None:
        """Record a fill in the store (counter only — see :class:`PositionStore`)."""

        self._store.apply_fill(fill)

    def apply_equity_update(self, equity_usd: float) -> None:
        """Update the venue's total equity in USD."""

        self._store.apply_equity_update(self._venue, equity_usd)

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    async def reconcile_once(self) -> _ReconcileSummary:
        """Fetch a fresh snapshot, diff against the store, apply truth, return diffs.

        The venue is authoritative — any drift causes the store to be
        rewritten to match. Per drift, one :class:`ReconcileDiff` event
        is published on the bus and included in the return value.
        """

        snapshot_positions = await self._snapshot_provider.fetch_position_snapshot()
        snapshot_equity = await self._snapshot_provider.fetch_equity_snapshot()

        diffs: list[ReconcileDiff] = []
        snap_by_key = {(p.venue, p.symbol): p for p in snapshot_positions}
        # The store is multi-venue (Phase 4 will run a second manager for
        # Bybit against the same store). Restrict the diff set to entries
        # owned by this manager's venue so we never touch another venue's
        # rows.
        store_by_key = {
            (p.venue, p.symbol): p
            for p in self._store.iter_positions()
            if p.venue == self._venue
        }

        for key in snap_by_key.keys() | store_by_key.keys():
            venue, symbol = key
            snap = snap_by_key.get(key)
            stored = store_by_key.get(key)

            if snap is None and stored is not None:
                # Venue says the symbol no longer exists; store has it.
                # Binance keeps flat rows in positionRisk indefinitely
                # so this is rare, but trust the venue regardless.
                diff = self._make_diff(
                    venue,
                    symbol,
                    kind="missing_on_venue",
                    detail={
                        "stored_qty": _fmt_float(stored.qty),
                        "stored_direction": stored.direction.value,
                    },
                )
                self._store.remove_position(venue, symbol)
                diffs.append(diff)
                self._publish_diff(diff)
                continue

            if snap is not None and stored is None:
                # Venue has a position we have never seen — possible if
                # the bootstrap was started after the position was opened
                # by another process. Apply silently if flat, but emit
                # a drift event if it's actually open.
                self._store.apply_position_update(snap)
                if snap.qty > 0:
                    diff = self._make_diff(
                        venue,
                        symbol,
                        kind="missing_in_store",
                        detail={
                            "venue_qty": _fmt_float(snap.qty),
                            "venue_direction": snap.direction.value,
                            "venue_entry_price": _fmt_float(snap.entry_price),
                        },
                    )
                    diffs.append(diff)
                    self._publish_diff(diff)
                continue

            # Both sides have the symbol. Compare authoritatively.
            assert snap is not None and stored is not None
            if _positions_differ(snap, stored):
                diff = self._make_diff(
                    venue,
                    symbol,
                    kind="drift",
                    detail={
                        "stored_qty": _fmt_float(stored.qty),
                        "venue_qty": _fmt_float(snap.qty),
                        "stored_direction": stored.direction.value,
                        "venue_direction": snap.direction.value,
                        "stored_entry_price": _fmt_float(stored.entry_price),
                        "venue_entry_price": _fmt_float(snap.entry_price),
                    },
                )
                self._store.apply_position_update(snap)
                diffs.append(diff)
                self._publish_diff(diff)

        # Equity drift -----------------------------------------------------
        stored_equity = self._store.get_equity_usd(self._venue)
        if stored_equity is None or abs(stored_equity - snapshot_equity) > _EQUITY_EPS:
            old_value = _fmt_float(stored_equity) if stored_equity is not None else "null"
            diff = self._make_diff(
                self._venue,
                "",
                kind="equity_drift",
                detail={
                    "stored_equity_usd": old_value,
                    "venue_equity_usd": _fmt_float(snapshot_equity),
                },
            )
            self._store.apply_equity_update(self._venue, snapshot_equity)
            diffs.append(diff)
            self._publish_diff(diff)

        return _ReconcileSummary(diffs=diffs)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _bootstrap(self) -> None:
        positions = await self._snapshot_provider.fetch_position_snapshot()
        equity = await self._snapshot_provider.fetch_equity_snapshot()
        for p in positions:
            self._store.apply_position_update(p)
        self._store.apply_equity_update(self._venue, equity)
        _log.info(
            "position_manager bootstrap: venue=%s positions=%d equity_usd=%s",
            self._venue.value,
            len(positions),
            equity,
        )

    async def _reconcile_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._reconcile_interval_s,
                )
            except TimeoutError:
                pass
            if self._stopping.is_set():
                return
            try:
                await self.reconcile_once()
            except Exception:  # pragma: no cover - defensive
                _log.exception("position_manager reconcile pass failed")

    def _make_diff(
        self, venue: Venue, symbol: str, *, kind: str, detail: dict[str, str]
    ) -> ReconcileDiff:
        return ReconcileDiff(
            venue=venue,
            symbol=symbol,
            kind=kind,
            detail=detail,
            ts=self._clock(),
        )

    def _publish_diff(self, diff: ReconcileDiff) -> None:
        if self._event_bus is None:
            return
        self._event_bus.publish(
            EventType.RECONCILE_DIFF.value, reconcile_diff_to_wire(diff)
        )


# ---------------------------------------------------------------------------
# Helpers (module-private)
# ---------------------------------------------------------------------------


def _positions_differ(a: PositionUpdate, b: Position) -> bool:
    """True if the two positions differ on anything we care about.

    ``b`` is the stored :class:`Position` snapshot — same fields as
    :class:`PositionUpdate` but lacks ``ts`` / producer correlation IDs.
    """

    if a.qty != b.qty:
        return True
    if a.direction != b.direction:
        return True
    if a.state != b.state:
        return True
    if abs(a.entry_price - b.entry_price) > _ENTRY_PRICE_EPS:
        return True
    return False


def _fmt_float(v: float) -> str:
    """Stable, locale-free string rendering of a float for diff payloads."""

    return repr(v)


__all__ = ["PositionManager"]
