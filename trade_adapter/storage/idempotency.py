"""Idempotency cache (D.6).

Two-tier cache: an in-memory hot tier keyed by ``signal_id`` and a
SQLite cold tier mirror (table ``idempotency`` in
:mod:`trade_adapter.storage.sqlite`).

Why two tiers
-------------
* Hot tier — every accept-path lookup is in-memory ``dict`` access,
  zero I/O, zero awaits beyond the lock acquire. Decision A20 + D.1
  require the accept path to be free of disk waits; this cache is the
  authoritative dedup guard during accept.
* Cold tier — SQLite is the truth across process restarts. On
  startup the hot tier is hydrated from SQLite (``hydrate``) so a
  process crash + restart still rejects a re-submitted ``signal_id``
  for the duration of its TTL.

Eviction
--------
* Per-entry TTL (default 1 h, overridable in config). Entries past
  their ``expires_at`` are treated as not-present.
* On insert, if the hot tier exceeds ``max_entries``, the oldest
  entry by ``inserted_at`` is evicted from the hot tier (the cold
  tier still holds it until expiry; subsequent lookups will fall
  through to SQLite).
* A periodic GC task (started via :meth:`start_gc`) deletes expired
  rows from the cold tier so the table does not grow without bound.

The cache stores opaque JSON-encoded responses; it does not know
about ``SignalAck`` or any other adapter type. The shape is the
caller's responsibility.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from collections.abc import Callable

from .sqlite import IdempotencyRow, SqliteDAO

_log = logging.getLogger(__name__)

DEFAULT_TTL_S = 3600.0
DEFAULT_MAX_ENTRIES = 100_000
DEFAULT_GC_INTERVAL_S = 60.0


class IdempotencyCache:
    """Two-tier idempotency cache."""

    def __init__(
        self,
        dao: SqliteDAO,
        *,
        ttl_s: float = DEFAULT_TTL_S,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        gc_interval_s: float = DEFAULT_GC_INTERVAL_S,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if ttl_s <= 0:
            raise ValueError("ttl_s must be > 0")
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self._dao = dao
        self._ttl_s = ttl_s
        self._max_entries = max_entries
        self._gc_interval_s = gc_interval_s
        self._clock = clock or time.time
        # OrderedDict preserves insertion order; we evict from the
        # left when over capacity.
        self._hot: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._gc_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()

    @property
    def hot_size(self) -> int:
        return len(self._hot)

    async def hydrate(self) -> int:
        """Load non-expired cold-tier rows into the hot tier."""

        now = self._clock()
        rows = await self._dao.list_idempotency_unexpired(now)
        # Most-recently-expiring entries should sit at the right of
        # the OrderedDict so eviction tosses the closest-to-expiry
        # ones first.
        rows.sort(key=lambda r: r.expires_at)
        for r in rows:
            self._hot[r.signal_id] = (r.response_json, r.expires_at)
            if len(self._hot) > self._max_entries:
                self._hot.popitem(last=False)
        _log.debug("idempotency hydrated entries=%d", len(self._hot))
        return len(self._hot)

    async def get(self, signal_id: str) -> str | None:
        """Return cached ``response_json`` if present and not expired."""

        now = self._clock()
        entry = self._hot.get(signal_id)
        if entry is not None:
            response_json, expires_at = entry
            if expires_at > now:
                return response_json
            # Expired: drop from hot tier. The cold tier GC will
            # delete it on its next pass.
            self._hot.pop(signal_id, None)
            return None
        # Hot miss. Fall through to cold tier (rare path).
        row = await self._dao.get_idempotency(signal_id)
        if row is None or row.expires_at <= now:
            return None
        # Repopulate the hot tier so subsequent hits stay fast.
        self._hot[signal_id] = (row.response_json, row.expires_at)
        if len(self._hot) > self._max_entries:
            self._hot.popitem(last=False)
        return row.response_json

    async def set(self, signal_id: str, response_json: str) -> None:
        """Store ``response_json`` in both tiers."""

        now = self._clock()
        expires_at = now + self._ttl_s
        # Hot tier: replace if exists, else insert.
        if signal_id in self._hot:
            self._hot.move_to_end(signal_id)
        self._hot[signal_id] = (response_json, expires_at)
        while len(self._hot) > self._max_entries:
            self._hot.popitem(last=False)
        # Cold tier: best-effort write-through.
        await self._dao.upsert_idempotency(
            IdempotencyRow(
                signal_id=signal_id,
                response_json=response_json,
                expires_at=expires_at,
            )
        )

    async def gc_now(self) -> int:
        """Sweep expired entries from both tiers. Returns rows deleted from cold tier."""

        now = self._clock()
        expired_hot = [k for k, (_, exp) in self._hot.items() if exp <= now]
        for k in expired_hot:
            self._hot.pop(k, None)
        deleted = await self._dao.delete_idempotency_expired(now)
        if expired_hot or deleted:
            _log.debug(
                "idempotency gc hot_evicted=%d cold_deleted=%d",
                len(expired_hot),
                deleted,
            )
        return deleted

    async def start_gc(self) -> None:
        if self._gc_task is not None:
            raise RuntimeError("GC already started")
        self._stopping.clear()
        self._gc_task = asyncio.create_task(self._gc_loop(), name="idempotency-gc")

    async def stop_gc(self) -> None:
        if self._gc_task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(self._gc_task, timeout=5.0)
        except TimeoutError:  # pragma: no cover - defensive
            self._gc_task.cancel()
            try:
                await self._gc_task
            except asyncio.CancelledError:
                pass
        self._gc_task = None

    async def _gc_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._gc_interval_s,
                )
            except TimeoutError:
                pass
            if self._stopping.is_set():
                return
            try:
                await self.gc_now()
            except Exception:  # pragma: no cover - defensive
                _log.exception("idempotency gc failed")
