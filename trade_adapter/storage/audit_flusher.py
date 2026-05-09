"""Background batched audit-log writer.

Implements decision **D.1** (post-cleanup): the synchronous accept
path never waits on disk. Producers append events to an in-memory
queue with no fsync; a background task drains the queue periodically
or when it reaches a batch threshold and writes the batch to SQLite
in a single transaction.

Design
------
* ``append(...)`` is non-blocking. It calls ``put_nowait`` against a
  bounded ``asyncio.Queue``; if that queue is somehow full (operator
  ran out of disk and the flusher cannot drain), the oldest pending
  audit row is dropped and a counter is incremented. Audit-log loss
  is preferred to stalling the trading hot path; an alert is also
  surfaced (via the ``dropped_count`` counter, which the metrics
  layer reads in Phase 3).
* The flusher runs as a single asyncio task that batches writes by
  ``flush_interval_s`` (default 100 ms) or ``batch_size`` (default
  256), whichever happens first.
* On ``stop()``, the flusher drains its queue once more before
  exiting so a graceful shutdown does not lose audit rows.

This module is consumed by:
    * ``trade_adapter.core.signal_router`` (Phase 3) — every accept /
      reject decision.
    * ``trade_adapter.core.position_manager`` (Phase 3) — every
      ``OrderRequest`` / ``Fill`` / state transition.
    * ``trade_adapter.core.outcome`` (Phase 3) — every
      ``OutcomeReport``.

In Phase 1 only the wiring is in place; the producers will land
later.
"""

from __future__ import annotations

import asyncio
import logging

from .sqlite import AuditRow, SqliteDAO

_log = logging.getLogger(__name__)

DEFAULT_FLUSH_INTERVAL_S = 0.1
DEFAULT_BATCH_SIZE = 256
DEFAULT_QUEUE_MAX = 4096


class AuditFlusher:
    """Drain audit rows into SQLite off the hot path."""

    def __init__(
        self,
        dao: SqliteDAO,
        *,
        flush_interval_s: float = DEFAULT_FLUSH_INTERVAL_S,
        batch_size: int = DEFAULT_BATCH_SIZE,
        queue_max: int = DEFAULT_QUEUE_MAX,
    ) -> None:
        if flush_interval_s <= 0:
            raise ValueError("flush_interval_s must be > 0")
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if queue_max < batch_size:
            raise ValueError("queue_max must be >= batch_size")

        self._dao = dao
        self._flush_interval_s = flush_interval_s
        self._batch_size = batch_size
        self._queue: asyncio.Queue[AuditRow] = asyncio.Queue(maxsize=queue_max)
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._dropped_count = 0
        self._flushed_count = 0
        self._batches_count = 0

    @property
    def dropped_count(self) -> int:
        return self._dropped_count

    @property
    def flushed_count(self) -> int:
        return self._flushed_count

    @property
    def batches_count(self) -> int:
        return self._batches_count

    def append(self, row: AuditRow) -> None:
        """Enqueue ``row`` for the next batch. Non-blocking.

        If the queue is full (back-pressure from a slow disk),
        drop-oldest and increment ``dropped_count``.
        """

        try:
            self._queue.put_nowait(row)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - tiny race
                pass
            self._dropped_count += 1
            try:
                self._queue.put_nowait(row)
            except asyncio.QueueFull:  # pragma: no cover - tiny race
                self._dropped_count += 1

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("AuditFlusher already started")
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="audit-flusher")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(self._task, timeout=5.0)
        except TimeoutError:  # pragma: no cover - defensive
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def flush_now(self) -> int:
        """Drain everything currently queued, in one batch. For tests."""

        return await self._drain_one_batch(max_rows=self._queue.qsize())

    # --- Internal ----------------------------------------------------

    async def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                # Wait either for a row or the flush interval to elapse.
                try:
                    first = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=self._flush_interval_s,
                    )
                except TimeoutError:
                    continue
                # Got at least one row; greedily collect up to batch_size.
                batch: list[AuditRow] = [first]
                while len(batch) < self._batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                await self._write_batch(batch)
            # Stopping: drain anything still pending so a graceful
            # shutdown does not lose audit rows.
            await self._drain_remaining()
        except asyncio.CancelledError:  # pragma: no cover
            await self._drain_remaining()
            raise
        except Exception:  # pragma: no cover - defensive
            _log.exception("audit-flusher crashed; remaining queue size=%d",
                           self._queue.qsize())
            raise

    async def _drain_remaining(self) -> None:
        while True:
            written = await self._drain_one_batch(max_rows=self._batch_size)
            if written == 0:
                return

    async def _drain_one_batch(self, *, max_rows: int) -> int:
        if max_rows <= 0:
            return 0
        batch: list[AuditRow] = []
        while len(batch) < max_rows:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if not batch:
            return 0
        await self._write_batch(batch)
        return len(batch)

    async def _write_batch(self, batch: list[AuditRow]) -> None:
        try:
            written = await self._dao.insert_audit_batch(batch)
        except Exception:  # pragma: no cover - defensive
            # SQLite failure is treated as a hard error per spec
            # ("Crash the process. State must be intact; restart and
            # recover."). Re-raise so the supervising task can decide.
            _log.exception("audit-flusher: insert_audit_batch failed; rows=%d",
                           len(batch))
            raise
        else:
            self._flushed_count += written
            self._batches_count += 1
