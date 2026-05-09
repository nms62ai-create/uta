"""Tests for the background batched audit-log writer (decision D.1)."""

from __future__ import annotations

import asyncio

import pytest

from trade_adapter.storage.audit_flusher import AuditFlusher
from trade_adapter.storage.sqlite import AuditRow, SqliteDAO

pytestmark = pytest.mark.asyncio


def _row(i: int) -> AuditRow:
    return AuditRow(
        ts=float(i),
        event_type="signal_received",
        payload_json="{}",
        correlation_id=f"c-{i}",
        signal_id=f"s-{i}",
    )


async def test_flush_now_drains_queue(dao: SqliteDAO) -> None:
    flusher = AuditFlusher(dao, batch_size=8)
    for i in range(3):
        flusher.append(_row(i))
    written = await flusher.flush_now()
    assert written == 3
    rows = await dao.execute("SELECT signal_id FROM audit_log ORDER BY ts")
    assert [r[0] for r in rows] == ["s-0", "s-1", "s-2"]


async def test_run_flushes_on_interval(dao: SqliteDAO) -> None:
    flusher = AuditFlusher(dao, flush_interval_s=0.05, batch_size=64)
    await flusher.start()
    try:
        for i in range(3):
            flusher.append(_row(i))
        # Wait long enough for at least one flush interval.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if flusher.flushed_count >= 3:
                break
        assert flusher.flushed_count == 3
    finally:
        await flusher.stop()


async def test_run_flushes_when_batch_full(dao: SqliteDAO) -> None:
    # Tight batch; loose interval so the batch trigger is the cause.
    flusher = AuditFlusher(dao, flush_interval_s=10.0, batch_size=4)
    await flusher.start()
    try:
        for i in range(4):
            flusher.append(_row(i))
        # The flusher pulled the first row off the queue (its
        # blocking get) and then greedily drained 3 more — that is
        # exactly batch_size=4, so a single batch fires within the
        # event loop. Give it one tick to commit.
        for _ in range(20):
            await asyncio.sleep(0.05)
            if flusher.flushed_count == 4:
                break
        assert flusher.flushed_count == 4
        assert flusher.batches_count == 1
    finally:
        await flusher.stop()


async def test_stop_drains_pending_rows(dao: SqliteDAO) -> None:
    flusher = AuditFlusher(dao, flush_interval_s=10.0, batch_size=128)
    await flusher.start()
    for i in range(5):
        flusher.append(_row(i))
    # Stop should drain remaining queue before exiting.
    await flusher.stop()
    rows = await dao.execute("SELECT COUNT(*) FROM audit_log")
    assert rows[0][0] == 5


async def test_full_queue_drops_oldest(dao: SqliteDAO) -> None:
    # queue_max must be >= batch_size; pick small but valid pair.
    flusher = AuditFlusher(dao, batch_size=2, queue_max=2)
    flusher.append(_row(0))
    flusher.append(_row(1))
    # Queue is now full. Pushing a third triggers drop-oldest.
    flusher.append(_row(2))
    assert flusher.dropped_count == 1
    written = await flusher.flush_now()
    assert written == 2
    rows = await dao.execute("SELECT signal_id FROM audit_log ORDER BY ts")
    # Drop-oldest: row 0 was the one dropped.
    assert [r[0] for r in rows] == ["s-1", "s-2"]


async def test_starting_twice_raises(dao: SqliteDAO) -> None:
    flusher = AuditFlusher(dao)
    await flusher.start()
    try:
        with pytest.raises(RuntimeError):
            await flusher.start()
    finally:
        await flusher.stop()


async def test_invalid_params_rejected(dao: SqliteDAO) -> None:
    with pytest.raises(ValueError):
        AuditFlusher(dao, flush_interval_s=0.0)
    with pytest.raises(ValueError):
        AuditFlusher(dao, batch_size=0)
    with pytest.raises(ValueError):
        AuditFlusher(dao, batch_size=10, queue_max=5)
