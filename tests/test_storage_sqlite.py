"""Tests for the SQLite DAO + schema."""

from __future__ import annotations

import pytest

from trade_adapter.storage.sqlite import (
    SCHEMA_VERSION,
    AuditRow,
    IdempotencyRow,
    SqliteDAO,
)

pytestmark = pytest.mark.asyncio


async def test_open_writes_schema_meta(dao: SqliteDAO) -> None:
    rows = await dao.execute(
        "SELECT key, value FROM schema_meta WHERE key = ?",
        ("schema_version",),
    )
    assert rows == [("schema_version", str(SCHEMA_VERSION))]


async def test_all_expected_tables_exist(dao: SqliteDAO) -> None:
    rows = await dao.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
    )
    names = {r[0] for r in rows}
    expected = {
        "schema_meta",
        "signals",
        "idempotency",
        "audit_log",
        "orders",
        "fills",
        "positions",
        "reconcile_events",
    }
    assert expected.issubset(names)


async def test_insert_audit_batch_and_read_back(dao: SqliteDAO) -> None:
    rows = [
        AuditRow(
            ts=float(i),
            event_type="signal_received",
            payload_json='{"x":1}',
            correlation_id=f"c-{i}",
            signal_id=f"s-{i}",
        )
        for i in range(3)
    ]
    written = await dao.insert_audit_batch(rows)
    assert written == 3

    out = await dao.execute(
        "SELECT ts, event_type, signal_id FROM audit_log ORDER BY ts",
    )
    assert out == [(0.0, "signal_received", "s-0"),
                   (1.0, "signal_received", "s-1"),
                   (2.0, "signal_received", "s-2")]


async def test_insert_audit_batch_empty_is_noop(dao: SqliteDAO) -> None:
    written = await dao.insert_audit_batch([])
    assert written == 0


async def test_insert_signal_idempotent(dao: SqliteDAO) -> None:
    kwargs = {
        "signal_id": "sig-1",
        "received_at": 1000.0,
        "source": "bot",
        "venue": "binance_um",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "intent": "OPEN",
        "correlation_id": "c-1",
        "payload_json": '{"x":1}',
    }
    await dao.insert_signal(**kwargs)
    # Second insert with same signal_id is silently ignored.
    await dao.insert_signal(**kwargs)
    rows = await dao.execute("SELECT signal_id FROM signals")
    assert rows == [("sig-1",)]


async def test_idempotency_round_trip(dao: SqliteDAO) -> None:
    await dao.upsert_idempotency(IdempotencyRow("sig-1", '{"a":1}', expires_at=2000.0))
    row = await dao.get_idempotency("sig-1")
    assert row is not None
    assert row.response_json == '{"a":1}'
    assert row.expires_at == 2000.0


async def test_idempotency_upsert_overwrites(dao: SqliteDAO) -> None:
    await dao.upsert_idempotency(IdempotencyRow("sig-1", "first", expires_at=2000.0))
    await dao.upsert_idempotency(IdempotencyRow("sig-1", "second", expires_at=3000.0))
    row = await dao.get_idempotency("sig-1")
    assert row is not None
    assert row.response_json == "second"
    assert row.expires_at == 3000.0


async def test_idempotency_list_unexpired_only(dao: SqliteDAO) -> None:
    await dao.upsert_idempotency(IdempotencyRow("a", "1", expires_at=1500.0))
    await dao.upsert_idempotency(IdempotencyRow("b", "2", expires_at=2500.0))
    rows = await dao.list_idempotency_unexpired(now=2000.0)
    assert {r.signal_id for r in rows} == {"b"}


async def test_idempotency_delete_expired(dao: SqliteDAO) -> None:
    await dao.upsert_idempotency(IdempotencyRow("a", "1", expires_at=1500.0))
    await dao.upsert_idempotency(IdempotencyRow("b", "2", expires_at=2500.0))
    deleted = await dao.delete_idempotency_expired(now=2000.0)
    assert deleted == 1
    rows = await dao.execute("SELECT signal_id FROM idempotency")
    assert rows == [("b",)]


async def test_close_then_open_replays_state(tmp_path) -> None:
    path = tmp_path / "state.db"
    d1 = SqliteDAO(path)
    await d1.open()
    await d1.upsert_idempotency(IdempotencyRow("sig-1", "x", expires_at=9e9))
    await d1.close()

    d2 = SqliteDAO(path)
    await d2.open()
    try:
        row = await d2.get_idempotency("sig-1")
        assert row is not None
        assert row.response_json == "x"
    finally:
        await d2.close()


async def test_dao_rejects_use_before_open(tmp_path) -> None:
    d = SqliteDAO(tmp_path / "x.db")
    row = AuditRow(
        ts=1.0,
        event_type="x",
        payload_json="{}",
        correlation_id=None,
        signal_id=None,
    )
    with pytest.raises(RuntimeError):
        await d.insert_audit_batch([row])
