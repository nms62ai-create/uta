"""SQLite schema + DAO.

Decision A5 (post-cleanup): SQLite is the single source of truth for
durable state. There is no Redis in v1.0; all hot caches and pub/sub
are in-process (see ``trade_adapter.bus``).

This module:

1. Owns the schema and a one-step migration (CREATE TABLE IF NOT EXISTS).
   The schema is kept tight: only the things that are actually written
   in Phase 1 (signals, the idempotency mirror, an audit-log table)
   plus *placeholder* tables for the trading hot path so Phase 2 can
   populate them without re-touching this file.
2. Wraps ``aiosqlite`` with a single connection serialized via an
   ``asyncio.Lock``. SQLite's ``WAL`` journal lets readers proceed
   while writers hold the lock; the lock is here to prevent writer
   interleaving inside the adapter, not to serialize SQLite itself.
3. Exposes typed insert helpers for the Phase 1 callers
   (``audit_flusher``, ``idempotency``).

No business logic, no I/O scheduling decisions — just durable record
keeping. The hot path never blocks on this module: writes are batched
through :mod:`trade_adapter.storage.audit_flusher` (decision D.1).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

_log = logging.getLogger(__name__)


SCHEMA_VERSION = 1


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Append-only signal log. Never updated after insertion.
CREATE TABLE IF NOT EXISTS signals (
    signal_id          TEXT PRIMARY KEY,
    received_at        REAL NOT NULL,
    source             TEXT NOT NULL,
    venue              TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    direction          TEXT NOT NULL,
    intent             TEXT NOT NULL,
    correlation_id     TEXT,
    payload_json       TEXT NOT NULL
);

-- Idempotency cache cold tier (D.6). Hydrated into the in-memory
-- hot tier on startup; written-through when the hot tier accepts a
-- new entry.
CREATE TABLE IF NOT EXISTS idempotency (
    signal_id     TEXT PRIMARY KEY,
    response_json TEXT NOT NULL,
    expires_at    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expires_at
    ON idempotency (expires_at);

-- Append-only audit-log (D.1). Each row is one event the adapter
-- accepted, rejected, or emitted. Written by the background flusher
-- in batched transactions; never on the accept path.
CREATE TABLE IF NOT EXISTS audit_log (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    correlation_id TEXT,
    signal_id  TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_ts            ON audit_log (ts);
CREATE INDEX IF NOT EXISTS idx_audit_correlation   ON audit_log (correlation_id);
CREATE INDEX IF NOT EXISTS idx_audit_signal        ON audit_log (signal_id);

-- Placeholder hot-path tables. Populated starting in Phase 2.
CREATE TABLE IF NOT EXISTS orders (
    client_order_id    TEXT PRIMARY KEY,
    exchange_order_id  TEXT,
    signal_id          TEXT,
    venue              TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    side               TEXT NOT NULL,
    order_type         TEXT NOT NULL,
    qty                REAL NOT NULL,
    price              REAL,
    stop_price         REAL,
    status             TEXT NOT NULL,
    created_at         REAL NOT NULL,
    updated_at         REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id            TEXT PRIMARY KEY,
    client_order_id    TEXT NOT NULL,
    exchange_order_id  TEXT NOT NULL,
    venue              TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    side               TEXT NOT NULL,
    qty                REAL NOT NULL,
    price              REAL NOT NULL,
    fee_usd            REAL NOT NULL,
    is_maker           INTEGER NOT NULL,
    ts                 REAL NOT NULL,
    signal_id          TEXT,
    correlation_id     TEXT
);

CREATE TABLE IF NOT EXISTS positions (
    venue              TEXT NOT NULL,
    symbol             TEXT NOT NULL,
    direction          TEXT NOT NULL,
    qty                REAL NOT NULL,
    entry_price        REAL NOT NULL,
    state              TEXT NOT NULL,
    liquidation_price  REAL,
    unrealized_pnl_usd REAL,
    margin_used_usd    REAL,
    opened_at          REAL,
    updated_at         REAL NOT NULL,
    PRIMARY KEY (venue, symbol)
);

CREATE TABLE IF NOT EXISTS reconcile_events (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    venue      TEXT NOT NULL,
    symbol     TEXT NOT NULL,
    kind       TEXT NOT NULL,
    detail_json TEXT NOT NULL
);
"""


@dataclass(slots=True)
class AuditRow:
    """One row to be appended to the audit log by the background flusher."""

    ts: float
    event_type: str
    payload_json: str
    correlation_id: str | None
    signal_id: str | None


@dataclass(slots=True)
class IdempotencyRow:
    signal_id: str
    response_json: str
    expires_at: float


class SqliteDAO:
    """Async DAO over a single ``aiosqlite`` connection."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        if self._conn is not None:
            return
        conn = await aiosqlite.connect(self._path)
        # WAL gives us non-blocking readers + a single writer; lifts
        # short locking windows on the audit-flusher batch path.
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        await conn.executescript(SCHEMA_SQL)
        await conn.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        await conn.commit()
        self._conn = conn
        _log.debug("sqlite open path=%s schema_version=%d", self._path, SCHEMA_VERSION)

    async def close(self) -> None:
        async with self._lock:
            if self._conn is None:
                return
            await self._conn.close()
            self._conn = None

    @property
    def path(self) -> str:
        return self._path

    # --- Audit log ---------------------------------------------------

    async def insert_audit_batch(self, rows: Iterable[AuditRow]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        async with self._lock:
            conn = self._require_conn()
            await conn.executemany(
                """
                INSERT INTO audit_log (ts, event_type, payload_json, correlation_id, signal_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (r.ts, r.event_type, r.payload_json, r.correlation_id, r.signal_id)
                    for r in rows
                ],
            )
            await conn.commit()
        return len(rows)

    # --- Signals -----------------------------------------------------

    async def insert_signal(
        self,
        *,
        signal_id: str,
        received_at: float,
        source: str,
        venue: str,
        symbol: str,
        direction: str,
        intent: str,
        correlation_id: str | None,
        payload_json: str,
    ) -> None:
        async with self._lock:
            conn = self._require_conn()
            await conn.execute(
                """
                INSERT OR IGNORE INTO signals
                    (signal_id, received_at, source, venue, symbol,
                     direction, intent, correlation_id, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    received_at,
                    source,
                    venue,
                    symbol,
                    direction,
                    intent,
                    correlation_id,
                    payload_json,
                ),
            )
            await conn.commit()

    # --- Idempotency cold tier --------------------------------------

    async def upsert_idempotency(self, row: IdempotencyRow) -> None:
        async with self._lock:
            conn = self._require_conn()
            await conn.execute(
                """
                INSERT INTO idempotency (signal_id, response_json, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT(signal_id) DO UPDATE SET
                    response_json = excluded.response_json,
                    expires_at    = excluded.expires_at
                """,
                (row.signal_id, row.response_json, row.expires_at),
            )
            await conn.commit()

    async def get_idempotency(self, signal_id: str) -> IdempotencyRow | None:
        async with self._lock:
            conn = self._require_conn()
            cursor = await conn.execute(
                "SELECT signal_id, response_json, expires_at "
                "FROM idempotency WHERE signal_id = ?",
                (signal_id,),
            )
            row = await cursor.fetchone()
            await cursor.close()
        if row is None:
            return None
        return IdempotencyRow(
            signal_id=row[0],
            response_json=row[1],
            expires_at=float(row[2]),
        )

    async def list_idempotency_unexpired(self, now: float) -> list[IdempotencyRow]:
        async with self._lock:
            conn = self._require_conn()
            cursor = await conn.execute(
                "SELECT signal_id, response_json, expires_at "
                "FROM idempotency WHERE expires_at > ?",
                (now,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
        return [
            IdempotencyRow(
                signal_id=r[0],
                response_json=r[1],
                expires_at=float(r[2]),
            )
            for r in rows
        ]

    async def delete_idempotency_expired(self, now: float) -> int:
        async with self._lock:
            conn = self._require_conn()
            cursor = await conn.execute(
                "DELETE FROM idempotency WHERE expires_at <= ?",
                (now,),
            )
            count = cursor.rowcount or 0
            await cursor.close()
            await conn.commit()
        return count

    # --- Internal ----------------------------------------------------

    def _require_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("SqliteDAO is not open")
        return self._conn

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        """Escape hatch for tests / future modules. Use sparingly."""

        async with self._lock:
            conn = self._require_conn()
            cursor = await conn.execute(sql, params)
            rows = await cursor.fetchall()
            await cursor.close()
            return rows
