"""State storage.

SQLite is the single source of truth (decision A5, post-cleanup) for
positions, orders, fills, the audit log, and the cold-tier idempotency
mirror. There is no Redis in v1.0; ephemeral pub/sub is in-process
(see :mod:`trade_adapter.bus`). Loss of the SQLite file is a hard
failure and the process is expected to exit.

Modules:
    sqlite.py          - Schema, DAO. Single ``aiosqlite`` connection
                         serialized via ``asyncio.Lock``. Tables:
                         ``signals``, ``orders``, ``fills``,
                         ``positions``, ``reconcile_events``,
                         ``idempotency``, ``audit_log``.
    audit_flusher.py   - Background batched writer (decision D.1).
                         Append events from the hot path into an
                         in-memory queue; the flusher drains them
                         into the ``audit_log`` table off the
                         accept path.
    idempotency.py     - Two-tier ``signal_id`` cache (decision D.6).
                         In-memory hot tier + SQLite cold tier;
                         hydrated on startup.

Audit semantics:
    - ``signals`` table is append-only. The DAO never exposes a DELETE.
    - ``fills`` table is append-only.
    - ``reconcile_events`` table is append-only.
    - ``audit_log`` table is append-only.
"""
