"""State storage.

SQLite is the single source of truth for positions, orders, fills, and
audit logs. Redis is used for ephemeral pub/sub and hot caches; loss of
Redis must not corrupt persistent state.

Modules:
    sqlite.py        - Schema, migrations, DAO. Single ``aiosqlite``
                       connection serialized via ``asyncio.Lock``.
                       Tables: signals, orders, fills, positions,
                       reconcile_events.
    redis_pubsub.py  - Internal pub/sub (``events.fill``, ``events.order``,
                       ``events.position``, ``events.alert``) and hot
                       cache (best bid/ask per symbol with short TTL).
                       Falls back to in-process queue if Redis is
                       unreachable.

Audit semantics:
    - ``signals`` table is append-only. The DAO never exposes a DELETE.
    - ``fills`` table is append-only.
    - ``reconcile_events`` table is append-only.
"""
