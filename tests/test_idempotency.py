"""Tests for the two-tier idempotency cache (decision D.6)."""

from __future__ import annotations

import pytest

from trade_adapter.storage.idempotency import IdempotencyCache
from trade_adapter.storage.sqlite import SqliteDAO

pytestmark = pytest.mark.asyncio


async def test_set_then_get_returns_cached_response(dao: SqliteDAO) -> None:
    cache = IdempotencyCache(dao, ttl_s=60.0)
    await cache.set("sig-1", '{"accepted": true}')
    got = await cache.get("sig-1")
    assert got == '{"accepted": true}'


async def test_unknown_signal_returns_none(dao: SqliteDAO) -> None:
    cache = IdempotencyCache(dao, ttl_s=60.0)
    assert await cache.get("nope") is None


async def test_expired_entry_is_dropped(dao: SqliteDAO) -> None:
    now = [1000.0]

    def clock() -> float:
        return now[0]

    cache = IdempotencyCache(dao, ttl_s=10.0, clock=clock)
    await cache.set("sig-1", "x")
    assert await cache.get("sig-1") == "x"
    now[0] = 1100.0
    assert await cache.get("sig-1") is None


async def test_hot_miss_falls_through_to_cold(dao: SqliteDAO) -> None:
    # Seed cache via cache 1, drop the in-memory state, hydrate cache 2.
    c1 = IdempotencyCache(dao, ttl_s=60.0)
    await c1.set("sig-1", "x")

    c2 = IdempotencyCache(dao, ttl_s=60.0)
    # No hydrate(): direct cold-tier read.
    assert await c2.get("sig-1") == "x"
    # After read, hot tier should now be populated.
    assert c2.hot_size == 1


async def test_hydrate_loads_unexpired_only(dao: SqliteDAO) -> None:
    now = [1000.0]

    def clock() -> float:
        return now[0]

    c1 = IdempotencyCache(dao, ttl_s=10.0, clock=clock)
    await c1.set("sig-1", "x")
    now[0] = 1005.0  # halfway to expiry; still valid.
    await c1.set("sig-2", "y")
    now[0] = 1011.0  # sig-1 now expired.

    c2 = IdempotencyCache(dao, ttl_s=10.0, clock=clock)
    n = await c2.hydrate()
    assert n == 1
    assert await c2.get("sig-2") == "y"
    assert await c2.get("sig-1") is None


async def test_set_replaces_existing_entry(dao: SqliteDAO) -> None:
    cache = IdempotencyCache(dao, ttl_s=60.0)
    await cache.set("sig-1", "first")
    await cache.set("sig-1", "second")
    assert await cache.get("sig-1") == "second"
    assert cache.hot_size == 1


async def test_max_entries_evicts_oldest(dao: SqliteDAO) -> None:
    cache = IdempotencyCache(dao, ttl_s=60.0, max_entries=2)
    await cache.set("a", "1")
    await cache.set("b", "2")
    await cache.set("c", "3")
    assert cache.hot_size == 2
    # 'a' was evicted from hot tier; cold tier still holds it (tested
    # via fall-through).
    assert await cache.get("a") == "1"


async def test_gc_now_deletes_expired_cold_rows(dao: SqliteDAO) -> None:
    now = [1000.0]

    def clock() -> float:
        return now[0]

    cache = IdempotencyCache(dao, ttl_s=10.0, clock=clock)
    await cache.set("sig-1", "x")
    await cache.set("sig-2", "y")
    now[0] = 1011.0
    deleted = await cache.gc_now()
    assert deleted == 2
    assert cache.hot_size == 0


async def test_invalid_ttl_rejected(dao: SqliteDAO) -> None:
    with pytest.raises(ValueError):
        IdempotencyCache(dao, ttl_s=0.0)
