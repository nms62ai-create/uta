"""Shared pytest fixtures."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest_asyncio

from trade_adapter.storage.sqlite import SqliteDAO


@pytest_asyncio.fixture
async def dao(tmp_path: Path) -> SqliteDAO:
    """A fresh ``SqliteDAO`` rooted at a temp directory."""

    d = SqliteDAO(tmp_path / "state.db")
    await d.open()
    try:
        yield d
    finally:
        await d.close()


@pytest_asyncio.fixture
async def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    return asyncio.DefaultEventLoopPolicy()
