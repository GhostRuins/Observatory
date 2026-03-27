"""asyncpg connection pool factory and safe query helpers (parameterised SQL only)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Sequence

import asyncpg
import structlog

logger = structlog.get_logger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def _migration_path() -> Path:
    """Return the path to the bundled initial migration SQL file."""
    return Path(__file__).resolve().parent / "migrations" / "001_initial.sql"


async def create_pool(dsn: str) -> asyncpg.Pool:
    """Create a new asyncpg pool for the given DSN."""
    return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10, command_timeout=120)


async def get_pool(dsn: str) -> asyncpg.Pool:
    """Return the process-wide pool, creating it on first use."""
    global _pool
    async with _pool_lock:
        if _pool is None:
            _pool = await create_pool(dsn)
            logger.info("database_pool_created")
    return _pool


async def close_pool() -> None:
    """Close the process-wide pool if it exists."""
    global _pool
    async with _pool_lock:
        if _pool is not None:
            await _pool.close()
            _pool = None
            logger.info("database_pool_closed")


async def apply_initial_migration(pool: asyncpg.Pool) -> None:
    """Apply 001_initial.sql idempotently (safe to run on every startup)."""
    sql_text = _migration_path().read_text(encoding="utf-8")
    async with pool.acquire() as conn:
        await conn.execute(sql_text)
    logger.info("migration_applied", migration="001_initial.sql")


async def fetch_one(
    pool: asyncpg.Pool,
    query: str,
    *args: Any,
) -> asyncpg.Record | None:
    """Execute a SELECT returning at most one row."""
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch_all(
    pool: asyncpg.Pool,
    query: str,
    *args: Any,
) -> list[asyncpg.Record]:
    """Execute a SELECT returning many rows."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        return list(rows)


async def execute(
    pool: asyncpg.Pool,
    query: str,
    *args: Any,
) -> str:
    """Execute an INSERT/UPDATE/DELETE and return the command status string."""
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


async def execute_many(
    pool: asyncpg.Pool,
    query: str,
    args: Sequence[Sequence[Any]],
) -> None:
    """Execute the same statement for many argument tuples."""
    async with pool.acquire() as conn:
        await conn.executemany(query, args)
