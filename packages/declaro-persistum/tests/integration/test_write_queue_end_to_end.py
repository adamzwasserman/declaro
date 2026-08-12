"""A caller must be able to use the waiting room against a real database.

This is the acceptance test, and it exists because of how the previous write
queue failed: 25 passing tests, all calling internals with arguments no real
caller supplied, and unreachable through the public API for five months.

So this uses only exported names, a real pool and a real table — and it
exercises the thing the room exists for, which is callers arriving at the
same instant.
"""

import asyncio

import pytest

from declaro_persistum import ConnectionPool, collect, deposit, drain, new_room
from declaro_persistum.retry import NO_RETRY


async def _pool(tmp_path, name="q.db"):
    pool = await ConnectionPool.sqlite(str(tmp_path / name))
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.commit()
    return pool


def _appender(pool):
    """The caller's own write function. The room never sees the pool."""

    async def execute(w):
        async with pool.acquire() as conn:
            await conn.execute(w["sql"], w["params"])
            await conn.commit()

    return execute


async def _rows(pool):
    async with pool.acquire() as conn:
        cursor = await conn.execute("SELECT id, name FROM users ORDER BY id")
        return await cursor.fetchall()


INSERT = "INSERT INTO users (id, name) VALUES (?, ?)"






async def _forever(room, execute):
    """The appender the caller owns. The library never starts one."""
    while True:
        await drain(room, execute, NO_RETRY)
        await asyncio.sleep(0)
