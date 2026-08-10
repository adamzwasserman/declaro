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


class TestDepositCollect:
    @pytest.mark.asyncio
    async def test_a_deposited_write_reaches_the_database(self, tmp_path):
        pool = await _pool(tmp_path)
        room = new_room()

        ticket = deposit(room, {"sql": INSERT, "params": (1, "ada")})
        assert await _rows(pool) == [], "nothing should be written before the drain"

        await drain(room, _appender(pool))
        receipt = await collect(room, ticket)

        assert receipt == {"id": ticket, "ok": True, "error": ""}
        assert await _rows(pool) == [(1, "ada")]
        await pool.close()

    @pytest.mark.asyncio
    async def test_ordered_dependent_writes_land_in_order(self, tmp_path):
        """multicardz's signup shape: a later write depends on an earlier one."""
        pool = await _pool(tmp_path)
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE routes (id INTEGER PRIMARY KEY, user_id INTEGER "
                "REFERENCES users(id))"
            )
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.commit()
        room = new_room()

        user = deposit(room, {"sql": INSERT, "params": (1, "ada")})
        route = deposit(room, {
            "sql": "INSERT INTO routes (id, user_id) VALUES (?, ?)",
            "params": (10, 1),
        })

        await drain(room, _appender(pool))

        assert (await collect(room, user))["ok"] is True
        assert (await collect(room, route))["ok"] is True, (
            "the dependent write ran before the row it references"
        )
        await pool.close()


class TestConcurrentCallers:
    """The reason the room exists."""

    @pytest.mark.asyncio
    async def test_twenty_five_callers_all_land(self, tmp_path):
        pool = await _pool(tmp_path)
        room = new_room()
        appending = asyncio.create_task(_forever(room, _appender(pool)))

        async def caller(n: int):
            ticket = deposit(room, {"sql": INSERT, "params": (n, f"user{n}")})
            return await collect(room, ticket)

        receipts = await asyncio.gather(*(caller(n) for n in range(25)))

        appending.cancel()
        assert all(r["ok"] for r in receipts), (
            [r for r in receipts if not r["ok"]]
        )
        assert len(await _rows(pool)) == 25
        assert room["waiting"] == {}, "tickets were left behind"
        await pool.close()

    @pytest.mark.asyncio
    async def test_one_callers_failure_does_not_touch_the_others(self, tmp_path):
        pool = await _pool(tmp_path)
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO users (id, name) VALUES (7, 'taken')")
            await conn.commit()
        room = new_room()
        appending = asyncio.create_task(_forever(room, _appender(pool)))

        async def caller(n: int):
            ticket = deposit(room, {"sql": INSERT, "params": (n, f"user{n}")})
            return await collect(room, ticket)

        receipts = await asyncio.gather(*(caller(n) for n in range(5, 10)))
        appending.cancel()

        failed = [r for r in receipts if not r["ok"]]
        assert len(failed) == 1, f"expected only id=7 to fail, got {failed}"
        assert "UNIQUE" in failed[0]["error"].upper()
        assert len(await _rows(pool)) == 5      # 4 new + the pre-existing row
        await pool.close()


async def _forever(room, execute):
    """The appender the caller owns. The library never starts one."""
    while True:
        await drain(room, execute)
        await asyncio.sleep(0)
