"""A caller must be able to actually use the queue against a real database.

This is the acceptance test, and it exists because of how the previous
write queue failed. That one had 25 passing unit tests and was unreachable
through the public API for five months, because every test called the
internals directly with arguments no real caller supplied. Green tests
reported a working feature that no consumer could invoke.

So this test uses only what a consumer uses: the exported functions, a real
pool, and a real table. If the queue ever becomes unusable from outside,
this fails.
"""

import pytest

from declaro_persistum import ConnectionPool, DrainFailed, add, drain, remove


async def _pool(tmp_path, name="q.db"):
    pool = await ConnectionPool.sqlite(str(tmp_path / name))
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.commit()
    return pool


def _writer(pool):
    """The caller's own write function. The queue never sees the pool."""

    async def execute(w):
        async with pool.acquire() as conn:
            await conn.execute(w["sql"], w["params"])
            await conn.commit()

    return execute


async def _names(pool):
    async with pool.acquire() as conn:
        cursor = await conn.execute("SELECT name FROM users ORDER BY id")
        return [row[0] for row in await cursor.fetchall()]


class TestAConsumerCanQueueAndDrain:
    @pytest.mark.asyncio
    async def test_queued_writes_reach_the_database(self, tmp_path):
        pool = await _pool(tmp_path)

        pending = ()
        pending = add(pending, {
            "key": "users:1",
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "params": (1, "ada"),
        })
        pending = add(pending, {
            "key": "users:2",
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "params": (2, "grace"),
        })

        assert await _names(pool) == [], "nothing should be written before the drain"

        pending = await drain(pending, _writer(pool), attempts=3)

        assert await _names(pool) == ["ada", "grace"]
        assert pending == (), "everything landed, so nothing should be outstanding"
        await pool.close()

    @pytest.mark.asyncio
    async def test_a_caller_can_drop_a_write_before_it_happens(self, tmp_path):
        pool = await _pool(tmp_path)

        pending = add((), {
            "key": "users:1",
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "params": (1, "ada"),
        })
        pending = remove(pending, "users:1")
        pending = await drain(pending, _writer(pool), attempts=3)

        assert await _names(pool) == []
        await pool.close()


class TestFailureLeavesTheWorkWithTheCaller:
    @pytest.mark.asyncio
    async def test_an_unwritable_row_comes_back_pending(self, tmp_path):
        """A real constraint violation, not a simulated one."""
        pool = await _pool(tmp_path)
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO users (id, name) VALUES (1, 'taken')")
            await conn.commit()

        pending = add((), {
            "key": "users:1",
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "params": (1, "ada"),          # duplicate primary key
        })

        with pytest.raises(DrainFailed) as caught:
            await drain(pending, _writer(pool), attempts=3)

        assert [w["key"] for w in caught.value.pending] == ["users:1"], (
            "the caller must get the outstanding write back to retry later"
        )
        assert await _names(pool) == ["taken"]
        await pool.close()

    @pytest.mark.asyncio
    async def test_the_caller_can_drain_again_without_double_applying(self, tmp_path):
        """The good write landed once; retrying must not write it twice."""
        pool = await _pool(tmp_path)

        pending = ()
        pending = add(pending, {
            "key": "users:1",
            "sql": "INSERT INTO users (id, name) VALUES (?, ?)",
            "params": (1, "ada"),
        })
        pending = add(pending, {
            "key": "users:2",
            "sql": "INSERT INTO nonexistent (id) VALUES (?)",   # will always fail
            "params": (2,),
        })

        with pytest.raises(DrainFailed) as caught:
            await drain(pending, _writer(pool), attempts=2)

        outstanding = caught.value.pending
        assert [w["key"] for w in outstanding] == ["users:2"]

        # Retry what is left. The row that landed is not in it.
        with pytest.raises(DrainFailed):
            await drain(outstanding, _writer(pool), attempts=2)

        assert await _names(pool) == ["ada"], (
            "the successful write was applied more than once"
        )
        await pool.close()
