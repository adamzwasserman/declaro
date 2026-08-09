"""Concurrent writers against a real Turso engine, not fakes.

The unit tests for concurrent writes use fake connection objects. They prove
the pool hands out one connection per writer and returns them, but they
cannot prove the engine accepts concurrent writers on one database, nor that
every writer's rows actually land.

These use a real local Turso database. Each writer gets a disjoint id range,
so a dropped frame names the writer whose connection lost it rather than
showing up as a bare count shortfall.

What this does NOT cover, and what remains unverified: the same shape
against a *sync* replica with a cloud remote and a push connection running,
on free-threaded CPython with the GIL off. That is the configuration 0.1.17
creates in production. It needs cloud credentials and a patched
free-threaded wheel, so it is run downstream rather than here.
"""

import asyncio

import pytest

from declaro_persistum.pool import ConnectionPool

WRITERS = 5
ROWS_PER_WRITER = 40
BASE = 10_000


def _expected_ids() -> set[int]:
    return {
        writer * BASE + row
        for writer in range(1, WRITERS + 1)
        for row in range(ROWS_PER_WRITER)
    }


class TestConcurrentWritersDeliverEveryRow:
    """Every writer's full range must be present afterwards."""

    @pytest.mark.asyncio
    async def test_disjoint_ranges_all_land(self, tmp_path):
        """N writers, disjoint id ranges, every row present and attributable."""
        pool = await ConnectionPool.turso(str(tmp_path / "w.db"), max_size=WRITERS)
        async with pool.acquire_write(concurrent=False) as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, writer INTEGER)")

        errors: list[str] = []

        async def writer(index: int) -> None:
            try:
                for row in range(ROWS_PER_WRITER):
                    async with pool.acquire_write() as conn:
                        await conn.execute(
                            "INSERT INTO t (id, writer) VALUES (?, ?)",
                            (index * BASE + row, index),
                        )
            except Exception as e:  # noqa: BLE001 - recorded, then asserted on
                errors.append(f"writer {index}: {type(e).__name__}: {e}")

        await asyncio.gather(*(writer(i) for i in range(1, WRITERS + 1)))
        assert errors == [], f"writers raised: {errors}"

        async with pool.acquire() as conn:
            cursor = await conn.execute("SELECT id FROM t")
            found = {row[0] for row in await cursor.fetchall()}

        missing = _expected_ids() - found
        if missing:
            per_writer: dict[int, int] = {}
            for missing_id in missing:
                per_writer[missing_id // BASE] = per_writer.get(missing_id // BASE, 0) + 1
            pytest.fail(
                f"{len(missing)} row(s) never landed. Lost per writer: "
                f"{dict(sorted(per_writer.items()))}"
            )

        assert found == _expected_ids()
        await pool.close()

    @pytest.mark.asyncio
    async def test_writers_actually_overlap_on_a_real_engine(self, tmp_path):
        """Two writers must be able to hold connections at the same time."""
        pool = await ConnectionPool.turso(str(tmp_path / "o.db"), max_size=4)
        async with pool.acquire_write(concurrent=False) as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")

        both_held = asyncio.Event()
        held = 0

        async def holder() -> None:
            nonlocal held
            async with pool.acquire_write():
                held += 1
                if held == 2:
                    both_held.set()
                await asyncio.wait_for(both_held.wait(), timeout=5.0)

        await asyncio.gather(holder(), holder())
        assert both_held.is_set(), "two writers could not hold connections at once"
        await pool.close()

    @pytest.mark.asyncio
    async def test_no_row_is_written_twice(self, tmp_path):
        """Concurrency must not duplicate rows or corrupt the primary key."""
        pool = await ConnectionPool.turso(str(tmp_path / "d.db"), max_size=WRITERS)
        async with pool.acquire_write(concurrent=False) as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, writer INTEGER)")

        async def writer(index: int) -> None:
            for row in range(ROWS_PER_WRITER):
                async with pool.acquire_write() as conn:
                    await conn.execute(
                        "INSERT INTO t (id, writer) VALUES (?, ?)",
                        (index * BASE + row, index),
                    )

        await asyncio.gather(*(writer(i) for i in range(1, WRITERS + 1)))

        async with pool.acquire() as conn:
            cursor = await conn.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM t")
            total, distinct = (await cursor.fetchall())[0]

        assert total == distinct == WRITERS * ROWS_PER_WRITER
        await pool.close()


class TestTransactionsOnARealEngine:
    """Multi-statement transactions, against a real database not a fake."""

    @pytest.mark.asyncio
    async def test_two_writes_commit_together(self, tmp_path):
        pool = await ConnectionPool.turso(str(tmp_path / "tx.db"))
        async with pool.acquire_write(concurrent=False) as conn:
            await conn.execute("CREATE TABLE card (id INTEGER PRIMARY KEY, tag TEXT)")
            await conn.execute("CREATE TABLE idx (id INTEGER PRIMARY KEY, n INTEGER)")

        async with pool.transaction():
            async with pool.acquire_write() as c:
                await c.execute("INSERT INTO card (id, tag) VALUES (1, 'a')")
            async with pool.acquire_write() as c:
                await c.execute("INSERT INTO idx (id, n) VALUES (1, 1)")

        async with pool.acquire() as conn:
            cur = await conn.execute("SELECT COUNT(*) FROM card")
            cards = (await cur.fetchall())[0][0]
            cur = await conn.execute("SELECT COUNT(*) FROM idx")
            idx = (await cur.fetchall())[0][0]

        assert (cards, idx) == (1, 1)
        await pool.close()

    @pytest.mark.asyncio
    async def test_a_failure_leaves_neither_write(self, tmp_path):
        """The derived-index case: the row and its index move together."""
        pool = await ConnectionPool.turso(str(tmp_path / "rb.db"))
        async with pool.acquire_write(concurrent=False) as conn:
            await conn.execute("CREATE TABLE card (id INTEGER PRIMARY KEY, tag TEXT)")

        with pytest.raises(RuntimeError, match="derived-index write failed"):
            async with pool.transaction():
                async with pool.acquire_write() as c:
                    await c.execute("INSERT INTO card (id, tag) VALUES (1, 'a')")
                raise RuntimeError("the derived-index write failed")

        async with pool.acquire() as conn:
            cur = await conn.execute("SELECT COUNT(*) FROM card")
            remaining = (await cur.fetchall())[0][0]

        assert remaining == 0, (
            f"{remaining} row(s) survived a rolled-back transaction; the row "
            f"and its index can now disagree"
        )
        await pool.close()
