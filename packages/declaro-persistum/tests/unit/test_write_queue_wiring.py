"""The write queue must actually be attached to the write and read paths.

This is the design: a write gets `threshold_ms` to finish. Under it, the
caller gets the real result and the queue is never touched. Over it, the
caller gets its row back immediately and the write continues in the
background. A later read folds queued writes in, so a caller always sees
what it just wrote.

That wiring existed and was removed on 2026-03-11 in 15f72b6, a pyturso
driver migration, as a side effect: "Simplify execute_with_pool: remove
write queue race (_race_write, _rescue) and read merge logic". The module
was left in place, so the design looked intact while nothing called it,
and it stayed that way for five months.

These tests exist so that cannot happen silently again. They assert the
WIRING; test_write_queue.py covers the queue's own behaviour.

They run against a real SQLite pool rather than fakes. A fake connection
is rejected by _execute_fetch's dispatch, so a fake here proves nothing
about the real path. The threshold is set absurdly low or absurdly high
to choose which branch is exercised, which needs no fake at all.
"""

import asyncio

import pytest

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query.executor import execute_with_pool

ALWAYS_QUEUE = 0.0001     # every real write is slower than this
NEVER_QUEUE = 60_000.0    # no real write is slower than this


async def _pool(tmp_path, name="w.db"):
    pool = await ConnectionPool.sqlite(str(tmp_path / name))
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute("INSERT INTO users (id, name) VALUES (1, 'before')")
        await conn.commit()
    return pool


def _update(_dialect):
    return {
        "sql": "UPDATE users SET name = ? WHERE id = ?",
        "params": ("after", 1),
        "dialect": "sqlite",
    }


def _select(_dialect):
    return {"sql": "SELECT id, name FROM users", "params": (), "dialect": "sqlite"}


ROW = {"id": 1, "name": "after"}


async def _name_of(pool, pk=1):
    async with pool.acquire() as conn:
        cur = await conn.execute("SELECT name FROM users WHERE id = ?", (pk,))
        rows = await cur.fetchall()
    return rows[0][0] if rows else None


class TestSlowWriteIsQueued:
    """Over the threshold the caller is released and the write continues."""

    @pytest.mark.asyncio
    async def test_caller_gets_its_row_back_and_the_write_is_queued(self, tmp_path):
        pool = await _pool(tmp_path)
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)

        result = await execute_with_pool(
            pool, _update, mode="all",
            table_name="users", pk_column="id", pk_value=1, data=ROW,
        )

        assert result == ROW, (
            f"a queued write must hand back the caller's row, got {result!r}"
        )
        assert pool._write_queue.is_pending("users", 1), (
            "the write exceeded the threshold but was not queued"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_the_write_still_lands_and_dequeues(self, tmp_path):
        """The timeout releases the caller; it must not cancel the write."""
        pool = await _pool(tmp_path)
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)

        await execute_with_pool(
            pool, _update, mode="all",
            table_name="users", pk_column="id", pk_value=1, data=ROW,
        )

        for _ in range(50):
            if not pool._write_queue.is_pending("users", 1):
                break
            await asyncio.sleep(0.02)

        assert not pool._write_queue.is_pending("users", 1), (
            "the background write never removed its queue entry"
        )
        assert await _name_of(pool) == "after", (
            "the write was cancelled by the timeout instead of continuing"
        )
        await pool.close()


class TestFastWriteBypassesTheQueue:
    """Under the threshold nothing is queued and the real result comes back."""

    @pytest.mark.asyncio
    async def test_fast_write_never_touches_the_queue(self, tmp_path):
        pool = await _pool(tmp_path)
        pool.configure_write_queue(threshold_ms=NEVER_QUEUE)

        result = await execute_with_pool(
            pool, _update, mode="all",
            table_name="users", pk_column="id", pk_value=1, data=ROW,
        )

        assert not pool._write_queue.is_pending("users", 1), (
            "a fast write must not be queued"
        )
        assert result != ROW, (
            "a fast write must return the real result, not the optimistic row"
        )
        assert await _name_of(pool) == "after"
        await pool.close()

    @pytest.mark.asyncio
    async def test_no_queue_attached_behaves_exactly_as_before(self, tmp_path):
        pool = await _pool(tmp_path)
        result = await execute_with_pool(
            pool, _update, mode="all",
            table_name="users", pk_column="id", pk_value=1, data=ROW,
        )
        assert result != ROW
        assert await _name_of(pool) == "after"
        await pool.close()


class TestBulkWritesAreNeverQueued:
    """No primary key means nothing to key on, merge, or hand back."""

    @pytest.mark.asyncio
    async def test_write_without_pk_waits_for_the_real_result(self, tmp_path):
        pool = await _pool(tmp_path)
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)

        result = await execute_with_pool(
            pool, _update, mode="all",
            table_name="users", pk_column="id", pk_value=None, data=None,
        )

        assert pool._write_queue._queue == {}, "a bulk write must not be queued"
        assert result != ROW
        assert await _name_of(pool) == "after"
        await pool.close()


class TestReadsSeeQueuedWrites:
    """Read-your-own-write. Without this the queue is a correctness hazard."""

    @pytest.mark.asyncio
    async def test_a_queued_insert_appears_in_a_later_select(self, tmp_path):
        pool = await _pool(tmp_path)
        pool.configure_write_queue(threshold_ms=NEVER_QUEUE)
        # Enqueue directly: this asserts the READ side in isolation, without
        # depending on the write side's timing.
        pool._write_queue.enqueue(
            "users", "id", 7, "insert", {"id": 7, "name": "grace"},
            "INSERT INTO users (id, name) VALUES (?, ?)", (7, "grace"), "sqlite",
        )

        result = await execute_with_pool(
            pool, _select, mode="all", table_name="users", pk_column="id",
        )

        assert any(dict(r).get("id") == 7 for r in result), (
            f"a queued write is invisible to the next read: {result}"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_reads_are_untouched_without_a_queue(self, tmp_path):
        pool = await _pool(tmp_path)
        result = await execute_with_pool(
            pool, _select, mode="all", table_name="users", pk_column="id",
        )
        assert len(result) == 1
        await pool.close()


class TestEveryBackendIsWired:
    """SQLite and PostgreSQL have no acquire_write; Turso does.

    The first integration attempt wired the queue only into the
    acquire_write branch, which silently skipped both other backends.
    """

    def test_the_write_path_does_not_require_acquire_write(self):
        import inspect

        from declaro_persistum.query import executor

        src = inspect.getsource(executor.execute_with_pool)
        assert 'if is_write_op(op) and hasattr(pool, "acquire_write"):' not in src, (
            "the queue is gated behind acquire_write again, which silently "
            "excludes SQLitePool and PostgreSQLPool"
        )


class TestWiringIsPresentAtAll:
    """A guard against the 15f72b6 failure: module present, nothing calling it."""

    def test_executor_still_calls_the_queue(self):
        import inspect

        from declaro_persistum.query import executor

        source = inspect.getsource(executor)
        assert "_write_queue" in source, (
            "execute_with_pool no longer looks for a write queue; it has been "
            "detached from the write path again"
        )
        assert "_race_write" in source and "queue.enqueue(" in source, (
            "the write path no longer enqueues"
        )


class TestSupervisorDoesNotDoubleApply:
    """A queued entry whose original write is still running must not be retried.

    The supervisor scans the queue every 100ms. Without a claim it re-applies
    a write that has not finished: the row is inserted twice, or the second
    attempt fails on the primary key and the entry retries forever. The
    original design had this race; it is why mark_in_flight exists.
    """

    @pytest.mark.asyncio
    async def test_an_inflight_entry_is_not_retried(self, tmp_path):
        pool = await ConnectionPool.sqlite(str(tmp_path / "d.db"))
        async with pool.acquire() as c:
            await c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            await c.commit()
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)
        queue = pool._write_queue

        # Exactly what _race_write leaves behind: queued, claimed, write running.
        queue.enqueue(
            "t", "id", 1, "insert", {"id": 1, "n": 1},
            "INSERT INTO t (id, n) VALUES (:id, :n)", {"id": 1, "n": 1}, "sqlite",
        )
        queue.mark_in_flight("t", 1)

        await asyncio.sleep(0.4)          # several supervisor ticks

        async with pool.acquire() as c:
            cur = await c.execute("SELECT COUNT(*) FROM t")
            count = (await cur.fetchall())[0][0]

        assert count == 0, (
            f"the supervisor applied the write {count} time(s) while the "
            f"original was still in flight -- a double-apply"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_releasing_the_claim_lets_the_supervisor_take_over(self, tmp_path):
        """A failed in-flight write must not strand the row."""
        pool = await ConnectionPool.sqlite(str(tmp_path / "r.db"))
        async with pool.acquire() as c:
            await c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)")
            await c.commit()
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)
        queue = pool._write_queue

        queue.enqueue(
            "t", "id", 1, "insert", {"id": 1, "n": 1},
            "INSERT INTO t (id, n) VALUES (:id, :n)", {"id": 1, "n": 1}, "sqlite",
        )
        queue.mark_in_flight("t", 1)
        await asyncio.sleep(0.2)
        queue.clear_in_flight("t", 1)     # the original attempt failed

        for _ in range(50):
            if not queue.is_pending("t", 1):
                break
            await asyncio.sleep(0.02)

        async with pool.acquire() as c:
            cur = await c.execute("SELECT COUNT(*) FROM t")
            count = (await cur.fetchall())[0][0]

        assert count == 1, (
            "after the claim was released the supervisor should have written "
            f"the row exactly once, saw {count}"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_the_claim_is_dropped_with_the_entry(self, tmp_path):
        """_in_flight must not outlive its queue entry (see 0.1.24's leak)."""
        pool = await ConnectionPool.sqlite(str(tmp_path / "c.db"))
        pool.configure_write_queue(threshold_ms=ALWAYS_QUEUE)
        queue = pool._write_queue

        queue.enqueue("t", "id", 1, "insert", {"id": 1},
                      "INSERT INTO t (id) VALUES (:id)", {"id": 1}, "sqlite")
        queue.mark_in_flight("t", 1)
        queue.remove_entry("t", 1)

        assert queue._in_flight == set(), (
            f"claim leaked after the entry was removed: {queue._in_flight}"
        )
        await pool.close()
