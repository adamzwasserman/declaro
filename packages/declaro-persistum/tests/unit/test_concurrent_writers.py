"""Writers on a cloud replica must actually run concurrently.

Turso supports concurrent writers through MVCC and `BEGIN CONCURRENT`.
Conflicts are detected at commit, at row granularity, so writers touching
different rows do not interfere. `Error::Busy`, `Error::BusySnapshot` and
errors containing "conflict" are documented and retryable — the prescribed
handling is to roll back and retry.

    https://docs.turso.tech/tursodb/concurrent-writes

0.1.22 added a blanket lock that serialised every writer on a cloud replica,
on the reading that two writers lose a write. That serialised away the
feature the engine exists to provide: the connections were opened per caller
and then queued one at a time behind the lock. Conflicts are retried now
instead of prevented.

The push does not take a lock either, so it blocks nobody.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool


class _Cursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = None
        self.rowcount = 1

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self):
        self.statements: list[str] = []

    async def execute(self, sql, *_a):
        self.statements.append(sql.strip())
        if "journal_mode" in sql and "mvcc" in sql:
            return _Cursor([("mvcc",)])
        return _Cursor([])

    async def commit(self):
        self.statements.append("COMMIT")

    async def rollback(self):
        self.statements.append("ROLLBACK")

    async def close(self):
        pass


class _Holder:
    def __init__(self, database_path, remote_url=None, _auth_token=None):
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None
        self.pushed = 0

    async def connect_async(self):
        self.conn = _Conn()

    async def push(self):
        self.pushed += 1

    async def pull(self):
        pass


async def _pool(tmp_path, monkeypatch, mvcc=True):
    import declaro_persistum.pool as pool_mod

    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)
    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_sync = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    pool._mvcc = mvcc
    return pool


class TestWritersOverlap:
    @pytest.mark.asyncio
    async def test_three_writers_hold_the_pool_at_the_same_time(
        self, tmp_path, monkeypatch
    ):
        """Serialised writers can never all be inside at once."""
        pool = await _pool(tmp_path, monkeypatch)
        inside = 0
        peak = 0
        release = asyncio.Event()

        async def writer():
            nonlocal inside, peak
            async with pool.acquire_write() as conn:
                inside += 1
                peak = max(peak, inside)
                if inside == 3:
                    release.set()
                await asyncio.wait_for(release.wait(), timeout=2.0)
                await conn.execute("INSERT INTO t VALUES (1)")
                inside -= 1

        await asyncio.gather(*(writer() for _ in range(3)))

        assert peak == 3, (
            f"only {peak} writer(s) were ever inside at once; the pool is "
            f"serialising writers that the engine can run concurrently"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_each_writer_gets_its_own_connection(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)
        holders = []
        release = asyncio.Event()

        async def writer():
            async with pool.acquire_write() as conn:
                holders.append(id(conn._holder))
                if len(holders) == 3:
                    release.set()
                await asyncio.wait_for(release.wait(), timeout=2.0)

        await asyncio.gather(*(writer() for _ in range(3)))

        assert len(set(holders)) == 3, (
            "concurrent writers shared a connection; COMPAT.md says a second "
            "write statement on one connection returns SQLITE_BUSY"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_begin_concurrent_is_used_under_mvcc(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.acquire_write() as conn:
            await conn.execute("INSERT INTO t VALUES (1)")

        assert any(
            s.startswith("BEGIN CONCURRENT")
            for s in pool._write_holder.conn.statements
        ), "MVCC is on but BEGIN CONCURRENT was not issued"
        await pool.close()


class TestThePushBlocksNobody:
    @pytest.mark.asyncio
    async def test_a_writer_runs_while_a_push_is_in_flight(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)
        push_started = asyncio.Event()
        let_push_finish = asyncio.Event()

        async def slow_push():
            push_started.set()
            await let_push_finish.wait()

        monkeypatch.setattr(pool._write_holder, "push", slow_push)

        pushing = asyncio.create_task(pool._push_once())
        await asyncio.wait_for(push_started.wait(), timeout=2.0)

        async def writer():
            async with pool.acquire_write() as conn:
                await conn.execute("INSERT INTO t VALUES (1)")

        await asyncio.wait_for(writer(), timeout=1.0)   # must not wait on the push

        let_push_finish.set()
        await pushing
        await pool.close()


class TestConflictsAreRetryable:
    def test_the_documented_conflict_errors_are_recognised(self):
        """Error::Busy, Error::BusySnapshot and "conflict" are all retryable."""
        for message in (
            "database is busy",
            "SQLITE_BUSY",
            "Busy snapshot",
            "write-write conflict detected",
        ):
            assert TursoPool._is_busy(Exception(message)), (
                f"{message!r} is a documented retryable conflict but was not "
                f"recognised as one"
            )

    def test_a_real_error_is_not_treated_as_a_conflict(self):
        assert not TursoPool._is_busy(Exception("no such table: users"))
