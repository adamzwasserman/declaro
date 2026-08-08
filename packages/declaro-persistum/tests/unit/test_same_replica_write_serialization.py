"""Two writers on one replica must not collide.

The sync engine cannot take two concurrent writers on one replica. It
returns "database tape error: database is busy", and it does so from
inside the write statement rather than at a transaction boundary — so the
boundary-only busy retry correctly declines to engage and the error
reaches the caller.

Measured downstream on a real remote, distinct cards so there is no
logical conflict, only database contention:

    K=2  -> 1 ok, 1 busy-500
    K=5  -> 4 ok, 1 busy-500
    K=10 -> 6 ok, 4 busy-500
    K=20 -> 16 ok, 4 busy-500

K=2 is two people editing different cards on one shared board at the same
moment. That is ordinary collaboration, not a stress rate.

So writers to one replica are serialized here. The wait is one write —
about 150ms — instead of a lost write. Writers to *different* replicas
still run fully in parallel, which is what 0.1.17 bought and what this
keeps.

This is not the lock 0.1.19 removed. That one was shared by reads, writes
and the push, so it made every reader wait on a cloud round trip. This one
covers writer-versus-writer only, on one replica, which is a constraint
the engine actually has.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _TapeConn:
    """Fails if a second writer is inside a write while one is active.

    Models the engine: the tape takes one writer at a time and rejects the
    second from inside the statement.
    """

    # Keyed by replica path: the engine's constraint is per replica, and a
    # shared counter would make two different replicas look like a collision.
    active_writers: dict[str, int] = {}
    collisions = 0

    def __init__(self, replica: str) -> None:
        self.replica = replica
        self.statements: list[str] = []

    async def execute(self, sql: str, *_a):
        self.statements.append(sql)
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)])
        if sql.strip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
            counts = type(self).active_writers
            counts[self.replica] = counts.get(self.replica, 0) + 1
            try:
                if counts[self.replica] > 1:
                    type(self).collisions += 1
                    raise RuntimeError(
                        "sync engine operation failed: database tape error: "
                        "database is busy"
                    )
                await asyncio.sleep(0.05)  # the write takes real time
            finally:
                counts[self.replica] -= 1
        return _FakeCursor([])

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _Holder:
    def __init__(self, database_path, remote_url=None, _auth_token=None) -> None:
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None

    async def connect_async(self) -> None:
        self.conn = _TapeConn(self.database_path)

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


async def _pool(tmp_path, monkeypatch, name="r.db"):
    import declaro_persistum.pool as pool_mod

    _TapeConn.active_writers = {}
    _TapeConn.collisions = 0
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / name
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db), remote_url="https://example.turso.io", auth_token="t", max_size=8
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool


class TestWritersToOneReplicaDoNotCollide:
    """The failure downstream started at K=2, so K=2 is the first case."""

    @pytest.mark.parametrize("writers", [2, 5, 10, 20])
    @pytest.mark.asyncio
    async def test_concurrent_writers_all_succeed(self, tmp_path, monkeypatch, writers):
        pool = await _pool(tmp_path, monkeypatch)
        errors: list[str] = []

        async def writer(index: int) -> None:
            try:
                async with pool.acquire_write() as conn:
                    await conn.execute(f"UPDATE cards SET pos = {index}")
            except Exception as e:  # noqa: BLE001 - collected, then asserted
                errors.append(str(e))

        await asyncio.gather(*(writer(i) for i in range(writers)))

        assert errors == [], (
            f"{len(errors)}/{writers} writers lost their write to tape "
            f"contention: {errors[:2]}"
        )
        assert _TapeConn.collisions == 0, (
            f"{_TapeConn.collisions} writers entered the tape concurrently"
        )


class TestDifferentReplicasStillRunInParallel:
    """Serializing one replica must not serialize the whole application."""

    @pytest.mark.asyncio
    async def test_two_pools_write_at_the_same_time(self, tmp_path, monkeypatch):
        """Cross-replica parallelism is what 0.1.17 bought; keep it."""
        pool_a = await _pool(tmp_path, monkeypatch, name="a.db")
        pool_b = await _pool(tmp_path, monkeypatch, name="b.db")

        overlapped = asyncio.Event()
        inside = 0

        async def writer(pool):
            nonlocal inside
            async with pool.acquire_write() as conn:
                inside += 1
                if inside == 2:
                    overlapped.set()
                await asyncio.wait_for(overlapped.wait(), timeout=2.0)
                await conn.execute("UPDATE cards SET pos = 1")

        await asyncio.gather(writer(pool_a), writer(pool_b))

        assert overlapped.is_set(), (
            "writers on different replicas could not run at the same time; "
            "the serialization is too broad"
        )


class TestReadsAreNotSerialisedByIt:
    """The 0.1.19 guarantee must survive: readers never wait on a writer."""

    @pytest.mark.asyncio
    async def test_a_read_runs_while_a_write_is_in_flight(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)
        write_started = asyncio.Event()
        release_write = asyncio.Event()

        async def slow_writer():
            async with pool.acquire_write():
                write_started.set()
                await release_write.wait()

        writer = asyncio.create_task(slow_writer())
        await write_started.wait()

        loop = asyncio.get_running_loop()
        start = loop.time()
        async with pool.acquire():
            pass
        elapsed = loop.time() - start

        release_write.set()
        await writer

        assert elapsed < 0.1, (
            f"a read waited {elapsed:.3f}s behind an in-flight write; the "
            f"write serialization must not touch readers"
        )
