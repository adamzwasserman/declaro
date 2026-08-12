"""Writers to one replica: concurrent under MVCC, serialised without it.

Measured against a real Turso Cloud replica, concurrent writers to one
replica, distinct rows so there is no logical conflict:

                    K=2    K=5    K=10   K=20
    MVCC on         2/2    4/5    9/10   20/20     (with no lock at all)
    MVCC off        1/2    3/5    3/10   6/20

With MVCC, twenty concurrent writers land twenty writes and a lock changes
nothing. Without it, single-writer is Turso's documented default and the
second writer is rejected from inside the write statement — so a lock is the
only thing that keeps those writes.

0.1.22 added that lock unconditionally, on a measurement taken in WAL mode.
That measurement is the MVCC-off row above: real, but a measurement of the
engine running without the feature rather than a limit of the engine. The
serialisation is now conditional on MVCC being off.

    https://docs.turso.tech/tursodb/concurrent-writes

The fake below models exactly that: it rejects a second concurrent writer
only when it is not in MVCC mode.

WHICH ARM A POOL GETS IS NO LONGER A TEST'S CHOICE. persistum runs MVCC on
local pools only, so the two arms here are two POOL SHAPES, not two settings
on one shape:

    MVCC arm  ->  local pool, no remote_url
    WAL arm   ->  synced pool, remote_url set

These tests previously built a synced pool for both arms and passed
`mvcc=True` to the constructor. That parameter is gone, and the
configuration it selected is the one persistum exists to make unreachable.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = None
        self.rowcount = 1

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _TapeConn:
    """A replica that rejects concurrent writers only outside MVCC."""

    active_writers: dict[str, int] = {}
    collisions = 0
    mvcc = True

    def __init__(self, replica: str) -> None:
        self.replica = replica
        self.statements: list[str] = []

    async def execute(self, sql: str, *_a):
        self.statements.append(sql)
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)] if type(self).mvcc else [("wal",)])
        if sql.strip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
            counts = type(self).active_writers
            counts[self.replica] = counts.get(self.replica, 0) + 1
            try:
                if counts[self.replica] > 1 and not type(self).mvcc:
                    type(self).collisions += 1
                    raise RuntimeError(
                        "sync engine operation failed: database tape error: "
                        "database is busy"
                    )
                await asyncio.sleep(0.02)
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


async def _pool(tmp_path, monkeypatch, *, mvcc: bool, name="r.db"):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _TapeConn.active_writers = {}
    _TapeConn.collisions = 0
    _TapeConn.mvcc = mvcc
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / name
    db.write_bytes(b"x" * 64)
    # MVCC iff local. The arm IS the pool shape; nothing else selects it.
    remote = (
        {} if mvcc else {"remote_url": "https://example.turso.io", "auth_token": "t"}
    )
    pool = TursoPool(str(db), max_size=24, **remote)
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_sync = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    assert pool._mvcc is mvcc, (
        "the pool did not land in the arm this test needs; the engine choice "
        "follows remote_url and nothing else"
    )
    return pool


async def _write_all(pool, k):
    errors = []

    async def writer(n):
        try:
            async with pool.acquire_write() as conn:
                await conn.execute(f"INSERT INTO t VALUES ({n})")
        except Exception as e:  # noqa: BLE001 - collected, then asserted on
            errors.append(str(e))

    await asyncio.gather(*(writer(n) for n in range(k)))
    return errors


class TestWithoutMvccWritersAreSerialised:
    """Single-writer is the documented default. Nothing may be lost to it."""

    @pytest.mark.parametrize("k", [2, 5, 10, 20])
    @pytest.mark.asyncio
    async def test_no_write_is_lost(self, tmp_path, monkeypatch, k):
        pool = await _pool(tmp_path, monkeypatch, mvcc=False)

        errors = await _write_all(pool, k)

        assert errors == [], (
            f"{len(errors)}/{k} writers lost their write with MVCC off; the "
            f"pool must serialise them: {errors[:2]}"
        )
        await pool.close()

    @pytest.mark.asyncio
    async def test_the_lock_is_engaged(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch, mvcc=False)
        assert pool._write_serialisation() is pool._replica_write_lock
        await pool.close()


class TestWithMvccWritersRunConcurrently:
    """Measured: 20/20 land with no lock at all. Do not serialise them."""

    @pytest.mark.asyncio
    async def test_no_lock_is_taken(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch, mvcc=True)
        assert pool._write_serialisation() is not pool._replica_write_lock, (
            "writers are serialised under MVCC, which throws away the "
            "concurrency the engine provides"
        )
        await pool.close()

    @pytest.mark.parametrize("k", [2, 5, 10, 20])
    @pytest.mark.asyncio
    async def test_no_write_is_lost(self, tmp_path, monkeypatch, k):
        pool = await _pool(tmp_path, monkeypatch, mvcc=True)

        errors = await _write_all(pool, k)

        assert errors == [], f"{len(errors)}/{k} writers lost a write: {errors[:2]}"
        await pool.close()

    @pytest.mark.asyncio
    async def test_writers_actually_overlap(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch, mvcc=True)
        peak = 0
        inside = 0
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

        assert peak == 3, f"only {peak} writer(s) were inside at once"
        await pool.close()


class TestDifferentReplicasNeverContend:
    @pytest.mark.asyncio
    async def test_two_replicas_run_in_parallel(self, tmp_path, monkeypatch):
        a = await _pool(tmp_path, monkeypatch, mvcc=True, name="a.db")
        b = await _pool(tmp_path, monkeypatch, mvcc=True, name="b.db")

        async def write(pool):
            async with pool.acquire_write() as conn:
                await conn.execute("INSERT INTO t VALUES (1)")

        await asyncio.gather(write(a), write(b))

        assert _TapeConn.collisions == 0
        await a.close()
        await b.close()
