"""Tests for MVCC on cloud pools, and for read concurrency.

Turso supports concurrent writes with BEGIN CONCURRENT over MVCC. The pool
enabled MVCC only when there was no remote_url, so every cloud pool ran
serialized writes while the engine below it supported concurrent ones.

The pool also served every read from one shared connection under one lock,
and held that lock for as long as the caller held the connection. max_size
therefore bounded how many callers could queue, not how many could proceed:
five concurrent readers doing 100ms of work each took 505ms.

Both defects are measured here, not argued.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

# Work time per reader. Long enough to separate parallel from serial with a
# wide margin, short enough to keep the suite fast.
READ_WORK = 0.10
READERS = 5


class _FakeConn:
    """Records the PRAGMAs and transaction statements issued against it."""

    def __init__(self, mvcc_supported: bool = True) -> None:
        self.statements: list[str] = []
        self._mvcc_supported = mvcc_supported

    async def execute(self, sql: str, *_a):
        self.statements.append(sql)
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc" if self._mvcc_supported else "wal",)])
        return _FakeCursor([])

    async def commit(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows


class _Holder:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def connect_async(self) -> None:
        pass

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


async def _init(pool, conn):
    """Run _initialize with the holder and background work stubbed out."""
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    orig = pool_mod._TursoConnectionHolder
    pool_mod._TursoConnectionHolder = lambda *_a, **_kw: _Holder(conn)  # type: ignore[assignment]
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    try:
        await pool._initialize()
    finally:
        pool_mod._TursoConnectionHolder = orig  # type: ignore[assignment]


class TestTheEngineChoiceIsPersistumsAlone:
    """MVCC iff local. A synced pool must never request it, and no caller can ask.

    This class previously asserted the opposite — that a cloud pool "must still
    request MVCC", on the reasoning that without it BEGIN CONCURRENT can never
    fire and writes serialize. Serializing was the correct outcome. MVCC on a
    synced replica creates local-only internal tables the sync engine cannot
    reconcile, so writes report success and never reach the primary. 0.1.29 was
    yanked for exactly that.
    """

    @pytest.mark.asyncio
    async def test_a_synced_pool_never_requests_mvcc(self, tmp_path):
        """The safety property. A remote_url means WAL, always."""
        db = tmp_path / "r.db"
        db.write_bytes(b"x" * 64)
        conn = _FakeConn(mvcc_supported=True)
        pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")

        await _init(pool, conn)

        assert not any("mvcc" in s for s in conn.statements), (
            "a synced pool asked for MVCC — that is the declaro-p39 stranding"
        )
        assert pool._mvcc is False

    @pytest.mark.asyncio
    async def test_a_local_pool_requests_mvcc(self, tmp_path):
        """Local is where MVCC is safe, and where the throughput is."""
        conn = _FakeConn(mvcc_supported=True)
        pool = TursoPool(str(tmp_path / "l.db"))

        await _init(pool, conn)

        assert pool._mvcc is True

    @pytest.mark.asyncio
    async def test_the_engine_still_has_the_last_word_locally(self, tmp_path):
        """Requesting is not granting. If the engine refuses, the pool records WAL."""
        conn = _FakeConn(mvcc_supported=False)
        pool = TursoPool(str(tmp_path / "l.db"))

        await _init(pool, conn)

        assert pool._mvcc is False

    def test_a_caller_cannot_ask_for_either_mode(self, tmp_path):
        """No opt-in, no opt-out. The parameter is gone in both directions."""
        for kw in ({"mvcc": True}, {"mvcc": False}, {"pooled_writes": True}):
            with pytest.raises(TypeError):
                TursoPool(str(tmp_path / "x.db"), **kw)


class _SlowReadHolder:
    """A holder whose connection is safe for concurrent use."""

    def __init__(self) -> None:
        self.conn = _FakeConn()

    async def connect_async(self) -> None:
        pass

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


async def _seed_real_db(path: str) -> None:
    """Create a genuine local database, so read connections can open it."""
    import turso.aio

    conn = await turso.aio.connect(path)
    await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    await conn.execute("INSERT INTO t VALUES (1, 'seed')")
    await conn.commit()
    await conn.close()


class TestReadsRunConcurrently:
    """max_size must bound how many readers proceed, not how many queue."""

    @pytest.mark.asyncio
    async def test_concurrent_reads_are_not_serialised(self, tmp_path):
        """Five readers holding a connection for READ_WORK must overlap."""
        db = tmp_path / "r.db"
        await _seed_real_db(str(db))
        pool = TursoPool(
            str(db),
            remote_url="https://example.turso.io",
            auth_token="t",
            max_size=READERS,
        )
        pool._write_holder = _SlowReadHolder()  # type: ignore[assignment]
        pool._mvcc = True

        async def reader():
            async with pool.acquire():
                await asyncio.sleep(READ_WORK)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(reader() for _ in range(READERS)))
        elapsed = loop.time() - start

        serial = READ_WORK * READERS
        assert elapsed < serial / 2, (
            f"{READERS} concurrent reads took {elapsed:.3f}s; "
            f"serial would be {serial:.2f}s and parallel about {READ_WORK:.2f}s. "
            f"Reads are serialised through one lock, so max_size is decorative."
        )
