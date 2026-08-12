"""No consumer operation may wait on the pool's own bookkeeping.

The pool exists to keep I/O realities away from callers. A caller should
never wait on a lock, never queue behind a concurrency cap, and never be
refused because the pool ran out of slots.

Three things violated that:

  - _conn_lock, still taken when a push fell back to the write connection
  - a read semaphore of max_size, so reader max_size+1 queued
  - a write semaphore of max_size, so writer max_size+1 queued, and could
    be refused outright with PoolExhaustedError

max_size now bounds how many idle connections are RETAINED, not how many
callers may proceed. Concurrency is unbounded; retention is bounded. A
caller above max_size opens a connection, uses it, and closes it on
release rather than waiting for someone else's.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

WORK = 0.10
MAX_SIZE = 2
CALLERS = 8


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    async def execute(self, sql: str, *_a):
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)])
        return _FakeCursor([])

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        self.closed = True


class _Holder:
    instances: list["_Holder"] = []

    def __init__(self, database_path, remote_url=None, _auth_token=None) -> None:
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None
        type(self).instances.append(self)

    async def connect_async(self) -> None:
        self.conn = _FakeConn()

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


async def _pool(tmp_path, monkeypatch, max_size=MAX_SIZE):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.instances = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io",
        auth_token="t",
        max_size=max_size,
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool


class TestReadersNeverQueue:
    """More readers than max_size must still run at once."""

    @pytest.mark.asyncio
    async def test_readers_above_max_size_do_not_wait(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)

        async def reader():
            async with pool.acquire():
                await asyncio.sleep(WORK)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(reader() for _ in range(CALLERS)))
        elapsed = loop.time() - start

        assert elapsed < WORK * 2, (
            f"{CALLERS} readers with max_size={MAX_SIZE} took {elapsed:.3f}s; "
            f"parallel is about {WORK:.2f}s. They are queueing behind a cap."
        )


class TestWritersNeverQueue:
    """More writers than max_size must still run at once, and never be refused."""

    @pytest.mark.asyncio
    async def test_writers_above_max_size_do_not_queue_behind_a_cap(
        self, tmp_path, monkeypatch
    ):
        """max_size must not bound how many writers proceed.

        Measured on a LOCAL pool. A cloud replica serializes its writers
        because the sync tape takes one at a time, and that wait is the
        engine's constraint rather than this pool's bookkeeping — the
        distinction the mandate turns on. What must never happen, on either,
        is queueing behind a cap this pool invented.
        """
        import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

        _Holder.instances = []
        monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)
        db = tmp_path / "local.db"
        db.write_bytes(b"x" * 64)
        pool = TursoPool(str(db), max_size=MAX_SIZE)
        pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)
        await pool._initialize()

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(WORK)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(writer() for _ in range(CALLERS)))
        elapsed = loop.time() - start

        assert elapsed < WORK * 2, (
            f"{CALLERS} writers with max_size={MAX_SIZE} took {elapsed:.3f}s; "
            f"parallel is about {WORK:.2f}s. They are queueing behind a cap."
        )

    @pytest.mark.asyncio
    async def test_a_writer_is_never_refused_for_lack_of_slots(
        self, tmp_path, monkeypatch
    ):
        """PoolExhaustedError must not be raised because the pool is busy."""
        pool = await _pool(tmp_path, monkeypatch, max_size=1)

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(0.05)

        # Would raise PoolExhaustedError under a hard cap of one.
        await asyncio.gather(*(writer() for _ in range(6)))


class TestRetentionIsBoundedEvenThoughConcurrencyIsNot:
    """Unbounded concurrency must not mean unbounded retained connections."""

    @pytest.mark.asyncio
    async def test_idle_connections_above_max_size_are_closed(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(0.05)

        await asyncio.gather(*(writer() for _ in range(CALLERS)))

        retained = pool._free_writers.qsize() if pool._free_writers else 0
        assert retained <= MAX_SIZE, (
            f"retained {retained} idle write connections with max_size={MAX_SIZE}"
        )

    @pytest.mark.asyncio
    async def test_idle_read_connections_above_max_size_are_closed(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)

        async def reader():
            async with pool.acquire():
                await asyncio.sleep(0.05)

        await asyncio.gather(*(reader() for _ in range(CALLERS)))

        retained = pool._free_readers.qsize() if pool._free_readers else 0
        assert retained <= MAX_SIZE


class TestNoLockAndNoCapExist:
    """The pool must not hold the machinery a caller could wait on."""

    @pytest.mark.asyncio
    async def test_pool_has_no_lock_and_no_semaphore(self, tmp_path, monkeypatch):
        """Absence is the assertion. There is nothing shared left to guard.

        Reads, writes and the push each have their own connections, so a
        lock or a semaphore on this pool could only ever make a caller wait
        for something it does not need.
        """
        pool = await _pool(tmp_path, monkeypatch)

        assert not hasattr(pool, "_conn_lock"), (
            "_conn_lock is back — a caller can stall on it"
        )
        assert not hasattr(pool, "_semaphore"), (
            "a read concurrency cap is back — readers will queue"
        )
        assert not hasattr(pool, "_write_semaphore"), (
            "a write concurrency cap is back — writers will queue"
        )

    @pytest.mark.asyncio
    async def test_push_runs_without_borrowing_the_write_connection(
        self, tmp_path, monkeypatch
    ):
        """A missing push connection must not be solved by taking the writer's.

        Borrowing it meant holding the write connection across a cloud round
        trip, stalling every write for its duration. A push deferred to the
        next cycle costs latency to the cloud; a push on the write
        connection costs the caller.
        """
        pool = await _pool(tmp_path, monkeypatch)
        write_conn = pool._write_holder.conn
        pool._push_holder = None

        await pool._push_once()

        assert pool._push_holder is None, (
            "the push opened a connection of its own; it uses the write "
            "connection and absorbs contention by retrying"
        )
        assert write_conn is not None, (
            "the push should have gone out on the write connection"
        )


class TestPoolOpenPaysOneHandshake:
    """Opening a pool must not pay a cloud handshake it does not need yet."""

    @staticmethod
    async def _open_only(tmp_path, monkeypatch):
        """Run _initialize with the background sync stubbed out.

        The background initial sync may open the push connection shortly
        after open, which is fine — it runs in a task, not on the path a
        caller awaits. Stubbing it isolates _initialize's own work.
        """
        import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

        _Holder.instances = []
        monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)
        db = tmp_path / "r.db"
        db.write_bytes(b"x" * 64)
        pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")
        pool._push_loop = lambda: asyncio.sleep(0)
        pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)
        pool._initial_sync = lambda: asyncio.sleep(0)
        await pool._initialize()
        return pool

    @pytest.mark.asyncio
    async def test_pool_open_pays_only_the_write_handshake(self, tmp_path, monkeypatch):
        """_initialize must open exactly one sync connection.

        A sync connection costs a cloud handshake — measured at ~790ms
        against a real remote. Opening the write connection and the push
        connection during _initialize made pool open pay two of them when
        only one is needed before the pool can serve a caller.
        """
        pool = await self._open_only(tmp_path, monkeypatch)

        assert len(_Holder.instances) == 1, (
            f"pool open opened {len(_Holder.instances)} sync connections; it "
            f"must open only the write connection, since each costs a "
            f"handshake on the path a caller waits on"
        )
        assert pool._push_holder is None

    @pytest.mark.asyncio
    async def test_the_push_still_opens_its_own_connection_when_it_runs(
        self, tmp_path, monkeypatch
    ):
        """Deferring the open must not mean never opening it."""
        pool = await self._open_only(tmp_path, monkeypatch)
        assert pool._push_holder is None

        await pool._push_once()

        assert pool._push_holder is None, (
            "the push no longer keeps a connection of its own"
        )
