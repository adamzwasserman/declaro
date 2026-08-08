"""Concurrent writers must actually run concurrently.

Turso supports concurrent writes through BEGIN CONCURRENT over MVCC, and the
pool asks for MVCC on every pool. That achieved nothing on its own: every
writer went through one shared connection under one lock, so callers were
serialized before they ever reached the connection. BEGIN CONCURRENT was
issued into a queue of one.

Each writer now takes its own sync connection, so max_size bounds how many
writers proceed rather than how many queue.

A push on a separate connection delivers the frames committed on a write
connection — verified downstream under free-threaded CPython with the GIL
off. These tests cover the pool's side: that writers overlap, that each gets
its own connection, and that the connections are returned and reused rather
than leaked.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

WRITE_WORK = 0.10
WRITERS = 5


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def execute(self, sql: str, *_a):
        self.statements.append(sql)
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)])
        return _FakeCursor([])

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _Holder:
    """Stands in for a sync connection holder, one per writer."""

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


async def _pool(tmp_path, monkeypatch, max_size=WRITERS):
    import declaro_persistum.pool as pool_mod

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


class TestWritersRunConcurrently:
    """max_size must bound how many writers proceed, not how many queue."""

    @pytest.mark.asyncio
    async def test_concurrent_writes_are_not_serialised(self, tmp_path, monkeypatch):
        """Five writers each holding a connection for WRITE_WORK must overlap."""
        pool = await _pool(tmp_path, monkeypatch)

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(WRITE_WORK)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(writer() for _ in range(WRITERS)))
        elapsed = loop.time() - start

        serial = WRITE_WORK * WRITERS
        assert elapsed < serial / 2, (
            f"{WRITERS} concurrent writes took {elapsed:.3f}s; serial is "
            f"{serial:.2f}s and parallel about {WRITE_WORK:.2f}s. Writers are "
            f"serialised through one lock, so BEGIN CONCURRENT cannot help."
        )

    @pytest.mark.asyncio
    async def test_each_concurrent_writer_gets_its_own_connection(
        self, tmp_path, monkeypatch
    ):
        """Two writers held at once must not share a connection object."""
        pool = await _pool(tmp_path, monkeypatch)
        seen: list[int] = []
        both_open = asyncio.Event()

        async def writer(index):
            async with pool.acquire_write() as conn:
                seen.append(id(conn._holder))
                if len(seen) == 2:
                    both_open.set()
                await both_open.wait()

        await asyncio.gather(writer(0), writer(1))

        assert len(set(seen)) == 2, "both writers used the same connection"

    @pytest.mark.asyncio
    async def test_write_connections_are_reused_not_leaked(self, tmp_path, monkeypatch):
        """Sequential writes must not open a new connection every time."""
        pool = await _pool(tmp_path, monkeypatch)

        for _ in range(20):
            async with pool.acquire_write():
                pass

        assert len(_Holder.instances) <= WRITERS + 2, (
            f"opened {len(_Holder.instances)} connections for 20 sequential "
            f"writes — they are not being returned to the pool"
        )

    @pytest.mark.asyncio
    async def test_retention_is_bounded_but_concurrency_is_not(
        self, tmp_path, monkeypatch
    ):
        """More callers than max_size must proceed, and not be retained after.

        max_size bounds how many idle connections are kept, not how many
        callers may run. Ten concurrent writers all proceed; at most
        max_size connections are still held afterwards.
        """
        pool = await _pool(tmp_path, monkeypatch, max_size=2)

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(0.02)

        await asyncio.gather(*(writer() for _ in range(10)))

        retained = pool._free_writers.qsize() if pool._free_writers else 0
        assert retained <= 2, f"retained {retained} idle write connections"


class TestRefreshCoversEveryConnection:
    """A migration must not leave connections on the old schema.

    refresh_connections exists to pick up schema changes. It reopened only
    the pool's original write connection. Once there are several write
    connections and several read connections, refreshing one of them leaves
    the rest reading and writing against a schema that no longer exists —
    which is the same stale-input class as the defects this pool has already
    shipped twice.

    It must also quiesce writers first. Closing a connection while a writer
    holds it is a use-after-close.
    """

    @pytest.mark.asyncio
    async def test_all_write_connections_are_reopened(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)

        # Force three write connections into existence.
        async def hold(barrier):
            async with pool.acquire_write():
                await barrier.wait()

        barrier = asyncio.Event()
        tasks = [asyncio.create_task(hold(barrier)) for _ in range(3)]
        await asyncio.sleep(0.05)
        barrier.set()
        await asyncio.gather(*tasks)

        writers_before = list(pool._write_holders)
        assert len(writers_before) >= 3
        conns_before = [w.conn for w in writers_before]

        await pool.refresh_connections()

        conns_after = [w.conn for w in pool._write_holders]
        assert all(c is not None for c in conns_after), "a writer was left closed"
        assert all(
            after is not before for after, before in zip(conns_after, conns_before)
        ), "some write connections kept their pre-migration connection"

    @pytest.mark.asyncio
    async def test_read_connections_are_discarded(self, tmp_path, monkeypatch):
        """Readers must not keep serving the pre-migration schema."""
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.acquire():
            pass
        assert pool._read_holders, "expected a read connection to exist"

        await pool.refresh_connections()

        assert not pool._read_holders, (
            "read connections survived a migration and still see the old schema"
        )

    @pytest.mark.asyncio
    async def test_refresh_does_not_block_an_in_flight_writer(
        self, tmp_path, monkeypatch
    ):
        """A migration must not stall a write that is already running.

        Refresh marks in-use connections stale and disposes of them on
        release, rather than waiting for writers to drain. The writer keeps
        its connection to the end of its own operation and is never
        interrupted.
        """
        pool = await _pool(tmp_path, monkeypatch)
        writer_done = asyncio.Event()

        async def slow_writer():
            async with pool.acquire_write():
                await asyncio.sleep(0.15)
            writer_done.set()

        writer = asyncio.create_task(slow_writer())
        await asyncio.sleep(0.02)

        loop = asyncio.get_running_loop()
        start = loop.time()
        await pool.refresh_connections()
        refresh_time = loop.time() - start

        assert refresh_time < 0.10, (
            f"refresh waited {refresh_time:.3f}s for an in-flight writer"
        )
        assert not writer_done.is_set(), "the writer should still be running"
        await writer


class TestConcurrentWritesUseMvcc:
    """BEGIN CONCURRENT must still be issued on each write connection."""

    @pytest.mark.asyncio
    async def test_begin_concurrent_issued_when_mvcc_granted(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)
        assert pool._mvcc is True

        async with pool.acquire_write(concurrent=True) as conn:
            pass

        assert any("BEGIN CONCURRENT" in s for s in conn._holder.conn.statements)

    @pytest.mark.asyncio
    async def test_ddl_path_does_not_use_begin_concurrent(self, tmp_path, monkeypatch):
        """concurrent=False is the DDL path; Turso rejects BEGIN CONCURRENT there."""
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.acquire_write(concurrent=False) as conn:
            pass

        assert not any("BEGIN CONCURRENT" in s for s in conn._holder.conn.statements)
