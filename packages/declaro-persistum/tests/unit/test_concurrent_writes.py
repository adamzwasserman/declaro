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


async def _pool(
    tmp_path, monkeypatch, max_size=WRITERS, remote=True,
):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.instances = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io" if remote else None,
        auth_token="t" if remote else None,
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
        """Five writers each holding a connection for WRITE_WORK must overlap.

        Local pool. A cloud replica serializes its writers by necessity —
        the sync tape takes one at a time — so this measures the pool's own
        behaviour, not the engine's constraint. See
        test_same_replica_write_serialization.py for the cloud case.
        """
        pool = await _pool(tmp_path, monkeypatch, remote=False)

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
        """Two writers held at once must not share a connection object.

        Local pool, for the same reason as above: on a cloud replica only
        one writer is inside at a time, so two could not be held at once.
        """
        pool = await _pool(tmp_path, monkeypatch, remote=False)
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
    async def test_stateless_writes_close_every_connection(
        self, tmp_path, monkeypatch
    ):
        """The default opens one connection per write, and closes each one.

        Twenty connections for twenty writes is the DESIGN, not a leak. The
        leak this guards against is a connection left open: `holder.conn`
        must be None once the write is done. Cost of the open is measured
        in pool.py's module docstring — 1.16ms on a live cloud replica.
        """
        pool = await _pool(tmp_path, monkeypatch)
        before = len(_Holder.instances)

        for _ in range(20):
            async with pool.acquire_write():
                pass

        opened = _Holder.instances[before:]
        assert len(opened) == 20, (
            f"expected one connection per write, saw {len(opened)}"
        )
        assert all(h.conn is None for h in opened), (
            "a stateless write left its connection open"
        )

    @pytest.mark.asyncio
    async def test_nothing_is_retained_and_concurrency_is_not_capped(
        self, tmp_path, monkeypatch
    ):
        """Ten concurrent writers all proceed under max_size=2, and NONE is kept.

        This asserted `retained <= max_size` against the write free list.
        That list is gone with the pooled write path, so the property is now
        the stronger one: retention is ZERO by construction, not bounded by a
        number someone configured. `max_size` never bounded how many callers
        may write, and now it does not bound writers at all.
        """
        pool = await _pool(tmp_path, monkeypatch, max_size=2)

        async def writer():
            async with pool.acquire_write():
                await asyncio.sleep(0.02)

        await asyncio.gather(*(writer() for _ in range(10)))

        assert not hasattr(pool, "_free_writers"), (
            "a write free list came back; the pooled write path is removed"
        )
        assert all(
            h.conn is None for h in _Holder.instances if h is not pool._write_holder
        ), "a write connection outlived its write"


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
    async def test_no_write_connection_survives_a_migration(
        self, tmp_path, monkeypatch
    ):
        """Reopening every write connection is a problem we no longer have.

        This test used to force three write connections into existence and
        assert refresh_connections() reopened all of them. Only the pooled
        opt-in ever held them. A stateless write closes its connection before
        acquire_write returns, so after the writes finish there is exactly
        one long-lived write connection — writer zero — and refresh reopens
        it. The stale-schema defect this class exists for cannot occur,
        because there is no second write connection to leave behind.
        """
        pool = await _pool(tmp_path, monkeypatch)

        async def hold(barrier):
            async with pool.acquire_write():
                await barrier.wait()

        barrier = asyncio.Event()
        tasks = [asyncio.create_task(hold(barrier)) for _ in range(3)]
        await asyncio.sleep(0.05)
        barrier.set()
        await asyncio.gather(*tasks)

        others = [h for h in _Holder.instances if h is not pool._write_holder]
        assert others, "expected the three writers to have opened connections"
        assert all(h.conn is None for h in others), (
            "a write connection was still open after its write returned; it "
            "would survive the migration and serve the pre-migration schema"
        )

        before = pool._write_holder.conn
        await pool.refresh_connections()

        assert pool._write_holder.conn is not None, "writer zero was left closed"
        assert pool._write_holder.conn is not before, "writer zero was not reopened"

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
        # This runs local-only, but NOT because MVCC is unavailable with a
        # remote. The comment here used to say that, citing declaro-9si. It
        # is wrong: MVCC is requested by default and DOES activate on a
        # cloud replica (journal_mode='mvcc', measured 2026-08-10, survives
        # CDC and a replica reopen). It is refused only on an EXISTING
        # wal+CDC replica.
        pool = await _pool(tmp_path, monkeypatch, remote=False)
        assert pool._mvcc is True

        async with pool.acquire_write(concurrent=True) as conn:
            # Read inside the block: a stateless write closes its connection
            # on the way out and clears holder.conn.
            statements = list(conn._holder.conn.statements)

        assert any("BEGIN CONCURRENT" in s for s in statements)

    @pytest.mark.asyncio
    async def test_ddl_path_does_not_use_begin_concurrent(self, tmp_path, monkeypatch):
        """concurrent=False is the DDL path; Turso rejects BEGIN CONCURRENT there."""
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.acquire_write(concurrent=False) as conn:
            statements = list(conn._holder.conn.statements)

        assert not any("BEGIN CONCURRENT" in s for s in statements)
