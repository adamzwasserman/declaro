"""The push must not hold a connection of its own.

Nothing waits on the push -- the write is already durable locally before it
runs -- so it can queue behind writes rather than hold a second connection
open against the replica.

This is NOT a claim that the replication engine takes a single writer. Turso
supports concurrent writers through MVCC and BEGIN CONCURRENT, and the pool
still opens a write connection per concurrent caller. `Error::Busy` at
commit is a documented, retryable conflict signal.
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
    opened: list["_Holder"] = []
    pushes: list["_Holder"] = []

    def __init__(self, database_path, remote_url=None, _auth_token=None):
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None
        type(self).opened.append(self)

    async def connect_async(self):
        self.conn = _Conn()

    async def push(self):
        type(self).pushes.append(self)

    async def pull(self):
        pass


async def _pool(tmp_path, monkeypatch):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.opened = []
    _Holder.pushes = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_replication = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool


class TestThePushHasNoConnectionOfItsOwn:
    @pytest.mark.asyncio
    async def test_push_opens_no_connection(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)
        before = len(_Holder.opened)

        await pool._push_once()

        assert len(_Holder.opened) == before, (
            "the push opened a connection of its own"
        )
        assert pool._push_holder is None
        await pool.close()

    @pytest.mark.asyncio
    async def test_push_goes_out_on_the_write_connection(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)

        await pool._push_once()

        assert _Holder.pushes == [pool._write_holder]
        await pool.close()

    @pytest.mark.asyncio
    async def test_a_busy_push_is_retried_not_dropped(self, tmp_path, monkeypatch):
        """The push is the one operation the pool can safely retry.

        It ships frames, so there are no caller statements to replay.
        Contention is absorbed here rather than serialised away, which is
        why no writer waits on a cloud round trip.
        """
        pool = await _pool(tmp_path, monkeypatch)
        attempts = []

        async def flaky_push():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError(
                    "replication engine operation failed: database tape error: "
                    "database is busy"
                )

        monkeypatch.setattr(pool._write_holder, "push", flaky_push)

        assert await pool._push_once() is True
        assert len(attempts) == 3, (
            f"a busy push was not retried: {len(attempts)} attempt(s)"
        )
        await pool.close()


class TestConcurrentWritersAreStillConcurrent:
    """Turso is explicitly concurrent-write. Do not take that away."""

    @pytest.mark.asyncio
    async def test_a_replicated_pool_does_not_open_a_connection_per_writer(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)
        release = asyncio.Event()

        async def writer():
            async with pool.acquire_write():
                await release.wait()

        held = [asyncio.create_task(writer()) for _ in range(3)]
        for _ in range(20):
            await asyncio.sleep(0)
        opened = len(_Holder.opened)
        release.set()
        await asyncio.gather(*held)

        # The fixture clears the counter BEFORE _initialize(), so writer zero
        # is the one open on the list. Three concurrent writers must add none.
        assert opened == 1, (
            f"a REPLICATED pool held {opened} write connections for three "
            f"concurrent writers. It must hold one: a replica takes a "
            f"single replica connection (declaro-eer), and opening per write is "
            f"what killed a consumer's box (declaro-dna).\n\n"
            f"This assertion previously read `opened > 1`, on the reasoning "
            f"that MVCC and BEGIN CONCURRENT exist so several connections can "
            f"write at once. True — on a LOCAL pool, where that is asserted in "
            f"test_synced_pools_hold_one_connection.py. A replicated pool runs WAL "
            f"and serialises its writers, so one connection is the correct "
            f"shape, not a collapse."
        )
        await pool.close()
