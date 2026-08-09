"""Several ORM writes must be able to commit as one transaction.

pool.transaction() was a no-op passthrough that yielded the pool itself.
Every ORM write took its own acquire_write, so two update_one calls were
two transactions however they were nested. Batching was only possible by
dropping to acquire_write and raw SQL, which defeats the point of having
an ORM.

That gap has a cost beyond tidiness. A caller who wants a row and its
derived index to move together has to choose between raw SQL and doing
them separately — and separately means a failure between the two leaves
the index disagreeing with the row it describes.

Inside transaction(), ORM writes share one connection and one commit:
everything lands or nothing does.
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


class _RecordingConn:
    """Records the statement stream so transaction shape is visible."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    async def execute(self, sql: str, *_a):
        self.statements.append(sql.strip())
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)])
        return _FakeCursor([])

    async def commit(self) -> None:
        self.commits += 1
        self.statements.append("COMMIT")

    async def rollback(self) -> None:
        self.rollbacks += 1
        self.statements.append("ROLLBACK")

    async def close(self) -> None:
        pass


class _Holder:
    conns: list[_RecordingConn] = []

    def __init__(self, database_path, remote_url=None, _auth_token=None) -> None:
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None

    async def connect_async(self) -> None:
        self.conn = _RecordingConn()
        type(self).conns.append(self.conn)

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


async def _pool(tmp_path, monkeypatch, remote=True):
    import declaro_persistum.pool as pool_mod

    _Holder.conns = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _Holder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io" if remote else None,
        auth_token="t" if remote else None,
    )
    pool._push_loop = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._enable_replica_fk_enforcement = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    pool._initial_sync = lambda: asyncio.sleep(0)  # type: ignore[assignment]
    await pool._initialize()
    return pool


class TestWritesShareOneTransaction:
    """Two writes inside transaction() must commit once, not twice."""

    @pytest.mark.asyncio
    async def test_two_writes_commit_together(self, tmp_path, monkeypatch):
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.transaction():
            async with pool.acquire_write() as c1:
                await c1.execute("UPDATE cards SET tags = 'a'")
            async with pool.acquire_write() as c2:
                await c2.execute("UPDATE tag_cooccurrence SET n = 1")

        conn = pool._write_holder.conn
        assert conn.commits == 1, (
            f"{conn.commits} commits for two writes inside one transaction; "
            f"they did not share it"
        )

    @pytest.mark.asyncio
    async def test_both_writes_go_to_one_connection(self, tmp_path, monkeypatch):
        """Sharing a transaction requires sharing the connection."""
        pool = await _pool(tmp_path, monkeypatch)
        seen = []

        async with pool.transaction():
            async with pool.acquire_write() as c1:
                seen.append(id(c1._holder))
            async with pool.acquire_write() as c2:
                seen.append(id(c2._holder))

        assert len(set(seen)) == 1, "the two writes used different connections"

    @pytest.mark.asyncio
    async def test_begin_is_issued_once(self, tmp_path, monkeypatch):
        """One transaction means one BEGIN, not one per write."""
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.transaction():
            async with pool.acquire_write() as c:
                await c.execute("UPDATE a SET x = 1")
            async with pool.acquire_write() as c:
                await c.execute("UPDATE b SET x = 2")

        begins = [s for s in pool._write_holder.conn.statements if s.startswith("BEGIN")]
        assert len(begins) == 1, f"expected one BEGIN, saw {len(begins)}"


class TestFailureRollsBackEverything:
    """All or nothing. A failure must not leave half the writes applied."""

    @pytest.mark.asyncio
    async def test_an_error_rolls_back_the_whole_transaction(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)

        with pytest.raises(ValueError, match="boom"):
            async with pool.transaction():
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE cards SET tags = 'a'")
                raise ValueError("boom")

        conn = pool._write_holder.conn
        assert conn.rollbacks == 1, "the transaction was not rolled back"
        assert conn.commits == 0, "a failed transaction still committed"

    @pytest.mark.asyncio
    async def test_a_failing_write_rolls_back_the_earlier_one(
        self, tmp_path, monkeypatch
    ):
        """The derived-index case: the row and its index move together."""
        pool = await _pool(tmp_path, monkeypatch)

        class _FailsSecond(_RecordingConn):
            async def execute(self, sql: str, *_a):
                if "tag_cooccurrence" in sql:
                    raise RuntimeError("index write failed")
                return await super().execute(sql, *_a)

        pool._write_holder.conn = _FailsSecond()

        with pytest.raises(RuntimeError, match="index write failed"):
            async with pool.transaction():
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE cards SET tags = 'a'")
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE tag_cooccurrence SET n = 1")

        assert pool._write_holder.conn.commits == 0


class TestOutsideATransactionNothingChanges:
    """Writes not inside transaction() keep committing individually."""

    @pytest.mark.asyncio
    async def test_separate_writes_still_commit_separately(
        self, tmp_path, monkeypatch
    ):
        pool = await _pool(tmp_path, monkeypatch)

        async with pool.acquire_write() as c:
            await c.execute("UPDATE a SET x = 1")
        async with pool.acquire_write() as c:
            await c.execute("UPDATE b SET x = 2")

        assert pool._write_holder.conn.commits == 2

    @pytest.mark.asyncio
    async def test_concurrent_tasks_do_not_share_a_transaction(
        self, tmp_path, monkeypatch
    ):
        """A transaction belongs to its task, not to the pool.

        Otherwise one request's transaction would swallow another's writes.
        """
        pool = await _pool(tmp_path, monkeypatch)
        inside_saw = []

        async def in_transaction():
            async with pool.transaction():
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE a SET x = 1")
                await asyncio.sleep(0.05)

        async def outside():
            await asyncio.sleep(0.01)
            async with pool.acquire_write() as c:
                await c.execute("UPDATE b SET x = 2")
            inside_saw.append(pool._write_holder.conn.commits)

        await asyncio.gather(in_transaction(), outside())

        # The outside write committed on its own while the transaction was
        # still open, so it did not join it.
        assert inside_saw and inside_saw[0] >= 1
