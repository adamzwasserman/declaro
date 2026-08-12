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
    conn_factory = _RecordingConn

    def __init__(self, database_path, remote_url=None, _auth_token=None) -> None:
        self.database_path = database_path
        self._remote_url = remote_url
        self.conn = None

    async def connect_async(self) -> None:
        self.conn = type(self).conn_factory()
        type(self).conns.append(self.conn)

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


# Writes are stateless by default: each one opens its own connection and
# closes it, so there is no single connection left to inspect afterwards.
# These read the whole population instead, which is what the assertions
# always meant — "one commit happened", not "one commit happened on the
# connection the pool happens to keep".


def _commits() -> int:
    return sum(c.commits for c in _Holder.conns)


def _rollbacks() -> int:
    return sum(c.rollbacks for c in _Holder.conns)


def _statements() -> list[str]:
    return [s for c in _Holder.conns for s in c.statements]


async def _pool(tmp_path, monkeypatch, remote=True):
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    _Holder.conns = []
    _Holder.conn_factory = _RecordingConn
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

        assert _commits() == 1, (
            f"{_commits()} commits for two writes inside one transaction; "
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
    async def test_begin_concurrent_is_issued_once_on_a_local_pool(
        self, tmp_path, monkeypatch
    ):
        """One transaction means one BEGIN CONCURRENT, not one per write."""
        pool = await _pool(tmp_path, monkeypatch, remote=False)

        async with pool.transaction():
            async with pool.acquire_write() as c:
                await c.execute("UPDATE a SET x = 1")
            async with pool.acquire_write() as c:
                await c.execute("UPDATE b SET x = 2")

        begins = [s for s in _statements() if s.startswith("BEGIN")]
        assert len(begins) == 1, f"expected one BEGIN CONCURRENT, saw {len(begins)}"

    @pytest.mark.asyncio
    async def test_a_synced_pool_issues_no_begin_and_is_still_atomic(
        self, tmp_path, monkeypatch
    ):
        """MVCC is local only, so a synced transaction has no BEGIN to issue.

        This test previously demanded one BEGIN on a synced pool and got it
        only because synced pools used to request MVCC — the configuration
        that strands writes. With that gone, the atomicity of a synced
        transaction rests entirely on pyturso's DB-API implicit transaction,
        which is a claim about the engine and therefore had to be MEASURED,
        not reasoned:

            pyturso 0.7.2, local file, no explicit BEGIN, 2026-08-12
              two INSERTs then rollback()          -> 0 rows survive
              INSERT then a failing statement,
                then rollback()                    -> 0 rows survive

        So the driver opens a transaction on the first DML and `commit()`
        closes it. One commit for the whole block is the guarantee; the
        BEGIN keyword is not.
        """
        pool = await _pool(tmp_path, monkeypatch, remote=True)

        async with pool.transaction():
            async with pool.acquire_write() as c:
                await c.execute("UPDATE a SET x = 1")
            async with pool.acquire_write() as c:
                await c.execute("UPDATE b SET x = 2")

        assert [s for s in _statements() if s.startswith("BEGIN")] == [], (
            "a synced pool issued BEGIN CONCURRENT; MVCC is local only"
        )
        assert _commits() == 1, (
            f"{_commits()} commits for two writes in one transaction — the "
            f"implicit transaction did not hold them together"
        )


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

        assert _rollbacks() == 1, "the transaction was not rolled back"
        assert _commits() == 0, "a failed transaction still committed"

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

        _Holder.conn_factory = _FailsSecond

        with pytest.raises(RuntimeError, match="index write failed"):
            async with pool.transaction():
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE cards SET tags = 'a'")
                async with pool.acquire_write() as c:
                    await c.execute("UPDATE tag_cooccurrence SET n = 1")

        assert _commits() == 0


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

        assert _commits() == 2

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
            # With MVCC the outside write gets its own connection, so count
            # commits across every write connection rather than writer zero.
            inside_saw.append(
                sum(c.commits for c in _Holder.conns if c is not None)
            )

        await asyncio.gather(in_transaction(), outside())

        # The outside write committed on its own while the transaction was
        # still open, so it did not join it.
        assert inside_saw and inside_saw[0] >= 1
