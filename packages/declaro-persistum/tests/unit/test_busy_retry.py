"""A busy database must not reach the caller as an error.

Concurrent writes to one cloud replica can fail with:

    sync engine operation failed: database tape error: database is busy

Measured downstream: 30 concurrent writes across 8 replicas produced 5
failures, surfaced to users as HTTP 500. A single writer never sees it; it
appears only when two writes land on one replica at once.

This is I/O nastiness reaching the consumer, which is the one thing this
pool exists to prevent. A busy database means "not now", not "no". The
pool absorbs it and retries.

Retries happen at transaction boundaries only — starting the transaction,
and committing it. Both are points where the safety argument is clear:

  - BEGIN failed, so nothing was staged and nothing applied
  - commit failed, so the transaction did not land; the staged statements
    are still there and re-committing lands the same set

A statement that fails mid-transaction is NOT retried here. The pool
cannot replay the caller's statements, and a half-applied transaction is
not something to guess about, so it propagates.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

BUSY = "sync engine operation failed: database tape error: database is busy"


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _BusyConn:
    """Raises 'database is busy' a set number of times, then succeeds."""

    def __init__(self, busy_on_begin: int = 0, busy_on_commit: int = 0) -> None:
        self.busy_on_begin = busy_on_begin
        self.busy_on_commit = busy_on_commit
        self.begins = 0
        self.commits = 0
        self.statements: list[str] = []

    async def execute(self, sql: str, *_a):
        self.statements.append(sql)
        if "journal_mode" in sql and "mvcc" in sql:
            return _FakeCursor([("mvcc",)])
        if sql.strip().upper().startswith("BEGIN"):
            self.begins += 1
            if self.busy_on_begin > 0:
                self.busy_on_begin -= 1
                raise RuntimeError(BUSY)
        return _FakeCursor([])

    async def commit(self) -> None:
        self.commits += 1
        if self.busy_on_commit > 0:
            self.busy_on_commit -= 1
            raise RuntimeError(BUSY)

    async def rollback(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _Holder:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def connect_async(self) -> None:
        pass

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


def _pool(tmp_path, monkeypatch, conn, **kw):
    """A pool whose every write connection is `conn`.

    Writes are stateless by default, so each one OPENS a connection rather
    than reusing the pool's. Patching `pool._write_holder` alone no longer
    reaches the write path — it would let the real driver dial the fake
    remote. The module-level holder is patched instead, and every holder
    hands back the same `conn` so the test can count begins and commits
    across the whole write.
    """
    import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py

    class _SharedHolder(_Holder):
        def __init__(self, *_a, **_kw) -> None:
            super().__init__(conn)

    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", _SharedHolder)

    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(
        str(db), remote_url="https://example.turso.io", auth_token="t", **kw
    )
    pool._write_holder = _Holder(conn)  # type: ignore[assignment]
    pool._mvcc = True
    return pool


class TestBusyIsAbsorbed:
    """The caller sees success, not an error."""

    @pytest.mark.asyncio
    async def test_busy_on_begin_is_retried(self, tmp_path, monkeypatch):
        """A contended transaction start must not surface to the caller."""
        conn = _BusyConn(busy_on_begin=3)
        pool = _pool(tmp_path, monkeypatch, conn)

        async with pool.acquire_write() as c:
            await c.execute("UPDATE cards SET x = 1")

        assert conn.begins == 4, "expected three retries then success"

    @pytest.mark.asyncio
    async def test_busy_on_commit_is_retried(self, tmp_path, monkeypatch):
        """A contended commit must not surface either.

        The statements are already staged; re-committing lands the same set.
        """
        conn = _BusyConn(busy_on_commit=2)
        pool = _pool(tmp_path, monkeypatch, conn)

        async with pool.acquire_write() as c:
            await c.execute("UPDATE cards SET x = 1")

        assert conn.commits == 3

    @pytest.mark.asyncio
    async def test_a_write_that_is_never_free_still_fails_eventually(
        self, tmp_path, monkeypatch
    ):
        """Retrying must be bounded. An unavailable database is a real error."""
        conn = _BusyConn(busy_on_commit=10_000)
        pool = _pool(tmp_path, monkeypatch, conn, busy_retry_budget_s=0.3)

        with pytest.raises(RuntimeError, match="busy"):
            async with pool.acquire_write() as c:
                await c.execute("UPDATE cards SET x = 1")

    @pytest.mark.asyncio
    async def test_retrying_is_bounded_in_time_not_just_count(
        self, tmp_path, monkeypatch
    ):
        """The budget is wall-clock, so a slow contended database still returns."""
        conn = _BusyConn(busy_on_commit=10_000)
        pool = _pool(tmp_path, monkeypatch, conn, busy_retry_budget_s=0.3)

        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(RuntimeError, match="busy"):
            async with pool.acquire_write() as c:
                await c.execute("UPDATE cards SET x = 1")
        elapsed = loop.time() - start

        assert elapsed < 2.0, f"busy retry ran for {elapsed:.2f}s past its budget"


class TestOnlyBusyIsRetried:
    """A real error must not be retried into a long stall."""

    @pytest.mark.asyncio
    async def test_a_non_busy_error_propagates_immediately(
        self, tmp_path, monkeypatch
    ):
        """A constraint violation is an answer, not a 'not now'."""

        class _FailingConn(_BusyConn):
            async def commit(self) -> None:
                raise RuntimeError("UNIQUE constraint failed: cards.id")

        conn = _FailingConn()
        pool = _pool(tmp_path, monkeypatch, conn)

        loop = asyncio.get_running_loop()
        start = loop.time()
        with pytest.raises(RuntimeError, match="UNIQUE constraint"):
            async with pool.acquire_write() as c:
                await c.execute("INSERT INTO cards VALUES (1)")
        elapsed = loop.time() - start

        assert elapsed < 0.2, "a non-busy error was retried"

    @pytest.mark.asyncio
    async def test_a_statement_failing_mid_transaction_is_not_retried(
        self, tmp_path, monkeypatch
    ):
        """The pool cannot replay caller statements, so it does not try."""

        class _BusyStatementConn(_BusyConn):
            async def execute(self, sql: str, *_a):
                if sql.strip().upper().startswith("UPDATE"):
                    raise RuntimeError(BUSY)
                return await super().execute(sql, *_a)

        conn = _BusyStatementConn()
        pool = _pool(tmp_path, monkeypatch, conn)

        with pytest.raises(RuntimeError, match="busy"):
            async with pool.acquire_write() as c:
                await c.execute("UPDATE cards SET x = 1")
