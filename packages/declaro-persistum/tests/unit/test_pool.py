"""
Unit tests for the connection pool.

Tests SQLite pool with in-memory database (no external dependencies).
PostgreSQL, Turso, and LibSQL tests are integration tests that require real databases.
"""

import asyncio
import pytest

from declaro_persistum.pool import (
    ConnectionPool,
    SQLitePool,
    PostgreSQLPool,
    TursoPool,
    LibSQLPool,
)
from declaro_persistum.exceptions import (
    PoolClosedError,
    PoolExhaustedError,
    PoolConnectionError,
)


class TestSQLitePool:
    """Tests for SQLite connection pool."""

    @pytest.mark.asyncio
    async def test_create_pool(self):
        """SQLite pool can be created."""
        pool = await ConnectionPool.sqlite(":memory:")
        assert isinstance(pool, SQLitePool)
        assert not pool.closed
        await pool.close()

    @pytest.mark.asyncio
    async def test_acquire_connection(self):
        """Can acquire and use a connection."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            # Create a table
            await conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            await conn.execute("INSERT INTO test (name) VALUES (?)", ("Alice",))
            await conn.commit()

            # Query it
            cursor = await conn.execute("SELECT name FROM test WHERE id = 1")
            row = await cursor.fetchone()
            assert row[0] == "Alice"

        await pool.close()

    @pytest.mark.asyncio
    async def test_multiple_connections(self):
        """Multiple connections can be acquired concurrently."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=3)

        async def worker(worker_id: int):
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
                await asyncio.sleep(0.01)  # Hold connection briefly
                return worker_id

        # Run 3 workers concurrently
        results = await asyncio.gather(
            worker(1),
            worker(2),
            worker(3),
        )
        assert set(results) == {1, 2, 3}
        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_size_limiting(self):
        """Pool limits concurrent connections."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=2)

        acquired = []
        released = asyncio.Event()

        async def holder():
            """Hold a connection until told to release."""
            async with pool.acquire() as conn:
                acquired.append(1)
                await released.wait()

        async def waiter():
            """Wait for connection then return immediately."""
            await asyncio.sleep(0.01)  # Let holders start first
            async with pool.acquire() as conn:
                return "got connection"

        # Start 2 holders (will acquire both slots)
        holder_tasks = [
            asyncio.create_task(holder()),
            asyncio.create_task(holder()),
        ]

        # Wait for holders to acquire
        await asyncio.sleep(0.05)
        assert len(acquired) == 2
        assert pool.available == 0

        # Release holders
        released.set()
        await asyncio.gather(*holder_tasks)

        # Now waiter can get connection
        result = await waiter()
        assert result == "got connection"

        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_closed_error(self):
        """Acquiring from closed pool raises PoolClosedError."""
        pool = await ConnectionPool.sqlite(":memory:")
        await pool.close()

        with pytest.raises(PoolClosedError):
            async with pool.acquire():
                pass

    @pytest.mark.asyncio
    async def test_pool_exhausted_timeout(self):
        """Pool raises PoolExhaustedError on timeout."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=1, acquire_timeout=0.1)

        async def holder():
            async with pool.acquire():
                await asyncio.sleep(1)  # Hold longer than timeout

        # Start holder
        holder_task = asyncio.create_task(holder())
        await asyncio.sleep(0.02)  # Let holder acquire

        # Try to acquire with timeout
        with pytest.raises(PoolExhaustedError, match="Timed out"):
            async with pool.acquire():
                pass

        holder_task.cancel()
        try:
            await holder_task
        except asyncio.CancelledError:
            pass

        await pool.close()

    @pytest.mark.asyncio
    async def test_wal_mode_enabled(self):
        """SQLite pool enables WAL mode."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            cursor = await conn.execute("PRAGMA journal_mode")
            row = await cursor.fetchone()
            assert row[0] == "memory"  # In-memory doesn't have WAL, but file DBs do

        await pool.close()

    @pytest.mark.asyncio
    async def test_size_and_available(self):
        """Pool reports size and available correctly."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=3)
        assert pool.size == 3
        assert pool.available == 3

        async with pool.acquire():
            assert pool.available == 2
            async with pool.acquire():
                assert pool.available == 1

        assert pool.available == 3
        await pool.close()

    @pytest.mark.asyncio
    async def test_closed_property(self):
        """Pool.closed reflects pool state."""
        pool = await ConnectionPool.sqlite(":memory:")
        assert not pool.closed
        await pool.close()
        assert pool.closed


class TestPostgreSQLPool:
    """Tests for PostgreSQL connection pool structure."""

    def test_pool_initialization(self):
        """PostgreSQLPool can be instantiated."""
        pool = PostgreSQLPool(
            "postgresql://localhost/test",
            min_size=2,
            max_size=10,
            acquire_timeout=5.0,
        )
        assert pool._min_size == 2
        assert pool._max_size == 10
        assert pool._acquire_timeout == 5.0
        assert not pool.closed

    @pytest.mark.asyncio
    async def test_pool_closed_before_use(self):
        """Closing pool before use works."""
        pool = PostgreSQLPool("postgresql://localhost/test")
        await pool.close()
        assert pool.closed

        with pytest.raises(PoolClosedError):
            async with pool.acquire():
                pass


class TestTursoPool:
    """Tests for Turso connection pool structure (pyturso)."""

    def test_pool_initialization(self):
        """TursoPool can be instantiated."""
        pool = TursoPool(
            ":memory:",
            max_size=5,
            acquire_timeout=10.0,
        )
        assert pool._database_path == ":memory:"
        assert pool._max_size == 5
        assert pool._acquire_timeout == 10.0
        assert not pool.closed

    @pytest.mark.asyncio
    async def test_pool_closed_before_use(self):
        """Closing pool before use works."""
        pool = TursoPool(":memory:")
        await pool.close()
        assert pool.closed

        with pytest.raises(PoolClosedError):
            async with pool.acquire():
                pass


class TestLibSQLPool:
    """Tests for LibSQL connection pool structure (Turso cloud)."""

    def test_pool_initialization(self):
        """LibSQLPool can be instantiated with sync_url and local_path."""
        pool = LibSQLPool(
            "libsql://db.turso.io",
            "./db/test.db",
            auth_token="secret",
            max_size=5,
            acquire_timeout=10.0,
        )
        assert pool._sync_url == "libsql://db.turso.io"
        assert pool._local_path == "./db/test.db"
        assert pool._auth_token == "secret"
        assert pool._max_size == 5
        assert pool._acquire_timeout == 10.0
        assert not pool.closed

    @pytest.mark.asyncio
    async def test_pool_closed_before_use(self):
        """Closing pool before use works."""
        pool = LibSQLPool("libsql://localhost", "./db/test.db")
        await pool.close()
        assert pool.closed

        with pytest.raises(PoolClosedError):
            async with pool.acquire():
                pass

    def test_libsql_holder_uses_auto_commit(self, tmp_path):
        """_LibSQLConnectionHolder.connect() uses isolation_level=None (auto-commit).

        The local replica file is READ-ONLY — it is a pulled copy of the
        Turso Cloud primary.  Any isolation_level other than None batches
        writes into a local WAL transaction that is NEVER forwarded to
        Turso Cloud.  commit() returns success but the write is silently
        discarded when sync() overwrites local from remote.

        With isolation_level=None each execute() is a direct HTTP call to
        Turso Cloud.  Slow writes (cold-start ~10s) are handled by the
        50ms write-race / write-queue in execute_with_pool.
        """
        import libsql

        db_path = str(tmp_path / "test.db")
        conn = libsql.connect(db_path, isolation_level=None)

        assert conn.isolation_level is None

        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'hello')")

        cursor = conn.execute("SELECT COUNT(*) FROM t")
        row = cursor.fetchone()
        assert row[0] == 1

        conn.close()


class TestConnectionPoolFactory:
    """Tests for ConnectionPool factory methods."""

    @pytest.mark.asyncio
    async def test_sqlite_factory(self):
        """ConnectionPool.sqlite() creates SQLitePool."""
        pool = await ConnectionPool.sqlite(":memory:")
        assert isinstance(pool, SQLitePool)
        await pool.close()

    @pytest.mark.asyncio
    async def test_sqlite_factory_with_options(self):
        """ConnectionPool.sqlite() accepts options."""
        pool = await ConnectionPool.sqlite(
            ":memory:",
            max_size=10,
            acquire_timeout=60.0,
        )
        assert pool._max_size == 10
        assert pool._acquire_timeout == 60.0
        await pool.close()

    def test_postgresql_returns_correct_type(self):
        """ConnectionPool.postgresql() returns PostgreSQLPool type."""
        # Can't actually test connection without real DB, but can check types
        assert hasattr(ConnectionPool, "postgresql")

    def test_turso_returns_correct_type(self):
        """ConnectionPool.turso() returns TursoPool type."""
        assert hasattr(ConnectionPool, "turso")

    def test_libsql_returns_correct_type(self):
        """ConnectionPool.libsql() returns LibSQLPool type."""
        assert hasattr(ConnectionPool, "libsql")


class TestPoolExceptions:
    """Tests for pool exception types."""

    def test_pool_closed_error(self):
        """PoolClosedError is a proper exception."""
        err = PoolClosedError("Pool is closed")
        assert str(err) == "Pool is closed"

    def test_pool_exhausted_error(self):
        """PoolExhaustedError is a proper exception."""
        err = PoolExhaustedError("No connections available")
        assert str(err) == "No connections available"

    def test_pool_connection_error(self):
        """PoolConnectionError is a proper exception."""
        err = PoolConnectionError("Connection failed")
        assert str(err) == "Connection failed"

    def test_exception_hierarchy(self):
        """Pool exceptions inherit from PoolError."""
        from declaro_persistum.exceptions import PoolError

        assert issubclass(PoolClosedError, PoolError)
        assert issubclass(PoolExhaustedError, PoolError)
        assert issubclass(PoolConnectionError, PoolError)


class TestPoolContextManager:
    """Tests for pool acquire() context manager behavior."""

    @pytest.mark.asyncio
    async def test_connection_released_on_success(self):
        """Connection is released after successful use."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=1)

        # Use and release
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")

        # Should be able to acquire again
        async with pool.acquire() as conn:
            await conn.execute("SELECT 2")

        await pool.close()

    @pytest.mark.asyncio
    async def test_connection_released_on_error(self):
        """Connection is released even when error occurs."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=1)

        # Use with error
        try:
            async with pool.acquire() as conn:
                await conn.execute("SELECT 1")
                raise ValueError("Test error")
        except ValueError:
            pass

        # Should still be able to acquire
        async with pool.acquire() as conn:
            await conn.execute("SELECT 2")

        await pool.close()

    @pytest.mark.asyncio
    async def test_concurrent_transactions(self):
        """Concurrent transactions work correctly."""
        pool = await ConnectionPool.sqlite(":memory:", max_size=3)

        # Each connection gets its own in-memory DB for :memory:
        # So we just verify concurrent acquire/release works
        results = []

        async def transaction(n: int):
            async with pool.acquire() as conn:
                await conn.execute("SELECT ?", (n,))
                await asyncio.sleep(0.01)
                results.append(n)

        await asyncio.gather(
            transaction(1),
            transaction(2),
            transaction(3),
        )

        assert sorted(results) == [1, 2, 3]
        await pool.close()
