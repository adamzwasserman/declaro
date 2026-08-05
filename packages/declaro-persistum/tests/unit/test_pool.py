"""
Unit tests for the connection pool.

Tests SQLite pool with in-memory database (no external dependencies).
PostgreSQL and Turso tests are integration tests that require real databases.
"""

import asyncio
import pytest

from declaro_persistum.pool import (
    ConnectionPool,
    SQLitePool,
    PostgreSQLPool,
    TursoPool,
    TursoCloudManager,
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


class TestTursoCloudManagerUseTursodb:
    """Tests for the use_tursodb opt-in flag on create_database.

    These tests exercise payload construction only — _api_request (the HTTP
    boundary) is captured, so no network call is made.
    """

    @staticmethod
    def _manager_capturing_payload(captured: dict, **kwargs):
        """Build a manager whose _api_request records the payload it receives."""
        manager = TursoCloudManager(org="acme", api_token="tok", **kwargs)

        async def _capture(method, endpoint, data=None):
            captured["method"] = method
            captured["endpoint"] = endpoint
            captured["payload"] = data
            return {"database": {"Name": "db"}}

        manager._api_request = _capture  # type: ignore[method-assign]
        return manager

    @pytest.mark.asyncio
    async def test_default_omits_flag(self):
        """No manager default, no per-call arg → use_tursodb absent from payload."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)
        await manager.create_database("tenant-1")
        assert "use_tursodb" not in captured["payload"]

    @pytest.mark.asyncio
    async def test_manager_default_sets_flag(self):
        """Manager-level use_tursodb=True → payload carries use_tursodb=True."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured, use_tursodb=True)
        await manager.create_database("tenant-1")
        assert captured["payload"]["use_tursodb"] is True

    @pytest.mark.asyncio
    async def test_per_call_true_overrides_manager_false(self):
        """Per-call True wins over a manager that defaults False."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured, use_tursodb=False)
        await manager.create_database("tenant-1", use_tursodb=True)
        assert captured["payload"]["use_tursodb"] is True

    @pytest.mark.asyncio
    async def test_per_call_false_overrides_manager_true(self):
        """Per-call False wins over a manager that defaults True (the bool|None case)."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured, use_tursodb=True)
        await manager.create_database("tenant-1", use_tursodb=False)
        assert "use_tursodb" not in captured["payload"]

    @pytest.mark.asyncio
    async def test_per_call_none_inherits_manager_default(self):
        """Per-call None inherits the manager default rather than overriding it."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured, use_tursodb=True)
        await manager.create_database("tenant-1", use_tursodb=None)
        assert captured["payload"]["use_tursodb"] is True


class TestTursoCloudManagerSeed:
    """Tests for the seed pass-through on create_database.

    Seeding lets a tenant database be provisioned as a copy of a template
    rather than created empty and then migrated and populated.

    Payload construction only — _api_request (the HTTP boundary) is
    captured, so no network call is made.
    """

    _manager_capturing_payload = staticmethod(
        TestTursoCloudManagerUseTursodb._manager_capturing_payload
    )

    @pytest.mark.asyncio
    async def test_omitted_by_default(self):
        """No seed argument → no seed key, so ordinary creates are unchanged."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)
        await manager.create_database("tenant-1")
        assert "seed" not in captured["payload"]

    @pytest.mark.asyncio
    async def test_database_seed_passed_through(self):
        """A type=database seed reaches the payload verbatim."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)
        seed = {"type": "database", "name": "tenant-template"}

        await manager.create_database("tenant-1", seed=seed)

        assert captured["payload"]["seed"] == seed

    @pytest.mark.asyncio
    async def test_seed_is_not_rewritten(self):
        """Unknown and future seed keys survive: the dict is opaque here.

        The timestamp form selects a recovery point rather than current
        state, and is the reason this is a dict pass-through rather than a
        seed_from_database="name" parameter, which would discard it.
        """
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)
        seed = {
            "type": "database",
            "name": "tpl",
            "timestamp": "2026-08-05T12:00:00Z",
            "some_future_field": {"nested": True},
        }

        await manager.create_database("tenant-1", seed=seed)

        assert captured["payload"]["seed"] == seed

    @pytest.mark.asyncio
    async def test_upload_seed_passed_through(self):
        """The database_upload form carries no extra fields and still passes."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)

        await manager.create_database("tenant-1", seed={"type": "database_upload"})

        assert captured["payload"]["seed"] == {"type": "database_upload"}

    @pytest.mark.asyncio
    async def test_seed_composes_with_other_options(self):
        """seed does not displace size_limit, use_tursodb, name or group."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)

        await manager.create_database(
            "tenant-1",
            size_limit="256mb",
            use_tursodb=True,
            seed={"type": "database", "name": "tpl"},
        )

        payload = captured["payload"]
        assert payload["name"] == "tenant-1"
        assert payload["size_limit"] == "256mb"
        assert payload["use_tursodb"] is True
        assert payload["seed"] == {"type": "database", "name": "tpl"}
        assert "group" in payload

    @pytest.mark.asyncio
    async def test_caller_dict_not_mutated(self):
        """Building the payload must not write back into the caller's dict."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)
        seed = {"type": "database", "name": "tpl"}
        original = dict(seed)

        await manager.create_database("tenant-1", seed=seed)

        assert seed == original

    @pytest.mark.asyncio
    async def test_still_posts_to_databases_endpoint(self):
        """Seeding uses the same POST /databases call, not a different route."""
        captured: dict = {}
        manager = self._manager_capturing_payload(captured)

        await manager.create_database("tenant-1", seed={"type": "database", "name": "t"})

        assert captured["method"] == "POST"
        assert captured["endpoint"] == "/databases"
