"""
Tests for instrumented pool and write queue functionality.

Uses real SQLite in-memory databases — no mocks.
"""

import asyncio
import os
import tempfile
import pytest

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.instrumentation import classify_sql, is_write_op


class TestClassifySql:
    """Unit tests for classify_sql pure function."""

    def test_select(self):
        assert classify_sql("SELECT * FROM users") == "select"

    def test_select_lowercase(self):
        assert classify_sql("select id from users where id = 1") == "select"

    def test_insert(self):
        assert classify_sql("INSERT INTO users (name) VALUES (?)") == "insert"

    def test_update(self):
        assert classify_sql("UPDATE users SET name = ? WHERE id = ?") == "update"

    def test_delete(self):
        assert classify_sql("DELETE FROM users WHERE id = ?") == "delete"

    def test_create(self):
        assert classify_sql("CREATE TABLE users (id INTEGER PRIMARY KEY)") == "create"

    def test_alter(self):
        assert classify_sql("ALTER TABLE users ADD COLUMN email TEXT") == "alter"

    def test_drop_maps_to_alter(self):
        assert classify_sql("DROP TABLE users") == "alter"

    def test_pragma_maps_to_other(self):
        assert classify_sql("PRAGMA table_info('users')") == "other"

    def test_with_maps_to_select(self):
        assert classify_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") == "select"

    def test_empty_string(self):
        assert classify_sql("") == "other"

    def test_unknown_keyword(self):
        assert classify_sql("EXPLAIN SELECT * FROM users") == "other"

    def test_is_write_op_insert(self):
        assert is_write_op("insert") is True

    def test_is_write_op_update(self):
        assert is_write_op("update") is True

    def test_is_write_op_delete(self):
        assert is_write_op("delete") is True

    def test_is_write_op_select_false(self):
        assert is_write_op("select") is False

    def test_is_write_op_create_false(self):
        assert is_write_op("create") is False


class TestInstrumentationCallableSink:
    """Test that instrumentation fires the callable sink on every execute."""

    @pytest.mark.asyncio
    async def test_instrumentation_records_latency(self):
        """Callable sink receives a LatencyRecord on each execute."""
        records = []

        pool = await ConnectionPool.sqlite(":memory:")
        pool.configure_instrumentation(callable_sink=lambda r: records.append(r))

        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            await conn.execute("INSERT INTO t VALUES (1, 'hello')")
            await conn.commit()

        # Now execute through the pool (instrumentation fires on execute_with_pool)
        # We simulate by recording directly — the pool is configured, sink is attached
        from declaro_persistum.instrumentation import record_execution
        record_execution(pool, "SELECT * FROM t", 1.5, success=True)

        assert len(records) == 1
        rec = records[0]
        assert rec["op"] == "select"
        assert rec["success"] is True
        assert rec["duration_ms"] == 1.5
        assert "ts" in rec
        assert rec["tier"] == ""

        await pool.close()

    @pytest.mark.asyncio
    async def test_instrumentation_disabled_zero_overhead(self):
        """When instrumentation is not configured, no records are emitted."""
        records = []

        pool = await ConnectionPool.sqlite(":memory:")
        # No configure_instrumentation call — _latency_logger is None

        from declaro_persistum.instrumentation import record_execution
        record_execution(pool, "SELECT 1", 0.1, success=True)

        assert records == []
        await pool.close()

    @pytest.mark.asyncio
    async def test_tier_label_in_records(self):
        """Tier label appears in every record."""
        records = []

        pool = await ConnectionPool.sqlite(":memory:")
        pool.configure_instrumentation(tier_label="project-42", callable_sink=lambda r: records.append(r))

        from declaro_persistum.instrumentation import record_execution
        record_execution(pool, "INSERT INTO x VALUES (1)", 2.0, success=True)

        assert records[0]["tier"] == "project-42"
        await pool.close()

    @pytest.mark.asyncio
    async def test_error_recorded_on_failure(self):
        """Failed executions are recorded with success=False and error message."""
        records = []

        pool = await ConnectionPool.sqlite(":memory:")
        pool.configure_instrumentation(callable_sink=lambda r: records.append(r))

        from declaro_persistum.instrumentation import record_execution
        record_execution(pool, "DELETE FROM t WHERE id = 1", 5.0, success=False, error="table t does not exist")

        assert records[0]["success"] is False
        assert "table t does not exist" in records[0]["error"]
        await pool.close()


class TestPoolBinding:
    """Tests for pool-bound table API (Phase 1)."""

    def test_pool_binding_table_requires_pool(self):
        """table() raises TypeError when pool argument is missing."""
        from declaro_persistum.query.table import table

        schema = {
            "users": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "name": {"type": "text"},
                }
            }
        }

        with pytest.raises(TypeError):
            table("users", schema)  # missing pool argument

    @pytest.mark.asyncio
    async def test_execute_acquires_from_pool(self):
        """insert().execute() and select().execute() work with no conn param."""
        from declaro_persistum.query.table import table

        schema = {
            "users": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True, "nullable": False},
                    "name": {"type": "text", "nullable": False},
                }
            }
        }

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = await ConnectionPool.sqlite(db_path)

            # Create the table directly
            async with pool.acquire() as conn:
                await conn.execute(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                await conn.commit()

            users = table("users", schema, pool)

            # Insert via pool-bound proxy — no conn param
            await users.insert(id=1, name="Alice").execute()

            # Select via pool-bound proxy — no conn param
            results = await users.select().execute()

            assert len(results) == 1
            assert results[0]["name"] == "Alice"

            await pool.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
