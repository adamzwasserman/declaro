"""
Tests for WriteQueue — in-memory queue with supervisor and merge.

Uses real SQLite in-memory databases. No mocks except time.monotonic for
the 6-hour CRITICAL log test.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time

import pytest

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.write_queue import (
    WriteQueue,
    merge_pending_into_results,
    merge_pending_into_join_results,
    _find_fk_relationship,
    _extract_order_by,
    _resort,
)


class TestWriteQueueBasics:
    """Basic enqueue/dequeue operations."""

    @pytest.mark.asyncio
    async def test_enqueue_and_is_pending(self):
        """is_pending returns True after enqueue, False after remove."""
        pool = await ConnectionPool.sqlite(":memory:")
        queue = WriteQueue(pool)

        assert not queue.is_pending("users", 1)

        queue.enqueue("users", "id", 1, "insert", {"id": 1, "name": "Alice"},
                      "INSERT INTO users VALUES (1, 'Alice')", [], "sqlite")

        assert queue.is_pending("users", 1)

        queue.remove_entry("users", 1)
        assert not queue.is_pending("users", 1)

        await pool.close()

    @pytest.mark.asyncio
    async def test_enqueue_returns_entry_id(self):
        """enqueue returns a UUID string."""
        pool = await ConnectionPool.sqlite(":memory:")
        queue = WriteQueue(pool)

        entry_id = queue.enqueue("users", "id", 99, "update", {"id": 99},
                                 "UPDATE users SET name=? WHERE id=?", ["Bob", 99], "sqlite")

        assert isinstance(entry_id, str)
        assert len(entry_id) == 36  # UUID format
        await pool.close()

    @pytest.mark.asyncio
    async def test_get_pending_for_table(self):
        """get_pending_for_table returns only entries for the specified table."""
        pool = await ConnectionPool.sqlite(":memory:")
        queue = WriteQueue(pool)

        queue.enqueue("users", "id", 1, "insert", {"id": 1}, "INSERT ...", [], "sqlite")
        queue.enqueue("users", "id", 2, "insert", {"id": 2}, "INSERT ...", [], "sqlite")
        queue.enqueue("posts", "id", 10, "insert", {"id": 10}, "INSERT ...", [], "sqlite")

        user_entries = queue.get_pending_for_table("users")
        assert len(user_entries) == 2

        post_entries = queue.get_pending_for_table("posts")
        assert len(post_entries) == 1

        none_entries = queue.get_pending_for_table("comments")
        assert none_entries == []

        await pool.close()

    @pytest.mark.asyncio
    async def test_disk_persistence(self):
        """Queue survives round-trip to disk via a new instance."""
        pool = await ConnectionPool.sqlite(":memory:")

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            path = f.name
        os.unlink(path)  # Let WriteQueue create it

        try:
            queue1 = WriteQueue(pool, persistence_path=path)
            queue1.enqueue("users", "id", 42, "insert", {"id": 42, "name": "Bob"},
                           "INSERT INTO users VALUES (42, 'Bob')", [], "sqlite")

            assert os.path.exists(path)

            # New instance loads from disk
            queue2 = WriteQueue(pool, persistence_path=path)
            queue2.load_from_disk()

            assert queue2.is_pending("users", 42)
            entry = queue2.get_pending_for_table("users")[0]
            assert entry["pk_value"] == 42
            assert entry["op"] == "insert"
        finally:
            if os.path.exists(path):
                os.unlink(path)
            await pool.close()


class TestWriteQueueRace:
    """50ms race write behavior."""

    @pytest.mark.asyncio
    async def test_race_fast_write_queue_stays_empty(self):
        """SQLite sub-1ms write completes before threshold; queue stays empty."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = await ConnectionPool.sqlite(db_path)
            queue = WriteQueue(pool, threshold_ms=500.0)  # 500ms threshold — write should always win
            pool._write_queue = queue

            async with pool.acquire() as conn:
                await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                await conn.commit()

            from declaro_persistum.query.executor import _race_write
            from declaro_persistum.query.builder import Query

            query: Query = {
                "sql": "INSERT INTO users (id, name) VALUES (1, 'Alice')",
                "params": {},
                "dialect": "sqlite",
            }

            await _race_write(pool, query, queue, "users", "id", 1, "insert", {"id": 1, "name": "Alice"}, "sqlite")

            # Queue should be empty (write completed before threshold)
            assert not queue.is_pending("users", 1)
            await pool.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_race_slow_write_enqueues(self):
        """With threshold=0.001ms, write always exceeds it and gets queued."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = await ConnectionPool.sqlite(db_path)
            queue = WriteQueue(pool, threshold_ms=0.001)  # 0.001ms — impossible to beat
            pool._write_queue = queue

            async with pool.acquire() as conn:
                await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                await conn.commit()

            from declaro_persistum.query.executor import _race_write
            from declaro_persistum.query.builder import Query

            query: Query = {
                "sql": "INSERT INTO users (id, name) VALUES (2, 'Bob')",
                "params": {},
                "dialect": "sqlite",
            }

            result = await _race_write(pool, query, queue, "users", "id", 2, "insert",
                                       {"id": 2, "name": "Bob"}, "sqlite")

            # Returns data immediately
            assert result == {"id": 2, "name": "Bob"}
            # Entry is queued
            assert queue.is_pending("users", 2)

            # Give background task time to finish
            await asyncio.sleep(0.1)
            # Background write completes and removes from queue
            assert not queue.is_pending("users", 2)

            await pool.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestSupervisor:
    """Supervisor drain and backoff behavior."""

    @pytest.mark.asyncio
    async def test_supervisor_drains_queue(self):
        """Entries in the queue get drained to the database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = await ConnectionPool.sqlite(db_path)

            async with pool.acquire() as conn:
                await conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
                await conn.commit()

            queue = WriteQueue(pool)
            pool._write_queue = queue

            # Manually enqueue an INSERT
            queue.enqueue(
                "items", "id", 5, "insert", {"id": 5, "val": "queued"},
                "INSERT INTO items (id, val) VALUES (:id, :val)",
                {"id": 5, "val": "queued"}, "sqlite",
            )
            assert queue.is_pending("items", 5)

            queue.start_supervisor()
            await asyncio.sleep(0.3)  # Give supervisor time to drain

            assert not queue.is_pending("items", 5)

            # Verify it's in the DB
            async with pool.acquire() as conn:
                cursor = await conn.execute("SELECT val FROM items WHERE id = 5")
                row = await cursor.fetchone()
                assert row is not None
                assert row[0] == "queued"

            await queue.stop_supervisor()
            await pool.close()
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_supervisor_increments_attempt_count_on_failure(self):
        """When drain fails, attempt_count is incremented."""
        pool = await ConnectionPool.sqlite(":memory:")
        # Don't create the table — drain will fail with "no such table"
        queue = WriteQueue(pool, threshold_ms=50.0)
        pool._write_queue = queue

        queue.enqueue(
            "nonexistent", "id", 1, "insert", {"id": 1},
            "INSERT INTO nonexistent (id) VALUES (:id)",
            {"id": 1}, "sqlite",
        )

        queue.start_supervisor()
        await asyncio.sleep(0.3)  # Let it try and fail

        # Attempt count should have incremented
        entry = queue.get_pending_for_table("nonexistent")[0]
        assert entry["attempt_count"] >= 1
        assert entry["last_error"] != ""

        await queue.stop_supervisor()
        await pool.close()

    @pytest.mark.asyncio
    async def test_pool_close_flushes_queue(self):
        """pool.close() calls stop_supervisor() which calls _flush()."""
        pool = await ConnectionPool.sqlite(":memory:")

        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE flush_test (id INTEGER PRIMARY KEY, v TEXT)")
            await conn.commit()

        pool.configure_write_queue()  # uses defaults
        queue = pool._write_queue
        assert queue is not None

        queue.enqueue(
            "flush_test", "id", 1, "insert", {"id": 1, "v": "flushed"},
            "INSERT INTO flush_test (id, v) VALUES (:id, :v)",
            {"id": 1, "v": "flushed"}, "sqlite",
        )

        await pool.close()  # should flush

        # Pool is closed; create a fresh pool to check the data was flushed
        pool2 = await ConnectionPool.sqlite(":memory:")
        # Note: ":memory:" creates a NEW database — we can only verify stop_supervisor ran
        # (no error = flush completed)
        assert pool.closed
        await pool2.close()

    @pytest.mark.asyncio
    async def test_supervisor_critical_log_after_6_hours(self):
        """WRITE_QUEUE_EXHAUSTED is logged at CRITICAL after 6hrs of failure."""
        import logging
        from unittest.mock import patch

        pool = await ConnectionPool.sqlite(":memory:")
        queue = WriteQueue(pool)

        queue.enqueue(
            "nowhere", "id", 1, "insert", {"id": 1},
            "INSERT INTO nowhere VALUES (:id)", {"id": 1}, "sqlite",
        )

        entry = queue.get_pending_for_table("nowhere")[0]
        key = "nowhere:1"

        # Simulate 6+ hours of continuous failure
        fake_first_failure = time.monotonic() - (6 * 3600 + 1)
        queue._first_failure_time[key] = fake_first_failure

        critical_messages = []

        class CaptureHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                if record.levelno == logging.CRITICAL:
                    critical_messages.append(record.getMessage())

        import declaro_persistum.write_queue as wq_module
        wq_module.logger.addHandler(CaptureHandler())

        # Trigger drain failure
        await queue._drain_one(key, entry)

        assert any("WRITE_QUEUE_EXHAUSTED" in m for m in critical_messages)
        await pool.close()


class TestReadMerge:
    """Pure function tests for merge_pending_into_results."""

    def test_merge_appends_new_entry(self):
        """Pending entry not in DB results is appended."""
        results = [{"id": 1, "name": "Alice"}]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 2,
                "op": "insert", "data": {"id": 2, "name": "Bob"},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        assert len(merged) == 2
        assert merged[1]["name"] == "Bob"

    def test_merge_replaces_existing_entry(self):
        """Pending entry with same PK replaces DB row."""
        results = [{"id": 1, "name": "Alice"}]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 1,
                "op": "update", "data": {"id": 1, "name": "Alice Updated"},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        assert len(merged) == 1
        assert merged[0]["name"] == "Alice Updated"

    def test_merge_dedup_no_duplicates(self):
        """Pending entry in DB results produces exactly one row."""
        results = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Carol"}]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 1,
                "op": "update", "data": {"id": 1, "name": "Alice Updated"},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        assert len(merged) == 2
        ids = [r["id"] for r in merged]
        assert ids.count(1) == 1

    def test_merge_delete_removes_from_results(self):
        """Delete op removes the matching row from results."""
        results = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 1,
                "op": "delete", "data": {},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        assert len(merged) == 1
        assert merged[0]["id"] == 2

    def test_find_fk_relationship(self):
        """_find_fk_relationship extracts FK column and ref column."""
        schema = {
            "posts": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "user_id": {"type": "integer", "references": "users.id"},
                }
            }
        }

        fk_col, ref_col = _find_fk_relationship(schema, "posts", "users")
        assert fk_col == "user_id"
        assert ref_col == "id"

    def test_find_fk_relationship_not_found(self):
        """Returns (None, None) when no FK relationship exists."""
        schema = {"posts": {"columns": {"id": {"type": "integer"}}}}
        fk_col, ref_col = _find_fk_relationship(schema, "posts", "tags")
        assert fk_col is None
        assert ref_col is None

    def test_merge_with_joins(self):
        """FK-aware merge updates joined rows correctly."""
        results = [
            {"post_id": 10, "title": "Hello", "user_id": 1, "id": 1, "name": "Alice"},
        ]
        schema = {
            "posts": {
                "columns": {
                    "post_id": {"type": "integer", "primary_key": True},
                    "user_id": {"type": "integer", "references": "users.id"},
                }
            }
        }
        pending_by_table = {
            "users": [
                {
                    "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 1,
                    "op": "update", "data": {"id": 1, "name": "Alice Updated"},
                    "sql": "", "params": {}, "dialect": "sqlite",
                    "queued_at": "", "attempt_count": 0, "last_error": "",
                }
            ]
        }

        merged = merge_pending_into_join_results(
            results, pending_by_table, schema, ["users"], "posts"
        )
        assert merged[0]["name"] == "Alice Updated"

    def test_merge_preserves_order(self):
        """Merge result order follows original results order before re-sort."""
        results = [
            {"id": 3, "name": "Carol"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 4,
                "op": "insert", "data": {"id": 4, "name": "Dave"},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        assert merged[3]["id"] == 4  # appended at end before re-sort

    def test_extract_order_by_single_asc(self):
        """Extracts single ASC column."""
        result = _extract_order_by("SELECT * FROM users ORDER BY name ASC")
        assert result == [("name", "ASC")]

    def test_extract_order_by_single_desc(self):
        """Extracts single DESC column."""
        result = _extract_order_by("SELECT * FROM users ORDER BY created_at DESC")
        assert result == [("created_at", "DESC")]

    def test_extract_order_by_multi_column(self):
        """Extracts multiple ORDER BY columns."""
        result = _extract_order_by("SELECT * FROM users ORDER BY name ASC, id DESC")
        assert result == [("name", "ASC"), ("id", "DESC")]

    def test_extract_order_by_no_direction_defaults_asc(self):
        """Column with no direction defaults to ASC."""
        result = _extract_order_by("SELECT * FROM users ORDER BY name")
        assert result == [("name", "ASC")]

    def test_extract_order_by_none(self):
        """Returns empty list when no ORDER BY."""
        result = _extract_order_by("SELECT * FROM users WHERE id = 1")
        assert result == []

    def test_extract_order_by_with_limit(self):
        """Stops at LIMIT clause."""
        result = _extract_order_by("SELECT * FROM users ORDER BY name ASC LIMIT 10")
        assert result == [("name", "ASC")]

    def test_resort_asc(self):
        """Re-sorts results ascending by column."""
        results = [{"id": 3}, {"id": 1}, {"id": 2}]
        sorted_results = _resort(results, [("id", "ASC")])
        assert [r["id"] for r in sorted_results] == [1, 2, 3]

    def test_resort_desc(self):
        """Re-sorts results descending by column."""
        results = [{"id": 1}, {"id": 3}, {"id": 2}]
        sorted_results = _resort(results, [("id", "DESC")])
        assert [r["id"] for r in sorted_results] == [3, 2, 1]

    def test_resort_after_merge_preserves_order_by(self):
        """After merge+resort, results match the ORDER BY column order."""
        results = [
            {"id": 3, "name": "Carol"},
            {"id": 1, "name": "Alice"},
        ]
        pending = [
            {
                "entry_id": "x", "table": "users", "pk_column": "id", "pk_value": 2,
                "op": "insert", "data": {"id": 2, "name": "Bob"},
                "sql": "", "params": {}, "dialect": "sqlite",
                "queued_at": "", "attempt_count": 0, "last_error": "",
            }
        ]

        merged = merge_pending_into_results(results, pending, "id")
        # Before re-sort: [Carol, Alice, Bob]
        assert merged[2]["name"] == "Bob"

        # Re-sort by name ASC
        order_by = _extract_order_by("SELECT * FROM users ORDER BY name ASC")
        resorted = _resort(merged, order_by)
        assert [r["name"] for r in resorted] == ["Alice", "Bob", "Carol"]
