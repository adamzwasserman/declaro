"""
Unit tests for slow query recording.

Tests SlowQueryObserver, slow query table setup, and query retrieval.
"""

import pytest
from datetime import datetime, timedelta
from typing import Any


class TestSlowQueryObserver:
    """Tests for SlowQueryObserver class."""

    def test_observer_records_above_threshold(self):
        """Observer records queries above threshold."""
        from declaro_persistum.observability.slow_queries import SlowQueryObserver

        recorded = []

        class MockRecorder:
            def record(self, fingerprint, sql, params, elapsed_ms):
                recorded.append({
                    "fingerprint": fingerprint,
                    "sql": sql,
                    "elapsed_ms": elapsed_ms,
                })

        observer = SlowQueryObserver(threshold_ms=100, recorder=MockRecorder())
        observer.on_query_executed("SELECT 1", {}, 150, "select_?")

        assert len(recorded) == 1
        assert recorded[0]["elapsed_ms"] == 150

    def test_observer_ignores_below_threshold(self):
        """Observer ignores queries below threshold."""
        from declaro_persistum.observability.slow_queries import SlowQueryObserver

        recorded = []

        class MockRecorder:
            def record(self, fingerprint, sql, params, elapsed_ms):
                recorded.append(elapsed_ms)

        observer = SlowQueryObserver(threshold_ms=100, recorder=MockRecorder())
        observer.on_query_executed("SELECT 1", {}, 50, "select_?")

        assert len(recorded) == 0

    def test_observer_configurable_threshold(self):
        """Observer threshold is configurable."""
        from declaro_persistum.observability.slow_queries import SlowQueryObserver

        recorded = []

        class MockRecorder:
            def record(self, fingerprint, sql, params, elapsed_ms):
                recorded.append(elapsed_ms)

        # Low threshold
        observer = SlowQueryObserver(threshold_ms=10, recorder=MockRecorder())
        observer.on_query_executed("SELECT 1", {}, 50, "select_?")

        assert len(recorded) == 1


class TestSlowQueryTableSetup:
    """Tests for slow query table setup."""

    def test_setup_table_sql_postgresql(self):
        """Generate CREATE TABLE SQL for PostgreSQL."""
        from declaro_persistum.observability.slow_queries import setup_slow_query_table_sql

        sql = setup_slow_query_table_sql("postgresql")

        assert "CREATE TABLE" in sql
        assert "declaro_slow_queries" in sql
        assert "fingerprint" in sql
        assert "sql_text" in sql
        assert "elapsed_ms" in sql
        assert "recorded_at" in sql

    def test_setup_table_sql_sqlite(self):
        """Generate CREATE TABLE SQL for SQLite."""
        from declaro_persistum.observability.slow_queries import setup_slow_query_table_sql

        sql = setup_slow_query_table_sql("sqlite")

        assert "CREATE TABLE" in sql
        assert "declaro_slow_queries" in sql

    def test_setup_table_has_index(self):
        """Slow query table has performance indexes."""
        from declaro_persistum.observability.slow_queries import setup_slow_query_table_sql

        sql = setup_slow_query_table_sql("postgresql")

        assert "INDEX" in sql or "CREATE INDEX" in sql


class TestSlowQueryRecording:
    """Tests for recording slow queries."""

    def test_record_slow_query_sql(self):
        """Generate INSERT SQL for slow query."""
        from declaro_persistum.observability.slow_queries import record_slow_query_sql

        sql = record_slow_query_sql()

        assert "INSERT INTO declaro_slow_queries" in sql
        assert "fingerprint" in sql
        assert "sql_text" in sql
        assert "elapsed_ms" in sql


class TestSlowQueryRetrieval:
    """Tests for retrieving slow queries."""

    def test_get_slow_queries_sql(self):
        """Generate SELECT SQL for slow queries."""
        from declaro_persistum.observability.slow_queries import get_slow_queries_sql

        sql = get_slow_queries_sql()

        assert "SELECT" in sql
        assert "FROM declaro_slow_queries" in sql
        assert "ORDER BY" in sql

    def test_get_slow_queries_with_since(self):
        """Generate SELECT SQL with time filter."""
        from declaro_persistum.observability.slow_queries import get_slow_queries_sql

        sql = get_slow_queries_sql(since_hours=24)

        assert "recorded_at" in sql

    def test_get_slow_queries_with_limit(self):
        """Generate SELECT SQL with limit."""
        from declaro_persistum.observability.slow_queries import get_slow_queries_sql

        sql = get_slow_queries_sql(limit=100)

        assert "LIMIT" in sql

    def test_get_slow_queries_by_fingerprint(self):
        """Generate SELECT SQL filtered by fingerprint."""
        from declaro_persistum.observability.slow_queries import get_slow_queries_by_fingerprint_sql

        sql = get_slow_queries_by_fingerprint_sql()

        assert "WHERE fingerprint = " in sql


class TestSlowQueryCleanup:
    """Tests for cleaning up old slow queries."""

    def test_cleanup_sql(self):
        """Generate DELETE SQL for old queries."""
        from declaro_persistum.observability.slow_queries import cleanup_slow_queries_sql

        sql = cleanup_slow_queries_sql()

        assert "DELETE FROM declaro_slow_queries" in sql
        assert "recorded_at" in sql

    def test_cleanup_with_retention(self):
        """Generate DELETE SQL with retention period."""
        from declaro_persistum.observability.slow_queries import cleanup_slow_queries_sql

        sql = cleanup_slow_queries_sql(retention_hours=168)

        assert "168" in sql or "recorded_at" in sql


class TestSlowQueryStats:
    """Tests for slow query statistics."""

    def test_get_stats_sql(self):
        """Generate SQL for slow query stats."""
        from declaro_persistum.observability.slow_queries import get_slow_query_stats_sql

        sql = get_slow_query_stats_sql()

        assert "SELECT" in sql
        assert "fingerprint" in sql
        assert "COUNT" in sql

    def test_stats_includes_avg_time(self):
        """Stats include average execution time."""
        from declaro_persistum.observability.slow_queries import get_slow_query_stats_sql

        sql = get_slow_query_stats_sql()

        assert "AVG" in sql

    def test_stats_includes_max_time(self):
        """Stats include maximum execution time."""
        from declaro_persistum.observability.slow_queries import get_slow_query_stats_sql

        sql = get_slow_query_stats_sql()

        assert "MAX" in sql

    def test_stats_groups_by_fingerprint(self):
        """Stats group by fingerprint."""
        from declaro_persistum.observability.slow_queries import get_slow_query_stats_sql

        sql = get_slow_query_stats_sql()

        assert "GROUP BY fingerprint" in sql


class TestSlowQueryConfig:
    """Tests for slow query configuration."""

    def test_config_from_dict(self):
        """Load config from dict."""
        from declaro_persistum.observability.slow_queries import SlowQueryConfig

        config = SlowQueryConfig.from_dict({
            "enabled": True,
            "threshold_ms": 500,
            "retention_hours": 168,
        })

        assert config.enabled is True
        assert config.threshold_ms == 500
        assert config.retention_hours == 168

    def test_config_defaults(self):
        """Config has sensible defaults."""
        from declaro_persistum.observability.slow_queries import SlowQueryConfig

        config = SlowQueryConfig()

        assert config.threshold_ms > 0
        assert config.retention_hours > 0


class TestIntegrationHelpers:
    """Tests for integration helper functions."""

    def test_create_observer_from_config(self):
        """Create observer from config."""
        from declaro_persistum.observability.slow_queries import (
            SlowQueryConfig,
            create_slow_query_observer,
        )

        config = SlowQueryConfig(enabled=True, threshold_ms=100)

        class MockRecorder:
            def record(self, fingerprint, sql, params, elapsed_ms):
                pass

        observer = create_slow_query_observer(config, MockRecorder())
        assert observer is not None

    def test_create_observer_disabled(self):
        """Create returns None when disabled."""
        from declaro_persistum.observability.slow_queries import (
            SlowQueryConfig,
            create_slow_query_observer,
        )

        config = SlowQueryConfig(enabled=False)
        observer = create_slow_query_observer(config, None)

        assert observer is None
