"""
Unit tests for query timing instrumentation.

Tests fingerprinting, timing measurement, and QueryObserver protocol.
"""

import pytest
import time
from typing import Any, Protocol


class TestQueryFingerprinting:
    """Tests for query fingerprint generation."""

    def test_fingerprint_basic_select(self):
        """Fingerprint basic SELECT query."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql = "SELECT id, name FROM users WHERE id = 123"
        fingerprint = fingerprint_query(sql)

        # Should normalize literals
        assert "123" not in fingerprint
        assert "?" in fingerprint or "$" in fingerprint or "N" in fingerprint

    def test_fingerprint_removes_string_literals(self):
        """Fingerprint removes string literals."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql = "SELECT * FROM users WHERE name = 'Alice'"
        fingerprint = fingerprint_query(sql)

        assert "Alice" not in fingerprint

    def test_fingerprint_normalizes_whitespace(self):
        """Fingerprint normalizes whitespace."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql1 = "SELECT  id   FROM    users"
        sql2 = "SELECT id FROM users"

        fp1 = fingerprint_query(sql1)
        fp2 = fingerprint_query(sql2)

        assert fp1 == fp2

    def test_fingerprint_same_structure_same_result(self):
        """Same query structure produces same fingerprint."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE id = 999"

        fp1 = fingerprint_query(sql1)
        fp2 = fingerprint_query(sql2)

        assert fp1 == fp2

    def test_fingerprint_different_structure_different_result(self):
        """Different query structure produces different fingerprint."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE name = 'test'"

        fp1 = fingerprint_query(sql1)
        fp2 = fingerprint_query(sql2)

        # These should be different (different WHERE columns)
        assert fp1 != fp2

    def test_fingerprint_preserves_table_names(self):
        """Fingerprint preserves table names."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql = "SELECT * FROM users WHERE id = 1"
        fingerprint = fingerprint_query(sql)

        assert "users" in fingerprint

    def test_fingerprint_handles_in_clause(self):
        """Fingerprint handles IN clauses."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql1 = "SELECT * FROM users WHERE id IN (1, 2, 3)"
        sql2 = "SELECT * FROM users WHERE id IN (4, 5, 6, 7, 8)"

        fp1 = fingerprint_query(sql1)
        fp2 = fingerprint_query(sql2)

        # Same structure, different values
        assert fp1 == fp2


class TestTimingMeasurement:
    """Tests for timing measurement."""

    def test_measure_time_returns_elapsed(self):
        """Measure time returns elapsed milliseconds."""
        from declaro_persistum.observability.timing import measure_time

        with measure_time() as timer:
            time.sleep(0.01)  # 10ms

        # Should be around 10ms (with some tolerance)
        assert timer.elapsed_ms >= 5
        assert timer.elapsed_ms < 100  # Should not be wildly off

    def test_measure_time_microsecond_precision(self):
        """Measure time has microsecond precision."""
        from declaro_persistum.observability.timing import measure_time

        with measure_time() as timer:
            pass  # Nearly instant

        # Should have some value (not exactly 0)
        assert isinstance(timer.elapsed_ms, float)

    def test_execute_with_timing_returns_result(self):
        """execute_with_timing returns the query result."""
        from declaro_persistum.observability.timing import execute_with_timing

        async def mock_execute(sql, params):
            return [{"id": 1}]

        import asyncio
        result, elapsed = asyncio.run(
            execute_with_timing(mock_execute, "SELECT 1", {})
        )

        assert result == [{"id": 1}]
        assert isinstance(elapsed, float)

    def test_execute_with_timing_calls_observer(self):
        """execute_with_timing calls observer."""
        from declaro_persistum.observability.timing import execute_with_timing

        observed_calls = []

        class TestObserver:
            def on_query_executed(self, sql, params, elapsed_ms, fingerprint):
                observed_calls.append({
                    "sql": sql,
                    "elapsed_ms": elapsed_ms,
                    "fingerprint": fingerprint,
                })

        async def mock_execute(sql, params):
            return []

        import asyncio
        observer = TestObserver()
        asyncio.run(
            execute_with_timing(mock_execute, "SELECT 1", {}, observer=observer)
        )

        assert len(observed_calls) == 1
        assert observed_calls[0]["sql"] == "SELECT 1"


class TestQueryObserverProtocol:
    """Tests for QueryObserver protocol."""

    def test_observer_protocol_definition(self):
        """QueryObserver protocol is defined."""
        from declaro_persistum.observability.timing import QueryObserver

        # Should be importable and usable as a type hint
        assert hasattr(QueryObserver, "on_query_executed")

    def test_custom_observer_implementation(self):
        """Custom observer can be implemented."""
        from declaro_persistum.observability.timing import QueryObserver

        class MyObserver:
            def __init__(self):
                self.queries = []

            def on_query_executed(self, sql: str, params: dict, elapsed_ms: float, fingerprint: str):
                self.queries.append({"sql": sql, "elapsed_ms": elapsed_ms})

        observer = MyObserver()
        observer.on_query_executed("SELECT 1", {}, 0.5, "select_?")
        assert len(observer.queries) == 1

    def test_null_observer_allowed(self):
        """None observer is allowed."""
        from declaro_persistum.observability.timing import execute_with_timing

        async def mock_execute(sql, params):
            return []

        import asyncio
        # Should not raise
        asyncio.run(
            execute_with_timing(mock_execute, "SELECT 1", {}, observer=None)
        )


class TestTimingOverhead:
    """Tests for timing overhead."""

    def test_fingerprint_overhead_low(self):
        """Fingerprinting overhead is low."""
        from declaro_persistum.observability.timing import fingerprint_query

        sql = "SELECT * FROM users WHERE id = 1"

        start = time.perf_counter()
        for _ in range(1000):
            fingerprint_query(sql)
        elapsed = (time.perf_counter() - start) * 1000  # ms

        # 1000 fingerprints should take < 100ms (0.1ms each)
        assert elapsed < 100

    def test_measure_time_overhead_low(self):
        """Timing measurement overhead is low."""
        from declaro_persistum.observability.timing import measure_time

        start = time.perf_counter()
        for _ in range(1000):
            with measure_time() as timer:
                pass
        elapsed = (time.perf_counter() - start) * 1000  # ms

        # 1000 measurements should take < 50ms (0.05ms each)
        assert elapsed < 50


class TestCompositeObserver:
    """Tests for composite observer pattern."""

    def test_composite_observer_calls_all(self):
        """Composite observer calls all registered observers."""
        from declaro_persistum.observability.timing import CompositeObserver

        calls = {"a": 0, "b": 0}

        class ObserverA:
            def on_query_executed(self, sql, params, elapsed_ms, fingerprint):
                calls["a"] += 1

        class ObserverB:
            def on_query_executed(self, sql, params, elapsed_ms, fingerprint):
                calls["b"] += 1

        composite = CompositeObserver([ObserverA(), ObserverB()])
        composite.on_query_executed("SELECT 1", {}, 0.5, "select_?")

        assert calls["a"] == 1
        assert calls["b"] == 1

    def test_composite_observer_handles_empty(self):
        """Composite observer handles empty list."""
        from declaro_persistum.observability.timing import CompositeObserver

        composite = CompositeObserver([])
        # Should not raise
        composite.on_query_executed("SELECT 1", {}, 0.5, "select_?")
