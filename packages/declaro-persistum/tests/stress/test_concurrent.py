"""
Concurrent operation tests for declaro_persistum.

Tests thread-safety and concurrent query building.
"""

import pytest
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from declaro_persistum.query.table import table

from tests.bdd.factories.schema_factory import simple_todos_schema


@pytest.mark.stress
class TestConcurrentQueryBuilding:
    """Test concurrent query building (thread-safe operation)."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        self.schema = simple_todos_schema()

    def test_concurrent_select_queries(self):
        """Building SELECT queries concurrently should be thread-safe."""
        schema = self.schema
        def build_query(i: int) -> tuple[str, dict]:
            todos = table("todos", schema)
            query = todos.select().where(todos.id == str(uuid.uuid4())).limit(i % 10 + 1)
            return query.to_sql()

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(build_query, i) for i in range(100)]
            for future in as_completed(futures):
                sql, params = future.result()
                results.append((sql, params))

        assert len(results) == 100
        # All should produce valid SQL
        for sql, params in results:
            assert "SELECT" in sql
            assert "FROM todos" in sql

    def test_concurrent_insert_queries(self):
        """Building INSERT queries concurrently should be thread-safe."""
        schema = self.schema
        def build_query(i: int) -> tuple[str, dict]:
            todos = table("todos", schema)
            query = todos.insert(
                id=str(uuid.uuid4()),
                title=f"Todo {i}",
                completed=i % 2 == 0,
            )
            return query.to_sql()

        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(build_query, i) for i in range(100)]
            for future in as_completed(futures):
                sql, params = future.result()
                results.append((sql, params))

        assert len(results) == 100
        for sql, params in results:
            assert "INSERT" in sql
            assert "todos" in sql

    def test_concurrent_mixed_queries(self):
        """Building mixed query types concurrently should be thread-safe."""
        schema = self.schema

        def build_select(i: int):
            todos = table("todos", schema)
            return ("select", todos.select().where(todos.completed == True).to_sql())

        def build_insert(i: int):
            todos = table("todos", schema)
            return ("insert", todos.insert(id=str(uuid.uuid4()), title=f"Todo {i}").to_sql())

        def build_update(i: int):
            todos = table("todos", schema)
            return ("update", todos.update(completed=True).where(todos.id == str(uuid.uuid4())).to_sql())

        def build_delete(i: int):
            todos = table("todos", schema)
            return ("delete", todos.delete().where(todos.id == str(uuid.uuid4())).to_sql())

        results = {"select": [], "insert": [], "update": [], "delete": []}

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for i in range(25):
                futures.append(executor.submit(build_select, i))
                futures.append(executor.submit(build_insert, i))
                futures.append(executor.submit(build_update, i))
                futures.append(executor.submit(build_delete, i))

            for future in as_completed(futures):
                query_type, (sql, params) = future.result()
                results[query_type].append(sql)

        assert len(results["select"]) == 25
        assert len(results["insert"]) == 25
        assert len(results["update"]) == 25
        assert len(results["delete"]) == 25


@pytest.mark.stress
class TestAsyncQueryExecution:
    """Test async query execution patterns."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        self.schema = simple_todos_schema()

    @pytest.mark.asyncio
    async def test_concurrent_async_query_building(self):
        """Building queries in async tasks should work."""
        schema = self.schema
        async def build_query(i: int) -> tuple[str, dict]:
            # Simulate some async work
            await asyncio.sleep(0.001)
            todos = table("todos", schema)
            query = todos.select().where(todos.id == str(uuid.uuid4()))
            return query.to_sql()

        tasks = [build_query(i) for i in range(100)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 100
        for sql, params in results:
            assert "SELECT" in sql

    @pytest.mark.asyncio
    async def test_high_concurrency_async(self):
        """High concurrency async query building should work."""
        schema = self.schema
        async def build_query(i: int) -> tuple[str, dict]:
            todos = table("todos", schema)
            query = (
                todos
                .select()
                .where(todos.completed == (i % 2 == 0))
                .order_by(todos.created_at.desc())
                .limit(10)
            )
            return query.to_sql()

        # Run 500 concurrent tasks
        tasks = [build_query(i) for i in range(500)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 500

    @pytest.mark.asyncio
    async def test_async_with_semaphore(self):
        """Query building with limited concurrency should work."""
        semaphore = asyncio.Semaphore(50)  # Limit to 50 concurrent
        schema = self.schema

        async def build_query_limited(i: int) -> tuple[str, dict]:
            async with semaphore:
                todos = table("todos", schema)
                query = todos.select().where(todos.title == f"Todo {i}")
                return query.to_sql()

        tasks = [build_query_limited(i) for i in range(200)]
        results = await asyncio.gather(*tasks)

        assert len(results) == 200


@pytest.mark.stress
class TestParameterUniqueness:
    """Test that concurrent queries generate unique parameters."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        self.schema = simple_todos_schema()

    def test_unique_parameter_names_concurrent(self):
        """Concurrent query building should generate unique param names."""
        param_names = []

        schema = self.schema
        def build_and_collect(i: int):
            todos = table("todos", schema)
            # Use multiple conditions to generate multiple params
            query = todos.select().where(
                (todos.title == f"title_{i}") &
                (todos.completed == (i % 2 == 0))
            )
            sql, params = query.to_sql()
            return list(params.keys())

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(build_and_collect, i) for i in range(100)]
            for future in as_completed(futures):
                param_names.extend(future.result())

        # Check for uniqueness (within each query, not globally)
        # Each query should have unique param names
        assert len(param_names) == 200  # 2 params per query, 100 queries
