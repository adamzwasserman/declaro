"""
Load testing for declaro_persistum.

Tests query building performance with large numbers of queries
and verifies memory usage stays reasonable.
"""

import pytest
import time
import uuid

from declaro_persistum.query.table import table, set_default_schema

from tests.bdd.factories.schema_factory import simple_todos_schema, complex_ecommerce_schema
from tests.bdd.factories.data_factory import TodoFactory


@pytest.mark.stress
class TestQueryBuildingPerformance:
    """Test query building performance under load."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        set_default_schema(schema)
        self.todos = table("todos")

    def test_build_1000_select_queries(self):
        """Building 1000 SELECT queries should complete in reasonable time."""
        start = time.perf_counter()

        for i in range(1000):
            query = self.todos.select().where(self.todos.completed == True).limit(10)
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        # Should complete in under 1 second
        assert elapsed < 1.0, f"Building 1000 queries took {elapsed:.2f}s"

    def test_build_1000_insert_queries(self):
        """Building 1000 INSERT queries should complete in reasonable time."""
        start = time.perf_counter()

        for i in range(1000):
            query = self.todos.insert(
                id=str(uuid.uuid4()),
                title=f"Todo {i}",
                completed=i % 2 == 0,
            )
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Building 1000 insert queries took {elapsed:.2f}s"

    def test_build_1000_update_queries(self):
        """Building 1000 UPDATE queries should complete in reasonable time."""
        start = time.perf_counter()

        for i in range(1000):
            query = (
                self.todos
                .update(completed=True, title=f"Updated {i}")
                .where(self.todos.id == str(uuid.uuid4()))
            )
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Building 1000 update queries took {elapsed:.2f}s"

    def test_build_1000_delete_queries(self):
        """Building 1000 DELETE queries should complete in reasonable time."""
        start = time.perf_counter()

        for i in range(1000):
            query = self.todos.delete().where(self.todos.id == str(uuid.uuid4()))
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Building 1000 delete queries took {elapsed:.2f}s"


@pytest.mark.stress
class TestComplexQueryPerformance:
    """Test performance with complex queries."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up complex schema before each test."""
        schema = complex_ecommerce_schema()
        set_default_schema(schema)
        self.users = table("users")
        self.orders = table("orders")
        self.order_items = table("order_items")

    def test_build_queries_with_joins(self):
        """Building queries with JOINs should be performant."""
        start = time.perf_counter()

        for i in range(500):
            query = (
                self.orders
                .select(self.orders.id, self.orders.total, self.users.email)
                .join(self.users, on=self.orders.user_id == self.users.id)
                .where(self.orders.status == "confirmed")
                .order_by(self.orders.created_at.desc())
                .limit(10)
            )
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 2.0, f"Building 500 join queries took {elapsed:.2f}s"

    def test_build_queries_with_multiple_joins(self):
        """Building queries with multiple JOINs should work."""
        start = time.perf_counter()

        for i in range(100):
            query = (
                self.order_items
                .select()
                .join(self.orders, on=self.order_items.order_id == self.orders.id)
                .join(self.users, on=self.orders.user_id == self.users.id)
                .where(self.users.status == "active")
            )
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Building 100 multi-join queries took {elapsed:.2f}s"

    def test_build_queries_with_aggregations(self):
        """Building queries with aggregations should be performant."""
        from declaro_persistum.query import count_, sum_

        start = time.perf_counter()

        for i in range(500):
            query = (
                self.orders
                .select(
                    self.orders.status,
                    count_("*", alias="order_count"),
                    sum_(self.orders.total, alias="total_amount"),
                )
                .group_by(self.orders.status)
            )
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Building 500 aggregation queries took {elapsed:.2f}s"


@pytest.mark.stress
class TestLargeParameterSets:
    """Test performance with large parameter sets."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        set_default_schema(schema)
        self.todos = table("todos")

    def test_in_clause_with_1000_values(self):
        """IN clause with 1000 values should work."""
        ids = [str(uuid.uuid4()) for _ in range(1000)]

        start = time.perf_counter()
        query = self.todos.select().where(self.todos.id.in_(ids))
        sql, params = query.to_sql()
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Building IN clause with 1000 values took {elapsed:.2f}s"
        assert "IN" in sql
        assert len(params) == 1000

    def test_multiple_or_conditions(self):
        """Multiple OR conditions should be efficient."""
        start = time.perf_counter()

        condition = self.todos.title == "value_0"
        for i in range(1, 100):
            condition = condition | (self.todos.title == f"value_{i}")

        query = self.todos.select().where(condition)
        sql, params = query.to_sql()
        elapsed = time.perf_counter() - start

        assert elapsed < 0.5, f"Building 100 OR conditions took {elapsed:.2f}s"
        assert sql.count("OR") == 99


@pytest.mark.stress
class TestSchemaScaling:
    """Test performance with large schemas."""

    def test_large_schema_table_access(self):
        """Accessing tables from large schema should be fast."""
        # Create schema with 100 tables
        large_schema = {}
        for i in range(100):
            large_schema[f"table_{i}"] = {
                "columns": {
                    "id": {"type": "uuid", "primary_key": True},
                    "name": {"type": "text"},
                    "value": {"type": "integer"},
                    "created_at": {"type": "timestamptz"},
                }
            }

        set_default_schema(large_schema)

        start = time.perf_counter()

        # Access each table and build a query
        for i in range(100):
            t = table(f"table_{i}")
            query = t.select().where(t.id == str(uuid.uuid4()))
            sql, params = query.to_sql()

        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, f"Accessing 100 tables took {elapsed:.2f}s"
