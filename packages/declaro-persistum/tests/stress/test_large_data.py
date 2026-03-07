"""
Large data and property-based tests for declaro_persistum.

Uses Hypothesis for property-based testing to verify correctness
with arbitrary inputs.
"""

import pytest
import uuid
from hypothesis import given, settings, strategies as st, assume

from declaro_persistum.query.table import table

from tests.bdd.factories.schema_factory import simple_todos_schema, simple_users_schema
from tests.bdd.factories.data_factory import (
    sql_safe_text,
    sql_integer,
    sql_boolean,
    sql_uuid,
)


@pytest.mark.stress
class TestPropertyBasedSelect:
    """Property-based tests for SELECT queries."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        self.todos = table("todos", schema)

    @given(limit=st.integers(min_value=0, max_value=10000))
    @settings(max_examples=100)
    def test_any_valid_limit(self, limit):
        """SELECT with any valid LIMIT should produce valid SQL."""
        query = self.todos.select().limit(limit)
        sql, params = query.to_sql()

        assert "SELECT" in sql
        assert f"LIMIT {limit}" in sql

    @given(offset=st.integers(min_value=0, max_value=10000))
    @settings(max_examples=100)
    def test_any_valid_offset(self, offset):
        """SELECT with any valid OFFSET should produce valid SQL."""
        query = self.todos.select().limit(10).offset(offset)
        sql, params = query.to_sql()

        assert "SELECT" in sql
        assert f"OFFSET {offset}" in sql

    @given(title=sql_safe_text.filter(lambda x: ":" not in x))  # Exclude colon which is param prefix
    @settings(max_examples=100)
    def test_any_string_in_where(self, title):
        """SELECT with any safe string in WHERE should work."""
        query = self.todos.select().where(self.todos.title == title)
        sql, params = query.to_sql()

        assert "SELECT" in sql
        assert "WHERE" in sql
        # Value should be parameterized, not inline
        assert title in params.values()

    @given(completed=sql_boolean)
    @settings(max_examples=50)
    def test_any_boolean_in_where(self, completed):
        """SELECT with any boolean in WHERE should work."""
        query = self.todos.select().where(self.todos.completed == completed)
        sql, params = query.to_sql()

        assert "SELECT" in sql
        assert "WHERE" in sql


@pytest.mark.stress
class TestPropertyBasedInsert:
    """Property-based tests for INSERT queries."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        self.todos = table("todos", schema)

    @given(
        title=sql_safe_text.filter(lambda x: len(x) > 0 and ":" not in x),  # Exclude colon which is param prefix
        completed=sql_boolean,
    )
    @settings(max_examples=100)
    def test_any_valid_insert(self, title, completed):
        """INSERT with any valid data should produce valid SQL."""
        query = self.todos.insert(
            id=str(uuid.uuid4()),
            title=title,
            completed=completed,
        )
        sql, params = query.to_sql()

        assert "INSERT INTO todos" in sql
        # INSERT params use prefixed keys like 'ins_title', not the raw values
        # Check that SQL contains proper parameter placeholders
        assert ":ins_title" in sql or "ins_title" in params

    @given(count=st.integers(min_value=1, max_value=100))
    @settings(max_examples=20)
    def test_batch_insert_building(self, count):
        """Building multiple INSERT queries should work."""
        queries = []
        for i in range(count):
            query = self.todos.insert(
                id=str(uuid.uuid4()),
                title=f"Todo {i}",
                completed=i % 2 == 0,
            )
            queries.append(query.to_sql())

        assert len(queries) == count
        for sql, params in queries:
            assert "INSERT" in sql


@pytest.mark.stress
class TestPropertyBasedUpdate:
    """Property-based tests for UPDATE queries."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    @given(
        name=sql_safe_text,
        status=st.sampled_from(["active", "inactive", "pending"]),
    )
    @settings(max_examples=100)
    def test_any_valid_update(self, name, status):
        """UPDATE with any valid data should produce valid SQL."""
        query = (
            self.users
            .update(name=name, status=status)
            .where(self.users.id == str(uuid.uuid4()))
        )
        sql, params = query.to_sql()

        assert "UPDATE users" in sql
        assert "SET" in sql
        assert "WHERE" in sql

    @given(age=sql_integer)
    @settings(max_examples=100)
    def test_any_integer_update(self, age):
        """UPDATE with any integer should work."""
        query = (
            self.users
            .update(age=age)
            .where(self.users.id == str(uuid.uuid4()))
        )
        sql, params = query.to_sql()

        assert "UPDATE users" in sql
        assert age in params.values()


@pytest.mark.stress
class TestPropertyBasedConditions:
    """Property-based tests for WHERE conditions."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    @given(values=st.lists(st.text(min_size=1, max_size=50), min_size=1, max_size=50))
    @settings(max_examples=50)
    def test_in_clause_with_any_list(self, values):
        """IN clause with any list of values should work."""
        # Filter out null bytes which aren't valid in SQL strings
        values = [v.replace("\x00", "") for v in values if v.replace("\x00", "")]
        assume(len(values) > 0)

        query = self.users.select().where(self.users.status.in_(values))
        sql, params = query.to_sql()

        assert "IN" in sql
        assert len(params) == len(values)

    @given(
        lower=st.integers(min_value=0, max_value=50),
        upper=st.integers(min_value=51, max_value=150),
    )
    @settings(max_examples=50)
    def test_between_with_any_range(self, lower, upper):
        """BETWEEN with any valid range should work."""
        query = self.users.select().where(self.users.age.between(lower, upper))
        sql, params = query.to_sql()

        assert "BETWEEN" in sql
        assert lower in params.values()
        assert upper in params.values()

    @given(pattern=st.text(min_size=1, max_size=20).filter(lambda x: "%" not in x and "_" not in x))
    @settings(max_examples=50)
    def test_like_with_any_pattern(self, pattern):
        """LIKE with any pattern should work."""
        # Filter out SQL wildcards and null bytes
        pattern = pattern.replace("\x00", "")
        assume(len(pattern) > 0)

        query = self.users.select().where(self.users.name.like(f"%{pattern}%"))
        sql, params = query.to_sql()

        assert "LIKE" in sql


@pytest.mark.stress
class TestLargeDataGeneration:
    """Tests with large generated datasets."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        self.todos = table("todos", schema)

    @given(count=st.integers(min_value=100, max_value=1000))
    @settings(max_examples=10, deadline=None)  # No deadline for large data tests
    def test_building_many_queries(self, count):
        """Building many queries should work correctly."""
        queries = []
        for i in range(count):
            query = (
                self.todos
                .select()
                .where(self.todos.title == f"Todo {i}")
                .limit(10)
            )
            sql, params = query.to_sql()
            queries.append((sql, params))

        assert len(queries) == count
        # Verify all queries are valid
        for sql, params in queries:
            assert "SELECT" in sql
            assert "WHERE" in sql
            assert "LIMIT 10" in sql

    @given(
        ids=st.lists(
            st.uuids().map(str),
            min_size=10,
            max_size=500,
            unique=True,
        )
    )
    @settings(max_examples=10, deadline=None)
    def test_in_clause_with_many_uuids(self, ids):
        """IN clause with many UUIDs should work."""
        query = self.todos.select().where(self.todos.id.in_(ids))
        sql, params = query.to_sql()

        assert "IN" in sql
        assert len(params) == len(ids)


@pytest.mark.stress
class TestComplexPropertyTests:
    """Complex property-based tests combining multiple features."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    @given(
        status=st.sampled_from(["active", "inactive", "pending"]),
        min_age=st.integers(min_value=0, max_value=50),
        max_age=st.integers(min_value=51, max_value=150),
        limit=st.integers(min_value=1, max_value=100),
        offset=st.integers(min_value=0, max_value=1000),
    )
    @settings(max_examples=50)
    def test_complex_query_combinations(self, status, min_age, max_age, limit, offset):
        """Complex queries with multiple clauses should work."""
        query = (
            self.users
            .select()
            .where(
                (self.users.status == status) &
                (self.users.age >= min_age) &
                (self.users.age <= max_age)
            )
            .order_by(self.users.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        sql, params = query.to_sql()

        assert "SELECT" in sql
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert f"LIMIT {limit}" in sql
        assert f"OFFSET {offset}" in sql
        assert status in params.values()
        assert min_age in params.values()
        assert max_age in params.values()
