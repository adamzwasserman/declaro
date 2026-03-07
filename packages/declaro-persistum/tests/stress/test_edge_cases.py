"""
Edge case and corner case tests.

These tests verify correct handling of boundary conditions,
special characters, NULL values, and other edge cases.
"""

import pytest
import uuid
from datetime import datetime, timezone

from declaro_persistum.query.table import table

from tests.bdd.factories.data_factory import (
    EDGE_CASE_STRINGS,
    EDGE_CASE_INTEGERS,
    EDGE_CASE_DATES,
    TodoFactory,
)
from tests.bdd.factories.schema_factory import simple_todos_schema, simple_users_schema


@pytest.mark.stress
class TestStringEdgeCases:
    """Test edge cases for string handling."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        self.todos = table("todos", schema)

    def test_empty_string_in_where(self):
        """WHERE clause with empty string should work."""
        query = self.todos.select().where(self.todos.title == "")
        sql, params = query.to_sql()
        assert "WHERE" in sql
        assert "" in params.values() or any(v == "" for v in params.values())

    def test_special_characters_in_where(self):
        """WHERE clause with special characters should be properly escaped."""
        special = "Robert'); DROP TABLE--"
        query = self.todos.select().where(self.todos.title == special)
        sql, params = query.to_sql()
        # SQL should use parameter binding, not inline the value
        assert "DROP TABLE" not in sql
        assert special in params.values()

    def test_unicode_in_where(self):
        """WHERE clause with Unicode should work."""
        unicode_str = "emoji: 👍🎉🚀"
        query = self.todos.select().where(self.todos.title == unicode_str)
        sql, params = query.to_sql()
        assert unicode_str in params.values()

    def test_very_long_string_in_where(self):
        """WHERE clause with very long string should work."""
        long_str = "a" * 10000
        query = self.todos.select().where(self.todos.title == long_str)
        sql, params = query.to_sql()
        assert long_str in params.values()

    @pytest.mark.parametrize("edge_string", [s for s in EDGE_CASE_STRINGS if s])
    def test_all_edge_strings_in_insert(self, edge_string):
        """INSERT should handle all edge case strings."""
        query = self.todos.insert(
            id=str(uuid.uuid4()),
            title=edge_string,
            completed=False,
        )
        sql, params = query.to_sql()
        assert "INSERT" in sql
        # Value should be in params, properly parameterized
        assert edge_string in params.values()


@pytest.mark.stress
class TestNullHandling:
    """Test NULL value handling."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    def test_is_null_condition(self):
        """IS NULL condition should generate correct SQL."""
        query = self.users.select().where(self.users.name.is_null())
        sql, params = query.to_sql()
        assert "IS NULL" in sql

    def test_is_not_null_condition(self):
        """IS NOT NULL condition should generate correct SQL."""
        query = self.users.select().where(self.users.name.is_not_null())
        sql, params = query.to_sql()
        assert "IS NOT NULL" in sql

    def test_null_in_insert(self):
        """INSERT with NULL value should work."""
        query = self.users.insert(
            id=str(uuid.uuid4()),
            email="test@test.com",
            name=None,
        )
        sql, params = query.to_sql()
        assert "INSERT" in sql
        # NULL should be handled correctly
        assert None in params.values() or "NULL" in sql


@pytest.mark.stress
class TestNumericEdgeCases:
    """Test edge cases for numeric handling."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    @pytest.mark.parametrize("edge_int", EDGE_CASE_INTEGERS)
    def test_integer_boundaries(self, edge_int):
        """WHERE clause should handle integer boundary values."""
        query = self.users.select().where(self.users.age == edge_int)
        sql, params = query.to_sql()
        assert edge_int in params.values()

    def test_zero_in_where(self):
        """WHERE clause with zero should work."""
        query = self.users.select().where(self.users.age == 0)
        sql, params = query.to_sql()
        assert 0 in params.values()

    def test_negative_in_where(self):
        """WHERE clause with negative number should work."""
        query = self.users.select().where(self.users.age == -1)
        sql, params = query.to_sql()
        assert -1 in params.values()


@pytest.mark.stress
class TestInClauseEdgeCases:
    """Test edge cases for IN clause."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    def test_in_with_empty_list(self):
        """IN clause with empty list should handle gracefully."""
        query = self.users.select().where(self.users.status.in_([]))
        sql, params = query.to_sql()
        # Empty IN should either return no results or be handled specially
        assert "SELECT" in sql

    def test_in_with_single_value(self):
        """IN clause with single value should work."""
        query = self.users.select().where(self.users.status.in_(["active"]))
        sql, params = query.to_sql()
        assert "IN" in sql

    def test_in_with_many_values(self):
        """IN clause with many values should work."""
        values = [f"status_{i}" for i in range(100)]
        query = self.users.select().where(self.users.status.in_(values))
        sql, params = query.to_sql()
        assert "IN" in sql
        # Should have 100 parameter placeholders
        assert len([v for v in params.values() if v in values]) == 100

    def test_in_with_duplicates(self):
        """IN clause with duplicate values should work."""
        values = ["active", "active", "inactive", "inactive"]
        query = self.users.select().where(self.users.status.in_(values))
        sql, params = query.to_sql()
        assert "IN" in sql


@pytest.mark.stress
class TestLimitOffsetEdgeCases:
    """Test edge cases for LIMIT and OFFSET."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_todos_schema()
        self.todos = table("todos", schema)

    def test_limit_zero(self):
        """LIMIT 0 should work."""
        query = self.todos.select().limit(0)
        sql, params = query.to_sql()
        assert "LIMIT 0" in sql

    def test_offset_zero(self):
        """OFFSET 0 should work."""
        query = self.todos.select().limit(10).offset(0)
        sql, params = query.to_sql()
        assert "OFFSET 0" in sql

    def test_very_large_limit(self):
        """Very large LIMIT should work."""
        query = self.todos.select().limit(1000000)
        sql, params = query.to_sql()
        assert "LIMIT 1000000" in sql

    def test_very_large_offset(self):
        """Very large OFFSET should work."""
        query = self.todos.select().limit(10).offset(1000000)
        sql, params = query.to_sql()
        assert "OFFSET 1000000" in sql


@pytest.mark.stress
class TestConditionCombinations:
    """Test complex condition combinations."""

    @pytest.fixture(autouse=True)
    def setup_schema(self):
        """Set up schema before each test."""
        schema = simple_users_schema()
        self.users = table("users", schema)

    def test_and_conditions(self):
        """Multiple AND conditions should work."""
        query = self.users.select().where(
            (self.users.status == "active") & (self.users.age > 18)
        )
        sql, params = query.to_sql()
        assert "AND" in sql

    def test_or_conditions(self):
        """Multiple OR conditions should work."""
        query = self.users.select().where(
            (self.users.status == "active") | (self.users.status == "pending")
        )
        sql, params = query.to_sql()
        assert "OR" in sql

    def test_nested_conditions(self):
        """Nested AND/OR conditions should work."""
        query = self.users.select().where(
            ((self.users.status == "active") & (self.users.age > 18)) |
            ((self.users.status == "pending") & (self.users.age < 65))
        )
        sql, params = query.to_sql()
        assert "AND" in sql
        assert "OR" in sql

    def test_many_conditions(self):
        """Many conditions combined should work."""
        condition = self.users.status == "active"
        for i in range(10):
            condition = condition & (self.users.age > i)

        query = self.users.select().where(condition)
        sql, params = query.to_sql()
        # Should have multiple AND clauses
        assert sql.count("AND") >= 9
