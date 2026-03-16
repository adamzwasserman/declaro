"""
Unit tests for Array abstraction.

Tests portable array patterns using junction tables with position column.
"""

import pytest
from typing import Any


class TestArrayTypeParsing:
    """Tests for parsing array type declarations."""

    def test_parse_array_type_text(self):
        """Parse array<text> type."""
        from declaro_persistum.abstractions.arrays import parse_array_type

        element_type = parse_array_type("array<text>")
        assert element_type == "text"

    def test_parse_array_type_integer(self):
        """Parse array<integer> type."""
        from declaro_persistum.abstractions.arrays import parse_array_type

        element_type = parse_array_type("array<integer>")
        assert element_type == "integer"

    def test_parse_array_type_uuid(self):
        """Parse array<uuid> type."""
        from declaro_persistum.abstractions.arrays import parse_array_type

        element_type = parse_array_type("array<uuid>")
        assert element_type == "uuid"

    def test_parse_array_type_not_array(self):
        """Non-array type returns None."""
        from declaro_persistum.abstractions.arrays import parse_array_type

        result = parse_array_type("text")
        assert result is None

    def test_parse_array_type_nested(self):
        """Nested arrays return inner type."""
        from declaro_persistum.abstractions.arrays import parse_array_type

        # array<array<text>> would have element type array<text>
        element_type = parse_array_type("array<array<text>>")
        assert element_type == "array<text>"


class TestJunctionTableGeneration:
    """Tests for generating junction tables."""

    def test_generate_junction_table_schema(self):
        """Generate junction table schema."""
        from declaro_persistum.abstractions.arrays import generate_junction_table

        schema = generate_junction_table("users", "tags", "text")
        assert "users_tags" in schema
        table = schema["users_tags"]

        # Should have columns
        assert "id" in table["columns"]
        assert "user_id" in table["columns"]
        assert "value" in table["columns"]
        assert "position" in table["columns"]

        # Value should be element type
        assert table["columns"]["value"]["type"] == "text"

        # Should have foreign key
        assert table["columns"]["user_id"]["references"] == "users.id"

        # Should have cascade delete
        assert table["columns"]["user_id"]["on_delete"] == "cascade"

    def test_generate_junction_table_custom_pk(self):
        """Generate junction table with custom primary key."""
        from declaro_persistum.abstractions.arrays import generate_junction_table

        schema = generate_junction_table("orders", "items", "uuid", parent_pk="order_id")
        table = schema["orders_items"]

        assert "order_id" in table["columns"]
        assert table["columns"]["order_id"]["references"] == "orders.order_id"

    def test_generate_junction_table_index(self):
        """Junction table has position index."""
        from declaro_persistum.abstractions.arrays import generate_junction_table

        schema = generate_junction_table("users", "tags", "text")
        table = schema["users_tags"]

        assert "indexes" in table
        assert "idx_users_tags_position" in table["indexes"]


class TestArrayOperations:
    """Tests for array operation SQL generation."""

    def test_array_insert_sql(self):
        """Generate INSERT for array element."""
        from declaro_persistum.abstractions.arrays import array_insert_sql

        sql = array_insert_sql("users", "tags")
        assert "INSERT INTO users_tags" in sql
        assert "user_id" in sql
        assert "value" in sql
        assert "position" in sql

    def test_array_append_sql(self):
        """Generate INSERT at end of array."""
        from declaro_persistum.abstractions.arrays import array_append_sql

        sql = array_append_sql("users", "tags")
        assert "INSERT INTO users_tags" in sql
        # Should calculate position from MAX
        assert "SELECT COALESCE(MAX(position), -1) + 1" in sql or "MAX(position)" in sql

    def test_array_get_sql(self):
        """Generate SELECT for array elements."""
        from declaro_persistum.abstractions.arrays import array_get_sql

        sql = array_get_sql("users", "tags")
        assert "SELECT" in sql
        assert "FROM users_tags" in sql
        assert "ORDER BY position" in sql

    def test_array_delete_sql(self):
        """Generate DELETE for array element."""
        from declaro_persistum.abstractions.arrays import array_delete_sql

        sql = array_delete_sql("users", "tags")
        assert "DELETE FROM users_tags" in sql

    def test_array_clear_sql(self):
        """Generate DELETE all elements."""
        from declaro_persistum.abstractions.arrays import array_clear_sql

        sql = array_clear_sql("users", "tags")
        assert "DELETE FROM users_tags" in sql
        assert "WHERE user_id" in sql


class TestArrayHydration:
    """Tests for reconstructing arrays from junction data."""

    def test_array_hydrate_empty(self):
        """Hydrate empty array."""
        from declaro_persistum.abstractions.arrays import array_hydrate

        rows: list[dict[str, Any]] = []
        result = array_hydrate(rows)
        assert result == []

    def test_array_hydrate_single(self):
        """Hydrate single element array."""
        from declaro_persistum.abstractions.arrays import array_hydrate

        rows = [{"value": "tag1", "position": 0}]
        result = array_hydrate(rows)
        assert result == ["tag1"]

    def test_array_hydrate_multiple(self):
        """Hydrate multiple element array."""
        from declaro_persistum.abstractions.arrays import array_hydrate

        rows = [
            {"value": "first", "position": 0},
            {"value": "second", "position": 1},
            {"value": "third", "position": 2},
        ]
        result = array_hydrate(rows)
        assert result == ["first", "second", "third"]

    def test_array_hydrate_unordered(self):
        """Hydrate array from unordered rows."""
        from declaro_persistum.abstractions.arrays import array_hydrate

        rows = [
            {"value": "third", "position": 2},
            {"value": "first", "position": 0},
            {"value": "second", "position": 1},
        ]
        result = array_hydrate(rows)
        # Should sort by position
        assert result == ["first", "second", "third"]

    def test_array_hydrate_with_gaps(self):
        """Hydrate array with position gaps."""
        from declaro_persistum.abstractions.arrays import array_hydrate

        rows = [
            {"value": "a", "position": 0},
            {"value": "c", "position": 5},  # Gap
            {"value": "b", "position": 2},
        ]
        result = array_hydrate(rows)
        # Should sort by position regardless of gaps
        assert result == ["a", "b", "c"]


class TestArrayUpdatePosition:
    """Tests for reordering array elements."""

    def test_array_move_sql(self):
        """Generate SQL to move element to new position."""
        from declaro_persistum.abstractions.arrays import array_move_sql

        sql = array_move_sql("users", "tags")
        assert "UPDATE users_tags" in sql
        assert "position" in sql

    def test_array_reindex_sql(self):
        """Generate SQL to normalize positions."""
        from declaro_persistum.abstractions.arrays import array_reindex_sql

        sql = array_reindex_sql("users", "tags", "postgresql")
        # Should renumber positions to 0, 1, 2, ...
        assert "UPDATE users_tags" in sql
        assert "ROW_NUMBER()" in sql or "position" in sql


class TestTursoDialect:
    """Tests for Turso dialect support in arrays."""

    def test_array_reindex_sql_turso(self):
        """Turso dialect uses SQLite-compatible syntax."""
        from declaro_persistum.abstractions.arrays import array_reindex_sql

        sql = array_reindex_sql("users", "tags", "turso")
        assert "UPDATE users_tags" in sql
        # SQLite version uses COUNT(*) subquery, not ROW_NUMBER()
        assert "COUNT(*)" in sql
        assert "ROW_NUMBER()" not in sql

    def test_array_reindex_sql_sqlite_vs_postgresql(self):
        """SQLite/Turso use different syntax than PostgreSQL."""
        from declaro_persistum.abstractions.arrays import array_reindex_sql

        pg_sql = array_reindex_sql("users", "tags", "postgresql")
        sqlite_sql = array_reindex_sql("users", "tags", "sqlite")
        turso_sql = array_reindex_sql("users", "tags", "turso")

        # PostgreSQL uses ROW_NUMBER()
        assert "ROW_NUMBER()" in pg_sql

        # SQLite variants use COUNT(*) subquery
        assert "COUNT(*)" in sqlite_sql
        assert "COUNT(*)" in turso_sql

        # SQLite/Turso should generate identical SQL
        assert sqlite_sql == turso_sql
