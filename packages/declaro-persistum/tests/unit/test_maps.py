"""
Unit tests for Map abstraction.

Tests portable map patterns using junction tables with key/value columns.
"""

import pytest
from typing import Any


class TestMapTypeParsing:
    """Tests for parsing map type declarations."""

    def test_parse_map_type_text_text(self):
        """Parse map<text, text> type."""
        from declaro_persistum.abstractions.maps import parse_map_type

        key_type, value_type = parse_map_type("map<text, text>")
        assert key_type == "text"
        assert value_type == "text"

    def test_parse_map_type_text_integer(self):
        """Parse map<text, integer> type."""
        from declaro_persistum.abstractions.maps import parse_map_type

        key_type, value_type = parse_map_type("map<text, integer>")
        assert key_type == "text"
        assert value_type == "integer"

    def test_parse_map_type_uuid_jsonb(self):
        """Parse map<uuid, jsonb> type."""
        from declaro_persistum.abstractions.maps import parse_map_type

        key_type, value_type = parse_map_type("map<uuid, jsonb>")
        assert key_type == "uuid"
        assert value_type == "jsonb"

    def test_parse_map_type_not_map(self):
        """Non-map type returns None."""
        from declaro_persistum.abstractions.maps import parse_map_type

        result = parse_map_type("text")
        assert result is None

    def test_parse_map_type_with_spaces(self):
        """Parse map type with extra spaces."""
        from declaro_persistum.abstractions.maps import parse_map_type

        key_type, value_type = parse_map_type("map< text , integer >")
        assert key_type == "text"
        assert value_type == "integer"


class TestMapJunctionTableGeneration:
    """Tests for generating map junction tables."""

    def test_generate_junction_table_schema(self):
        """Generate junction table schema for map."""
        from declaro_persistum.abstractions.maps import generate_junction_table

        schema = generate_junction_table("users", "metadata", "text", "text")
        assert "users_metadata" in schema
        table = schema["users_metadata"]

        # Should have columns
        assert "id" in table["columns"]
        assert "user_id" in table["columns"]
        assert "key" in table["columns"]
        assert "value" in table["columns"]

        # Key/value should match types
        assert table["columns"]["key"]["type"] == "text"
        assert table["columns"]["value"]["type"] == "text"

        # Should have foreign key
        assert table["columns"]["user_id"]["references"] == "users.id"

        # Should have cascade delete
        assert table["columns"]["user_id"]["on_delete"] == "cascade"

    def test_generate_junction_table_unique_key(self):
        """Map junction table has unique key per parent."""
        from declaro_persistum.abstractions.maps import generate_junction_table

        schema = generate_junction_table("users", "settings", "text", "text")
        table = schema["users_settings"]

        # Should have unique constraint on (user_id, key)
        assert "indexes" in table or "constraints" in table

    def test_generate_junction_table_jsonb_value(self):
        """Generate junction table with JSONB value."""
        from declaro_persistum.abstractions.maps import generate_junction_table

        schema = generate_junction_table("users", "preferences", "text", "jsonb")
        table = schema["users_preferences"]

        assert table["columns"]["value"]["type"] == "jsonb"


class TestMapOperations:
    """Tests for map operation SQL generation."""

    def test_map_set_sql(self):
        """Generate UPSERT for map entry."""
        from declaro_persistum.abstractions.maps import map_set_sql

        sql = map_set_sql("users", "metadata", "postgresql")
        # Should be INSERT ... ON CONFLICT UPDATE
        assert "INSERT INTO users_metadata" in sql
        assert "ON CONFLICT" in sql or "UPSERT" in sql.upper()

    def test_map_get_sql(self):
        """Generate SELECT for single key."""
        from declaro_persistum.abstractions.maps import map_get_sql

        sql = map_get_sql("users", "metadata")
        assert "SELECT" in sql
        assert "FROM users_metadata" in sql
        assert "WHERE" in sql

    def test_map_get_all_sql(self):
        """Generate SELECT for all entries."""
        from declaro_persistum.abstractions.maps import map_get_all_sql

        sql = map_get_all_sql("users", "metadata")
        assert "SELECT" in sql
        assert "FROM users_metadata" in sql

    def test_map_delete_sql(self):
        """Generate DELETE for single key."""
        from declaro_persistum.abstractions.maps import map_delete_sql

        sql = map_delete_sql("users", "metadata")
        assert "DELETE FROM users_metadata" in sql
        assert "WHERE" in sql

    def test_map_clear_sql(self):
        """Generate DELETE all entries."""
        from declaro_persistum.abstractions.maps import map_clear_sql

        sql = map_clear_sql("users", "metadata")
        assert "DELETE FROM users_metadata" in sql
        assert "user_id" in sql

    def test_map_keys_sql(self):
        """Generate SELECT for all keys."""
        from declaro_persistum.abstractions.maps import map_keys_sql

        sql = map_keys_sql("users", "metadata")
        assert "SELECT" in sql
        assert "key" in sql


class TestMapHydration:
    """Tests for reconstructing maps from junction data."""

    def test_map_hydrate_empty(self):
        """Hydrate empty map."""
        from declaro_persistum.abstractions.maps import map_hydrate

        rows: list[dict[str, Any]] = []
        result = map_hydrate(rows)
        assert result == {}

    def test_map_hydrate_single(self):
        """Hydrate single entry map."""
        from declaro_persistum.abstractions.maps import map_hydrate

        rows = [{"key": "name", "value": "Alice"}]
        result = map_hydrate(rows)
        assert result == {"name": "Alice"}

    def test_map_hydrate_multiple(self):
        """Hydrate multiple entry map."""
        from declaro_persistum.abstractions.maps import map_hydrate

        rows = [
            {"key": "name", "value": "Alice"},
            {"key": "email", "value": "alice@example.com"},
            {"key": "role", "value": "admin"},
        ]
        result = map_hydrate(rows)
        assert result == {
            "name": "Alice",
            "email": "alice@example.com",
            "role": "admin",
        }

    def test_map_hydrate_preserves_types(self):
        """Hydrate preserves value types."""
        from declaro_persistum.abstractions.maps import map_hydrate

        rows = [
            {"key": "count", "value": 42},
            {"key": "active", "value": True},
            {"key": "rate", "value": 3.14},
        ]
        result = map_hydrate(rows)
        assert result["count"] == 42
        assert result["active"] is True
        assert result["rate"] == 3.14


class TestMapSQLiteFallback:
    """Tests for SQLite-specific map operations."""

    def test_map_set_sql_sqlite(self):
        """Generate INSERT OR REPLACE for SQLite."""
        from declaro_persistum.abstractions.maps import map_set_sql

        sql = map_set_sql("users", "metadata", "sqlite")
        # SQLite uses INSERT OR REPLACE
        assert "INSERT" in sql
        assert "REPLACE" in sql or "ON CONFLICT" in sql


class TestTursoDialect:
    """Tests for Turso dialect support in maps."""

    def test_map_set_sql_turso(self):
        """Turso dialect uses SQLite-compatible syntax."""
        from declaro_persistum.abstractions.maps import map_set_sql

        sql = map_set_sql("users", "metadata", "turso")
        # Should use :value placeholder (SQLite style)
        assert "SET value = :value" in sql
        # Should NOT use EXCLUDED.value (PostgreSQL style)
        assert "EXCLUDED" not in sql

    def test_map_set_sql_sqlite_vs_postgresql(self):
        """SQLite/Turso use different syntax than PostgreSQL."""
        from declaro_persistum.abstractions.maps import map_set_sql

        pg_sql = map_set_sql("users", "metadata", "postgresql")
        sqlite_sql = map_set_sql("users", "metadata", "sqlite")
        turso_sql = map_set_sql("users", "metadata", "turso")

        # PostgreSQL uses EXCLUDED.value
        assert "EXCLUDED.value" in pg_sql

        # SQLite variants use :value placeholder
        assert "SET value = :value" in sqlite_sql
        assert "SET value = :value" in turso_sql

        # SQLite/Turso should generate identical SQL
        assert sqlite_sql == turso_sql
