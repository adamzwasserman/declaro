"""Tests for schema loading."""

import pytest
from pathlib import Path

from declaro_persistum.loader import (
    load_schema,
    load_snapshot,
    save_snapshot,
    load_decisions,
    save_decisions,
)
from declaro_persistum.exceptions import LoaderError


class TestLoadSchema:
    """Tests for load_schema function."""

    def test_load_empty_directory(self, temp_schema_dir):
        """Loading from empty directory raises error."""
        with pytest.raises(LoaderError):
            load_schema(temp_schema_dir)

    def test_load_single_table(self, temp_schema_with_users):
        """Load schema with single table file."""
        schema = load_schema(temp_schema_with_users)

        assert "users" in schema
        assert "id" in schema["users"]["columns"]
        assert schema["users"]["columns"]["id"]["type"] == "uuid"

    def test_load_multiple_tables(self, temp_schema_dir):
        """Load schema with multiple table files."""
        # Create users.toml
        (temp_schema_dir / "tables" / "users.toml").write_text('''
[users]
[users.columns.id]
type = "uuid"
''')
        # Create orders.toml
        (temp_schema_dir / "tables" / "orders.toml").write_text('''
[orders]
[orders.columns.id]
type = "uuid"
''')

        schema = load_schema(temp_schema_dir)

        assert "users" in schema
        assert "orders" in schema

    def test_load_from_root_schema_file(self, temp_schema_dir):
        """Load from schema.toml in root directory."""
        (temp_schema_dir / "schema.toml").write_text('''
[users]
[users.columns.id]
type = "uuid"
''')

        schema = load_schema(temp_schema_dir)

        assert "users" in schema

    def test_load_with_all_column_options(self, temp_schema_dir):
        """Load column with all options."""
        (temp_schema_dir / "tables" / "full.toml").write_text('''
[full_table]
[full_table.columns.id]
type = "uuid"
nullable = false
default = "gen_random_uuid()"
primary_key = true
unique = true
check = "length(id) > 0"
''')

        schema = load_schema(temp_schema_dir)

        col = schema["full_table"]["columns"]["id"]
        assert col["type"] == "uuid"
        assert col["nullable"] is False
        assert col["default"] == "gen_random_uuid()"
        assert col["primary_key"] is True
        assert col["unique"] is True
        assert col["check"] == "length(id) > 0"

    def test_load_with_foreign_key(self, temp_schema_dir):
        """Load column with foreign key reference."""
        (temp_schema_dir / "tables" / "orders.toml").write_text('''
[orders]
[orders.columns.user_id]
type = "uuid"
references = "users.id"
on_delete = "cascade"
on_update = "restrict"
''')

        schema = load_schema(temp_schema_dir)

        col = schema["orders"]["columns"]["user_id"]
        assert col["references"] == "users.id"
        assert col["on_delete"] == "cascade"
        assert col["on_update"] == "restrict"

    def test_load_with_indexes(self, temp_schema_dir):
        """Load table with indexes."""
        (temp_schema_dir / "tables" / "users.toml").write_text('''
[users]
[users.columns.email]
type = "text"

[users.indexes.email_idx]
columns = ["email"]
unique = true
''')

        schema = load_schema(temp_schema_dir)

        assert "email_idx" in schema["users"]["indexes"]
        assert schema["users"]["indexes"]["email_idx"]["columns"] == ["email"]
        assert schema["users"]["indexes"]["email_idx"]["unique"] is True

    def test_load_with_composite_primary_key(self, temp_schema_dir):
        """Load table with composite primary key."""
        (temp_schema_dir / "tables" / "mapping.toml").write_text('''
[user_roles]
primary_key = ["user_id", "role_id"]

[user_roles.columns.user_id]
type = "uuid"

[user_roles.columns.role_id]
type = "uuid"
''')

        schema = load_schema(temp_schema_dir)

        assert schema["user_roles"]["primary_key"] == ["user_id", "role_id"]

    def test_load_nonexistent_directory(self):
        """Loading from nonexistent directory raises error."""
        with pytest.raises(LoaderError):
            load_schema("/nonexistent/path")


class TestSnapshot:
    """Tests for snapshot loading and saving."""

    def test_save_and_load_snapshot(self, temp_schema_dir):
        """Save and load a snapshot."""
        schema = {
            "users": {
                "columns": {
                    "id": {"type": "uuid", "primary_key": True},
                    "email": {"type": "text"},
                }
            }
        }

        save_snapshot(temp_schema_dir, schema, "postgresql")

        # Verify file exists
        snapshot_path = temp_schema_dir / "snapshot.toml"
        assert snapshot_path.exists()

        # Load it back
        loaded = load_snapshot(temp_schema_dir)

        assert "users" in loaded
        assert loaded["users"]["columns"]["id"]["type"] == "uuid"

    def test_load_nonexistent_snapshot(self, temp_schema_dir):
        """Loading nonexistent snapshot raises error."""
        with pytest.raises(LoaderError):
            load_snapshot(temp_schema_dir)


class TestDecisions:
    """Tests for decision loading and saving."""

    def test_save_and_load_decisions(self, temp_schema_dir):
        """Save and load decisions."""
        decisions = {
            "users_name_to_full_name": {
                "type": "rename",
                "table": "users",
                "from_column": "name",
                "to_column": "full_name",
            }
        }

        save_decisions(temp_schema_dir, decisions)

        # Verify file exists
        pending_path = temp_schema_dir / "migrations" / "pending.toml"
        assert pending_path.exists()

        # Load it back
        loaded = load_decisions(temp_schema_dir)

        assert "users_name_to_full_name" in loaded
        assert loaded["users_name_to_full_name"]["type"] == "rename"

    def test_load_empty_decisions(self, temp_schema_dir):
        """Loading from directory without decisions returns empty dict."""
        decisions = load_decisions(temp_schema_dir)
        assert decisions == {}
