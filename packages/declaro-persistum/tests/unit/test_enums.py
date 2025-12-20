"""
Unit tests for Enum support.

Tests the Enum TypedDict, loader parsing, and applier generation.
"""

import pytest
from typing import Any

from declaro_persistum.types import Enum, Schema


class TestEnumTypedDict:
    """Tests for Enum TypedDict structure."""

    def test_enum_with_values(self):
        """Enum with list of values."""
        enum: Enum = {
            "name": "user_status",
            "values": ["active", "inactive", "pending", "deleted"],
        }
        assert enum["name"] == "user_status"
        assert len(enum["values"]) == 4
        assert "active" in enum["values"]

    def test_enum_minimal(self):
        """Enum with minimal required fields."""
        enum: Enum = {
            "name": "priority",
            "values": ["low", "medium", "high"],
        }
        assert enum["name"] == "priority"
        assert enum["values"] == ["low", "medium", "high"]

    def test_enum_with_schema_reference(self):
        """Enum can be referenced in column type."""
        enum: Enum = {
            "name": "order_status",
            "values": ["pending", "shipped", "delivered", "cancelled"],
        }
        # Column type references enum by name
        column_type = f"enum:{enum['name']}"
        assert column_type == "enum:order_status"


class TestEnumLoading:
    """Tests for loading enums from TOML."""

    def test_parse_enum_values(self):
        """Parse enum values from TOML-like structure."""
        from declaro_persistum.loader import parse_enum

        toml_data = {
            "name": "user_role",
            "values": ["admin", "user", "guest"],
        }
        enum = parse_enum(toml_data)
        assert enum["name"] == "user_role"
        assert enum["values"] == ["admin", "user", "guest"]

    def test_parse_enum_with_description(self):
        """Parse enum with optional description."""
        from declaro_persistum.loader import parse_enum

        toml_data = {
            "name": "task_status",
            "values": ["todo", "in_progress", "done"],
            "description": "Status of a task in the workflow",
        }
        enum = parse_enum(toml_data)
        assert enum["name"] == "task_status"
        assert enum.get("description") == "Status of a task in the workflow"


class TestEnumApplierPostgreSQL:
    """Tests for PostgreSQL enum applier."""

    def test_create_enum_sql(self):
        """Generate CREATE TYPE ... AS ENUM SQL."""
        from declaro_persistum.applier.postgresql import generate_create_enum

        enum: Enum = {
            "name": "user_status",
            "values": ["active", "inactive", "pending"],
        }
        sql = generate_create_enum(enum)
        assert "CREATE TYPE user_status AS ENUM" in sql
        assert "'active'" in sql
        assert "'inactive'" in sql
        assert "'pending'" in sql

    def test_drop_enum_sql(self):
        """Generate DROP TYPE SQL."""
        from declaro_persistum.applier.postgresql import generate_drop_enum

        sql = generate_drop_enum("user_status")
        assert sql == "DROP TYPE IF EXISTS user_status"

    def test_alter_enum_add_value(self):
        """Generate ALTER TYPE ... ADD VALUE SQL."""
        from declaro_persistum.applier.postgresql import generate_alter_enum_add_value

        sql = generate_alter_enum_add_value("user_status", "suspended")
        assert "ALTER TYPE user_status ADD VALUE" in sql
        assert "'suspended'" in sql

    def test_column_using_enum(self):
        """Column can reference enum type."""
        from declaro_persistum.applier.postgresql import generate_column_sql

        column = {"type": "enum:user_status", "nullable": False}
        sql = generate_column_sql("status", column, enums={"user_status"})
        assert "user_status" in sql
        assert "NOT NULL" in sql


class TestEnumApplierSQLite:
    """Tests for SQLite enum applier (CHECK constraint fallback)."""

    def test_create_enum_check_constraint(self):
        """SQLite uses CHECK constraint instead of enum type."""
        from declaro_persistum.applier.sqlite import generate_enum_check

        enum: Enum = {
            "name": "user_status",
            "values": ["active", "inactive", "pending"],
        }
        check_sql = generate_enum_check("status", enum)
        assert "CHECK" in check_sql
        assert "status IN" in check_sql
        assert "'active'" in check_sql
        assert "'inactive'" in check_sql
        assert "'pending'" in check_sql

    def test_column_with_enum_check(self):
        """Column with enum type gets CHECK constraint in SQLite."""
        from declaro_persistum.applier.sqlite import generate_column_sql

        enums = {
            "user_status": {
                "name": "user_status",
                "values": ["active", "inactive"],
            }
        }
        column = {"type": "enum:user_status", "nullable": False}
        sql = generate_column_sql("status", column, enums=enums)
        assert "TEXT" in sql  # SQLite stores as TEXT
        assert "NOT NULL" in sql
        assert "CHECK" in sql
        assert "'active'" in sql


class TestEnumValidation:
    """Tests for enum value validation."""

    def test_validate_enum_values_not_empty(self):
        """Enum must have at least one value."""
        from declaro_persistum.loader import validate_enum

        enum: Enum = {
            "name": "empty_enum",
            "values": [],
        }
        with pytest.raises(ValueError, match="at least one value"):
            validate_enum(enum)

    def test_validate_enum_values_unique(self):
        """Enum values must be unique."""
        from declaro_persistum.loader import validate_enum

        enum: Enum = {
            "name": "dup_enum",
            "values": ["a", "b", "a"],
        }
        with pytest.raises(ValueError, match="duplicate"):
            validate_enum(enum)

    def test_validate_enum_name_valid(self):
        """Enum name must be valid identifier."""
        from declaro_persistum.loader import validate_enum

        enum: Enum = {
            "name": "123-invalid",
            "values": ["a", "b"],
        }
        with pytest.raises(ValueError, match="valid identifier"):
            validate_enum(enum)


class TestEnumDiff:
    """Tests for enum diff detection."""

    def test_detect_new_enum(self):
        """Detect when enum is added."""
        from declaro_persistum.differ import diff_enums

        old_enums: dict[str, Enum] = {}
        new_enums: dict[str, Enum] = {
            "user_status": {
                "name": "user_status",
                "values": ["active", "inactive"],
            }
        }
        operations = diff_enums(old_enums, new_enums)
        assert len(operations) == 1
        assert operations[0]["op"] == "create_enum"

    def test_detect_dropped_enum(self):
        """Detect when enum is removed."""
        from declaro_persistum.differ import diff_enums

        old_enums: dict[str, Enum] = {
            "user_status": {
                "name": "user_status",
                "values": ["active", "inactive"],
            }
        }
        new_enums: dict[str, Enum] = {}
        operations = diff_enums(old_enums, new_enums)
        assert len(operations) == 1
        assert operations[0]["op"] == "drop_enum"

    def test_detect_enum_value_added(self):
        """Detect when enum value is added."""
        from declaro_persistum.differ import diff_enums

        old_enums: dict[str, Enum] = {
            "user_status": {
                "name": "user_status",
                "values": ["active", "inactive"],
            }
        }
        new_enums: dict[str, Enum] = {
            "user_status": {
                "name": "user_status",
                "values": ["active", "inactive", "pending"],
            }
        }
        operations = diff_enums(old_enums, new_enums)
        assert len(operations) == 1
        assert operations[0]["op"] == "add_enum_value"
        assert operations[0]["details"]["value"] == "pending"
