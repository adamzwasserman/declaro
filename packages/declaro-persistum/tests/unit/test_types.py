"""Tests for type definitions."""

import pytest
from typing import get_type_hints

from declaro_persistum.types import (
    Column,
    Index,
    Table,
    Schema,
    Operation,
    DiffResult,
    Ambiguity,
    ApplyResult,
)


class TestColumn:
    """Tests for Column TypedDict."""

    def test_column_with_all_fields(self):
        """Column can have all optional fields."""
        col: Column = {
            "type": "uuid",
            "nullable": False,
            "default": "gen_random_uuid()",
            "primary_key": True,
            "unique": True,
            "references": "users.id",
            "on_delete": "cascade",
            "on_update": "cascade",
            "check": "length(name) > 0",
            "renamed_from": "old_name",
            "is_new": True,
        }
        assert col["type"] == "uuid"
        assert col["nullable"] is False
        assert col["on_delete"] == "cascade"

    def test_column_with_minimal_fields(self):
        """Column can have just required fields."""
        col: Column = {"type": "text"}
        assert col["type"] == "text"

    def test_column_is_dict(self):
        """Column is a plain dict at runtime."""
        col: Column = {"type": "integer"}
        assert isinstance(col, dict)


class TestTable:
    """Tests for Table TypedDict."""

    def test_table_with_columns(self):
        """Table with columns defined."""
        table: Table = {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "name": {"type": "text"},
            }
        }
        assert "id" in table["columns"]
        assert table["columns"]["id"]["type"] == "integer"

    def test_table_with_composite_pk(self):
        """Table with composite primary key."""
        table: Table = {
            "columns": {
                "user_id": {"type": "uuid"},
                "role_id": {"type": "uuid"},
            },
            "primary_key": ["user_id", "role_id"],
        }
        assert table["primary_key"] == ["user_id", "role_id"]

    def test_table_with_indexes(self):
        """Table with indexes defined."""
        table: Table = {
            "columns": {"email": {"type": "text"}},
            "indexes": {
                "email_idx": {"columns": ["email"], "unique": True},
            },
        }
        assert "email_idx" in table["indexes"]


class TestSchema:
    """Tests for Schema type."""

    def test_schema_is_dict_of_tables(self):
        """Schema is a dict mapping table names to Table dicts."""
        schema: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}}},
            "orders": {"columns": {"id": {"type": "uuid"}}},
        }
        assert len(schema) == 2
        assert "users" in schema
        assert "orders" in schema


class TestOperation:
    """Tests for Operation TypedDict."""

    def test_create_table_operation(self):
        """CREATE TABLE operation."""
        op: Operation = {
            "op": "create_table",
            "table": "users",
            "details": {
                "columns": {"id": {"type": "uuid"}},
            },
        }
        assert op["op"] == "create_table"
        assert op["table"] == "users"

    def test_add_column_operation(self):
        """ADD COLUMN operation."""
        op: Operation = {
            "op": "add_column",
            "table": "users",
            "details": {
                "column": "email",
                "definition": {"type": "text", "nullable": False},
            },
        }
        assert op["op"] == "add_column"
        assert op["details"]["column"] == "email"


class TestDiffResult:
    """Tests for DiffResult TypedDict."""

    def test_diff_result_structure(self):
        """DiffResult has required fields."""
        result: DiffResult = {
            "operations": [],
            "dependencies": {},
            "execution_order": [],
            "ambiguities": [],
        }
        assert result["operations"] == []
        assert result["dependencies"] == {}

    def test_diff_result_with_operations(self):
        """DiffResult with operations."""
        result: DiffResult = {
            "operations": [
                {"op": "create_table", "table": "users", "details": {}},
            ],
            "dependencies": {0: []},
            "execution_order": [0],
            "ambiguities": [],
        }
        assert len(result["operations"]) == 1
        assert result["execution_order"] == [0]


class TestAmbiguity:
    """Tests for Ambiguity TypedDict."""

    def test_rename_ambiguity(self):
        """Ambiguity for possible rename."""
        amb: Ambiguity = {
            "type": "possible_rename",
            "table": "users",
            "from_column": "name",
            "to_column": "full_name",
            "column": None,
            "confidence": 0.8,
            "message": "Column 'name' removed, 'full_name' added",
        }
        assert amb["type"] == "possible_rename"
        assert amb["confidence"] == 0.8


class TestApplyResult:
    """Tests for ApplyResult TypedDict."""

    def test_successful_apply_result(self):
        """Successful apply result."""
        result: ApplyResult = {
            "success": True,
            "executed_sql": ["CREATE TABLE users (id uuid)"],
            "operations_applied": 1,
            "error": None,
            "error_operation": None,
        }
        assert result["success"] is True
        assert result["operations_applied"] == 1

    def test_failed_apply_result(self):
        """Failed apply result."""
        result: ApplyResult = {
            "success": False,
            "executed_sql": [],
            "operations_applied": 0,
            "error": "Syntax error",
            "error_operation": 0,
        }
        assert result["success"] is False
        assert result["error"] == "Syntax error"
