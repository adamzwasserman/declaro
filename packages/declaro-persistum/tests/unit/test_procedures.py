"""
Unit tests for Stored Procedure support.

Tests the Procedure TypedDict, loader parsing, and applier generation.
PostgreSQL only - SQLite raises NotSupportedError.
"""

import pytest
from typing import Any

from declaro_persistum.types import Procedure, Parameter


class TestProcedureTypedDict:
    """Tests for Procedure TypedDict structure."""

    def test_procedure_basic(self):
        """Basic procedure definition."""
        procedure: Procedure = {
            "name": "calculate_total",
            "language": "sql",
            "returns": "numeric",
            "parameters": [
                {"name": "order_id", "type": "uuid"},
            ],
            "body": "SELECT SUM(quantity * price) FROM order_items WHERE order_id = $1;",
        }
        assert procedure["name"] == "calculate_total"
        assert procedure["language"] == "sql"
        assert procedure["returns"] == "numeric"

    def test_procedure_plpgsql(self):
        """Procedure with PL/pgSQL."""
        procedure: Procedure = {
            "name": "update_inventory",
            "language": "plpgsql",
            "returns": "void",
            "parameters": [
                {"name": "product_id", "type": "uuid"},
                {"name": "quantity", "type": "integer"},
            ],
            "body": """
DECLARE
    current_stock integer;
BEGIN
    SELECT stock INTO current_stock FROM products WHERE id = product_id;
    IF current_stock >= quantity THEN
        UPDATE products SET stock = stock - quantity WHERE id = product_id;
    ELSE
        RAISE EXCEPTION 'Insufficient stock';
    END IF;
END;
""",
        }
        assert procedure["language"] == "plpgsql"
        assert "DECLARE" in procedure["body"]

    def test_procedure_with_defaults(self):
        """Procedure parameters with defaults."""
        procedure: Procedure = {
            "name": "get_users",
            "language": "sql",
            "returns": "SETOF users",
            "parameters": [
                {"name": "status", "type": "text", "default": "'active'"},
                {"name": "limit_count", "type": "integer", "default": "100"},
            ],
            "body": "SELECT * FROM users WHERE status = $1 LIMIT $2;",
        }
        assert procedure["parameters"][0].get("default") == "'active'"
        assert procedure["parameters"][1].get("default") == "100"

    def test_procedure_returns_table(self):
        """Procedure that returns a table."""
        procedure: Procedure = {
            "name": "search_users",
            "language": "sql",
            "returns": "TABLE(id uuid, email text, name text)",
            "parameters": [
                {"name": "search_term", "type": "text"},
            ],
            "body": "SELECT id, email, name FROM users WHERE name ILIKE '%' || $1 || '%';",
        }
        assert "TABLE(" in procedure["returns"]


class TestParameterTypedDict:
    """Tests for Parameter TypedDict structure."""

    def test_parameter_required(self):
        """Parameter with name and type."""
        param: Parameter = {
            "name": "user_id",
            "type": "uuid",
        }
        assert param["name"] == "user_id"
        assert param["type"] == "uuid"

    def test_parameter_with_default(self):
        """Parameter with default value."""
        param: Parameter = {
            "name": "status",
            "type": "text",
            "default": "'active'",
        }
        assert param.get("default") == "'active'"


class TestProcedureLoading:
    """Tests for loading procedures from TOML."""

    def test_parse_procedure(self):
        """Parse procedure from TOML."""
        from declaro_persistum.loader import parse_procedure

        toml_data = {
            "language": "sql",
            "returns": "integer",
            "parameters": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "integer"},
            ],
            "body": "SELECT $1 + $2;",
        }
        procedure = parse_procedure("add_numbers", toml_data)
        assert procedure["name"] == "add_numbers"
        assert len(procedure["parameters"]) == 2

    def test_parse_procedure_no_params(self):
        """Parse procedure without parameters."""
        from declaro_persistum.loader import parse_procedure

        toml_data = {
            "language": "sql",
            "returns": "timestamp",
            "body": "SELECT now();",
        }
        procedure = parse_procedure("get_current_time", toml_data)
        assert procedure.get("parameters", []) == []


class TestProcedureApplierPostgreSQL:
    """Tests for PostgreSQL procedure applier."""

    def test_create_function_sql(self):
        """Generate CREATE FUNCTION SQL."""
        from declaro_persistum.applier.postgresql import generate_create_function

        procedure: Procedure = {
            "name": "add_numbers",
            "language": "sql",
            "returns": "integer",
            "parameters": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "integer"},
            ],
            "body": "SELECT $1 + $2;",
        }
        sql = generate_create_function(procedure)
        assert "CREATE OR REPLACE FUNCTION add_numbers" in sql
        assert "x integer" in sql
        assert "y integer" in sql
        assert "RETURNS integer" in sql
        assert "LANGUAGE sql" in sql
        assert "SELECT $1 + $2" in sql

    def test_create_function_plpgsql(self):
        """Generate PL/pgSQL function."""
        from declaro_persistum.applier.postgresql import generate_create_function

        procedure: Procedure = {
            "name": "increment",
            "language": "plpgsql",
            "returns": "integer",
            "parameters": [
                {"name": "val", "type": "integer"},
            ],
            "body": "BEGIN RETURN val + 1; END;",
        }
        sql = generate_create_function(procedure)
        assert "LANGUAGE plpgsql" in sql

    def test_create_function_with_defaults(self):
        """Generate function with parameter defaults."""
        from declaro_persistum.applier.postgresql import generate_create_function

        procedure: Procedure = {
            "name": "greet",
            "language": "sql",
            "returns": "text",
            "parameters": [
                {"name": "name", "type": "text", "default": "'World'"},
            ],
            "body": "SELECT 'Hello, ' || $1 || '!';",
        }
        sql = generate_create_function(procedure)
        assert "name text DEFAULT 'World'" in sql

    def test_drop_function_sql(self):
        """Generate DROP FUNCTION SQL."""
        from declaro_persistum.applier.postgresql import generate_drop_function

        procedure: Procedure = {
            "name": "add_numbers",
            "language": "sql",
            "returns": "integer",
            "parameters": [
                {"name": "x", "type": "integer"},
                {"name": "y", "type": "integer"},
            ],
            "body": "SELECT $1 + $2;",
        }
        sql = generate_drop_function(procedure)
        assert "DROP FUNCTION IF EXISTS add_numbers(integer, integer)" in sql


class TestProcedureApplierSQLite:
    """Tests for SQLite procedure applier (not supported)."""

    def test_create_function_raises_not_supported(self):
        """SQLite raises NotSupportedError for stored procedures."""
        from declaro_persistum.applier.sqlite import generate_create_function
        from declaro_persistum.errors import NotSupportedError

        procedure: Procedure = {
            "name": "add_numbers",
            "language": "sql",
            "returns": "integer",
            "parameters": [
                {"name": "x", "type": "integer"},
            ],
            "body": "SELECT $1 + 1;",
        }
        with pytest.raises(NotSupportedError) as exc_info:
            generate_create_function(procedure)

        error = exc_info.value
        assert "stored procedure" in str(error).lower()
        assert "SQLite" in str(error)
        # Should provide helpful alternatives
        assert "application layer" in str(error) or "Options" in str(error)


class TestProcedureValidation:
    """Tests for procedure validation."""

    def test_validate_procedure_language(self):
        """Procedure language must be valid."""
        from declaro_persistum.loader import validate_procedure

        procedure: Procedure = {
            "name": "test",
            "language": "invalid",  # type: ignore
            "returns": "void",
            "body": "SELECT 1;",
        }
        with pytest.raises(ValueError, match="language"):
            validate_procedure(procedure)

    def test_validate_procedure_requires_body(self):
        """Procedure must have body."""
        from declaro_persistum.loader import validate_procedure

        procedure: Procedure = {
            "name": "test",
            "language": "sql",
            "returns": "void",
        }
        with pytest.raises(ValueError, match="body"):
            validate_procedure(procedure)

    def test_validate_procedure_requires_returns(self):
        """Procedure must have returns."""
        from declaro_persistum.loader import validate_procedure

        procedure: Procedure = {
            "name": "test",
            "language": "sql",
            "body": "SELECT 1;",
        }
        with pytest.raises(ValueError, match="returns"):
            validate_procedure(procedure)

    def test_validate_parameter_requires_name(self):
        """Parameter must have name."""
        from declaro_persistum.loader import validate_procedure

        procedure: Procedure = {
            "name": "test",
            "language": "sql",
            "returns": "void",
            "parameters": [
                {"type": "integer"},  # type: ignore - missing name
            ],
            "body": "SELECT 1;",
        }
        with pytest.raises(ValueError, match="name"):
            validate_procedure(procedure)


class TestProcedureDiff:
    """Tests for procedure diff detection."""

    def test_detect_new_procedure(self):
        """Detect when procedure is added."""
        from declaro_persistum.differ import diff_procedures

        old_procedures: dict[str, Procedure] = {}
        new_procedures: dict[str, Procedure] = {
            "add_numbers": {
                "name": "add_numbers",
                "language": "sql",
                "returns": "integer",
                "parameters": [{"name": "x", "type": "integer"}],
                "body": "SELECT $1 + 1;",
            }
        }
        operations = diff_procedures(old_procedures, new_procedures)
        assert len(operations) == 1
        assert operations[0]["op"] == "create_function"

    def test_detect_dropped_procedure(self):
        """Detect when procedure is removed."""
        from declaro_persistum.differ import diff_procedures

        old_procedures: dict[str, Procedure] = {
            "add_numbers": {
                "name": "add_numbers",
                "language": "sql",
                "returns": "integer",
                "parameters": [{"name": "x", "type": "integer"}],
                "body": "SELECT $1 + 1;",
            }
        }
        new_procedures: dict[str, Procedure] = {}
        operations = diff_procedures(old_procedures, new_procedures)
        assert len(operations) == 1
        assert operations[0]["op"] == "drop_function"

    def test_detect_procedure_body_changed(self):
        """Detect when procedure body changes."""
        from declaro_persistum.differ import diff_procedures

        old_procedures: dict[str, Procedure] = {
            "add_numbers": {
                "name": "add_numbers",
                "language": "sql",
                "returns": "integer",
                "parameters": [{"name": "x", "type": "integer"}],
                "body": "SELECT $1 + 1;",
            }
        }
        new_procedures: dict[str, Procedure] = {
            "add_numbers": {
                "name": "add_numbers",
                "language": "sql",
                "returns": "integer",
                "parameters": [{"name": "x", "type": "integer"}],
                "body": "SELECT $1 + 2;",  # Changed
            }
        }
        operations = diff_procedures(old_procedures, new_procedures)
        # CREATE OR REPLACE handles this
        assert len(operations) == 1
        assert operations[0]["op"] == "create_function"
