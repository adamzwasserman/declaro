"""Unit tests for Turso applier CHECK constraint handling.

Turso now supports CHECK constraints natively, so the applier emits
CHECK clauses directly in SQL (no longer requires Python-side emulation).
"""

import pytest

from declaro_persistum.abstractions.check_compat import (
    clear_registry,
    get_affected_tables,
    get_table_validators,
)
from declaro_persistum.applier.turso import TursoApplier


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry before each test."""
    clear_registry()
    yield
    clear_registry()


class TestTursoApplierCheckConstraints:
    """Test that Turso applier emits CHECK constraints in SQL natively."""

    def test_create_table_includes_check_syntax(self):
        """Verify CHECK constraint appears in generated SQL."""
        applier = TursoApplier()

        operations = [
            {
                "op": "create_table",
                "table": "events",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "start": {"type": "integer"},
                        "end": {
                            "type": "integer",
                            "check": "start <= end",
                        },
                    },
                },
            }
        ]

        sql_statements = applier.generate_sql(operations, [0])

        # SQL SHOULD contain CHECK (native support)
        assert "CHECK" in sql_statements[0]
        assert "start <= end" in sql_statements[0]

    def test_create_table_does_not_register_python_validators(self):
        """Verify CHECK constraints are NOT registered for Python emulation."""
        applier = TursoApplier()

        operations = [
            {
                "op": "create_table",
                "table": "events",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "start": {"type": "integer"},
                        "end": {
                            "type": "integer",
                            "check": "start <= end",
                        },
                    },
                },
            }
        ]

        applier.generate_sql(operations, [0])

        # No Python-side validators should be registered (native CHECK used)
        validators = get_table_validators("events")
        assert len(validators) == 0

    def test_multiple_check_constraints_in_sql(self):
        """Verify multiple CHECK constraints all appear in SQL."""
        applier = TursoApplier()

        operations = [
            {
                "op": "create_table",
                "table": "products",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "price": {"type": "real", "check": "price > 0"},
                        "stock": {"type": "integer", "check": "stock >= 0"},
                    },
                },
            }
        ]

        sql = applier.generate_sql(operations, [0])[0]

        assert "CHECK (price > 0)" in sql
        assert "CHECK (stock >= 0)" in sql

    def test_no_check_no_registration(self):
        """Verify tables without CHECK constraints don't register anything."""
        applier = TursoApplier()

        operations = [
            {
                "op": "create_table",
                "table": "simple_table",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "name": {"type": "text"},
                    },
                },
            }
        ]

        applier.generate_sql(operations, [0])

        validators = get_table_validators("simple_table")
        assert len(validators) == 0

        affected = get_affected_tables()
        assert "simple_table" not in affected

    def test_sql_valid_with_check(self):
        """Verify generated SQL with CHECK is valid syntax."""
        applier = TursoApplier()

        operations = [
            {
                "op": "create_table",
                "table": "events",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "start": {"type": "integer", "nullable": False},
                        "end": {
                            "type": "integer",
                            "nullable": False,
                            "check": "start <= end",
                        },
                    },
                },
            }
        ]

        sql = applier.generate_sql(operations, [0])[0]

        # Verify basic structure
        assert sql.startswith("CREATE TABLE")
        assert '"events"' in sql
        assert '"id"' in sql
        assert "INTEGER" in sql
        assert "PRIMARY KEY" in sql
        assert "NOT NULL" in sql
        # CHECK is now included
        assert "CHECK (start <= end)" in sql
