"""Integration tests for Turso (libSQL).

These tests can run against:
1. Local libSQL file (default) - for CI/quick testing
2. Turso cloud database - set TEST_TURSO_URL and TEST_TURSO_AUTH_TOKEN
"""

import os
import tempfile
import pytest

import libsql_experimental as libsql


# Determine test mode
TURSO_URL = os.environ.get("TEST_TURSO_URL")
TURSO_TOKEN = os.environ.get("TEST_TURSO_AUTH_TOKEN")
USE_CLOUD = TURSO_URL and TURSO_TOKEN


@pytest.fixture
async def turso_connection():
    """Create Turso/libSQL test connection."""
    if USE_CLOUD:
        # Cloud Turso database with embedded replica
        conn = libsql.connect(":memory:", sync_url=TURSO_URL, auth_token=TURSO_TOKEN)

        # Clean up any existing test tables
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'test_%'"
        )
        for row in cursor.fetchall():
            conn.execute(f"DROP TABLE IF EXISTS {row[0]}")

        yield conn

        # Cleanup
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'test_%'"
        )
        for row in cursor.fetchall():
            conn.execute(f"DROP TABLE IF EXISTS {row[0]}")

    else:
        # Local file-based libSQL (SQLite-compatible)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        conn = libsql.connect(db_path)
        yield conn

        # Cleanup
        os.unlink(db_path)


class TestTursoInspector:
    """Integration tests for Turso inspector."""

    async def test_introspect_empty_database(self, turso_connection):
        """Introspecting empty database returns empty dict."""
        from declaro_persistum.inspector.turso import TursoInspector

        inspector = TursoInspector()
        schema = await inspector.introspect(turso_connection)

        assert schema == {}

    async def test_introspect_simple_table(self, turso_connection):
        """Introspect a simple table."""
        from declaro_persistum.inspector.turso import TursoInspector

        # Create a table (sync API)
        turso_connection.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        inspector = TursoInspector()
        schema = await inspector.introspect(turso_connection)

        assert "test_users" in schema
        assert "id" in schema["test_users"]["columns"]
        assert "email" in schema["test_users"]["columns"]
        assert schema["test_users"]["columns"]["id"]["primary_key"] is True
        assert schema["test_users"]["columns"]["email"]["unique"] is True

    async def test_introspect_foreign_key(self, turso_connection):
        """Introspect table with foreign key."""
        from declaro_persistum.inspector.turso import TursoInspector

        turso_connection.execute(
            "CREATE TABLE test_users (id INTEGER PRIMARY KEY)"
        )
        turso_connection.execute("""
            CREATE TABLE test_orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES test_users(id) ON DELETE CASCADE
            )
        """)

        inspector = TursoInspector()
        schema = await inspector.introspect(turso_connection)

        assert "test_orders" in schema
        user_id_col = schema["test_orders"]["columns"]["user_id"]
        assert user_id_col["references"] == "test_users.id"
        assert user_id_col["on_delete"] == "cascade"


class TestTursoApplier:
    """Integration tests for Turso applier."""

    async def test_create_table(self, turso_connection):
        """Apply CREATE TABLE operation."""
        from declaro_persistum.applier.turso import TursoApplier

        applier = TursoApplier()
        operations = [
            {
                "op": "create_table",
                "table": "test_table",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "name": {"type": "text"},
                    }
                },
            }
        ]

        result = await applier.apply(turso_connection, operations, [0])

        assert result["success"] is True
        assert result["operations_applied"] == 1

        # Verify table exists (sync API)
        cursor = turso_connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        )
        assert len(cursor.fetchall()) == 1

    async def test_add_column(self, turso_connection):
        """Apply ADD COLUMN operation."""
        from declaro_persistum.applier.turso import TursoApplier

        # Create table first (sync API)
        turso_connection.execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY)"
        )

        applier = TursoApplier()
        operations = [
            {
                "op": "add_column",
                "table": "test_table",
                "details": {
                    "column": "email",
                    "definition": {"type": "text", "default": "''"},
                },
            }
        ]

        result = await applier.apply(turso_connection, operations, [0])

        assert result["success"] is True

        # Verify column exists (sync API)
        cursor = turso_connection.execute("PRAGMA table_info('test_table')")
        columns = [row[1] for row in cursor.fetchall()]
        assert "email" in columns


class TestFullMigrationCycle:
    """Integration tests for complete migration workflow."""

    async def test_diff_and_apply(self, turso_connection):
        """Test complete diff -> apply cycle."""
        from declaro_persistum.inspector.turso import TursoInspector
        from declaro_persistum.applier.turso import TursoApplier
        from declaro_persistum.differ import diff

        inspector = TursoInspector()
        applier = TursoApplier()

        # Start with empty database
        current = await inspector.introspect(turso_connection)
        # Filter out any non-test tables
        current = {k: v for k, v in current.items() if k.startswith("test_")}
        assert current == {}

        # Define target schema
        target = {
            "test_users": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True, "nullable": False},
                    "email": {"type": "text", "nullable": False, "unique": True},
                }
            }
        }

        # Compute diff
        diff_result = diff(current, target)
        assert len(diff_result["operations"]) > 0

        # Apply
        apply_result = await applier.apply(
            turso_connection,
            diff_result["operations"],
            diff_result["execution_order"],
        )
        assert apply_result["success"] is True

        # Verify final state
        final = await inspector.introspect(turso_connection)
        assert "test_users" in final
        assert "id" in final["test_users"]["columns"]
        assert "email" in final["test_users"]["columns"]
