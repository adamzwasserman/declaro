"""Integration tests for PostgreSQL.

These tests require a running PostgreSQL database.
Set TEST_POSTGRESQL_URL environment variable to run.
"""

import os
import pytest

# Skip all tests in this module if no PostgreSQL URL is set
pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRESQL_URL"),
    reason="TEST_POSTGRESQL_URL not set",
)


@pytest.fixture
async def pg_connection():
    """Create PostgreSQL test connection."""
    import asyncpg

    url = os.environ.get("TEST_POSTGRESQL_URL")
    conn = await asyncpg.connect(url)

    # Create a test schema
    await conn.execute("CREATE SCHEMA IF NOT EXISTS declaro_test")
    await conn.execute("SET search_path TO declaro_test")

    yield conn

    # Cleanup
    await conn.execute("DROP SCHEMA declaro_test CASCADE")
    await conn.close()


class TestPostgreSQLInspector:
    """Integration tests for PostgreSQL inspector."""

    async def test_introspect_empty_schema(self, pg_connection):
        """Introspecting empty schema returns empty dict."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        inspector = PostgreSQLInspector()
        schema = await inspector.introspect(pg_connection, schema_name="declaro_test")

        assert schema == {}

    async def test_introspect_simple_table(self, pg_connection):
        """Introspect a simple table."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        # Create a table
        await pg_connection.execute("""
            CREATE TABLE users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)

        inspector = PostgreSQLInspector()
        schema = await inspector.introspect(pg_connection, schema_name="declaro_test")

        assert "users" in schema
        assert "id" in schema["users"]["columns"]
        assert "email" in schema["users"]["columns"]
        assert schema["users"]["columns"]["id"]["primary_key"] is True
        assert schema["users"]["columns"]["email"]["unique"] is True

    async def test_introspect_foreign_key(self, pg_connection):
        """Introspect table with foreign key."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        await pg_connection.execute("""
            CREATE TABLE users (id UUID PRIMARY KEY);
            CREATE TABLE orders (
                id UUID PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE
            )
        """)

        inspector = PostgreSQLInspector()
        schema = await inspector.introspect(pg_connection, schema_name="declaro_test")

        assert "orders" in schema
        user_id_col = schema["orders"]["columns"]["user_id"]
        assert user_id_col["references"] == "users.id"
        assert user_id_col["on_delete"] == "cascade"


class TestPostgreSQLApplier:
    """Integration tests for PostgreSQL applier."""

    async def test_create_table(self, pg_connection):
        """Apply CREATE TABLE operation."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        applier = PostgreSQLApplier()
        operations = [
            {
                "op": "create_table",
                "table": "test_table",
                "details": {
                    "columns": {
                        "id": {"type": "uuid", "primary_key": True},
                        "name": {"type": "text"},
                    }
                },
            }
        ]

        result = await applier.apply(pg_connection, operations, [0])

        assert result["success"] is True
        assert result["operations_applied"] == 1

        # Verify table exists
        exists = await pg_connection.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'declaro_test' AND table_name = 'test_table'
            )
        """)
        assert exists is True

    async def test_add_column(self, pg_connection):
        """Apply ADD COLUMN operation."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        # Create table first
        await pg_connection.execute("""
            CREATE TABLE test_table (id UUID PRIMARY KEY)
        """)

        applier = PostgreSQLApplier()
        operations = [
            {
                "op": "add_column",
                "table": "test_table",
                "details": {
                    "column": "email",
                    "definition": {"type": "text", "nullable": False, "default": "''"},
                },
            }
        ]

        result = await applier.apply(pg_connection, operations, [0])

        assert result["success"] is True

        # Verify column exists
        exists = await pg_connection.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'declaro_test'
                  AND table_name = 'test_table'
                  AND column_name = 'email'
            )
        """)
        assert exists is True


class TestFullMigrationCycle:
    """Integration tests for complete migration workflow."""

    async def test_diff_and_apply(self, pg_connection):
        """Test complete diff -> apply cycle."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector
        from declaro_persistum.applier.postgresql import PostgreSQLApplier
        from declaro_persistum.differ import diff

        inspector = PostgreSQLInspector()
        applier = PostgreSQLApplier()

        # Start with empty schema
        current = await inspector.introspect(pg_connection, schema_name="declaro_test")
        assert current == {}

        # Define target schema
        target = {
            "users": {
                "columns": {
                    "id": {"type": "uuid", "primary_key": True, "nullable": False},
                    "email": {"type": "text", "nullable": False, "unique": True},
                }
            }
        }

        # Compute diff
        diff_result = diff(current, target)
        assert len(diff_result["operations"]) > 0

        # Apply
        apply_result = await applier.apply(
            pg_connection,
            diff_result["operations"],
            diff_result["execution_order"],
        )
        assert apply_result["success"] is True

        # Verify final state
        final = await inspector.introspect(pg_connection, schema_name="declaro_test")
        assert "users" in final
        assert "id" in final["users"]["columns"]
        assert "email" in final["users"]["columns"]
