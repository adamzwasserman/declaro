"""Integration tests for SQLite.

These tests use in-memory SQLite databases.
"""

import pytest


@pytest.fixture
async def sqlite_connection():
    """Create SQLite test connection."""
    import aiosqlite

    conn = await aiosqlite.connect(":memory:")

    yield conn

    await conn.close()


class TestSQLiteInspector:
    """Integration tests for SQLite inspector."""

    async def test_introspect_empty_database(self, sqlite_connection):
        """Introspecting empty database returns empty dict."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        inspector = SQLiteInspector()
        schema = await inspector.introspect(sqlite_connection)

        assert schema == {}

    async def test_introspect_simple_table(self, sqlite_connection):
        """Introspect a simple table."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        # Create a table
        await sqlite_connection.execute("""
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        await sqlite_connection.commit()

        inspector = SQLiteInspector()
        schema = await inspector.introspect(sqlite_connection)

        assert "users" in schema
        assert "id" in schema["users"]["columns"]
        assert "email" in schema["users"]["columns"]
        assert schema["users"]["columns"]["id"]["primary_key"] is True

    async def test_introspect_foreign_key(self, sqlite_connection):
        """Introspect table with foreign key."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        await sqlite_connection.execute("PRAGMA foreign_keys = ON")
        await sqlite_connection.execute("""
            CREATE TABLE users (id TEXT PRIMARY KEY)
        """)
        await sqlite_connection.execute("""
            CREATE TABLE orders (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        await sqlite_connection.commit()

        inspector = SQLiteInspector()
        schema = await inspector.introspect(sqlite_connection)

        assert "orders" in schema
        user_id_col = schema["orders"]["columns"]["user_id"]
        assert user_id_col["references"] == "users.id"
        assert user_id_col["on_delete"] == "cascade"

    async def test_introspect_index(self, sqlite_connection):
        """Introspect table with index."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        await sqlite_connection.execute("""
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL
            )
        """)
        await sqlite_connection.execute("""
            CREATE INDEX users_email_idx ON users(email)
        """)
        await sqlite_connection.commit()

        inspector = SQLiteInspector()
        schema = await inspector.introspect(sqlite_connection)

        assert "users_email_idx" in schema["users"]["indexes"]
        assert schema["users"]["indexes"]["users_email_idx"]["columns"] == ["email"]


class TestSQLiteApplier:
    """Integration tests for SQLite applier."""

    async def test_create_table(self, sqlite_connection):
        """Apply CREATE TABLE operation."""
        from declaro_persistum.applier.sqlite import SQLiteApplier

        applier = SQLiteApplier()
        operations = [
            {
                "op": "create_table",
                "table": "test_table",
                "details": {
                    "columns": {
                        "id": {"type": "text", "primary_key": True},
                        "name": {"type": "text"},
                    }
                },
            }
        ]

        result = await applier.apply(sqlite_connection, operations, [0])

        assert result["success"] is True
        assert result["operations_applied"] == 1

        # Verify table exists
        cursor = await sqlite_connection.execute("""
            SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'
        """)
        row = await cursor.fetchone()
        assert row is not None

    async def test_add_column(self, sqlite_connection):
        """Apply ADD COLUMN operation."""
        from declaro_persistum.applier.sqlite import SQLiteApplier

        # Create table first
        await sqlite_connection.execute("""
            CREATE TABLE test_table (id TEXT PRIMARY KEY)
        """)
        await sqlite_connection.commit()

        applier = SQLiteApplier()
        operations = [
            {
                "op": "add_column",
                "table": "test_table",
                "details": {
                    "column": "email",
                    "definition": {"type": "text"},
                },
            }
        ]

        result = await applier.apply(sqlite_connection, operations, [0])

        assert result["success"] is True

        # Verify column exists
        cursor = await sqlite_connection.execute("PRAGMA table_info(test_table)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        assert "email" in column_names

    async def test_rename_column(self, sqlite_connection):
        """Apply RENAME COLUMN operation."""
        from declaro_persistum.applier.sqlite import SQLiteApplier

        # Create table with column
        await sqlite_connection.execute("""
            CREATE TABLE test_table (id TEXT PRIMARY KEY, old_name TEXT)
        """)
        await sqlite_connection.commit()

        applier = SQLiteApplier()
        operations = [
            {
                "op": "rename_column",
                "table": "test_table",
                "details": {
                    "from_column": "old_name",
                    "to_column": "new_name",
                },
            }
        ]

        result = await applier.apply(sqlite_connection, operations, [0])

        assert result["success"] is True

        # Verify column renamed
        cursor = await sqlite_connection.execute("PRAGMA table_info(test_table)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        assert "new_name" in column_names
        assert "old_name" not in column_names


class TestSQLiteFullCycle:
    """Integration tests for complete SQLite migration workflow."""

    async def test_diff_and_apply(self, sqlite_connection):
        """Test complete diff -> apply cycle with SQLite."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector
        from declaro_persistum.applier.sqlite import SQLiteApplier
        from declaro_persistum.differ import diff

        inspector = SQLiteInspector()
        applier = SQLiteApplier()

        # Start with empty database
        current = await inspector.introspect(sqlite_connection)
        assert current == {}

        # Define target schema
        target = {
            "users": {
                "columns": {
                    "id": {"type": "text", "primary_key": True, "nullable": False},
                    "email": {"type": "text", "nullable": False, "unique": True},
                }
            }
        }

        # Compute diff
        diff_result = diff(current, target)
        assert len(diff_result["operations"]) > 0

        # Apply
        apply_result = await applier.apply(
            sqlite_connection,
            diff_result["operations"],
            diff_result["execution_order"],
        )
        assert apply_result["success"] is True

        # Verify final state
        final = await inspector.introspect(sqlite_connection)
        assert "users" in final
        assert "id" in final["users"]["columns"]
        assert "email" in final["users"]["columns"]
