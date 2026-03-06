"""
Test SQLite applier integration with table reconstruction.

Verifies that the SQLite applier correctly handles ALTER COLUMN operations
by calling the table_reconstruction abstraction.
"""

import pytest
import aiosqlite

from declaro_persistum.applier.sqlite import SQLiteApplier
from declaro_persistum.types import Operation
from declaro_persistum.abstractions.pragma_compat import pragma_table_info


@pytest.mark.asyncio
async def test_applier_handles_alter_column_nullability():
    """Test that SQLite applier handles ALTER COLUMN via reconstruction."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        await conn.execute("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")
        await conn.commit()

        # Define ALTER COLUMN operation
        operations: list[Operation] = [
            {
                "op": "alter_column",
                "table": "users",
                "details": {
                    "column": "email",
                    "changes": {"nullable": False},
                },
            }
        ]

        # Apply operation
        applier = SQLiteApplier()
        result = await applier.apply(conn, operations, [0])

        # Verify success
        assert result["success"] is True
        assert result["operations_applied"] == 1

        # Verify schema changed
        rows = await pragma_table_info(conn, "users")
        email_col = [row for row in rows if row[1] == "email"][0]
        assert email_col[3] == 1, "email should be NOT NULL"

        # Verify data preserved
        cursor = await conn.execute("SELECT COUNT(*) FROM users")
        count = (await cursor.fetchone())[0]
        assert count == 1, "Data should be preserved"


@pytest.mark.asyncio
async def test_applier_handles_alter_column_type():
    """Test that SQLite applier handles column type changes."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, value INTEGER)")
        await conn.execute("INSERT INTO metrics (id, value) VALUES (1, 42)")
        await conn.commit()

        # Define ALTER COLUMN operation
        operations: list[Operation] = [
            {
                "op": "alter_column",
                "table": "metrics",
                "details": {
                    "column": "value",
                    "changes": {"type": "TEXT"},
                },
            }
        ]

        # Apply operation
        applier = SQLiteApplier()
        result = await applier.apply(conn, operations, [0])

        # Verify success
        assert result["success"] is True

        # Verify schema changed
        rows = await pragma_table_info(conn, "metrics")
        value_col = [row for row in rows if row[1] == "value"][0]
        assert value_col[2] == "TEXT", "value type should be TEXT"


@pytest.mark.asyncio
async def test_applier_handles_alter_column_default():
    """Test that SQLite applier handles default value changes."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE settings (id INTEGER PRIMARY KEY, status TEXT)")
        await conn.commit()

        # Define ALTER COLUMN operation
        operations: list[Operation] = [
            {
                "op": "alter_column",
                "table": "settings",
                "details": {
                    "column": "status",
                    "changes": {"default": "'active'"},
                },
            }
        ]

        # Apply operation
        applier = SQLiteApplier()
        result = await applier.apply(conn, operations, [0])

        # Verify success
        assert result["success"] is True

        # Verify schema changed
        rows = await pragma_table_info(conn, "settings")
        status_col = [row for row in rows if row[1] == "status"][0]
        assert status_col[4] == "'active'", "status should have default 'active'"


@pytest.mark.asyncio
async def test_applier_dry_run_shows_reconstruction_marker():
    """Test that dry run shows table reconstruction marker."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        await conn.commit()

        # Define ALTER COLUMN operation
        operations: list[Operation] = [
            {
                "op": "alter_column",
                "table": "users",
                "details": {
                    "column": "email",
                    "changes": {"nullable": False},
                },
            }
        ]

        # Dry run
        applier = SQLiteApplier()
        result = await applier.apply(conn, operations, [0], dry_run=True)

        # Verify dry run result
        assert result["success"] is True
        assert len(result["executed_sql"]) == 1
        assert "Table reconstruction required" in result["executed_sql"][0]


@pytest.mark.asyncio
async def test_applier_rollback_on_reconstruction_error():
    """Test that applier rolls back on reconstruction error."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table with NULL values
        await conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")
        await conn.execute("INSERT INTO posts (id, title) VALUES (1, 'Post 1')")
        await conn.execute("INSERT INTO posts (id, title) VALUES (2, NULL)")
        await conn.commit()

        # Try to make title NOT NULL - should fail
        operations: list[Operation] = [
            {
                "op": "alter_column",
                "table": "posts",
                "details": {
                    "column": "title",
                    "changes": {"nullable": False},
                },
            }
        ]

        applier = SQLiteApplier()
        with pytest.raises(Exception):
            await applier.apply(conn, operations, [0])

        # Verify table unchanged (original schema preserved)
        rows = await pragma_table_info(conn, "posts")
        title_col = [row for row in rows if row[1] == "title"][0]
        assert title_col[3] == 0, "title should still be nullable (rolled back)"


@pytest.mark.asyncio
async def test_applier_multiple_operations_with_reconstruction():
    """Test that applier handles multiple operations including reconstruction."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT)")
        await conn.commit()

        # Define multiple operations
        operations: list[Operation] = [
            {
                "op": "add_column",
                "table": "orders",
                "details": {
                    "column": "amount",
                    "definition": {"type": "INTEGER", "nullable": True},
                },
            },
            {
                "op": "alter_column",
                "table": "orders",
                "details": {
                    "column": "status",
                    "changes": {"nullable": False},
                },
            },
        ]

        # Apply operations
        applier = SQLiteApplier()
        result = await applier.apply(conn, operations, [0, 1])

        # Verify success
        assert result["success"] is True
        assert result["operations_applied"] == 2

        # Verify schema
        rows = await pragma_table_info(conn, "orders")
        columns = {row[1]: row for row in rows}

        assert "amount" in columns, "amount column should be added"
        assert columns["status"][3] == 1, "status should be NOT NULL"
