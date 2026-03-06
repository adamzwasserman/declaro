"""
Unit tests for table_reconstruction abstraction.

Tests the core functionality of table reconstruction for ALTER COLUMN operations.
"""

import pytest
import aiosqlite

from declaro_persistum.abstractions.table_reconstruction import (
    reconstruct_table,
    alter_column_nullability,
    alter_column_type,
    alter_column_default,
)
from declaro_persistum.abstractions.pragma_compat import pragma_table_info


@pytest.mark.asyncio
async def test_alter_column_nullability_to_not_null():
    """Test changing column from nullable to NOT NULL."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
        await conn.execute("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")
        await conn.commit()

        # Alter column
        await alter_column_nullability(conn, "users", "email", False)
        await conn.commit()

        # Verify schema
        rows = await pragma_table_info(conn, "users")
        email_col = [row for row in rows if row[1] == "email"][0]
        assert email_col[3] == 1, "email should be NOT NULL"

        # Verify data preserved
        cursor = await conn.execute("SELECT COUNT(*) FROM users")
        count = (await cursor.fetchone())[0]
        assert count == 1, "Data should be preserved"


@pytest.mark.asyncio
async def test_alter_column_nullability_to_nullable():
    """Test changing column from NOT NULL to nullable."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
        await conn.execute("INSERT INTO products (id, name) VALUES (1, 'Product 1')")
        await conn.commit()

        # Alter column
        await alter_column_nullability(conn, "products", "name", True)
        await conn.commit()

        # Verify schema
        rows = await pragma_table_info(conn, "products")
        name_col = [row for row in rows if row[1] == "name"][0]
        assert name_col[3] == 0, "name should be nullable"

        # Verify data preserved
        cursor = await conn.execute("SELECT name FROM products WHERE id = 1")
        result = await cursor.fetchone()
        assert result[0] == "Product 1", "Data should be preserved"


@pytest.mark.asyncio
async def test_alter_column_type():
    """Test changing column type."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, value INTEGER)")
        await conn.execute("INSERT INTO metrics (id, value) VALUES (1, 42)")
        await conn.commit()

        # Alter column type
        await alter_column_type(conn, "metrics", "value", "TEXT")
        await conn.commit()

        # Verify schema
        rows = await pragma_table_info(conn, "metrics")
        value_col = [row for row in rows if row[1] == "value"][0]
        assert value_col[2] == "TEXT", "value type should be TEXT"

        # Verify data preserved (SQLite stores as text now)
        cursor = await conn.execute("SELECT value FROM metrics WHERE id = 1")
        result = await cursor.fetchone()
        assert str(result[0]) == "42", "Data should be preserved"


@pytest.mark.asyncio
async def test_alter_column_default():
    """Test adding/changing default value."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table
        await conn.execute("CREATE TABLE settings (id INTEGER PRIMARY KEY, status TEXT)")
        await conn.commit()

        # Add default value
        await alter_column_default(conn, "settings", "status", "'active'")
        await conn.commit()

        # Verify schema
        rows = await pragma_table_info(conn, "settings")
        status_col = [row for row in rows if row[1] == "status"][0]
        assert status_col[4] == "'active'", "status should have default 'active'"

        # Test default is used
        await conn.execute("INSERT INTO settings (id) VALUES (1)")
        await conn.commit()
        cursor = await conn.execute("SELECT status FROM settings WHERE id = 1")
        result = await cursor.fetchone()
        assert result[0] == "active", "Default value should be used"


@pytest.mark.asyncio
async def test_remove_column_default():
    """Test removing default value."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table with default
        await conn.execute(
            "CREATE TABLE configs (id INTEGER PRIMARY KEY, value TEXT DEFAULT 'default')"
        )
        await conn.commit()

        # Remove default
        await alter_column_default(conn, "configs", "value", None)
        await conn.commit()

        # Verify schema
        rows = await pragma_table_info(conn, "configs")
        value_col = [row for row in rows if row[1] == "value"][0]
        assert value_col[4] is None, "value should have no default"


@pytest.mark.asyncio
async def test_reconstruct_preserves_data():
    """Test that reconstruction preserves all data."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table with data
        await conn.execute(
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, email TEXT, name TEXT)"
        )
        for i in range(100):
            await conn.execute(
                f"INSERT INTO customers (id, email, name) VALUES ({i}, 'user{i}@test.com', 'User {i}')"
            )
        await conn.commit()

        # Reconstruct with email as NOT NULL
        columns = {
            "id": {"type": "INTEGER", "primary_key": True},
            "email": {"type": "TEXT", "nullable": False},
            "name": {"type": "TEXT", "nullable": True},
        }
        await reconstruct_table(conn, "customers", columns)
        await conn.commit()

        # Verify all data preserved
        cursor = await conn.execute("SELECT COUNT(*) FROM customers")
        count = (await cursor.fetchone())[0]
        assert count == 100, "All rows should be preserved"

        # Verify schema change
        rows = await pragma_table_info(conn, "customers")
        email_col = [row for row in rows if row[1] == "email"][0]
        assert email_col[3] == 1, "email should be NOT NULL"


@pytest.mark.asyncio
async def test_reconstruct_preserves_foreign_keys():
    """Test that reconstruction preserves foreign key relationships."""
    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("PRAGMA foreign_keys = ON")

        # Create parent and child tables
        await conn.execute("CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)")
        await conn.execute(
            """CREATE TABLE books (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                author_id INTEGER REFERENCES authors(id)
            )"""
        )

        await conn.execute("INSERT INTO authors (id, name) VALUES (1, 'Author 1')")
        await conn.execute(
            "INSERT INTO books (id, title, author_id) VALUES (1, 'Book 1', 1)"
        )
        await conn.commit()

        # Reconstruct books table
        columns = {
            "id": {"type": "INTEGER", "primary_key": True},
            "title": {"type": "TEXT", "nullable": True},  # Changed to nullable
            "author_id": {"type": "INTEGER", "references": "authors.id"},
        }
        await reconstruct_table(conn, "books", columns)
        await conn.commit()

        # Verify FK still enforced
        await conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(Exception):
            await conn.execute(
                "INSERT INTO books (id, title, author_id) VALUES (2, 'Book 2', 999)"
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_reconstruct_fails_with_null_in_not_null_column():
    """Test that reconstruction fails when NULLs exist for NOT NULL column."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table with NULL values
        await conn.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")
        await conn.execute("INSERT INTO posts (id, title) VALUES (1, 'Post 1')")
        await conn.execute("INSERT INTO posts (id, title) VALUES (2, NULL)")
        await conn.commit()

        # Try to make title NOT NULL - should fail
        with pytest.raises(Exception):
            await alter_column_nullability(conn, "posts", "title", False)
            await conn.commit()


@pytest.mark.asyncio
async def test_reconstruct_with_composite_primary_key():
    """Test reconstruction preserves composite primary key."""
    async with aiosqlite.connect(":memory:") as conn:
        # Create table with composite PK
        await conn.execute(
            "CREATE TABLE composite_pk (a INTEGER, b INTEGER, data TEXT, PRIMARY KEY(a, b))"
        )
        await conn.execute("INSERT INTO composite_pk (a, b, data) VALUES (1, 1, 'test')")
        await conn.commit()

        # Reconstruct with data as NOT NULL
        columns = {
            "a": {"type": "INTEGER", "primary_key": True},
            "b": {"type": "INTEGER", "primary_key": True},
            "data": {"type": "TEXT", "nullable": False},
        }
        await reconstruct_table(conn, "composite_pk", columns)
        await conn.commit()

        # Verify composite PK preserved
        rows = await pragma_table_info(conn, "composite_pk")
        pk_cols = [row[1] for row in rows if row[5] > 0]  # pk > 0
        assert len(pk_cols) == 2, "Should have 2 PK columns"
        assert "a" in pk_cols and "b" in pk_cols, "Both a and b should be PK"
