"""
Unit tests for abstractions/reconstruction.py

Tests the table reconstruction abstraction for SQLite/Turso ALTER operations.
"""

import pytest
import aiosqlite

from declaro_persistum.abstractions.reconstruction import (
    execute_reconstruction_async,
    execute_reconstruction_sync,
    generate_create_table_sql,
    generate_data_copy_sql,
    get_reconstruction_columns,
)
from declaro_persistum.types import Column, Operation


# =============================================================================
# Pure Function Tests (No DB)
# =============================================================================


class TestPureFunctions:
    """Test pure SQL generation functions without database interaction."""

    def test_generate_create_table_sql_basic(self):
        """Test CREATE TABLE generation with basic column types."""
        columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "name": {"type": "TEXT", "nullable": False},
            "age": {"type": "INTEGER", "nullable": True},
        }

        sql = generate_create_table_sql("users", columns)

        assert 'CREATE TABLE "users"' in sql
        assert '"id" INTEGER PRIMARY KEY' in sql
        assert '"name" TEXT NOT NULL' in sql
        assert '"age" INTEGER' in sql

    def test_generate_create_table_sql_with_foreign_key(self):
        """Test CREATE TABLE with inline foreign key."""
        columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "user_id": {
                "type": "INTEGER",
                "nullable": False,
                "references": "users.id",
                "on_delete": "cascade",
            },
        }

        sql = generate_create_table_sql("posts", columns)

        assert 'REFERENCES "users"("id")' in sql
        assert "ON DELETE CASCADE" in sql

    def test_generate_create_table_sql_composite_primary_key(self):
        """Test CREATE TABLE with composite primary key."""
        columns: dict[str, Column] = {
            "user_id": {"type": "INTEGER", "primary_key": True},
            "role_id": {"type": "INTEGER", "primary_key": True},
            "assigned_at": {"type": "TEXT"},
        }

        sql = generate_create_table_sql("user_roles", columns)

        assert 'PRIMARY KEY ("user_id", "role_id")' in sql
        # Inline PRIMARY KEY should not be present for composite keys
        assert '"user_id" INTEGER,' in sql or '"user_id" INTEGER\n' in sql

    def test_generate_create_table_sql_with_default_and_unique(self):
        """Test CREATE TABLE with DEFAULT and UNIQUE constraints."""
        columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "email": {"type": "TEXT", "nullable": False, "unique": True},
            "status": {"type": "TEXT", "default": "'active'"},
        }

        sql = generate_create_table_sql("accounts", columns)

        assert '"email" TEXT NOT NULL UNIQUE' in sql
        assert "DEFAULT 'active'" in sql

    def test_generate_create_table_sql_with_check_constraint(self):
        """Test CREATE TABLE with CHECK constraint."""
        columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "age": {"type": "INTEGER", "check": "age >= 18"},
        }

        sql = generate_create_table_sql("adults", columns)

        assert "CHECK (age >= 18)" in sql

    def test_generate_data_copy_sql_basic(self):
        """Test INSERT...SELECT SQL generation."""
        sql = generate_data_copy_sql("users", "users_new", ["id", "name", "email"])

        assert sql == 'INSERT INTO "users_new" ("id", "name", "email") SELECT "id", "name", "email" FROM "users"'

    def test_generate_data_copy_sql_empty_columns(self):
        """Test data copy with empty column list raises ValueError."""
        with pytest.raises(ValueError, match="No common columns"):
            generate_data_copy_sql("users", "users_new", [])

    def test_get_reconstruction_columns_alter_column(self):
        """Test column computation for alter_column operation."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "email": {"type": "TEXT", "nullable": True},
        }

        operation: Operation = {
            "op": "alter_column",
            "table": "users",
            "details": {
                "column": "email",
                "changes": {"nullable": False},
            },
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert new_columns["email"]["nullable"] is False
        assert new_columns["id"] == current_columns["id"]

    def test_get_reconstruction_columns_drop_column(self):
        """Test column computation for drop_column operation."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "email": {"type": "TEXT"},
            "phone": {"type": "TEXT"},
        }

        operation: Operation = {
            "op": "drop_column",
            "table": "users",
            "details": {"column": "phone"},
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert "phone" not in new_columns
        assert "id" in new_columns
        assert "email" in new_columns

    def test_get_reconstruction_columns_add_foreign_key(self):
        """Test column computation for add_foreign_key operation."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "user_id": {"type": "INTEGER"},
        }

        operation: Operation = {
            "op": "add_foreign_key",
            "table": "posts",
            "details": {
                "column": "user_id",
                "references": "users.id",
                "on_delete": "cascade",
            },
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert new_columns["user_id"]["references"] == "users.id"
        assert new_columns["user_id"]["on_delete"] == "cascade"

    def test_get_reconstruction_columns_drop_foreign_key(self):
        """Test column computation for drop_foreign_key operation."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "user_id": {
                "type": "INTEGER",
                "references": "users.id",
                "on_delete": "cascade",
            },
        }

        operation: Operation = {
            "op": "drop_foreign_key",
            "table": "posts",
            "details": {"column": "user_id"},
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert "references" not in new_columns["user_id"]
        assert "on_delete" not in new_columns["user_id"]

    def test_get_reconstruction_columns_alter_multiple_properties(self):
        """Test altering multiple column properties at once."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "email": {"type": "TEXT", "nullable": True},
        }

        operation: Operation = {
            "op": "alter_column",
            "table": "users",
            "details": {
                "column": "email",
                "changes": {
                    "nullable": False,
                    "unique": True,
                },
            },
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert new_columns["email"]["nullable"] is False
        assert new_columns["email"]["unique"] is True

    def test_generate_create_table_sql_non_string_type_fallback(self):
        """Test that _map_type handles non-string types gracefully.

        If a column type is accidentally a dict (e.g., from unprocessed differ output),
        it should default to TEXT rather than crashing with AttributeError.
        """
        columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "broken": {"type": {"from": "TEXT", "to": "INTEGER"}},  # type: ignore
        }

        sql = generate_create_table_sql("test", columns)

        # Should not crash; non-string type defaults to TEXT
        assert "TEXT" in sql
        assert '"broken"' in sql

    def test_get_reconstruction_columns_differ_format_changes(self):
        """Test that differ-format changes (from/to dicts) are handled correctly.

        The differ produces changes like {"type": {"from": "TEXT", "to": "INTEGER"}}
        instead of simple values. Reconstruction must extract the "to" value.
        """
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "score": {"type": "TEXT", "nullable": True},
        }

        operation: Operation = {
            "op": "alter_column",
            "table": "scores",
            "details": {
                "column": "score",
                "changes": {
                    "type": {"from": "TEXT", "to": "INTEGER"},
                    "nullable": {"from": True, "to": False},
                },
            },
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        # Should extract "to" values, not store the dicts
        assert new_columns["score"]["type"] == "INTEGER"
        assert new_columns["score"]["nullable"] is False

    def test_get_reconstruction_columns_remove_default(self):
        """Test removing default value from column."""
        current_columns: dict[str, Column] = {
            "id": {"type": "INTEGER", "primary_key": True},
            "status": {"type": "TEXT", "default": "'active'"},
        }

        operation: Operation = {
            "op": "alter_column",
            "table": "users",
            "details": {
                "column": "status",
                "changes": {"default": None},
            },
        }

        new_columns = get_reconstruction_columns(current_columns, operation)

        assert "default" not in new_columns["status"]


# =============================================================================
# Integration Tests (With DB)
# =============================================================================


@pytest.mark.asyncio
class TestReconstructionAsync:
    """Test async reconstruction execution with database."""

    async def test_execute_reconstruction_async_preserves_data(self):
        """Test that reconstruction preserves existing data."""
        async with aiosqlite.connect(":memory:") as conn:
            # Create initial table
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"
            )
            await conn.execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')")
            await conn.execute("INSERT INTO users (id, email) VALUES (2, 'bob@example.com')")
            await conn.commit()

            # Reconstruct with email as NOT NULL
            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "email": {"type": "TEXT", "nullable": False},
            }

            await execute_reconstruction_async(conn, "users", new_columns)

            # Verify data preserved
            cursor = await conn.execute("SELECT id, email FROM users ORDER BY id")
            rows = await cursor.fetchall()

            assert len(rows) == 2
            assert rows[0] == (1, "alice@example.com")
            assert rows[1] == (2, "bob@example.com")

    async def test_execute_reconstruction_async_preserves_indexes(self):
        """Test that reconstruction recreates user-defined indexes."""
        async with aiosqlite.connect(":memory:") as conn:
            # Create table with index
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"
            )
            await conn.execute('CREATE INDEX idx_email ON users(email)')
            await conn.commit()

            # Reconstruct
            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "email": {"type": "TEXT", "nullable": False},
            }

            await execute_reconstruction_async(conn, "users", new_columns)

            # Verify index exists
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_email'"
            )
            result = await cursor.fetchone()

            assert result is not None
            assert result[0] == "idx_email"

    async def test_execute_reconstruction_async_with_foreign_keys(self):
        """Test reconstruction preserves foreign key relationships."""
        async with aiosqlite.connect(":memory:") as conn:
            # Enable foreign keys
            await conn.execute("PRAGMA foreign_keys = ON")

            # Create parent and child tables
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
            )
            await conn.execute(
                'CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))'
            )
            await conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
            await conn.execute("INSERT INTO posts (id, user_id) VALUES (1, 1)")
            await conn.commit()

            # Reconstruct posts table (add column)
            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "user_id": {"type": "INTEGER", "references": "users.id"},
                "title": {"type": "TEXT"},  # Added column
            }

            await execute_reconstruction_async(conn, "posts", new_columns)

            # Verify data preserved
            cursor = await conn.execute("SELECT id, user_id FROM posts")
            rows = await cursor.fetchall()
            assert rows[0] == (1, 1)

            # Verify schema updated
            cursor = await conn.execute("PRAGMA table_info(posts)")
            cols = await cursor.fetchall()
            col_names = [col[1] for col in cols]
            assert "title" in col_names

    async def test_execute_reconstruction_async_empty_table(self):
        """Test reconstruction of empty table."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"
            )
            await conn.commit()

            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "email": {"type": "TEXT", "nullable": False},
            }

            await execute_reconstruction_async(conn, "users", new_columns)

            # Verify table exists with new schema
            cursor = await conn.execute("PRAGMA table_info(users)")
            cols = await cursor.fetchall()

            assert len(cols) == 2
            assert cols[1][1] == "email"  # Column name
            assert cols[1][3] == 1  # NOT NULL

    async def test_execute_reconstruction_async_no_common_columns(self):
        """Test reconstruction when old and new schemas have no overlap."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute(
                "CREATE TABLE old_table (old_col1 TEXT, old_col2 TEXT)"
            )
            await conn.execute("INSERT INTO old_table VALUES ('a', 'b')")
            await conn.commit()

            # Entirely new schema
            new_columns: dict[str, Column] = {
                "new_col1": {"type": "INTEGER"},
                "new_col2": {"type": "TEXT"},
            }

            await execute_reconstruction_async(conn, "old_table", new_columns)

            # Verify table exists but is empty
            cursor = await conn.execute("SELECT * FROM old_table")
            rows = await cursor.fetchall()

            assert len(rows) == 0

    async def test_execute_reconstruction_async_composite_primary_key(self):
        """Test reconstruction of table with composite primary key."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute(
                """
                CREATE TABLE user_roles (
                    user_id INTEGER,
                    role_id INTEGER,
                    assigned_at TEXT,
                    PRIMARY KEY (user_id, role_id)
                )
                """
            )
            await conn.execute("INSERT INTO user_roles VALUES (1, 2, '2024-01-01')")
            await conn.commit()

            new_columns: dict[str, Column] = {
                "user_id": {"type": "INTEGER", "primary_key": True},
                "role_id": {"type": "INTEGER", "primary_key": True},
                "assigned_at": {"type": "TEXT", "nullable": False},
            }

            await execute_reconstruction_async(conn, "user_roles", new_columns)

            # Verify data preserved
            cursor = await conn.execute("SELECT * FROM user_roles")
            rows = await cursor.fetchall()

            assert len(rows) == 1
            assert rows[0] == (1, 2, "2024-01-01")

    async def test_execute_reconstruction_async_multiple_foreign_keys(self):
        """Test reconstruction with multiple foreign keys on same table."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute("PRAGMA foreign_keys = ON")

            # Create referenced tables
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            await conn.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY)")
            await conn.execute("INSERT INTO users VALUES (1)")
            await conn.execute("INSERT INTO categories VALUES (1)")

            # Create table with multiple FKs
            await conn.execute(
                """
                CREATE TABLE posts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    category_id INTEGER REFERENCES categories(id)
                )
                """
            )
            await conn.execute("INSERT INTO posts VALUES (1, 1, 1)")
            await conn.commit()

            # Reconstruct
            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "user_id": {"type": "INTEGER", "references": "users.id"},
                "category_id": {"type": "INTEGER", "references": "categories.id"},
                "title": {"type": "TEXT"},  # New column
            }

            await execute_reconstruction_async(conn, "posts", new_columns)

            # Verify data preserved
            cursor = await conn.execute("SELECT id, user_id, category_id FROM posts")
            rows = await cursor.fetchall()

            assert rows[0] == (1, 1, 1)

    async def test_execute_reconstruction_async_column_with_unique_and_fk(self):
        """Test reconstruction of column that has both UNIQUE and FK."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            await conn.execute("INSERT INTO users VALUES (1)")

            await conn.execute(
                """
                CREATE TABLE profiles (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER UNIQUE REFERENCES users(id)
                )
                """
            )
            await conn.execute("INSERT INTO profiles VALUES (1, 1)")
            await conn.commit()

            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "user_id": {
                    "type": "INTEGER",
                    "unique": True,
                    "references": "users.id",
                },
                "bio": {"type": "TEXT"},  # New column
            }

            await execute_reconstruction_async(conn, "profiles", new_columns)

            # Verify schema
            cursor = await conn.execute("PRAGMA table_info(profiles)")
            cols = await cursor.fetchall()

            # Verify data
            cursor = await conn.execute("SELECT * FROM profiles")
            rows = await cursor.fetchall()

            assert rows[0][:2] == (1, 1)

    async def test_execute_reconstruction_async_fk_violation_raises_error(self):
        """Test that FK violations after reconstruction raise errors."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")

            # Create posts without FK constraint
            await conn.execute(
                "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER)"
            )
            # Insert data that violates FK
            await conn.execute("INSERT INTO posts VALUES (1, 999)")  # Non-existent user
            await conn.commit()

            # Try to add FK constraint via reconstruction
            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "user_id": {"type": "INTEGER", "references": "users.id"},
            }

            with pytest.raises(ValueError, match="Foreign key constraint violations"):
                await execute_reconstruction_async(conn, "posts", new_columns)

    async def test_execute_reconstruction_async_no_data_preservation(self):
        """Test reconstruction with preserve_data=False."""
        async with aiosqlite.connect(":memory:") as conn:
            await conn.execute(
                "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"
            )
            await conn.execute("INSERT INTO users VALUES (1, 'alice@example.com')")
            await conn.commit()

            new_columns: dict[str, Column] = {
                "id": {"type": "INTEGER", "primary_key": True},
                "email": {"type": "TEXT"},
            }

            await execute_reconstruction_async(
                conn, "users", new_columns, preserve_data=False
            )

            # Verify table is empty
            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()

            assert len(rows) == 0


# Note: Sync tests would be similar but use synchronous connection
# Turso Database/pyturso would be needed for actual sync testing
# For now, the async tests cover the core logic since both implementations
# share the same pure functions and follow the same flow
