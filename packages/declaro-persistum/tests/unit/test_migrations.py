"""
Unit tests for schema migration skip-if-clean optimization.

Tests the SHA-256 hash-based dirty flag that skips introspection
when the schema file hasn't changed since the last successful migration.
"""

import asyncio
import pytest
import tempfile
from pathlib import Path

from declaro_persistum.migrations import (
    META_TABLE,
    _compute_schema_hash,
    _ensure_meta_table,
    _get_stored_hash,
    _schema_is_clean,
    _store_hash,
    apply_migrations_async,
)
from declaro_persistum.pool import ConnectionPool


class TestComputeSchemaHash:
    """Tests for _compute_schema_hash (pure function)."""

    def test_returns_hex_string(self, tmp_path: Path):
        """Hash is a 64-char hex string (SHA-256)."""
        schema = tmp_path / "models.py"
        schema.write_text("class Foo: pass")
        h = _compute_schema_hash(schema, "1.0.0")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_content_same_hash(self, tmp_path: Path):
        """Identical file contents at same version produce identical hash."""
        a = tmp_path / "a.py"
        b = tmp_path / "b.py"
        a.write_text("x = 1")
        b.write_text("x = 1")
        assert _compute_schema_hash(a, "1.0.0") == _compute_schema_hash(b, "1.0.0")

    def test_different_content_different_hash(self, tmp_path: Path):
        """Different contents at same version produce different hashes."""
        a = tmp_path / "a.py"
        b = tmp_path / "b.py"
        a.write_text("x = 1")
        b.write_text("x = 2")
        assert _compute_schema_hash(a, "1.0.0") != _compute_schema_hash(b, "1.0.0")


class TestMetaTable:
    """Tests for _declaro_meta table operations."""

    @pytest.mark.asyncio
    async def test_ensure_creates_table(self):
        """_ensure_meta_table creates the table."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (META_TABLE,),
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == META_TABLE
        await pool.close()

    @pytest.mark.asyncio
    async def test_ensure_is_idempotent(self):
        """Calling _ensure_meta_table twice doesn't error."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            await _ensure_meta_table(conn)
        await pool.close()

    @pytest.mark.asyncio
    async def test_store_and_get_hash(self):
        """Can store and retrieve a schema hash."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, "models.py", "abc123")
            result = await _get_stored_hash(conn, "models.py")
            assert result == "abc123"
        await pool.close()

    @pytest.mark.asyncio
    async def test_get_missing_hash_returns_none(self):
        """Missing hash returns None."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            result = await _get_stored_hash(conn, "nonexistent.py")
            assert result is None
        await pool.close()

    @pytest.mark.asyncio
    async def test_store_hash_upserts(self):
        """Storing a hash twice updates the value."""
        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, "models.py", "first")
            await _store_hash(conn, "models.py", "second")
            result = await _get_stored_hash(conn, "models.py")
            assert result == "second"
        await pool.close()


class TestSchemaIsClean:
    """Tests for _schema_is_clean check."""

    @pytest.mark.asyncio
    async def test_clean_when_hash_matches(self, tmp_path: Path):
        """Returns True when stored hash matches file hash and user tables exist."""
        schema = tmp_path / "models.py"
        schema.write_text("x = 1")
        file_hash = _compute_schema_hash(schema, "1.0.0")

        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            # A user table must exist — otherwise the stale-hash guard
            # correctly treats the DB as empty (cloud destroy/recreate).
            await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
            await conn.commit()
            await _ensure_meta_table(conn)
            await _store_hash(conn, "models.py", file_hash)
            assert await _schema_is_clean(conn, schema, file_hash) is True
        await pool.close()

    @pytest.mark.asyncio
    async def test_dirty_when_hash_matches_but_no_user_tables(self, tmp_path: Path):
        """Returns False when hash matches but DB has no user tables (stale hash).

        Simulates cloud DB destroy/recreate: the local replica retains the
        hash but the cloud DB is empty.  The schema defines a table, so
        an empty DB means the hash is stale.
        """
        # Use __tablename__ (declaro's actual convention). Earlier versions of
        # this test used `class Meta: table_name = 'users'` — which
        # pydantic_loader does not recognize, so load_models_from_module
        # returned {}, the empty-schema branch of _schema_is_clean fired, and
        # the test silently asserted on a schema that defined no tables. The
        # test's docstring claims "the schema defines a table"; this setup
        # now actually does that.
        schema = tmp_path / "models.py"
        schema.write_text(
            "from pydantic import BaseModel\n"
            "\n"
            "class User(BaseModel):\n"
            "    __tablename__ = 'users'\n"
            "    id: int\n"
            "    name: str\n"
        )
        file_hash = _compute_schema_hash(schema, "1.0.0")

        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, "models.py", file_hash)
            # No user tables — simulates cloud DB destroy/recreate
            assert await _schema_is_clean(conn, schema, file_hash) is False
        await pool.close()

    @pytest.mark.asyncio
    async def test_dirty_when_hash_differs(self, tmp_path: Path):
        """Returns False when stored hash doesn't match."""
        schema = tmp_path / "models.py"
        schema.write_text("x = 1")

        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, "models.py", "old_hash")
            new_hash = _compute_schema_hash(schema, "1.0.0")
            assert await _schema_is_clean(conn, schema, new_hash) is False
        await pool.close()

    @pytest.mark.asyncio
    async def test_dirty_when_no_stored_hash(self, tmp_path: Path):
        """Returns False when no hash has been stored (first run)."""
        schema = tmp_path / "models.py"
        schema.write_text("x = 1")
        file_hash = _compute_schema_hash(schema, "1.0.0")

        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            await _ensure_meta_table(conn)
            assert await _schema_is_clean(conn, schema, file_hash) is False
        await pool.close()

    @pytest.mark.asyncio
    async def test_dirty_when_no_meta_table(self, tmp_path: Path):
        """Returns False when _declaro_meta table doesn't exist (first run)."""
        schema = tmp_path / "models.py"
        schema.write_text("x = 1")
        file_hash = _compute_schema_hash(schema, "1.0.0")

        pool = await ConnectionPool.sqlite(":memory:")
        async with pool.acquire() as conn:
            # _ensure_meta_table is called inside _schema_is_clean,
            # so even without pre-creating it, it should return False (no stored hash)
            assert await _schema_is_clean(conn, schema, file_hash) is False
        await pool.close()


class TestApplyMigrationsSkipIfClean:
    """Tests for skip-if-clean in apply_migrations_async.

    Uses file-backed SQLite because :memory: creates separate DBs per
    connection slot, so the meta table wouldn't persist across acquire() calls.
    """

    @pytest.mark.asyncio
    async def test_skipped_field_in_result(self, tmp_path: Path):
        """Result dict includes 'skipped' field."""
        db = tmp_path / "test.db"
        pool = await ConnectionPool.sqlite(str(db))
        result = await apply_migrations_async(
            pool, "sqlite", "/nonexistent/models.py"
        )
        assert "skipped" in result
        await pool.close()

    @pytest.mark.asyncio
    async def test_missing_schema_not_skipped(self, tmp_path: Path):
        """Missing schema file returns skipped=False."""
        db = tmp_path / "test.db"
        pool = await ConnectionPool.sqlite(str(db))
        result = await apply_migrations_async(
            pool, "sqlite", "/nonexistent/models.py"
        )
        assert result["skipped"] is False
        assert result["success"] is False
        await pool.close()

    @pytest.mark.asyncio
    async def test_force_bypasses_clean_check(self, tmp_path: Path):
        """force=True runs full migration even if schema hash matches."""
        schema = tmp_path / "models.py"
        schema.write_text("")  # Empty = no tables
        db = tmp_path / "test.db"

        pool = await ConnectionPool.sqlite(str(db))

        # First run — stores hash
        result1 = await apply_migrations_async(pool, "sqlite", schema)
        assert result1["skipped"] is False

        # Second run without force — should skip
        result2 = await apply_migrations_async(pool, "sqlite", schema)
        assert result2["skipped"] is True

        # Third run with force — should NOT skip
        result3 = await apply_migrations_async(
            pool, "sqlite", schema, force=True
        )
        assert result3["skipped"] is False

        await pool.close()

    @pytest.mark.asyncio
    async def test_second_call_skips_when_clean(self, tmp_path: Path):
        """Second call with unchanged schema skips introspection."""
        schema = tmp_path / "models.py"
        schema.write_text("")  # Empty = no tables
        db = tmp_path / "test.db"

        pool = await ConnectionPool.sqlite(str(db))

        result1 = await apply_migrations_async(pool, "sqlite", schema)
        assert result1["skipped"] is False

        result2 = await apply_migrations_async(pool, "sqlite", schema)
        assert result2["skipped"] is True
        assert result2["success"] is True
        assert result2["operations_applied"] == 0

        await pool.close()

    @pytest.mark.asyncio
    async def test_changed_schema_runs_migration(self, tmp_path: Path):
        """Changed schema file triggers full migration."""
        schema = tmp_path / "models.py"
        schema.write_text("")  # Empty first
        db = tmp_path / "test.db"

        pool = await ConnectionPool.sqlite(str(db))

        result1 = await apply_migrations_async(pool, "sqlite", schema)
        assert result1["skipped"] is False

        # Change schema content
        schema.write_text("# changed")

        result2 = await apply_migrations_async(pool, "sqlite", schema)
        assert result2["skipped"] is False

        await pool.close()
