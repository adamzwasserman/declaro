"""
Automatic schema migration support for declaro_persistum.

Provides async migration functions that:
1. Load target schema from Pydantic models
2. Introspect current database state
3. Compute diff and apply changes

Uses a SHA-256 hash of the schema file to skip introspection when the schema
hasn't changed since the last successful migration (skip-if-clean).

Usage:
    # Async
    await apply_migrations_async(pool, dialect, schema_path)
"""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Union

from declaro_persistum.applier.protocol import create_applier
from declaro_persistum.differ import diff
from declaro_persistum.inspector.protocol import create_inspector
from declaro_persistum.pydantic_loader import load_models_from_module
from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.types import ApplyResult, Operation, Schema

META_TABLE = "_declaro_meta"

logger = logging.getLogger(__name__)


def _acquire_write_or_read(pool: Any) -> Any:
    """Return pool.acquire_write(concurrent=False) if available, else pool.acquire().

    DDL and hash writes must go through the write connection on pools
    with split read/write paths (e.g. TursoPool) so that changes reach
    the cloud primary rather than only the local replica file.

    concurrent=False because DDL (CREATE TABLE, ALTER TABLE, etc.)
    requires exclusive transactions — BEGIN CONCURRENT is rejected by
    Turso for DDL statements.
    """
    if hasattr(pool, "acquire_write"):
        return pool.acquire_write(concurrent=False)
    return pool.acquire()


# ---------------------------------------------------------------------------
# Schema hash helpers (skip-if-clean)
# ---------------------------------------------------------------------------


def _compute_schema_hash(schema_path: Path) -> str:
    """Compute SHA-256 hex digest of a schema file's contents + declaro version.

    The declaro_persistum version is mixed into the hash so that any version
    bump invalidates the skip-if-clean cache. Without this, fixing a loader
    or applier bug in declaro would leave consumers with a stale "clean"
    hash from the buggy version, silently hiding the fix until the user
    edited their model file or passed force=True.

    First startup after a declaro upgrade therefore performs one
    introspection pass even if the schema file is unchanged. Cost is
    measured in milliseconds for typical schemas; the alternative is
    silent schema corruption persisting across upgrades.
    """
    # Late import to avoid circular dependency at module load time.
    from declaro_persistum import __version__

    h = hashlib.sha256()
    h.update(schema_path.read_bytes())
    h.update(b"\x00")  # delimiter so file content can't collide with version
    h.update(__version__.encode("ascii"))
    return h.hexdigest()


async def _ensure_meta_table(conn: Any) -> None:
    """Create the _declaro_meta table if it doesn't exist."""
    sql = (
        f'CREATE TABLE IF NOT EXISTS "{META_TABLE}" ('
        f"key TEXT PRIMARY KEY, "
        f"value TEXT NOT NULL, "
        f"updated_at TEXT NOT NULL"
        f")"
    )
    if hasattr(conn, "fetch"):
        # asyncpg
        await conn.execute(sql)
    else:
        await conn.execute(sql, ())
        await conn.commit()


async def _get_stored_hash(conn: Any, schema_name: str) -> str | None:
    """Get the stored schema hash from _declaro_meta. Returns None if missing."""
    key = f"schema_hash:{schema_name}"
    if hasattr(conn, "fetch"):
        row = await conn.fetchrow(
            f'SELECT value FROM "{META_TABLE}" WHERE key = $1', key
        )
        return row["value"] if row else None
    else:
        cursor = await conn.execute(
            f'SELECT value FROM "{META_TABLE}" WHERE key = ?', (key,)
        )
        row = await cursor.fetchone()
        return row[0] if row else None


async def _store_hash(conn: Any, schema_name: str, schema_hash: str) -> None:
    """Store (upsert) the schema hash in _declaro_meta."""
    key = f"schema_hash:{schema_name}"
    now = datetime.now(UTC).isoformat()
    if hasattr(conn, "fetch"):
        await conn.execute(
            f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
            f"VALUES ($1, $2, $3) "
            f"ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3",
            key, schema_hash, now,
        )
    else:
        await conn.execute(
            f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
            f"VALUES (?, ?, ?) "
            f"ON CONFLICT (key) DO UPDATE SET "
            f"value = excluded.value, updated_at = excluded.updated_at",
            (key, schema_hash, now),
        )
        await conn.commit()


async def _schema_is_clean(
    conn: Any, schema_path: Path, schema_hash: str
) -> bool:
    """Check if schema file hash matches the stored hash (skip-if-clean).

    A hash match alone is not sufficient: if the cloud DB was destroyed and
    recreated, the local replica retains the stale hash while the cloud is
    empty.  We guard against this by verifying that at least one user table
    (anything other than ``_declaro_meta``) exists — unless the schema itself
    defines zero tables (empty module), in which case an empty DB is expected.
    """
    try:
        await _ensure_meta_table(conn)
        stored = await _get_stored_hash(conn, schema_path.name)
        if stored != schema_hash:
            return False
        # Hash matches — verify cloud DB is not empty (stale-hash guard).
        # Skip this check for empty schemas (no tables defined).
        has_tables = await _has_user_tables(conn)
        if has_tables:
            return True
        # No user tables — only trust the hash if the schema file itself
        # produces no tables (empty module).  Otherwise, the DB was likely
        # destroyed and recreated.
        try:
            target = load_models_from_module(schema_path)
            return len(target) == 0
        except Exception:
            return False
    except Exception:
        # Meta table doesn't exist or query failed — treat as dirty
        return False


async def _has_user_tables(conn: Any) -> bool:
    """Return True if the database has at least one table besides _declaro_meta."""
    sql = (
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != ?"
    )
    if hasattr(conn, "fetch"):
        # asyncpg — use information_schema instead
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        return any(r["tablename"] != META_TABLE for r in rows)
    cursor = await conn.execute(sql, (META_TABLE,))
    row = await cursor.fetchone()
    return row is not None


async def _recover_orphaned_tmp_tables(pool: Any) -> int:
    """Detect and recover orphaned temp tables from failed reconstruction.

    Reconstruction uses temp tables named ``_declaro_tmp_<table>`` (current)
    or ``<table>_new`` (legacy).  If reconstruction partially committed
    (CREATE tmp + DROP original) but failed before RENAME, the database has
    the tmp table but not the original.  This renames them back to recover.

    Returns the number of tables recovered.
    """
    async with _acquire_write_or_read(pool) as conn:
        # Detect both naming patterns:
        #   _declaro_tmp_<table>  (current)
        #   <table>_new           (legacy)
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' "
            "  AND (name LIKE '_declaro_tmp_%' OR name LIKE '%\\_new' ESCAPE '\\')"
        )
        tmp_tables = await cursor.fetchall()

        recovered = 0
        for row in tmp_tables:
            tmp_name = row[0]

            # Derive original table name from whichever pattern matched
            if tmp_name.startswith("_declaro_tmp_"):
                # Strip prefix and UUID suffix: _declaro_tmp_<table>_<8hex>
                remainder = tmp_name[len("_declaro_tmp_"):]
                # The last 9 chars are _{8hex} — strip them
                if len(remainder) > 9 and remainder[-9] == "_":
                    original_name = remainder[:-9]
                else:
                    # No UUID suffix (old format) — use remainder as-is
                    original_name = remainder
            elif tmp_name.endswith("_new"):
                original_name = tmp_name[:-4]
            else:
                continue

            # Check if the original table exists
            check = await conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (original_name,),
            )
            original_exists = await check.fetchone()

            try:
                if original_exists:
                    # Original exists — tmp is leftover junk, drop it
                    await conn.execute(f'DROP TABLE "{tmp_name}"')
                    await conn.commit()
                    logger.warning(
                        f"Dropped leftover tmp table: {tmp_name}"
                    )
                else:
                    # Original missing — rename tmp to recover
                    await conn.execute(
                        f'ALTER TABLE "{tmp_name}" RENAME TO "{original_name}"'
                    )
                    await conn.commit()
                    logger.warning(
                        f"Recovered orphaned table: {tmp_name} → {original_name}"
                    )
                    recovered += 1
            except Exception as e:
                logger.error(
                    f"Failed to clean up tmp table {tmp_name}: {e}"
                )

        # Invalidate stored hash so migration re-runs after recovery
        if recovered:
            try:
                await conn.execute(
                    f'DELETE FROM "{META_TABLE}" WHERE key LIKE ?',
                    ("schema_hash:%",),
                )
                await conn.commit()
                logger.info(
                    f"Cleared schema hash after recovering {recovered} orphaned table(s)"
                )
            except Exception:
                pass  # Meta table might not exist yet

    return recovered


async def apply_migrations_async(
    pool: Any,
    dialect: str,
    schema_path: Union[str, Path],
    *,
    expand_enums: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """
    Apply automatic schema migrations asynchronously.

    Loads schema from Pydantic models, compares with actual database,
    and applies changes if differences are found.

    Uses a SHA-256 hash of the schema file to skip introspection when
    the schema hasn't changed since the last successful migration.

    Args:
        pool: The database connection pool (async)
        dialect: Database dialect ('sqlite', 'turso', 'postgresql')
        schema_path: Path to Python module containing Pydantic models
        expand_enums: Whether to expand Literal types to lookup tables (default True)
        force: Bypass the skip-if-clean check and always run full introspection

    Returns:
        Dict with migration result:
        - success: bool
        - operations_applied: int
        - tables_in_schema: int
        - tables_in_database: int
        - skipped: bool (True if schema was clean and migration was skipped)
        - error: str | None

    Raises:
        Exception: If migration fails critically (logs error but may not crash)
    """
    schema_path = Path(schema_path)

    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": f"Schema file not found: {schema_path}",
        }

    # Pre-flight: recover orphaned _new tables from failed reconstruction.
    # SQLite/Turso only — Postgres reconstruction does not produce these temp
    # tables, and `_recover_orphaned_tmp_tables` queries `sqlite_master` which
    # does not exist on Postgres (would crash startup).
    if dialect in ("sqlite", "turso"):
        await _recover_orphaned_tmp_tables(pool)

    # Skip-if-clean: compare schema file hash with stored hash
    schema_hash = _compute_schema_hash(schema_path)

    if not force:
        async with pool.acquire() as conn:
            if await _schema_is_clean(conn, schema_path, schema_hash):
                logger.info(f"Schema unchanged (hash match) — skipping migration")
                return {
                    "success": True,
                    "operations_applied": 0,
                    "tables_in_schema": 0,
                    "tables_in_database": 0,
                    "skipped": True,
                    "error": None,
                }

    logger.info(f"Loading schema from {schema_path}")
    target_schema = load_models_from_module(schema_path)

    if not target_schema:
        logger.warning("No tables found in schema models")
        # Store hash so next call skips
        async with _acquire_write_or_read(pool) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": None,
        }

    logger.info(f"Loaded {len(target_schema)} tables from schema models")

    # Expand Literal types to lookup tables + FK constraints
    if expand_enums:
        target_schema = expand_schema_enums(target_schema)
        logger.info(
            f"Expanded schema to {len(target_schema)} tables (including enum lookup tables)"
        )

    # Introspect current database state
    inspector = create_inspector(dialect)
    async with pool.acquire() as conn:
        current_schema = await inspector.introspect(conn)

    logger.info(f"Introspected {len(current_schema)} tables from database")

    # Compute diff
    diff_result = diff(current_schema, target_schema)

    if not diff_result["operations"]:
        logger.info("Schema is up to date - no migrations needed")
        # Store hash so next call skips introspection
        async with _acquire_write_or_read(pool) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "skipped": False,
            "error": None,
        }

    # Log planned operations
    logger.info(f"Found {len(diff_result['operations'])} schema differences")
    for op in diff_result["operations"]:
        logger.info(f"  - {op['op']} on table {op.get('table', 'N/A')}")

    # Check for ambiguities
    if diff_result.get("ambiguities"):
        logger.warning(f"Ambiguous changes detected: {diff_result['ambiguities']}")
        logger.warning("Skipping migration - manual intervention required")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "skipped": False,
            "error": f"Ambiguous changes: {diff_result['ambiguities']}",
        }

    # On embedded replicas (remote_url set), skip reconstruction ops —
    # the sync engine can't replicate DDL, and partial sync (DROP reaches
    # cloud but CREATE doesn't) destroys tables on both sides.
    # Use `declaro migrate-remote` for schema changes that need reconstruction.
    is_cloud_replica = hasattr(pool, "_remote_url") and pool._remote_url
    operations = diff_result["operations"]
    execution_order = diff_result["execution_order"]

    if is_cloud_replica:
        from declaro_persistum.applier.shared import requires_reconstruction

        safe_indices = set()
        for op_idx in execution_order:
            op = operations[op_idx]
            if requires_reconstruction(op):
                logger.warning(
                    f"Skipping {op['op']} on {op.get('table', 'N/A')} — "
                    f"reconstruction is unsafe on embedded replicas. "
                    f"Run 'declaro migrate-remote' to apply this change to cloud."
                )
            else:
                safe_indices.add(op_idx)
        execution_order = [i for i in execution_order if i in safe_indices]

        if not execution_order:
            logger.info(
                "All pending operations require reconstruction — "
                "skipping auto-migration. Use 'declaro migrate-remote'."
            )
            return {
                "success": True,
                "operations_applied": 0,
                "tables_in_schema": len(target_schema),
                "tables_in_database": len(current_schema),
                "skipped": True,
                "error": None,
            }

    # Pause cloud push during DDL — a push mid-transaction would sync
    # partial state.
    if hasattr(pool, "pause_push"):
        pool.pause_push()

    # Apply safe migrations
    applier = create_applier(dialect)
    try:
        async with _acquire_write_or_read(pool) as conn:
            result = await applier.apply(
                conn, operations, execution_order
            )
    finally:
        if hasattr(pool, "resume_push"):
            pool.resume_push()

    if result["success"]:
        logger.info(f"Successfully applied {result['operations_applied']} migrations")
        if result.get("error"):
            logger.warning(f"Migration completed with warnings: {result['error']}")
        for sql in result["executed_sql"]:
            logger.debug(f"  Executed: {sql}")
        # Store hash after successful migration
        async with _acquire_write_or_read(pool) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": result["operations_applied"],
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema) + result["operations_applied"],
            "skipped": False,
            "error": None,
        }
    else:
        error_msg = result.get("error", "Unknown error")
        logger.error(f"Migration failed: {error_msg}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "skipped": False,
            "error": error_msg,
        }
