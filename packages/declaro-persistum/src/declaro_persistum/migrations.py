
from declaro_persistum.types import Dialect

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
    await apply_migrations_async(db, dialect, schema_path)
"""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier.protocol import create_applier
from declaro_persistum.database import (
    Database,
    is_replicated,
    reading,
    refresh,
)
from declaro_persistum.differ import diff, diff_views
from declaro_persistum.inspector import introspect_with_views
from declaro_persistum.pydantic_loader import (
    load_declarations,
    load_models_from_module,
)

META_TABLE = "_declaro_meta"

logger = logging.getLogger(__name__)


def _for_ddl(db: Database) -> Any:
    """The database's own DDL door. Never a read, and not always a write.

    This called `writing(db)` for every backend, which is right for three of
    the four cases and silently wrong for the fourth. On a LOCAL Turso
    database `writing` is an MVCC connection, and a table created on one is
    invisible to any other connection that has already read — the migration
    reports success and a live reader goes on seeing the old schema.

    Measured 2026-08-14, pyturso 0.7.2, the other connection open throughout:

        A writes, then DDL                  the other connection is fine
        the other connection READ first     "Parse error: no such table: t"
        ALTER instead of CREATE             fine
        the other connection WROTE first    fine

    A prior read is the trigger, and that row fails every run.

    THE FIX IS NOT A BRANCH HERE. On a replicated database `writing` is the
    only correct answer: the held connection is the one bound to the primary,
    and `migrating` would open a fresh non-sync connection whose DDL never
    leaves the machine. PostgreSQL and SQLite want `writing` too. So the door
    is a property of the database, settled at open, and this function asks the
    value rather than deciding for it.

    On a local Turso database the door is `migrating`, which is WAL and needs
    the file to itself. If another connection is holding a read, it raises
    "database is locked" instead of migrating. That is the point: a loud,
    true failure in place of a silent one, and it is what `crew.py` already
    requires when it says migration must finish before a crew starts.
    """
    return db["for_ddl"](db)


# ---------------------------------------------------------------------------
# Schema hash helpers (skip-if-clean)
# ---------------------------------------------------------------------------


def _compute_schema_hash(schema_path: Path, version: str) -> str:
    """Compute SHA-256 hex digest of (schema file bytes, NUL, version) — pure.

    ``version`` is the declaro_persistum version string the caller wants
    mixed into the hash so that any version bump invalidates the
    skip-if-clean cache. Passing it explicitly (rather than reading the
    module-level constant inside this function) keeps the hash a pure
    function of its arguments — testable with explicit version values, no
    monkeypatching of module globals required.

    Mixing the version in is what makes loader/applier fixes propagate to
    existing deployments: a buggy version's stored "clean" hash differs
    from the new version's computed hash, so the runner performs a fresh
    introspection on first startup after upgrade.
    """
    h = hashlib.sha256()
    h.update(schema_path.read_bytes())
    h.update(b"\x00")  # delimiter so file content cannot collide with version
    h.update(version.encode("ascii"))
    return h.hexdigest()


def _dialect_needs_orphan_recovery(dialect: Dialect) -> bool:
    """True iff this dialect uses the sqlite_master-based temp-table scheme
    that `_recover_orphaned_tmp_tables` was written for.

    Postgres reconstruction does not produce ``_declaro_tmp_*`` tables and
    has no ``sqlite_master`` system table, so running the recovery scan on
    a Postgres database crashes startup with UndefinedTableError.
    """
    return dialect in ("sqlite", "turso")


def partition_replica_operations(
    operations: list[dict[str, Any]], execution_order: list[int]
) -> tuple[list[int], list[dict[str, Any]]]:
    """Partition migration ops for an embedded replica.

    Reconstruction ops (add/drop FK, alter column) can't replicate through the
    replication engine, so they are deferred to ``declaro migrate-remote``. Returns
    ``(safe_order, skipped)`` where ``safe_order`` is ``execution_order`` filtered
    to safe ops, and ``skipped`` describes each deferred op as ``{"op", "table"}``
    so callers can see what was NOT applied instead of mistaking a no-op for
    success.
    """
    from declaro_persistum.applier.shared import requires_reconstruction

    safe_order: list[int] = []
    skipped: list[dict[str, Any]] = []
    for op_idx in execution_order:
        op = operations[op_idx]
        if requires_reconstruction(op):
            skipped.append({"op": op["op"], "table": op.get("table")})
        else:
            safe_order.append(op_idx)
    return safe_order, skipped


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


def _stamp_key(schema_name: str, schema_hash: str) -> str:
    """Build the skip-if-clean stamp key — pure.

    The hash is part of the key, not only the value. The key used to be the
    schema file's name alone, so two services migrating one database wrote
    the same row. They disagree whenever their hashes differ — different
    models files sharing a filename, or different library versions, since the
    version is mixed into the hash on purpose. Each then read the other's
    stamp, saw a mismatch, re-introspected and re-stamped, forever. Neither
    was wrong and neither could win.

    With the hash in the key, each distinct (schema, version) records its own
    row and no service can evict another's. Two services applying genuinely
    the same schema on the same version still share one row, which is
    correct: they are recording the same fact.

    Rows for superseded hashes are left behind rather than pruned. Pruning by
    schema name is what caused the collision — it would delete the other
    service's stamp. The cost is a few rows in a metadata table.
    """
    return f"schema_hash:{schema_name}:{schema_hash}"


async def _get_stored_hash(conn: Any, schema_name: str, schema_hash: str) -> str | None:
    """Return the stored stamp for this exact schema and version, else None."""
    key = _stamp_key(schema_name, schema_hash)
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
    key = _stamp_key(schema_name, schema_hash)
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

    NOTHING IS CAUGHT HERE ANY MORE. Two `except Exception: return False`
    blocks used to wrap this, and "return False" means "the schema is dirty,
    go and migrate". So the answer to "is the schema clean?" was False in
    three unrelated situations that need three different responses:

        the stored hash differs          -> migrate, correct
        the database would not answer    -> migrated against a broken
                                            connection, one layer down
        the schema module will not load  -> reported as dirty, then the
                                            migration re-imports the same
                                            broken module and fails anyway,
                                            with the real ImportError two
                                            call frames from where it was
                                            first seen and swallowed

    The outer block also carried the comment "Meta table doesn't exist", but
    `_ensure_meta_table` is the first thing called and it is CREATE TABLE IF
    NOT EXISTS. Absence was already handled; the catch only hid everything
    else.

    A question this function cannot answer is not a "no". It raises, and the
    caller at `apply_migrations_async` sees the real error at the point it
    happened.
    """
    await _ensure_meta_table(conn)
    stored = await _get_stored_hash(conn, schema_path.name, schema_hash)
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
    target = load_models_from_module(schema_path)
    return len(target) == 0


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


async def _recover_orphaned_tmp_tables(db: Database) -> int:
    """Detect and recover orphaned temp tables from failed reconstruction.

    Reconstruction uses temp tables named ``_declaro_tmp_<table>_<8hex>``.  If
    it partially committed (CREATE tmp + DROP original) but failed before
    RENAME, the database has the tmp table and not the original.  This renames
    them back.

    Only that prefix is matched.  It is a name persistum owns, so anything
    carrying it was created here and is ours to move.

    Returns the number of tables recovered.
    """
    async with _for_ddl(db) as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name LIKE '_declaro_tmp_%'"
        )
        tmp_tables = await cursor.fetchall()

        recovered = 0
        for row in tmp_tables:
            tmp_name = row[0]

            if tmp_name.startswith("_declaro_tmp_"):
                # Strip prefix and UUID suffix: _declaro_tmp_<table>_<8hex>
                remainder = tmp_name[len("_declaro_tmp_"):]
                # The last 9 chars are _{8hex} — strip them
                if len(remainder) > 9 and remainder[-9] == "_":
                    original_name = remainder[:-9]
                else:
                    original_name = remainder
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
            except Exception as exc:
                # The meta table legitimately does not exist on a fresh
                # database. Anything else here means the stale schema hash
                # survived, so the next start will believe the schema is
                # current when it is not -- worth seeing rather than hiding.
                logger.warning(
                    "Could not clear the schema hash after recovering %d "
                    "orphaned table(s): %s. If the meta table exists, the next "
                    "start may skip a migration it should have run.",
                    recovered, exc,
                )

    return recovered


async def apply_migrations_async(
    db: Database,
    dialect: Dialect,
    schema_path: str | Path,
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
        db: The database
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

    # Everything below reads the database to decide what to change: the
    # A migration diffs the live schema against the target, so the local copy
    # must be current first. A warm open does not replicate, so without this the
    # diff is computed against whatever the copy held when it was last touched
    # and emits DDL for a schema that has moved.
    await refresh(db)

    # Pre-flight: recover orphaned _new tables from failed reconstruction.
    # Decision lives in the pure helper so it can be tested without mocks.
    if _dialect_needs_orphan_recovery(dialect):
        await _recover_orphaned_tmp_tables(db)

    # Skip-if-clean: compare schema file hash with stored hash.
    # Version is passed explicitly so _compute_schema_hash stays a pure
    # function of its arguments (Honest Code: configuration as parameters).
    from declaro_persistum import __version__

    schema_hash = _compute_schema_hash(schema_path, __version__)

    if not force:
        async with reading(db) as conn:
            if await _schema_is_clean(conn, schema_path, schema_hash):
                logger.info("Schema unchanged (hash match) — skipping migration")
                return {
                    "success": True,
                    "operations_applied": 0,
                    "tables_in_schema": 0,
                    "tables_in_database": 0,
                    "skipped": True,
                    "error": None,
                }

    logger.info(f"Loading schema from {schema_path}")
    target_schema, target_views = load_declarations(schema_path)

    if not target_schema:
        logger.warning("No tables found in schema models")
        # Store hash so next call skips
        async with _for_ddl(db) as conn:
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

    # Introspect current database state, VIEWS INCLUDED. Asking without them
    # is what made views unreachable: the inspectors, the differ and the
    # appliers all handled views, and the apply path never carried one.
    async with reading(db) as conn:
        current_schema, current_views = await introspect_with_views(conn, dialect)

    logger.info(
        f"Introspected {len(current_schema)} tables and "
        f"{len(current_views)} views from database"
    )

    # Compute diff. Tables and views are diffed by different functions because
    # they are different things: a table is reconciled column by column, a view
    # is replaced whole when its query changes.
    diff_result = diff(current_schema, target_schema, dialect=dialect)

    # VIEWS GO LAST, AND THEIR INDICES GO INTO THE ORDER OR NOTHING RUNS THEM.
    # `execution_order` is the differ's topological sort over the TABLE
    # operations, so an operation appended after it is invisible to the
    # applier: the first version of this appended the view ops and not their
    # indices, and the migration reported success having created no view.
    # Appending is also the right order rather than merely a convenient one,
    # since a view selects from tables and must be created after them.
    view_operations = diff_views(current_views, target_views)
    first_view_index = len(diff_result["operations"])
    diff_result["operations"] = [*diff_result["operations"], *view_operations]
    diff_result["execution_order"] = [
        *diff_result["execution_order"],
        *range(first_view_index, first_view_index + len(view_operations)),
    ]

    if not diff_result["operations"]:
        logger.info("Schema is up to date - no migrations needed")
        # Store hash so next call skips introspection
        async with _for_ddl(db) as conn:
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
    # the replication engine can't replicate DDL, and partial sync (DROP reaches
    # cloud but CREATE doesn't) destroys tables on both sides.
    # Use `declaro migrate-remote` for schema changes that need reconstruction.
    is_cloud_replica = is_replicated(db)
    operations = diff_result["operations"]
    execution_order = diff_result["execution_order"]
    skipped_operations: list[dict[str, Any]] = []

    if is_cloud_replica:
        execution_order, skipped_operations = partition_replica_operations(
            operations, execution_order
        )
        for skipped in skipped_operations:
            logger.warning(
                f"Skipping {skipped['op']} on {skipped.get('table', 'N/A')} — "
                f"reconstruction is unsafe on embedded replicas. "
                f"Run 'declaro migrate-remote' to apply this change to cloud."
            )

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
                "skipped_operations": skipped_operations,
                "error": None,
            }

    # No pause. A migration is a writer, and it holds the serialise lock for
    # the whole of `_for_ddl`; opportunistic replication defers to a waiting
    # writer, so nothing can push mid-transaction.
    applier = create_applier(dialect)
    async with _for_ddl(db) as conn:
        result = await applier.apply(conn, operations, execution_order)

    if result["success"]:
        logger.info(f"Successfully applied {result['operations_applied']} migrations")
        if result.get("error"):
            logger.warning(f"Migration completed with warnings: {result['error']}")
        for sql in result["executed_sql"]:
            logger.debug(f"  Executed: {sql}")
        # Store hash after successful migration
        async with _for_ddl(db) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": result["operations_applied"],
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema) + result["operations_applied"],
            "skipped": bool(skipped_operations),
            "skipped_operations": skipped_operations,
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
            "skipped_operations": skipped_operations,
            "error": error_msg,
        }
