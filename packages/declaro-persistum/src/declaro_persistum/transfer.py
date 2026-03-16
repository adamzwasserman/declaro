"""
Bulk data transfer between databases.

Orchestrates streaming row transfer from a source database to a target,
handling FK ordering, progress tracking, and resumability.

Usage:
    result = await bulk_transfer(
        source_pool, target_pool,
        "sqlite", "turso",
        schema_path="/path/to/models.py",
    )
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal, TypedDict, Union

from declaro_persistum.abstractions.enums import is_enum_table
from declaro_persistum.bulk_loader import BulkLoader, create_bulk_loader
from declaro_persistum.exceptions import TransferError
from declaro_persistum.inspector.protocol import create_inspector
from declaro_persistum.migrations import apply_migrations_async

logger = logging.getLogger(__name__)

# Internal table used to track transfer progress
TRANSFER_STATE_TABLE = "_dp_transfer_state"


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class TableTransferProgress(TypedDict):
    """Progress update for a single table transfer."""

    table: str
    status: Literal["pending", "in_progress", "completed", "failed", "skipped"]
    rows_transferred: int
    total_rows: int
    error: str | None


class BulkTransferResult(TypedDict):
    """Result of a bulk_transfer() call."""

    success: bool
    tables_transferred: int
    tables_skipped: int
    tables_failed: int
    total_rows: int
    errors: dict[str, str]
    duration_seconds: float


# ---------------------------------------------------------------------------
# FK graph + toposort
# ---------------------------------------------------------------------------


def _build_table_fk_graph(
    schema: dict[str, Any],
    tables: list[str],
) -> dict[str, set[str]]:
    """
    Build a FK dependency graph from schema column references.

    Returns:
        Dict mapping table_name -> set of tables it depends on (parents).
    """
    table_set = set(tables)
    graph: dict[str, set[str]] = {t: set() for t in tables}

    for table_name in tables:
        table_def = schema.get(table_name, {})
        columns = table_def.get("columns", {})
        for col_def in columns.values():
            ref = col_def.get("references")
            if ref and "." in ref:
                parent_table = ref.split(".")[0]
                if parent_table in table_set and parent_table != table_name:
                    graph[table_name].add(parent_table)

        # Also check foreign_keys list (introspected schema format)
        for fk in table_def.get("foreign_keys", []):
            parent_table = fk.get("references_table", "")
            if parent_table in table_set and parent_table != table_name:
                graph[table_name].add(parent_table)

    return graph


def _toposort_tables(
    schema: dict[str, Any],
    tables: list[str],
) -> tuple[list[str], list[str]]:
    """
    Topologically sort tables so parents are loaded before children.

    Uses Kahn's algorithm. If cycles are detected, the remaining
    tables are returned as circular_refs.

    Returns:
        (sorted_tables, circular_refs)
    """
    graph = _build_table_fk_graph(schema, tables)

    # Compute in-degree
    in_degree: dict[str, int] = {t: 0 for t in tables}
    for deps in graph.values():
        for dep in deps:
            if dep in in_degree:
                in_degree[dep] += 1  # dep is depended ON, but that's reverse
    # Actually: in_degree should count how many parents each table has
    in_degree = {t: len(deps) for t, deps in graph.items()}

    queue: deque[str] = deque(t for t, d in in_degree.items() if d == 0)
    sorted_tables: list[str] = []

    while queue:
        table = queue.popleft()
        sorted_tables.append(table)
        # Find tables that depend on this one (children)
        for t, deps in graph.items():
            if table in deps:
                in_degree[t] -= 1
                if in_degree[t] == 0:
                    queue.append(t)

    circular_refs = [t for t in tables if t not in sorted_tables]
    return sorted_tables, circular_refs


# ---------------------------------------------------------------------------
# Progress table management
# ---------------------------------------------------------------------------

_CREATE_PROGRESS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{TRANSFER_STATE_TABLE}" (
    table_name TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    rows_transferred INTEGER NOT NULL DEFAULT 0,
    total_rows INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    error TEXT
)
"""


async def _ensure_progress_table(conn: Any, loader: BulkLoader) -> None:
    """Create the transfer progress table if it doesn't exist."""
    # Detect connection type by checking for asyncpg-style execute
    if hasattr(conn, "fetch"):
        # asyncpg
        await conn.execute(_CREATE_PROGRESS_TABLE_SQL)
    else:
        # DB-API 2.0 style (aiosqlite, turso)
        await conn.execute(_CREATE_PROGRESS_TABLE_SQL, ())
        await conn.commit()


async def _get_table_status(
    conn: Any, table_name: str
) -> dict[str, Any] | None:
    """Get transfer status for a table. Returns None if no record."""
    if hasattr(conn, "fetch"):
        # asyncpg
        row = await conn.fetchrow(
            f'SELECT * FROM "{TRANSFER_STATE_TABLE}" WHERE table_name = $1',
            table_name,
        )
        if row:
            return dict(row)
    else:
        cursor = await conn.execute(
            f'SELECT table_name, status, rows_transferred, total_rows, '
            f'started_at, completed_at, error FROM "{TRANSFER_STATE_TABLE}" '
            f'WHERE table_name = ?',
            (table_name,),
        )
        row = await cursor.fetchone()
        if row:
            return {
                "table_name": row[0],
                "status": row[1],
                "rows_transferred": row[2],
                "total_rows": row[3],
                "started_at": row[4],
                "completed_at": row[5],
                "error": row[6],
            }
    return None


async def _upsert_progress(
    conn: Any,
    table_name: str,
    status: str,
    rows_transferred: int = 0,
    total_rows: int = 0,
    error: str | None = None,
) -> None:
    """Insert or update transfer progress for a table."""
    now = datetime.now(UTC).isoformat()

    if hasattr(conn, "fetch"):
        # asyncpg - use INSERT ON CONFLICT
        await conn.execute(
            f'INSERT INTO "{TRANSFER_STATE_TABLE}" '
            f"(table_name, status, rows_transferred, total_rows, started_at, error) "
            f"VALUES ($1, $2, $3, $4, $5, $6) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"status = $2, rows_transferred = $3, total_rows = $4, error = $6"
            + (", completed_at = $5" if status in ("completed", "failed") else ""),
            table_name, status, rows_transferred, total_rows, now, error,
        )
    else:
        await conn.execute(
            f'INSERT INTO "{TRANSFER_STATE_TABLE}" '
            f"(table_name, status, rows_transferred, total_rows, started_at, error) "
            f"VALUES (?, ?, ?, ?, ?, ?) "
            f"ON CONFLICT (table_name) DO UPDATE SET "
            f"status = excluded.status, rows_transferred = excluded.rows_transferred, "
            f"total_rows = excluded.total_rows, error = excluded.error"
            + (", completed_at = excluded.started_at" if status in ("completed", "failed") else ""),
            (table_name, status, rows_transferred, total_rows, now, error),
        )
        await conn.commit()


# ---------------------------------------------------------------------------
# Single table transfer
# ---------------------------------------------------------------------------


async def _transfer_table(
    source_conn: Any,
    target_conn: Any,
    source_loader: BulkLoader,
    target_loader: BulkLoader,
    table: str,
    columns: list[str],
    batch_size: int,
    on_progress: Callable[[TableTransferProgress], None] | None,
) -> TableTransferProgress:
    """
    Stream batches from source to target for a single table.

    Returns progress dict with final status.
    """
    total_rows = await source_loader.count_rows(source_conn, table)
    rows_transferred = 0

    progress: TableTransferProgress = {
        "table": table,
        "status": "in_progress",
        "rows_transferred": 0,
        "total_rows": total_rows,
        "error": None,
    }

    await _upsert_progress(target_conn, table, "in_progress", 0, total_rows)

    if on_progress:
        on_progress(progress)

    offset = 0
    while offset < total_rows:
        batch = await source_loader.read_rows(
            source_conn, table, columns, offset=offset, limit=batch_size
        )
        if not batch:
            break

        await target_loader.load_rows(target_conn, table, columns, batch)

        # Commit after each batch for resumability
        if hasattr(target_conn, "commit"):
            await target_conn.commit()

        rows_transferred += len(batch)
        offset += len(batch)

        await _upsert_progress(
            target_conn, table, "in_progress", rows_transferred, total_rows
        )

        progress["rows_transferred"] = rows_transferred
        if on_progress:
            on_progress(progress)

    progress["status"] = "completed"
    progress["rows_transferred"] = rows_transferred
    await _upsert_progress(
        target_conn, table, "completed", rows_transferred, total_rows
    )

    if on_progress:
        on_progress(progress)

    return progress


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def bulk_transfer(
    source_pool: Any,
    target_pool: Any,
    source_dialect: str,
    target_dialect: str,
    schema_path: Union[str, Path],
    *,
    batch_size: int = 1000,
    tables: list[str] | None = None,
    expand_enums: bool = True,
    on_progress: Callable[[TableTransferProgress], None] | None = None,
    resume: bool = True,
) -> BulkTransferResult:
    """
    Transfer data from source database to target database.

    Steps:
        1. Apply schema migrations to target
        2. Introspect source for table names + FK graph
        3. Filter out enum tables and progress table
        4. FK toposort (parents first)
        5. Create progress table in target
        6. Stream each table in FK order with batch commits

    Args:
        source_pool: Source database connection pool
        target_pool: Target database connection pool
        source_dialect: Source database dialect
        target_dialect: Target database dialect
        schema_path: Path to Python module with Pydantic models
        batch_size: Rows per batch (default 1000)
        tables: Specific tables to transfer (None = all user tables)
        expand_enums: Expand Literal types to enum lookup tables
        on_progress: Callback for per-table progress updates
        resume: Skip completed tables, restart in_progress ones

    Returns:
        BulkTransferResult with transfer statistics

    Raises:
        TransferError: If schema migration or critical setup fails
    """
    start = datetime.now(UTC)

    # Step 1: Schema migration on target
    logger.info("Applying schema migrations to target database")
    migration_result = await apply_migrations_async(
        target_pool, target_dialect, schema_path, expand_enums=expand_enums
    )
    if not migration_result["success"]:
        raise TransferError(
            f"Schema migration failed on target: {migration_result.get('error')}",
            phase="schema_migration",
        )
    logger.info(
        f"Schema migration complete: {migration_result['operations_applied']} operations applied"
    )

    # Step 2: Introspect source
    inspector = create_inspector(source_dialect)
    async with source_pool.acquire() as source_conn:
        source_schema = await inspector.introspect(source_conn)

    all_tables = list(source_schema.keys())
    logger.info(f"Source has {len(all_tables)} tables")

    # Step 3: Filter
    if tables is not None:
        transfer_tables = [t for t in tables if t in source_schema]
    else:
        transfer_tables = [
            t for t in all_tables
            if not is_enum_table(t) and t != TRANSFER_STATE_TABLE
        ]

    logger.info(f"Transferring {len(transfer_tables)} tables (filtered from {len(all_tables)})")

    # Step 4: FK toposort
    sorted_tables, circular_refs = _toposort_tables(source_schema, transfer_tables)
    if circular_refs:
        logger.warning(
            f"Circular FK references detected: {circular_refs}. "
            f"FK checks will be disabled during transfer."
        )
        # Add circular tables to the end
        sorted_tables.extend(circular_refs)

    logger.info(f"Transfer order: {sorted_tables}")

    # Step 5+6: Transfer tables
    source_loader = create_bulk_loader(source_dialect)
    target_loader = create_bulk_loader(target_dialect)

    tables_transferred = 0
    tables_skipped = 0
    tables_failed = 0
    total_rows = 0
    errors: dict[str, str] = {}

    async with source_pool.acquire() as source_conn:
        async with target_pool.acquire() as target_conn:
            # Create progress table
            await _ensure_progress_table(target_conn, target_loader)

            # Disable FK checks if circular refs detected
            if circular_refs:
                try:
                    await target_loader.disable_fk_checks(target_conn)
                except Exception as e:
                    logger.warning(f"Could not disable FK checks: {e}")

            try:
                for table in sorted_tables:
                    table_def = source_schema.get(table, {})
                    columns = list(table_def.get("columns", {}).keys())
                    if not columns:
                        logger.warning(f"Skipping {table}: no columns found")
                        tables_skipped += 1
                        continue

                    # Check resume status
                    if resume:
                        status = await _get_table_status(target_conn, table)
                        if status and status["status"] == "completed":
                            logger.info(f"Skipping {table}: already completed")
                            tables_skipped += 1
                            total_rows += status.get("rows_transferred", 0)
                            if on_progress:
                                on_progress({
                                    "table": table,
                                    "status": "skipped",
                                    "rows_transferred": status.get("rows_transferred", 0),
                                    "total_rows": status.get("total_rows", 0),
                                    "error": None,
                                })
                            continue

                        if status and status["status"] == "in_progress":
                            logger.info(
                                f"Restarting {table}: was in_progress, "
                                f"deleting existing rows"
                            )
                            await target_loader.delete_rows(target_conn, table)
                            if hasattr(target_conn, "commit"):
                                await target_conn.commit()

                    # Transfer the table
                    try:
                        progress = await _transfer_table(
                            source_conn,
                            target_conn,
                            source_loader,
                            target_loader,
                            table,
                            columns,
                            batch_size,
                            on_progress,
                        )
                        tables_transferred += 1
                        total_rows += progress["rows_transferred"]
                        logger.info(
                            f"Transferred {table}: "
                            f"{progress['rows_transferred']}/{progress['total_rows']} rows"
                        )
                    except Exception as e:
                        tables_failed += 1
                        error_msg = f"{type(e).__name__}: {e}"
                        errors[table] = error_msg
                        logger.error(f"Failed to transfer {table}: {error_msg}")

                        await _upsert_progress(
                            target_conn, table, "failed", error=error_msg
                        )
                        if on_progress:
                            on_progress({
                                "table": table,
                                "status": "failed",
                                "rows_transferred": 0,
                                "total_rows": 0,
                                "error": error_msg,
                            })
            finally:
                # Re-enable FK checks
                if circular_refs:
                    try:
                        await target_loader.enable_fk_checks(target_conn)
                    except Exception as e:
                        logger.warning(f"Could not re-enable FK checks: {e}")

    duration = (datetime.now(UTC) - start).total_seconds()

    result: BulkTransferResult = {
        "success": tables_failed == 0,
        "tables_transferred": tables_transferred,
        "tables_skipped": tables_skipped,
        "tables_failed": tables_failed,
        "total_rows": total_rows,
        "errors": errors,
        "duration_seconds": duration,
    }

    logger.info(
        f"Transfer complete: {tables_transferred} transferred, "
        f"{tables_skipped} skipped, {tables_failed} failed, "
        f"{total_rows} total rows in {duration:.1f}s"
    )

    return result
