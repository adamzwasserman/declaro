"""
Bulk data transfer between databases.

Orchestrates streaming row transfer from a source database to a target,
handling FK ordering, progress tracking, and resumability.

Usage:
    result = await bulk_transfer(
        source, target,
        "sqlite", "turso",
        schema_path="/path/to/models.py",
    )
"""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypedDict

from declaro_persistum.abstractions.enums import is_enum_table
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.bulk_loader import (
    BULK_LOADERS,
    TRANSFER_STATE_TABLE,
    BulkLoader,
)
from declaro_persistum.database import Database, reading
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE, TransferError
from declaro_persistum.inspector import introspect
from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.types import NOTHING, Dialect, Nothing, resolved

logger = logging.getLogger(__name__)

# Internal table used to track transfer progress


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
    """Result of a bulk_transfer(, source=NOTHING, target=NOTHING, source_dialect=NOTHING, target_dialect=NOTHING, schema_path=NOTHING, batch_size=NOTHING, tables=NOTHING, expand_enums=NOTHING, on_progress=NOTHING, resume=NOTHING) call."""

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


@helper
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


@helper
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
    in_degree: dict[str, int] = dict.fromkeys(tables, 0)
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



@boundary
async def _ensure_progress_table(conn: Any, loader: BulkLoader) -> None:
    """Create the transfer progress table if it is not there.

    THE LOADER ANSWERS THIS NOW, AND THE CONNECTION USED TO. All three of
    these functions asked `hasattr(conn, "fetch")` to decide between asyncpg
    and the DB-API drivers, which is a dispatch on type in a package built on
    dispatch tables. The dialect is known at the top of `bulk_transfer`, the
    loader is resolved from it there, and it was passed into this function and
    ignored.

    The sniff was not wrong: asyncpg has `fetch`, aiosqlite and pyturso do
    not, measured on all three. What it cost is that the correlation was
    asserted nowhere, so a driver that grew a `fetch` method would have sent a
    connection down the wrong branch, and every one of these three writes to
    the table that decides whether a resumed transfer skips or repeats work.
    """
    await loader["ensure_progress"](conn)


@helper
async def _get_table_status(
    conn: Any, loader: BulkLoader, table_name: str
) -> dict[str, Any] | None:
    """One table's transfer status, or None if it has none."""
    return await loader["read_progress"](conn, table_name)


@boundary
async def _upsert_progress(
    conn: Any,
    loader: BulkLoader,
    table_name: str,
    status: str,
    rows_transferred: int,
    total_rows: int,
    error: str | Nothing,
) -> None:
    """Record where a table's transfer got to.

    The clock is read here and passed down, so both families stamp the same
    instant rather than each calling `now()` for itself.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    error = resolved(error, None)
    await loader["write_progress"](
        conn,
        table_name,
        status,
        rows_transferred,
        total_rows,
        error,
        datetime.now(UTC).isoformat(),
    )


# ---------------------------------------------------------------------------
# Single table transfer
# ---------------------------------------------------------------------------


@boundary
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
    total_rows = await source_loader["count_rows"](source_conn, table)
    rows_transferred = 0

    progress: TableTransferProgress = {
        "table": table,
        "status": "in_progress",
        "rows_transferred": 0,
        "total_rows": total_rows,
        "error": None,
    }

    await _upsert_progress(
        target_conn, target_loader, table, "in_progress", 0, total_rows, error=NOTHING)

    if on_progress:
        on_progress(progress)

    offset = 0
    while offset < total_rows:
        batch = await source_loader["read_rows"](
            source_conn, table, columns, offset=offset, limit=batch_size
        )
        if not batch:
            break

        await target_loader["load_rows"](target_conn, table, columns, batch)

        # Commit after each batch for resumability. The loader knows whether
        # its engine needs the call; asyncpg owns its own transaction.
        await target_loader["commit"](target_conn)

        rows_transferred += len(batch)
        offset += len(batch)

        await _upsert_progress(
            target_conn, target_loader, table, "in_progress",
            rows_transferred, total_rows, error=NOTHING)

        progress["rows_transferred"] = rows_transferred
        if on_progress:
            on_progress(progress)

    progress["status"] = "completed"
    progress["rows_transferred"] = rows_transferred
    await _upsert_progress(
        target_conn, target_loader, table, "completed",
        rows_transferred, total_rows, error=NOTHING)

    if on_progress:
        on_progress(progress)

    return progress


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


@boundary
async def bulk_transfer(
    source: Database,
    target: Database,
    source_dialect: Dialect,
    target_dialect: Dialect,
    schema_path: str | Path,
    *,
    batch_size: int | Nothing,
    tables: list[str] | Nothing,
    expand_enums: bool | Nothing,
    on_progress: Callable[[TableTransferProgress], None] | Nothing,
    resume: bool | Nothing,
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
        source: Source database
        target: Target database
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    batch_size = resolved(batch_size, 1000)
    expand_enums = resolved(expand_enums, True)
    resume = resolved(resume, True)
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    tables = resolved(tables, None)
    on_progress = resolved(on_progress, None)
    start = datetime.now(UTC)

    # Step 1: Schema migration on target
    logger.info("Applying schema migrations to target database")
    migration_result = await apply_migrations_async(
        target, target_dialect, schema_path, expand_enums=expand_enums, force=NOTHING)
    if not migration_result["success"]:
        raise TransferError(
            f"Schema migration failed on target: {migration_result.get('error')}",
            phase="schema_migration", table=NOTHING, original_error=NOTHING)
    logger.info(
        f"Schema migration complete: {migration_result['operations_applied']} operations applied"
    )

    # Step 2: Introspect source

    async with reading(source) as source_conn:
        source_schema = await introspect(source_conn, source_dialect)

    all_tables = list(source_schema.keys())
    logger.info(f"Source has {len(all_tables)} tables")

    # Step 3: Filter
    if tables is not None:
        # A NAME THAT IS NOT THERE IS A REFUSAL, NOT A FILTER. This was
        # `[t for t in tables if t in source_schema]`, so a table the caller
        # named and the source does not have was dropped: not skipped, not
        # counted, not reported.
        #
        # Measured 2026-08-17, asking for ["users", "orders_typo"] against a
        # source holding only `users`:
        #
        #     success=True transferred=1 skipped=0 failed=0 errors={}
        #
        # `tables=[...]` is the caller stating that these exist and it wants
        # them. Intersecting that with reality turns a typo, a stale deploy
        # script or a half-finished rename into a green result with rows left
        # in the old database.
        #
        # Refused HERE, before anything moves. A partial transfer that then
        # raises is worse than either outcome.
        missing = [t for t in tables if t not in source_schema]
        if missing:
            raise TransferError(
                f"These tables were asked for and the source does not have "
                f"them: {', '.join(sorted(missing))}. The source holds "
                f"{', '.join(sorted(all_tables))}. Nothing has been "
                f"transferred.", phase=NOTHING, table=NOTHING, original_error=NOTHING)
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
    source_loader = BULK_LOADERS[source_dialect]
    target_loader = BULK_LOADERS[target_dialect]

    tables_transferred = 0
    tables_skipped = 0
    tables_failed = 0
    total_rows = 0
    errors: dict[str, str] = {}

    async with reading(source) as source_conn, reading(target) as target_conn:
        # Create progress table
        await _ensure_progress_table(target_conn, target_loader)

        # Disable FK checks if circular refs detected
        if circular_refs:
            # DISABLING IS A PRECONDITION, NOT A COURTESY, so a failure stops
            # the run. This was a warning, and the transfer then proceeded with
            # checks still on: every table in the cycle failed on its first row
            # and each failure was recorded against that table. One failed
            # pragma became N table failures, and the report named N tables
            # that were all fine.
            #
            # The tables in `circular_refs` reference each other, so there is
            # no load order that satisfies the constraints. Carrying on is not
            # a degraded transfer, it is a guaranteed one.
            try:
                await target_loader["disable_fk_checks"](target_conn)
            except Exception as exc:
                raise TransferError(
                    f"Could not disable foreign key checks on the target, and "
                    f"these tables have circular references so no load order "
                    f"satisfies them: {', '.join(sorted(circular_refs))}. "
                    f"Transferring now would fail on the first row of each and "
                    f"report those tables as the problem.", phase=NOTHING, table=NOTHING, original_error=NOTHING) from exc

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
                    status = await _get_table_status(target_conn, target_loader, table)
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

                    # `failed` MEANS WHAT `in_progress` MEANS: PARTLY COPIED.
                    # A table is marked failed after batches have already been
                    # written, so the rows that landed before the failure are
                    # still there. Only `in_progress` used to be cleared, so a
                    # retry collided with its own first attempt and failed
                    # again, for good. Measured 2026-08-18 with three rows and
                    # a colliding row in the target:
                    #
                    #     first run    UNIQUE constraint failed: widgets.id
                    #     status left  ('failed', 0), two rows landed
                    #     the colliding row is removed, and the run resumed
                    #     resumed run  UNIQUE constraint failed: widgets.id
                    #
                    # The operator removed the cause and the retry failed
                    # identically, which made resume useless for the one case
                    # it exists to serve.
                    if status and status["status"] in ("in_progress", "failed"):
                        logger.info(
                            f"Restarting {table}: was {status['status']}, "
                            f"deleting existing rows"
                        )
                        await target_loader["delete_rows"](target_conn, table)
                        await target_loader["commit"](target_conn)

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
                except A_BUG_IN_THIS_PACKAGE:
                    # OURS, NOT THE ENGINE'S, so it must not be filed against
                    # a table. A bug of ours fails on every table, and this
                    # loop turned one defect into one "failure" per table:
                    # measured, a TypeError from `_transfer_table` produced
                    # `tables_failed=4` with none of the four at fault. The
                    # operator reads the report, opens the database, and finds
                    # four healthy tables.
                    raise
                except Exception as e:
                    tables_failed += 1
                    error_msg = f"{type(e).__name__}: {e}"
                    errors[table] = error_msg
                    logger.error(f"Failed to transfer {table}: {error_msg}")

                    # A TABLE THAT FAILED MOVED NO ROWS, which is zero rather
                    # than absent. These two carried `= 0` in the signature, so
                    # every caller that omitted them was recorded as having
                    # moved nothing whether or not that was true.
                    await _upsert_progress(
                        target_conn, target_loader, table, "failed",
                        rows_transferred=0, total_rows=0, error=error_msg)
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
                    await target_loader["enable_fk_checks"](target_conn)
                except Exception as exc:
                    # TEARDOWN, so this does NOT raise: it runs in a `finally`
                    # and would replace whatever failure is already in flight
                    # with its own.
                    #
                    # It is an ERROR rather than a warning because of what it
                    # leaves behind. The connection is still enforcing nothing,
                    # so a caller that reuses it writes against unchecked
                    # constraints and never knows. That is worth waking someone
                    # for even though the transfer itself succeeded.
                    logger.error(
                        "Could not re-enable foreign key checks on the target "
                        "after the transfer: %s. The connection is left NOT "
                        "enforcing them, so anything written on it from here "
                        "is unchecked.",
                        exc,
                        exc_info=True,
                    )

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
