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

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

from declaro_persistum.boundary import helper
from declaro_persistum.bulk_loader import (
    BULK_LOADERS,
    BulkLoader,
)

# The tables and what each entry does. Re-exported so nothing that
# imports from here had to change.
from declaro_persistum.bulk_loader import (  # noqa: F401
    TRANSFER_STATE_TABLE as TRANSFER_STATE_TABLE,
)
from declaro_persistum.database import Database, reading
from declaro_persistum.inspector import introspect
from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.transfer_steps import (
    _MORE_ROWS as _MORE_ROWS,
)
from declaro_persistum.transfer_steps import (
    _MORE_TO_PLACE as _MORE_TO_PLACE,
)
from declaro_persistum.transfer_steps import (
    _REPORTING as _REPORTING,
)
from declaro_persistum.transfer_steps import (
    _RESUME_ACTION as _RESUME_ACTION,
)
from declaro_persistum.transfer_steps import (
    _RESUME_MEANING as _RESUME_MEANING,
)
from declaro_persistum.transfer_steps import (
    _TRANSFER_ORDER as _TRANSFER_ORDER,
)
from declaro_persistum.transfer_steps import (
    _WHEN_A_NAMED_TABLE_IS_MISSING as _WHEN_A_NAMED_TABLE_IS_MISSING,
)
from declaro_persistum.transfer_steps import (
    _WHEN_RESUMING as _WHEN_RESUMING,
)
from declaro_persistum.transfer_steps import (
    _WHEN_THE_CHECKS_WERE_TURNED_OFF as _WHEN_THE_CHECKS_WERE_TURNED_OFF,
)
from declaro_persistum.transfer_steps import (
    _WHEN_THE_TABLE_HAS_COLUMNS as _WHEN_THE_TABLE_HAS_COLUMNS,
)
from declaro_persistum.transfer_steps import (
    _WHEN_THE_TARGET_MIGRATED as _WHEN_THE_TARGET_MIGRATED,
)
from declaro_persistum.transfer_steps import (
    _WHEN_THERE_ARE_CYCLES as _WHEN_THERE_ARE_CYCLES,
)
from declaro_persistum.transfer_steps import (
    _WHICH_TABLES as _WHICH_TABLES,
)
from declaro_persistum.transfer_steps import (  # noqa: F401
    TableTransferProgress as TableTransferProgress,
)
from declaro_persistum.transfer_steps import (
    _already_transferred as _already_transferred,
)
from declaro_persistum.transfer_steps import (
    _clear_and_transfer as _clear_and_transfer,
)
from declaro_persistum.transfer_steps import (
    _every_table_the_caller_named as _every_table_the_caller_named,
)
from declaro_persistum.transfer_steps import (
    _every_user_table as _every_user_table,
)
from declaro_persistum.transfer_steps import (
    _from_the_beginning as _from_the_beginning,
)
from declaro_persistum.transfer_steps import (
    _from_where_it_got_to as _from_where_it_got_to,
)
from declaro_persistum.transfer_steps import (
    _get_table_status as _get_table_status,
)
from declaro_persistum.transfer_steps import (
    _how_a_recorded_table_stands as _how_a_recorded_table_stands,
)
from declaro_persistum.transfer_steps import (
    _leave_fk_checks_on as _leave_fk_checks_on,
)
from declaro_persistum.transfer_steps import (
    _leave_the_checks_as_they_are as _leave_the_checks_as_they_are,
)
from declaro_persistum.transfer_steps import (
    _next_batch as _next_batch,
)
from declaro_persistum.transfer_steps import (
    _nothing_left_to_read as _nothing_left_to_read,
)
from declaro_persistum.transfer_steps import (
    _nothing_more_to_place as _nothing_more_to_place,
)
from declaro_persistum.transfer_steps import (
    _place_the_next_layer as _place_the_next_layer,
)
from declaro_persistum.transfer_steps import (
    _read_one_batch as _read_one_batch,
)
from declaro_persistum.transfer_steps import (
    _record_a_failed_table as _record_a_failed_table,
)
from declaro_persistum.transfer_steps import (
    _refuse_a_failed_migration as _refuse_a_failed_migration,
)
from declaro_persistum.transfer_steps import (
    _refuse_the_missing_tables as _refuse_the_missing_tables,
)
from declaro_persistum.transfer_steps import (
    _report as _report,
)
from declaro_persistum.transfer_steps import (
    _Run as _Run,
)
from declaro_persistum.transfer_steps import (
    _skip_a_table_with_no_columns as _skip_a_table_with_no_columns,
)
from declaro_persistum.transfer_steps import (
    _stream_the_table as _stream_the_table,
)
from declaro_persistum.transfer_steps import (
    _TableOutcome as _TableOutcome,
)
from declaro_persistum.transfer_steps import (
    _tell_nobody as _tell_nobody,
)
from declaro_persistum.transfer_steps import (
    _tell_the_caller as _tell_the_caller,
)
from declaro_persistum.transfer_steps import (
    _the_order_the_toposort_gave as _the_order_the_toposort_gave,
)
from declaro_persistum.transfer_steps import (
    _the_tables_the_caller_named as _the_tables_the_caller_named,
)
from declaro_persistum.transfer_steps import (
    _the_target_is_ready as _the_target_is_ready,
)
from declaro_persistum.transfer_steps import (
    _transfer_from_scratch as _transfer_from_scratch,
)
from declaro_persistum.transfer_steps import (
    _transfer_or_resume as _transfer_or_resume,
)
from declaro_persistum.transfer_steps import (
    _transfer_table as _transfer_table,
)
from declaro_persistum.transfer_steps import (
    _turn_fk_checks_back_on as _turn_fk_checks_back_on,
)
from declaro_persistum.transfer_steps import (
    _turn_fk_checks_off as _turn_fk_checks_off,
)
from declaro_persistum.transfer_steps import (
    _upsert_progress as _upsert_progress,
)
from declaro_persistum.transfer_steps import (
    _warn_and_append_the_cycle as _warn_and_append_the_cycle,
)
from declaro_persistum.transfer_steps import (
    logger as logger,
)
from declaro_persistum.types import NOTHING, Dialect, Nothing, Schema, Table, resolved


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
def _parents_named_by(table_def: Table) -> list[str]:
    """Every table this one names as a parent, in either schema shape.

    A declared schema puts the parent in a column's `references`, written
    `parent.column`. An introspected one puts it in a `foreign_keys` list,
    under `references_table`. Both are read, and the caller decides which of
    the names are in the transfer set.
    """
    referenced = [
        col_def.get("references")
        for col_def in table_def.get("columns", {}).values()
    ]
    from_columns = [ref.split(".")[0] for ref in referenced if ref and "." in ref]
    from_keys = [
        fk.get("references_table", "") for fk in table_def.get("foreign_keys", [])
    ]
    return from_columns + from_keys


@helper
def _build_table_fk_graph(
    schema: Schema,
    tables: list[str],
) -> dict[str, set[str]]:
    """
    Build a FK dependency graph from schema column references.

    Returns:
        Dict mapping table_name -> set of tables it depends on (parents).
    """
    table_set = set(tables)
    return {
        table_name: {
            parent
            for parent in _parents_named_by(schema.get(table_name, {}))
            if parent in table_set and parent != table_name
        }
        for table_name in tables
    }


@helper
def _toposort_tables(
    schema: Schema,
    tables: list[str],
) -> tuple[list[str], list[str]]:
    """
    Topologically sort tables so parents are loaded before children.

    Parents are placed before children, a layer at a time. Tables left over
    reference each other, and they are returned as circular_refs rather than
    raised: bulk transfer loads what it can and reports the rest.

    Returns:
        (sorted_tables, circular_refs)
    """
    graph = _build_table_fk_graph(schema, tables)
    sorted_tables = _place_the_next_layer(graph, [])
    return sorted_tables, [t for t in tables if t not in sorted_tables]


# ---------------------------------------------------------------------------
# Progress table management
# ---------------------------------------------------------------------------



@helper
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
async def _transfer_one_table(
    run: _Run, source_schema: Schema, table: str
) -> _TableOutcome:
    """Everything one table goes through, from its columns to its rows."""
    columns = list(source_schema.get(table, {}).get("columns", {}).keys())
    return await _WHEN_THE_TABLE_HAS_COLUMNS[bool(columns)](run, table, columns)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


@helper
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
    the_tables = resolved(tables, None)
    the_on_progress = resolved(on_progress, None)
    start = datetime.now(UTC)

    # Step 1: Schema migration on target
    logger.info("Applying schema migrations to target database")
    migration_result = await apply_migrations_async(
        target, target_dialect, schema_path, expand_enums=expand_enums, force=NOTHING)
    _WHEN_THE_TARGET_MIGRATED[migration_result["success"]](migration_result)
    logger.info(
        f"Schema migration complete: {migration_result['operations_applied']} operations applied"
    )

    # Step 2: Introspect source
    async with reading(source) as source_conn:
        source_schema = await introspect(source_conn, source_dialect)

    all_tables = list(source_schema.keys())
    logger.info(f"Source has {len(all_tables)} tables")

    # Step 3: Filter
    transfer_tables = _WHICH_TABLES[the_tables is not None](
        the_tables, all_tables, source_schema
    )

    logger.info(f"Transferring {len(transfer_tables)} tables (filtered from {len(all_tables)})")

    # Step 4: FK toposort
    sorted_tables, circular_refs = _toposort_tables(source_schema, transfer_tables)
    transfer_order = _TRANSFER_ORDER[bool(circular_refs)](sorted_tables, circular_refs)

    logger.info(f"Transfer order: {transfer_order}")

    # Step 5+6: Transfer tables
    source_loader = BULK_LOADERS[source_dialect]
    target_loader = BULK_LOADERS[target_dialect]

    async with reading(source) as source_conn, reading(target) as target_conn:
        run: _Run = {
            "source_conn": source_conn,
            "target_conn": target_conn,
            "source_loader": source_loader,
            "target_loader": target_loader,
            "batch_size": batch_size,
            "on_progress": the_on_progress,
            "resume": resume,
        }

        # Create progress table
        await _ensure_progress_table(target_conn, target_loader)

        # Disable FK checks when the schema has a cycle in it.
        await _WHEN_THERE_ARE_CYCLES[bool(circular_refs)](
            target_loader, target_conn, circular_refs
        )

        try:
            outcomes = [
                await _transfer_one_table(run, source_schema, table)
                for table in transfer_order
            ]
        finally:
            # Re-enable FK checks
            await _WHEN_THE_CHECKS_WERE_TURNED_OFF[bool(circular_refs)](
                target_loader, target_conn
            )

    duration = (datetime.now(UTC) - start).total_seconds()

    transferred = [one for one in outcomes if one["outcome"] == "transferred"]
    skipped = [one for one in outcomes if one["outcome"] == "skipped"]
    failed = [one for one in outcomes if one["outcome"] == "failed"]

    result: BulkTransferResult = {
        "success": not failed,
        "tables_transferred": len(transferred),
        "tables_skipped": len(skipped),
        "tables_failed": len(failed),
        "total_rows": sum(one["rows"] for one in outcomes),
        "errors": {one["table"]: cast(str, one["error"]) for one in failed},
        "duration_seconds": duration,
    }

    logger.info(
        f"Transfer complete: {len(transferred)} transferred, "
        f"{len(skipped)} skipped, {len(failed)} failed, "
        f"{result['total_rows']} total rows in {duration:.1f}s"
    )

    return result
