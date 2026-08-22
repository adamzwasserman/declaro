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
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypedDict, cast

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
def _parents_named_by(table_def: dict[str, Any]) -> list[str]:
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
    schema: dict[str, Any],
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
def _nothing_more_to_place(
    _graph: dict[str, set[str]], placed: list[str]
) -> list[str]:
    """A pass that placed no table means the rest reference each other."""
    return placed


@helper
def _place_the_next_layer(
    graph: dict[str, set[str]], placed: list[str]
) -> list[str]:
    """Add every table whose parents are all placed, then go round again.

    One call places a whole layer, in the order the caller asked for the
    tables. When a pass finds nothing to place, what is left is a cycle and
    there is no order for it, so the recursion stops there and the caller
    reports those tables rather than sorting them.
    """
    standing = set(placed)
    ready = [
        table
        for table, parents in graph.items()
        if table not in standing and parents <= standing
    ]
    return _MORE_TO_PLACE[bool(ready)](graph, placed + ready)


_MORE_TO_PLACE: dict[bool, Callable[[dict[str, set[str]], list[str]], list[str]]] = {
    True: _place_the_next_layer,
    False: _nothing_more_to_place,
}


@helper
def _toposort_tables(
    schema: dict[str, Any],
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


@helper
def _tell_the_caller(
    on_progress: Callable[[TableTransferProgress], None],
    progress: TableTransferProgress,
) -> TableTransferProgress:
    """A caller asked to be told, so tell it."""
    on_progress(progress)
    return progress


@helper
def _tell_nobody(
    _on_progress: Callable[[TableTransferProgress], None] | None,
    progress: TableTransferProgress,
) -> TableTransferProgress:
    """No caller asked to be told, so the progress goes no further."""
    return progress


_REPORTING: dict[
    bool,
    Callable[
        [Callable[[TableTransferProgress], None] | None, TableTransferProgress],
        TableTransferProgress,
    ],
] = {
    True: _tell_the_caller,
    False: _tell_nobody,
}


@helper
def _report(
    on_progress: Callable[[TableTransferProgress], None] | None,
    progress: TableTransferProgress,
) -> TableTransferProgress:
    """Hand one progress update to the caller's callback, if it gave one.

    Six sites wrote `if on_progress: on_progress(...)`, which is one question
    asked six times about a value that was settled at the top of
    `bulk_transfer`.
    """
    return _REPORTING[on_progress is not None](on_progress, progress)


@helper
async def _nothing_left_to_read(
    _loader: BulkLoader,
    _conn: Any,
    _table: str,
    _columns: list[str],
    _offset: int,
    _batch_size: int,
) -> list[Any]:
    """The offset has reached the row count, so there is nothing to read."""
    return []


@boundary
async def _read_one_batch(
    loader: BulkLoader,
    conn: Any,
    table: str,
    columns: list[str],
    offset: int,
    batch_size: int,
) -> list[Any]:
    """One batch of rows from the source, starting at `offset`."""
    return await loader["read_rows"](
        conn, table, columns, offset=offset, limit=batch_size
    )


_MORE_ROWS: dict[
    bool,
    Callable[[BulkLoader, Any, str, list[str], int, int], Any],
] = {
    True: _read_one_batch,
    False: _nothing_left_to_read,
}


@boundary
async def _next_batch(
    loader: BulkLoader,
    conn: Any,
    table: str,
    columns: list[str],
    offset: int,
    batch_size: int,
    total_rows: int,
) -> list[Any]:
    """The next batch of rows, or an empty one when the table is exhausted.

    An empty batch is also what a source hands back when it has run out
    early, so the loop that reads this stops on the batch rather than on the
    offset, and neither case needs a `break`.
    """
    return await _MORE_ROWS[offset < total_rows](
        loader, conn, table, columns, offset, batch_size
    )


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

    _report(on_progress, progress)

    offset = 0
    batch = await _next_batch(
        source_loader, source_conn, table, columns, offset, batch_size, total_rows
    )
    while batch:
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
        _report(on_progress, progress)

        batch = await _next_batch(
            source_loader, source_conn, table, columns, offset, batch_size, total_rows
        )

    progress["status"] = "completed"
    progress["rows_transferred"] = rows_transferred
    await _upsert_progress(
        target_conn, target_loader, table, "completed",
        rows_transferred, total_rows, error=NOTHING)

    _report(on_progress, progress)

    return progress


# ---------------------------------------------------------------------------
# One table, from the source to the target
# ---------------------------------------------------------------------------
#
# The loop that ran these questions asked six of them inline and carried five
# running totals. Each question is now a table with one entry per answer, and
# each table hands back what happened to it. The totals are counted from those
# records at the end, so nothing has to be kept in step while the run is going
# on.


class _TableOutcome(TypedDict):
    """What became of one table, and how many of its rows moved."""

    table: str
    outcome: Literal["transferred", "skipped", "failed"]
    rows: int
    error: str | None


class _Run(TypedDict):
    """What every table in one transfer shares: two connections, two loaders,
    the batch size, the caller's callback and whether it asked to resume."""

    source_conn: Any
    target_conn: Any
    source_loader: BulkLoader
    target_loader: BulkLoader
    batch_size: int
    on_progress: Callable[[TableTransferProgress], None] | None
    resume: bool


@boundary
async def _record_a_failed_table(
    run: _Run, table: str, failure: Exception
) -> _TableOutcome:
    """The engine refused this table, and only this table is blamed."""
    error_msg = f"{type(failure).__name__}: {failure}"
    logger.error(f"Failed to transfer {table}: {error_msg}")

    # A TABLE THAT FAILED MOVED NO ROWS, which is zero rather than absent.
    # These two carried `= 0` in the signature, so every caller that omitted
    # them was recorded as having moved nothing whether or not that was true.
    await _upsert_progress(
        run["target_conn"], run["target_loader"], table, "failed",
        rows_transferred=0, total_rows=0, error=error_msg)
    _report(
        run["on_progress"],
        {
            "table": table,
            "status": "failed",
            "rows_transferred": 0,
            "total_rows": 0,
            "error": error_msg,
        },
    )
    return {"table": table, "outcome": "failed", "rows": 0, "error": error_msg}


@boundary
async def _stream_the_table(
    run: _Run,
    table: str,
    columns: list[str],
    _status: dict[str, Any] | None,
) -> _TableOutcome:
    """Copy every row of one table, and say how it went."""
    try:
        progress = await _transfer_table(
            run["source_conn"],
            run["target_conn"],
            run["source_loader"],
            run["target_loader"],
            table,
            columns,
            run["batch_size"],
            run["on_progress"],
        )
    except A_BUG_IN_THIS_PACKAGE:
        # OURS, NOT THE ENGINE'S, so it must not be filed against a table. A
        # bug of ours fails on every table, and this used to turn one defect
        # into one "failure" per table: measured, a TypeError from
        # `_transfer_table` produced `tables_failed=4` with none of the four at
        # fault. The operator reads the report, opens the database, and finds
        # four healthy tables.
        raise
    except Exception as failure:
        return await _record_a_failed_table(run, table, failure)

    logger.info(
        f"Transferred {table}: "
        f"{progress['rows_transferred']}/{progress['total_rows']} rows"
    )
    return {
        "table": table,
        "outcome": "transferred",
        "rows": progress["rows_transferred"],
        "error": None,
    }


@boundary
async def _transfer_from_scratch(
    run: _Run,
    table: str,
    columns: list[str],
    status: dict[str, Any] | None,
) -> _TableOutcome:
    """Nothing was recorded against this table, so it transfers as it stands."""
    return await _stream_the_table(run, table, columns, status)


@boundary
async def _already_transferred(
    run: _Run,
    table: str,
    _columns: list[str],
    status: dict[str, Any] | None,
) -> _TableOutcome:
    """The progress table says this one finished, so its rows are counted and
    nothing is copied again."""
    recorded = cast(dict[str, Any], status)
    rows = recorded.get("rows_transferred", 0)
    logger.info(f"Skipping {table}: already completed")
    _report(
        run["on_progress"],
        {
            "table": table,
            "status": "skipped",
            "rows_transferred": rows,
            "total_rows": recorded.get("total_rows", 0),
            "error": None,
        },
    )
    return {"table": table, "outcome": "skipped", "rows": rows, "error": None}


@boundary
async def _clear_and_transfer(
    run: _Run,
    table: str,
    columns: list[str],
    status: dict[str, Any] | None,
) -> _TableOutcome:
    """Part of this table landed on an earlier run, so it goes before it does.

    `failed` MEANS WHAT `in_progress` MEANS: PARTLY COPIED. A table is marked
    failed after batches have already been written, so the rows that landed
    before the failure are still there. Only `in_progress` used to be cleared,
    so a retry collided with its own first attempt and failed again, for good.
    Measured 2026-08-18 with three rows and a colliding row in the target:

        first run    UNIQUE constraint failed: widgets.id
        status left  ('failed', 0), two rows landed
        the colliding row is removed, and the run resumed
        resumed run  UNIQUE constraint failed: widgets.id

    The operator removed the cause and the retry failed identically, which
    made resume useless for the one case it exists to serve.
    """
    recorded = cast(dict[str, Any], status)
    logger.info(
        f"Restarting {table}: was {recorded['status']}, deleting existing rows"
    )
    await run["target_loader"]["delete_rows"](run["target_conn"], table)
    await run["target_loader"]["commit"](run["target_conn"])
    return await _stream_the_table(run, table, columns, status)


# What a recorded status means to a resumed run. A status this does not name
# is one no resume rule covers, and the table transfers from the beginning.
_RESUME_MEANING = {
    "completed": "already done",
    "in_progress": "restart",
    "failed": "restart",
    "pending": "fresh",
    "skipped": "fresh",
}

_RESUME_ACTION: dict[
    str,
    Callable[
        [_Run, str, list[str], dict[str, Any] | None],
        Coroutine[Any, Any, _TableOutcome],
    ],
] = {
    "already done": _already_transferred,
    "restart": _clear_and_transfer,
    "fresh": _transfer_from_scratch,
}


@helper
def _how_a_recorded_table_stands(status: dict[str, Any] | None) -> str:
    """What a resumed run does about this table: skip it, clear it, or copy it.

    A table with no row in the progress table has nothing recorded, and so
    does one whose recorded status no rule names.
    """
    recorded = [
        meaning
        for row in [status]
        if row
        for named, meaning in _RESUME_MEANING.items()
        if named == row["status"]
    ]
    return next(iter(recorded), "fresh")


@boundary
async def _from_where_it_got_to(
    run: _Run, table: str, columns: list[str]
) -> _TableOutcome:
    """`resume` was asked for, so the progress table decides what happens."""
    status = await _get_table_status(run["target_conn"], run["target_loader"], table)
    return await _RESUME_ACTION[_how_a_recorded_table_stands(status)](
        run, table, columns, status
    )


@boundary
async def _from_the_beginning(
    run: _Run, table: str, columns: list[str]
) -> _TableOutcome:
    """`resume` was declined, so the progress table is not consulted."""
    return await _stream_the_table(run, table, columns, None)


_WHEN_RESUMING: dict[
    bool,
    Callable[[_Run, str, list[str]], Coroutine[Any, Any, _TableOutcome]],
] = {
    True: _from_where_it_got_to,
    False: _from_the_beginning,
}


@boundary
async def _transfer_or_resume(
    run: _Run, table: str, columns: list[str]
) -> _TableOutcome:
    """The table has columns, so the only question left is where to start."""
    return await _WHEN_RESUMING[run["resume"]](run, table, columns)


@boundary
async def _skip_a_table_with_no_columns(
    _run: _Run, table: str, _columns: list[str]
) -> _TableOutcome:
    """The source names this table and declares no column of it, so there is
    nothing to select and nothing to insert."""
    logger.warning(f"Skipping {table}: no columns found")
    return {"table": table, "outcome": "skipped", "rows": 0, "error": None}


_WHEN_THE_TABLE_HAS_COLUMNS: dict[
    bool,
    Callable[[_Run, str, list[str]], Coroutine[Any, Any, _TableOutcome]],
] = {
    True: _transfer_or_resume,
    False: _skip_a_table_with_no_columns,
}


@boundary
async def _transfer_one_table(
    run: _Run, source_schema: dict[str, Any], table: str
) -> _TableOutcome:
    """Everything one table goes through, from its columns to its rows."""
    columns = list(source_schema.get(table, {}).get("columns", {}).keys())
    return await _WHEN_THE_TABLE_HAS_COLUMNS[bool(columns)](run, table, columns)


# ---------------------------------------------------------------------------
# What the run settles before any row moves
# ---------------------------------------------------------------------------


@helper
def _refuse_a_failed_migration(migration_result: dict[str, Any]) -> dict[str, Any]:
    """The target does not hold the schema, so there is nowhere to put rows."""
    raise TransferError(
        f"Schema migration failed on target: {migration_result.get('error')}",
        phase="schema_migration", table=NOTHING, original_error=NOTHING)


@helper
def _the_target_is_ready(migration_result: dict[str, Any]) -> dict[str, Any]:
    """The target holds the schema this transfer is about to fill."""
    return migration_result


_WHEN_THE_TARGET_MIGRATED: dict[
    bool, Callable[[dict[str, Any]], dict[str, Any]]
] = {
    True: _the_target_is_ready,
    False: _refuse_a_failed_migration,
}


@helper
def _refuse_the_missing_tables(
    _tables: list[str], all_tables: list[str], missing: list[str]
) -> list[str]:
    """Named tables the source does not have, refused before anything moves."""
    raise TransferError(
        f"These tables were asked for and the source does not have "
        f"them: {', '.join(sorted(missing))}. The source holds "
        f"{', '.join(sorted(all_tables))}. Nothing has been "
        f"transferred.", phase=NOTHING, table=NOTHING, original_error=NOTHING)


@helper
def _every_table_the_caller_named(
    tables: list[str], _all_tables: list[str], _missing: list[str]
) -> list[str]:
    """Nothing is missing, so every name the caller gave is transferred."""
    return list(tables)


_WHEN_A_NAMED_TABLE_IS_MISSING: dict[
    bool, Callable[[list[str], list[str], list[str]], list[str]]
] = {
    True: _refuse_the_missing_tables,
    False: _every_table_the_caller_named,
}


@helper
def _the_tables_the_caller_named(
    tables: list[str] | None,
    all_tables: list[str],
    source_schema: dict[str, Any],
) -> list[str]:
    """The names the caller gave, once the source is known to have them all.

    A NAME THAT IS NOT THERE IS A REFUSAL, NOT A FILTER. This was
    `[t for t in tables if t in source_schema]`, so a table the caller named
    and the source does not have was dropped: not skipped, not counted, not
    reported.

    Measured 2026-08-17, asking for ["users", "orders_typo"] against a source
    holding only `users`:

        success=True transferred=1 skipped=0 failed=0 errors={}

    `tables=[...]` is the caller stating that these exist and it wants them.
    Intersecting that with reality turns a typo, a stale deploy script or a
    half-finished rename into a green result with rows left in the old
    database.

    Refused HERE, before anything moves. A partial transfer that then raises
    is worse than either outcome.
    """
    named = list(tables or [])
    missing = [t for t in named if t not in source_schema]
    return _WHEN_A_NAMED_TABLE_IS_MISSING[bool(missing)](named, all_tables, missing)


@helper
def _every_user_table(
    _tables: list[str] | None,
    all_tables: list[str],
    _source_schema: dict[str, Any],
) -> list[str]:
    """No names given, so everything the source holds that is not ours."""
    return [
        t for t in all_tables
        if not is_enum_table(t) and t != TRANSFER_STATE_TABLE
    ]


_WHICH_TABLES: dict[
    bool,
    Callable[[list[str] | None, list[str], dict[str, Any]], list[str]],
] = {
    True: _the_tables_the_caller_named,
    False: _every_user_table,
}


@boundary
def _warn_and_append_the_cycle(
    sorted_tables: list[str], circular_refs: list[str]
) -> list[str]:
    """The tables in a cycle have no order among themselves, so they go last."""
    logger.warning(
        f"Circular FK references detected: {circular_refs}. "
        f"FK checks will be disabled during transfer."
    )
    return [*sorted_tables, *circular_refs]


@helper
def _the_order_the_toposort_gave(
    sorted_tables: list[str], _circular_refs: list[str]
) -> list[str]:
    """No cycle, so the topological order covers every table."""
    return sorted_tables


_TRANSFER_ORDER: dict[bool, Callable[[list[str], list[str]], list[str]]] = {
    True: _warn_and_append_the_cycle,
    False: _the_order_the_toposort_gave,
}


@boundary
async def _turn_fk_checks_off(
    loader: BulkLoader, conn: Any, circular_refs: list[str]
) -> int:
    """DISABLING IS A PRECONDITION, NOT A COURTESY, so a failure stops the run.

    This was a warning, and the transfer then proceeded with checks still on:
    every table in the cycle failed on its first row and each failure was
    recorded against that table. One failed pragma became N table failures,
    and the report named N tables that were all fine.

    The tables in `circular_refs` reference each other, so there is no load
    order that satisfies the constraints. Carrying on is not a degraded
    transfer, it is a guaranteed failure.
    """
    try:
        await loader["disable_fk_checks"](conn)
    except Exception as exc:
        raise TransferError(
            f"Could not disable foreign key checks on the target, and "
            f"these tables have circular references so no load order "
            f"satisfies them: {', '.join(sorted(circular_refs))}. "
            f"Transferring now would fail on the first row of each and "
            f"report those tables as the problem.", phase=NOTHING, table=NOTHING, original_error=NOTHING) from exc
    return None


@boundary
async def _leave_fk_checks_on(
    _loader: BulkLoader, _conn: Any, _circular_refs: list[str]
) -> int:
    """No cycle, so every table has a load order its keys are happy with."""
    return 0


# HOW MANY CONNECTIONS HAD ENFORCEMENT TURNED OFF: one or none. Both halves
# answer it, so neither is a function with no body, and the count is what the
# restore step needs to know afterwards.
_WHEN_THERE_ARE_CYCLES: dict[
    bool,
    Callable[[BulkLoader, Any, list[str]], Coroutine[Any, Any, int]],
] = {
    True: _turn_fk_checks_off,
    False: _leave_fk_checks_on,
}


@boundary
async def _turn_fk_checks_back_on(loader: BulkLoader, conn: Any) -> int:
    """TEARDOWN, so this does NOT raise: it runs in a `finally` and would
    replace whatever failure is already in flight with its own.

    It is an ERROR rather than a warning because of what it leaves behind. The
    connection is still enforcing nothing, so a caller that reuses it writes
    against unchecked constraints and never knows. That is worth waking
    someone for even though the transfer itself succeeded.
    """
    try:
        await loader["enable_fk_checks"](conn)
    except Exception as exc:
        logger.error(
            "Could not re-enable foreign key checks on the target "
            "after the transfer: %s. The connection is left NOT "
            "enforcing them, so anything written on it from here "
            "is unchecked.",
            exc,
            exc_info=True,
        )
    return 1


@boundary
async def _leave_the_checks_as_they_are(_loader: BulkLoader, _conn: Any) -> int:
    """They were never turned off, so there is nothing to put back."""
    return 0


# How many connections had enforcement put back: one or none.
_WHEN_THE_CHECKS_WERE_TURNED_OFF: dict[
    bool, Callable[[BulkLoader, Any], Coroutine[Any, Any, int]]
] = {
    True: _turn_fk_checks_back_on,
    False: _leave_the_checks_as_they_are,
}


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
    tables = resolved(tables, None)
    on_progress = resolved(on_progress, None)
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
    transfer_tables = _WHICH_TABLES[tables is not None](
        tables, all_tables, source_schema
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
            "on_progress": on_progress,
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
