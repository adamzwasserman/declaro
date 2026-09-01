"""How a migration decides what to do, one question at a time.

migrations.py reached 1604 lines holding two subjects. Everything that builds
a statement, stamps a schema hash or reads the database back is in
`migration_sql`; this file holds the chain of questions a migration answers
before it runs anything, and the record it carries between them.

Every name from `migration_sql` is re-exported here, so nothing that imports
from `migrations` had to change.
"""

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.boundary import helper
from declaro_persistum.database import (
    Database,
    is_replicated,
    reading,
    refresh,
)
from declaro_persistum.differ import diff, diff_views
from declaro_persistum.inspector import introspect_with_views
from declaro_persistum.migration_sql import (
    _PLACEHOLDER as _PLACEHOLDER,
)
from declaro_persistum.migration_sql import (
    _STAMP_READERS as _STAMP_READERS,
)
from declaro_persistum.migration_sql import (
    _STORE_HASH as _STORE_HASH,
)
from declaro_persistum.migration_sql import (
    _SUFFIX_WIDTH as _SUFFIX_WIDTH,
)
from declaro_persistum.migration_sql import (
    _TABLE_COUNTERS as _TABLE_COUNTERS,
)
from declaro_persistum.migration_sql import (
    _USER_TABLES as _USER_TABLES,
)
from declaro_persistum.migration_sql import (
    _WHEN_A_DIRECTORY as _WHEN_A_DIRECTORY,
)
from declaro_persistum.migration_sql import (
    _WHEN_A_SUFFIX_IS_THERE as _WHEN_A_SUFFIX_IS_THERE,
)
from declaro_persistum.migration_sql import (
    _WHEN_ANYTHING_WAS_RECOVERED as _WHEN_ANYTHING_WAS_RECOVERED,
)
from declaro_persistum.migration_sql import (
    _WHEN_NOT_OURS as _WHEN_NOT_OURS,
)
from declaro_persistum.migration_sql import (
    _WHEN_THE_DATABASE_IS_POPULATED as _WHEN_THE_DATABASE_IS_POPULATED,
)
from declaro_persistum.migration_sql import (
    _WHEN_THE_ORIGINAL_SURVIVED as _WHEN_THE_ORIGINAL_SURVIVED,
)
from declaro_persistum.migration_sql import (
    _WHEN_THE_STAMP_MATCHES as _WHEN_THE_STAMP_MATCHES,
)
from declaro_persistum.migration_sql import (
    _WRITERS as _WRITERS,
)

# Re-exported so nothing that imports from here had to change.
from declaro_persistum.migration_sql import (  # noqa: F401
    META_TABLE as META_TABLE,
)
from declaro_persistum.migration_sql import (
    TMP_PREFIX as TMP_PREFIX,
)
from declaro_persistum.migration_sql import (
    _clear_the_stamp as _clear_the_stamp,
)
from declaro_persistum.migration_sql import (
    _compute_schema_hash as _compute_schema_hash,
)
from declaro_persistum.migration_sql import (
    _count_tables_on_dbapi as _count_tables_on_dbapi,
)
from declaro_persistum.migration_sql import (
    _count_tables_on_postgres as _count_tables_on_postgres,
)
from declaro_persistum.migration_sql import (
    _drop_the_leftover as _drop_the_leftover,
)
from declaro_persistum.migration_sql import (
    _engine_needs_orphan_recovery as _engine_needs_orphan_recovery,
)
from declaro_persistum.migration_sql import (
    _engine_of as _engine_of,
)
from declaro_persistum.migration_sql import (
    _ensure_meta_table as _ensure_meta_table,
)
from declaro_persistum.migration_sql import (
    _for_ddl as _for_ddl,
)
from declaro_persistum.migration_sql import (
    _get_stored_hash as _get_stored_hash,
)
from declaro_persistum.migration_sql import (
    _has_user_tables as _has_user_tables,
)
from declaro_persistum.migration_sql import (
    _leave_the_stamp as _leave_the_stamp,
)
from declaro_persistum.migration_sql import (
    _Migration as _Migration,
)
from declaro_persistum.migration_sql import (
    _placeholder as _placeholder,
)
from declaro_persistum.migration_sql import (
    _read_stamp_from_dbapi as _read_stamp_from_dbapi,
)
from declaro_persistum.migration_sql import (
    _read_stamp_from_postgres as _read_stamp_from_postgres,
)
from declaro_persistum.migration_sql import (
    _recover_one as _recover_one,
)
from declaro_persistum.migration_sql import (
    _recover_orphaned_tmp_tables as _recover_orphaned_tmp_tables,
)
from declaro_persistum.migration_sql import (
    _refuse_a_directory as _refuse_a_directory,
)
from declaro_persistum.migration_sql import (
    _refuse_a_foreign_name as _refuse_a_foreign_name,
)
from declaro_persistum.migration_sql import (
    _rename_into_place as _rename_into_place,
)
from declaro_persistum.migration_sql import (
    _run_on_dbapi as _run_on_dbapi,
)
from declaro_persistum.migration_sql import (
    _run_on_postgres as _run_on_postgres,
)
from declaro_persistum.migration_sql import (
    _schema_is_clean as _schema_is_clean,
)
from declaro_persistum.migration_sql import (
    _stamp_key as _stamp_key,
)
from declaro_persistum.migration_sql import (
    _store_hash as _store_hash,
)
from declaro_persistum.migration_sql import (
    _the_database_holds_tables as _the_database_holds_tables,
)
from declaro_persistum.migration_sql import (
    _the_database_is_empty as _the_database_is_empty,
)
from declaro_persistum.migration_sql import (
    _the_schema_file as _the_schema_file,
)
from declaro_persistum.migration_sql import (
    _the_stamp_agrees as _the_stamp_agrees,
)
from declaro_persistum.migration_sql import (
    _the_stamp_disagrees as _the_stamp_disagrees,
)
from declaro_persistum.migration_sql import (
    _the_whole_remainder as _the_whole_remainder,
)
from declaro_persistum.migration_sql import (
    _without_our_prefix as _without_our_prefix,
)
from declaro_persistum.migration_sql import (
    _without_the_suffix as _without_the_suffix,
)
from declaro_persistum.migration_sql import (
    logger as logger,
)
from declaro_persistum.migration_sql import (
    meta_table_sql as meta_table_sql,
)
from declaro_persistum.migration_sql import (
    original_table_name as original_table_name,
)
from declaro_persistum.migration_sql import (
    orphaned_tmp_tables_sql as orphaned_tmp_tables_sql,
)
from declaro_persistum.migration_sql import (
    partition_replica_operations as partition_replica_operations,
)
from declaro_persistum.migration_sql import (
    store_hash_sql as store_hash_sql,
)
from declaro_persistum.migration_sql import (
    stored_hash_sql as stored_hash_sql,
)
from declaro_persistum.migration_sql import (
    temp_table_name as temp_table_name,
)
from declaro_persistum.migration_sql import (
    user_tables_sql as user_tables_sql,
)
from declaro_persistum.pydantic_loader import (
    load_declarations,
)
from declaro_persistum.reporting import say
from declaro_persistum.types import (
    NOTHING,
    Engine,
    Nothing,
    Schema,
    View,
    is_nothing,
    resolved,
)
from declaro_persistum.validator import validate_schema


@helper
def _still_to_do(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """No answer yet, so the schema gets read and diffed."""
    return {
        "db": db,
        "engine": engine,
        "schema_path": schema_path,
        "schema_hash": schema_hash,
        "expand_enums": expand_enums,
        "answer": NOTHING,
    }


@helper
async def _no_schema_file(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    _force: bool,
) -> _Migration:
    """Nothing to migrate from, and the caller is told which path was tried."""
    say(__name__, "warning", f"Schema file not found: {schema_path}")
    return {
        "db": db,
        "engine": engine,
        "schema_path": schema_path,
        "schema_hash": "",
        "expand_enums": expand_enums,
        "answer": {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": f"Schema file not found: {schema_path}",
        },
    }


@helper
async def _no_orphan_recovery(_db: Database) -> int:
    """This engine writes no `_declaro_tmp_` tables and has no sqlite_master,
    so there is nothing to scan for and nothing came back."""
    return 0


_ORPHAN_RECOVERY: dict[bool, Callable[[Database], Awaitable[int]]] = {
    True: _recover_orphaned_tmp_tables,
    False: _no_orphan_recovery,
}


@helper
async def _read_the_schema_file(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    force: bool,
) -> _Migration:
    """Bring the database up to date, recover what a crash left, take the hash.

    A migration diffs the live schema against the target, so the local copy
    must be current first. A warm open does not replicate, so without the
    refresh the diff is computed against whatever the copy held when it was
    last touched and emits DDL for a schema that has moved.
    """
    await refresh(db)

    # Pre-flight: recover orphaned tmp tables from a failed reconstruction.
    # Which engines have any lives in the pure helper so it can be tested
    # without a database.
    await _ORPHAN_RECOVERY[_engine_needs_orphan_recovery(engine)](db)

    # Version is passed explicitly so _compute_schema_hash stays a pure
    # function of its arguments (Honest Code: configuration as parameters).
    from declaro_persistum import __version__

    schema_hash = _compute_schema_hash(schema_path, __version__)
    return await _WHEN_THE_STAMP_IS_WORTH_READING[not force](
        db, engine, schema_path, expand_enums, schema_hash
    )


_WHEN_THE_SCHEMA_FILE_IS_THERE: dict[
    bool,
    Callable[[Database, Engine, Path, bool, bool], Awaitable[_Migration]],
] = {
    True: _read_the_schema_file,
    False: _no_schema_file,
}


@helper
async def _migrate_whatever_the_stamp_says(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """`force` was asked for, so the stamp is not read and the work goes on."""
    return _still_to_do(db, engine, schema_path, expand_enums, schema_hash)


@helper
async def _nothing_has_changed(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """The stamp matches and the tables are there, so the migration is over."""
    say(__name__, "info", "Schema unchanged (hash match) — skipping migration")
    return {
        "db": db,
        "engine": engine,
        "schema_path": schema_path,
        "schema_hash": schema_hash,
        "expand_enums": expand_enums,
        "answer": {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": True,
            "error": None,
        },
    }


_WHEN_THE_DATABASE_IS_CURRENT: dict[
    bool,
    Callable[[Database, Engine, Path, bool, str], Awaitable[_Migration]],
] = {
    True: _nothing_has_changed,
    False: _migrate_whatever_the_stamp_says,
}


@helper
async def _consult_the_stamp(
    db: Database,
    engine: Engine,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """Skip-if-clean: compare the schema file's hash with the stored one."""
    async with reading(db) as conn:
        current = await _schema_is_clean(conn, schema_path, schema_hash)
    return await _WHEN_THE_DATABASE_IS_CURRENT[current](
        db, engine, schema_path, expand_enums, schema_hash
    )


_WHEN_THE_STAMP_IS_WORTH_READING: dict[
    bool,
    Callable[[Database, Engine, Path, bool, str], Awaitable[_Migration]],
] = {
    True: _consult_the_stamp,
    False: _migrate_whatever_the_stamp_says,
}


@helper
def _load_the_schema(schema_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """The tables and views one models file declares."""
    say(__name__, "info", f"Loading schema from {schema_path}")
    return load_declarations(schema_path)


@helper
async def _stamp(plan: _Migration) -> None:
    """Record the hash so the next start can skip introspection."""
    async with _for_ddl(plan["db"]) as conn:
        await _ensure_meta_table(conn)
        await _store_hash(conn, plan["schema_path"].name, plan["schema_hash"])


@helper
def _how_the_migration_stands(answer: Any, errors: list[str]) -> str:
    """Which of the three things happens next, named rather than nested.

    The migration may already hold its answer, the schema may be invalid, or
    there are tables to migrate. The first two are asked in that order because
    a migration that never loaded a schema has nothing to validate.
    """
    settled = [
        name
        for name, holds in (
            ("answered", not is_nothing(answer)),
            ("invalid", bool(errors)),
        )
        if holds
    ]
    return next(iter(settled), "carry on")


@helper
async def _the_answer_already_settled(
    plan: _Migration,
    _target_schema: Schema,
    _target_views: dict[str, View],
    _errors: list[str],
) -> dict[str, Any]:
    """The file was missing or the stamp was clean. That answer stands."""
    return cast(dict[str, Any], plan["answer"])


@helper
async def _refuse_an_invalid_schema(
    _plan: _Migration,
    target_schema: Schema,
    _target_views: dict[str, View],
    errors: list[str],
) -> dict[str, Any]:
    """Every error reaches the caller, in the result, and is not also written.

    This wrote one line per error AND returned them all in `error`. The line
    was a second copy of a fact the caller already had, on a channel the
    caller could not read, test or decline.
    """
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": 0,
        "skipped": False,
        "error": "; ".join(errors),
    }


@helper
async def _stamp_an_empty_schema(
    plan: _Migration,
    _target_schema: Schema,
    _target_views: dict[str, View],
) -> dict[str, Any]:
    """A models file declaring no table is stamped, so the next call skips."""
    say(__name__, "warning", "No tables found in schema models")
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": 0,
        "tables_in_schema": 0,
        "tables_in_database": 0,
        "skipped": False,
        "error": None,
    }


@helper
def _expand_the_enums(target_schema: Schema) -> dict[str, Any]:
    """Every Literal column becomes a lookup table and an FK to it."""
    expanded = expand_schema_enums(target_schema)
    say(
        __name__,
        "info",
        f"Expanded schema to {len(expanded)} tables (including enum lookup tables)",
    )
    return expanded


@helper
def _the_schema_as_declared(target_schema: Schema) -> dict[str, Any]:
    """Expansion was declined, so a Literal column stays a plain column."""
    return target_schema


_ENUM_EXPANSION: dict[bool, Callable[[dict[str, Any]], dict[str, Any]]] = {
    True: _expand_the_enums,
    False: _the_schema_as_declared,
}


@helper
async def _nothing_to_change(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    _diff_result: dict[str, Any],
) -> dict[str, Any]:
    """The database already holds what the schema declares."""
    say(__name__, "info", "Schema is up to date - no migrations needed")
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "error": None,
    }


@helper
async def _refuse_ambiguous_changes(
    _plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """A rename and a drop-plus-add look alike, and only a person can say."""
    say(__name__, "warning", f"Ambiguous changes detected: {diff_result['ambiguities']}")
    say(__name__, "warning", "Skipping migration - manual intervention required")
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "error": f"Ambiguous changes: {diff_result['ambiguities']}",
    }


@helper
async def _record_the_migration(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    skipped_operations: list[dict[str, Any]],
    result: dict[str, Any],
) -> dict[str, Any]:
    """The operations landed, so the hash is stamped and the run reported."""
    say(__name__, "info", f"Successfully applied {result['operations_applied']} migrations")
    [
        say(__name__, "warning", f"Migration completed with warnings: {warning}")
        for warning in [result.get("error")]
        if warning
    ]
    [say(__name__, "debug", f"  Executed: {sql}") for sql in result["executed_sql"]]
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": result["operations_applied"],
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema) + result["operations_applied"],
        "skipped": bool(skipped_operations),
        "skipped_operations": skipped_operations,
        "error": None,
    }


@helper
async def _report_a_failed_apply(
    _plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    skipped_operations: list[dict[str, Any]],
    result: dict[str, Any],
) -> dict[str, Any]:
    """The applier refused, and nothing is stamped over a failed run."""
    error_msg = result.get("error", "Unknown error")
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "skipped_operations": skipped_operations,
        "error": error_msg,
    }


_WHEN_THE_OPERATIONS_LANDED: dict[
    bool,
    Callable[
        [
            _Migration,
            dict[str, Any],
            dict[str, Any],
            list[dict[str, Any]],
            dict[str, Any],
        ],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _record_the_migration,
    False: _report_a_failed_apply,
}


@helper
async def _apply_and_report(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    operations: list[dict[str, Any]],
    execution_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run the operations the order names, then report what happened.

    No pause. A migration is a writer, and it holds the serialise lock for the
    whole of `_for_ddl`; opportunistic replication defers to a waiting writer,
    so nothing can push mid-transaction.
    """
    async with _for_ddl(plan["db"]) as conn:
        result = await apply_operations(
            conn, operations, execution_order, plan["engine"], dry_run=False
        )
    return await _WHEN_THE_OPERATIONS_LANDED[result["success"]](
        plan, target_schema, current_schema, skipped_operations, result
    )


@helper
async def _run_the_safe_operations(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
    safe_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Some operations replicate, so those run and the rest are reported."""
    return await _apply_and_report(
        plan,
        target_schema,
        current_schema,
        diff_result["operations"],
        safe_order,
        skipped_operations,
    )


@helper
async def _report_everything_deferred(
    _plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    _diff_result: dict[str, Any],
    _safe_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Every pending operation needs reconstruction, so none of them run."""
    say(
        __name__,
        "info",
        "All pending operations require reconstruction — "
        "skipping auto-migration. Use 'declaro migrate-remote'.",
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


_WHEN_ANYTHING_IS_SAFE_TO_RUN: dict[
    bool,
    Callable[
        [
            _Migration,
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            list[int],
            list[dict[str, Any]],
        ],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _run_the_safe_operations,
    False: _report_everything_deferred,
}


@helper
async def _run_every_operation(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """Not a replica, so everything the differ emitted runs here."""
    return await _apply_and_report(
        plan,
        target_schema,
        current_schema,
        diff_result["operations"],
        diff_result["execution_order"],
        [],
    )


@helper
async def _defer_what_cannot_replicate(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """On an embedded replica, reconstruction is left for `migrate-remote`.

    The replication engine cannot replicate DDL, and a partial sync -- the
    DROP reaches the cloud and the CREATE does not -- destroys the table on
    both sides.
    """
    safe_order, skipped_operations = partition_replica_operations(
        diff_result["operations"], diff_result["execution_order"]
    )
    [
        say(
            __name__,
            "warning",
            f"Skipping {skipped['op']} on {skipped.get('table', 'N/A')} — "
            f"reconstruction is unsafe on embedded replicas. "
            f"Run 'declaro migrate-remote' to apply this change to cloud.",
        )
        for skipped in skipped_operations
    ]
    return await _WHEN_ANYTHING_IS_SAFE_TO_RUN[bool(safe_order)](
        plan,
        target_schema,
        current_schema,
        diff_result,
        safe_order,
        skipped_operations,
    )


_WHEN_THE_TARGET_IS_A_REPLICA: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _defer_what_cannot_replicate,
    False: _run_every_operation,
}


@helper
async def _choose_what_to_run(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """A replica runs less than a primary does, and says what it left out."""
    return await _WHEN_THE_TARGET_IS_A_REPLICA[is_replicated(plan["db"])](
        plan, target_schema, current_schema, diff_result
    )


_WHEN_THE_CHANGES_ARE_AMBIGUOUS: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _refuse_ambiguous_changes,
    False: _choose_what_to_run,
}


@helper
async def _consider_what_changed(
    plan: _Migration,
    target_schema: Schema,
    current_schema: Schema,
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """Log the planned operations, then ask whether any of them is ambiguous."""
    say(__name__, "info", f"Found {len(diff_result['operations'])} schema differences")
    [
        say(__name__, "info", f"  - {op['op']} on table {op.get('table', 'N/A')}")
        for op in diff_result["operations"]
    ]
    return await _WHEN_THE_CHANGES_ARE_AMBIGUOUS[bool(diff_result.get("ambiguities"))](
        plan, target_schema, current_schema, diff_result
    )


_WHEN_THERE_IS_WORK: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _consider_what_changed,
    False: _nothing_to_change,
}


@helper
async def _migrate_the_tables(
    plan: _Migration,
    target_schema: Schema,
    target_views: dict[str, View],
) -> dict[str, Any]:
    """Introspect the database, diff it against the schema, run the result."""
    say(__name__, "info", f"Loaded {len(target_schema)} tables from schema models")

    expanded = _ENUM_EXPANSION[plan["expand_enums"]](target_schema)

    # Introspect current database state, VIEWS INCLUDED. Asking without them
    # is what made views unreachable: the inspectors, the differ and the
    # appliers all handled views, and the apply path never carried one.
    async with reading(plan["db"]) as conn:
        current_schema, current_views = await introspect_with_views(conn, plan["engine"])

    say(
        __name__,
        "info",
        f"Introspected {len(current_schema)} tables and {len(current_views)} views from database",
    )

    # Compute diff. Tables and views are diffed by different functions because
    # they are different things: a table is reconciled column by column, a view
    # is replaced whole when its query changes.
    diff_result = diff(current_schema, expanded, engine=plan["engine"], decisions=NOTHING)

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

    return await _WHEN_THERE_IS_WORK[bool(diff_result["operations"])](
        plan, expanded, current_schema, diff_result
    )


_WHEN_THE_SCHEMA_DECLARES_TABLES: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any]],
        Awaitable[dict[str, Any]],
    ],
] = {
    True: _migrate_the_tables,
    False: _stamp_an_empty_schema,
}


@helper
async def _migrate_the_declared_schema(
    plan: _Migration,
    target_schema: Schema,
    target_views: dict[str, View],
    _errors: list[str],
) -> dict[str, Any]:
    """The schema is valid, so the only question left is whether it has any
    tables in it."""
    return await _WHEN_THE_SCHEMA_DECLARES_TABLES[bool(target_schema)](
        plan, target_schema, target_views
    )


_NEXT_STEP: dict[
    str,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], list[str]],
        Awaitable[dict[str, Any]],
    ],
] = {
    "answered": _the_answer_already_settled,
    "invalid": _refuse_an_invalid_schema,
    "carry on": _migrate_the_declared_schema,
}

# What a schema nobody read has to say for itself. A migration that already
# holds its answer never loads a models file, so there is nothing to validate
# and no error to report.
_NOTHING_TO_VALIDATE: dict[str, list[str]] = {"errors": [], "warnings": []}


@helper
async def apply_migrations_async(
    db: Database,
    schema_path: str | Path,
    *,
    expand_enums: bool | Nothing,
    force: bool | Nothing,
) -> dict[str, Any]:
    """
    Apply automatic schema migrations asynchronously.

    Loads schema from Pydantic models, compares with actual database,
    and applies changes if differences are found.

    Uses a SHA-256 hash of the schema file to skip introspection when
    the schema hasn't changed since the last successful migration.

    THE ENGINE IS NOT A PARAMETER. It used to be, and a caller could name one
    engine while handing over a database on another. Do that and the wrong
    introspector runs: a SQLite database with `engine="postgresql"` raised
    `'Connection' object has no attribute 'fetch'`, which reads as a driver
    bug rather than as the caller naming the wrong engine. The database
    already knows what it is, so it is read here rather than asked for.

    Args:
        db: The database, which carries the engine it was opened on
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    expand_enums = resolved(expand_enums, True)
    force = resolved(force, False)
    file = Path(schema_path)
    engine: Engine = db["engine"]

    plan = await _WHEN_THE_SCHEMA_FILE_IS_THERE[file.exists()](
        db, engine, file, expand_enums, force
    )

    # THE VALIDATOR IS ASKED HERE, AND WAS ASKED NOWHERE ON THIS PATH BEFORE.
    # Every rule it holds fired only under `declaro validate`, a command an
    # author has to think to run, so the checks existed and did not protect
    # the operation they were written for.
    #
    # Most invalid schemas fail at the engine too, and what is lost is the
    # message. A table named `quo"ted` reached SQLite as `near "ted": syntax
    # error`, naming a fragment of the name, where the validator names the
    # table, the character, the reason and the action.
    #
    # Some do not fail at all. A foreign key pointing at a table nobody
    # declared is caught by comparing the schema against itself, and SQLite
    # accepts the DDL without complaint because it does not check a reference
    # at creation time. That column then points at nothing and every deploy
    # reports success.
    #
    # WARNINGS ARE NOT ERRORS. The validator separates them and only the
    # errors stop the migration; an unknown type is a warning and keeps going.
    #
    # Measured before wiring it in: every schema this package ships validates
    # clean, and of the fourteen models files the tests build, thirteen
    # validate clean and none has an error.
    #
    # THE FILTER IS THE GUARD. A migration that already has its answer must
    # not import a models file, and a comprehension whose filter excludes
    # everything never evaluates its body. That is what carries the laziness a
    # branch used to carry.
    loaded = [_load_the_schema(file) for _ in [()] if is_nothing(plan["answer"])]
    validated = [validate_schema(schema) for schema, _views in loaded]

    target_schema, target_views = next(iter(loaded), ({}, {}))
    validation = next(iter(validated), _NOTHING_TO_VALIDATE)

    return await _NEXT_STEP[_how_the_migration_stands(plan["answer"], validation["errors"])](
        plan, target_schema, target_views, validation["errors"]
    )
