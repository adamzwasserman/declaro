"""Apply migration operations to SQLite. Functions, not a class.

`SQLiteApplier` was stateless and held one real method, `apply()`. The SQL
GENERATORS beside it — generate_column_sql, generate_create_view,
generate_create_trigger and the rest — were already module-level functions.
So the pure/impure split was already drawn, and drawn in the right place; the
class simply sat around the impure half for no reason.

SQLite has limited ALTER TABLE support, so some operations cannot be expressed
as an ALTER at all and go through table reconstruction instead: create the new
shape, copy the data across, drop the old, rename the new.

SQL generation is shared with Turso through `applier.shared`.
"""

from collections.abc import Awaitable, Callable
from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    dry_run_preview,
    generate_operation_sql,
    requires_reconstruction,
    single_change_property,
    statements_for,
)
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE, MigrationError
from declaro_persistum.types import (
    NOTHING,
    ApplyResult,
    DbapiConnection,
    Nothing,
    Operation,
    View,
    resolved,
)


@helper
def get_dialect() -> str:
    """Return dialect identifier."""
    return "sqlite"


@helper
def get_transaction_mode() -> Literal["all_or_nothing", "per_operation"]:
    """SQLite supports transactional DDL."""
    return "all_or_nothing"


@boundary
async def _restore_enforcement(connection: DbapiConnection, enforced: int) -> None:
    """Put `PRAGMA foreign_keys` back to what the caller had.

    RESTORING MEANS PUTTING IT BACK, NOT TURNING IT ON. A caller who disabled
    enforcement deliberately before migrating has not asked for it to be
    switched on underneath them.

    Only meaningful outside a transaction. Every call site below is after a
    commit or a rollback for that reason.
    """
    await connection.execute(f"PRAGMA foreign_keys = {'ON' if enforced else 'OFF'}")


@helper
async def apply(
    connection: DbapiConnection,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool | Nothing,
) -> ApplyResult:
    """
    Apply migration operations within a transaction.

    SQLite DDL is transactional, allowing safe rollback on failure.
    Uses per-operation execution with reconstruction for unsupported operations.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    dry_run = resolved(dry_run, False)

    # WHETHER THE STATEMENTS ARE PRINTED OR RUN. Both entries take the same
    # three arguments and hand back the same shape of result.
    return await _WHEN_DRY_RUN[bool(dry_run)](connection, operations, execution_order)


@helper
async def _preview(
    _connection: DbapiConnection,
    operations: list[Operation],
    execution_order: list[int],
) -> ApplyResult:
    """A dry run asks the database nothing, so the statements are the result."""
    return dry_run_preview(operations, execution_order)


@helper
async def _run_reconstruction(connection: DbapiConnection, operation: Operation) -> list[str]:
    """An operation SQLite cannot express as an ALTER, so the table is rebuilt."""
    await _execute_with_reconstruction(connection, operation)
    return [f"Table reconstruction for {operation['table']}"]


@boundary
async def _run_statements(connection: DbapiConnection, operation: Operation) -> list[str]:
    """An operation SQLite runs directly, one statement at a time.

    THE LIST IS EXECUTED AS IT WAS BUILT. This took a joined string and cut it
    with `split(";")`, which is wrong inside a trigger body and inside any
    string literal holding one.

    One result comes back per statement, so the count of results is the count
    of statements that reached the engine. A statement the engine refuses
    takes the comprehension with it and nothing comes back at all.
    """
    statements = statements_for(operation)
    cursors = [await connection.execute(statement) for statement in statements]
    return statements[: len(cursors)]


# WHETHER THE TABLE IS REBUILT OR THE STATEMENTS ARE RUN. Both entries return
# the executed_sql entries the operation produced.
_HOW_TO_RUN: dict[bool, Callable[[DbapiConnection, Operation], Awaitable[list[str]]]] = {
    True: _run_reconstruction,
    False: _run_statements,
}


@boundary
async def _execute_operation(connection: DbapiConnection, operation: Operation) -> list[str]:
    """Run one operation, rolling the batch back if it fails."""
    try:
        return await _HOW_TO_RUN[requires_reconstruction(operation)](connection, operation)
    except A_BUG_IN_THIS_PACKAGE:
        # OURS, AND THE ENGINE WAS NEVER ASKED. A KeyError here means
        # the operation does not match what the differ emits, so the
        # bug is in whoever built it. Wrapping it as
        # "Failed to execute operation" sends an operator to look at a
        # database that is fine.
        #
        # Measured 2026-08-17 with an operation missing its `changes`
        # key: `MigrationError: Failed to execute operation`, cause
        # `KeyError: 'changes'`, and no statement ever ran.
        #
        # The rollback still happens first: raising must not leave a
        # transaction open behind it.
        await connection.rollback()
        raise
    except Exception as e:
        await connection.rollback()
        raise MigrationError(
            "Failed to execute operation", operation=operation, original_error=e, sql=NOTHING
        ) from e


@boundary
async def _run_batch(
    connection: DbapiConnection,
    operations: list[Operation],
    execution_order: list[int],
) -> ApplyResult:
    """Every operation inside one transaction, with enforcement put back after."""
    # WHAT THE CALLER HAD, so it can be given back. Read BEFORE anything else
    # runs, because the first statement of the batch opens a transaction and
    # SQLite then ignores every write to this pragma until it closes.
    cursor = await connection.execute("PRAGMA foreign_keys")
    row = await cursor.fetchone()
    entry_enforcement = row[0] if row else 1

    try:
        # Enforce during the migration, whatever the caller had. A
        # reconstruction turns this off around its table swap and the value is
        # restored after the commit below.
        await connection.execute("PRAGMA foreign_keys = ON")

        # AN EXPLICIT TRANSACTION, AFTER THE PRAGMA AND BEFORE THE DDL.
        #
        # SQLite's DDL is transactional; Python's `sqlite3` driver does not
        # put it in a transaction. Its default `isolation_level` is `""`,
        # which opens one implicitly before DML and NOT before DDL, so every
        # CREATE and DROP here ran in autocommit and the `rollback()` on the
        # failure paths below had nothing to undo.
        #
        # Measured 2026-08-17. Two operations where the second is refused:
        #
        #     apply raised MigrationError
        #     tables after the failed migration: ['good']
        #
        # and the same probe with a `BEGIN` leaves none. A failed migration
        # was leaving the database in a shape nobody designed, while the
        # applier believed it had rolled back and its docstring said "SQLite
        # DDL is transactional, allowing safe rollback on failure" -- true of
        # the engine, false of the driver in its default mode.
        #
        # `applier/turso.py` has issued its own BEGIN all along. This module
        # is the one that did not.
        #
        # It goes AFTER the pragma because `PRAGMA foreign_keys` is a silent
        # no-op inside an open transaction.
        await connection.execute("BEGIN")

        # Per-operation execution
        executed = [
            entry
            for op_idx in execution_order
            for entry in await _execute_operation(connection, operations[op_idx])
        ]

        await connection.commit()

        # RESTORE HERE, BECAUSE HERE IS WHERE IT LANDS. A reconstruction turns
        # enforcement off to drop a table other rows reference, and its own
        # attempt to put it back runs inside this transaction where SQLite
        # IGNORES the pragma. It does not raise, so the `try/except` around it
        # catches nothing and the code reads as though it worked.
        #
        # Measured 2026-08-17 through this function: enforcement 1 before,
        # success True, enforcement 0 after. Every later write on the
        # connection was then accepted against constraints nobody was
        # checking, silently, because the migration had succeeded.
        #
        #     inside an open transaction   PRAGMA foreign_keys = ON -> still 0
        #     after the transaction ends   -> 1
        #
        # `applier/turso.py` already did this correctly on its FAILURE path by
        # rolling back first. The success path did not.
        await _restore_enforcement(connection, entry_enforcement)

        return {
            "success": True,
            "executed_sql": executed,
            "operations_applied": len(executed),
            "error": None,
            "error_operation": None,
        }

    except MigrationError:
        # A per-operation failure already rolled back. Restore here too: the
        # caller catches this and its connection must not be left enforcing
        # nothing, which is the state a half-done reconstruction leaves.
        await _restore_enforcement(connection, entry_enforcement)
        raise
    except A_BUG_IN_THIS_PACKAGE:
        # The outer twin of the guard in `_execute_operation`. Without it the
        # KeyError escapes the per-operation wrapper and is caught here
        # instead, coming back as `MigrationError: Migration failed:
        # 'changes'` -- the same misattribution one frame further out.
        await connection.rollback()
        await _restore_enforcement(connection, entry_enforcement)
        raise
    except Exception as e:
        await connection.rollback()
        await _restore_enforcement(connection, entry_enforcement)
        raise MigrationError(
            f"Migration failed: {e}", original_error=e, operation=NOTHING, sql=NOTHING
        ) from e


_WHEN_DRY_RUN: dict[
    bool,
    Callable[[DbapiConnection, list[Operation], list[int]], Awaitable[ApplyResult]],
] = {
    True: _preview,
    False: _run_batch,
}


@helper
async def _execute_with_reconstruction(connection: DbapiConnection, operation: Operation) -> None:
    """
    Execute an operation using table reconstruction (async).

    Fresh introspection is performed before each reconstruction to ensure
    we have the latest schema state. Uses specialized functions for
    single-property changes when possible.
    """
    from declaro_persistum.abstractions.table_reconstruction import (
        _get_full_table_schema,
    )

    table = operation["table"]

    # Fresh introspection for current state (includes FKs + unique constraints)
    columns = await _get_full_table_schema(connection, table)

    # Apply reconstruction changes (pure)
    columns = apply_reconstruction_changes(columns, operation)

    # Use specialized functions for single-property alter_column changes
    single = single_change_property(operation)
    await _WHEN_ONE_PROPERTY[single is not None](connection, table, columns, operation, single)


@helper
async def _alter_one_property(
    connection: DbapiConnection,
    table: str,
    _columns: dict[str, Any],
    operation: Operation,
    single: tuple[str, Any] | None,
) -> None:
    """One property changed, so SQLite has a single ALTER that does it.

    `single_change_property` already refuses any key outside the three this
    table holds, so the lookup here cannot miss. It used to be read
    with `.get` and the result compared against None, which is a second answer
    to a question the first function had already answered: the miss it guarded
    was unreachable, and had it ever happened the operation would have fallen
    through to a rebuild rather than saying so.
    """

    # TWO OF THE THREE ENTRIES WERE THE SAME FUNCTION with a different word
    # written in one place, so the word is the argument now. `default` keeps
    # its own entry because removing a default deletes the key rather than
    # setting it to None, and a column with `default: None` is a different
    # declaration from a column with no default.
    change_key, value = single  # type: ignore[misc]
    column = operation["details"]["column"]
    await _ALTER_ONE_PROPERTY[change_key == "default"](connection, table, column, change_key, value)


@helper
async def _set_the_property(
    connection: DbapiConnection, table: str, column: str, prop: str, value: Any
) -> None:
    """A property SQLite rebuilds the table to change."""
    from declaro_persistum.abstractions.table_reconstruction import (
        alter_column_property,
    )

    await alter_column_property(connection, table, column, prop, value)


@helper
async def _set_or_remove_the_default(
    connection: DbapiConnection, table: str, column: str, _prop: str, value: Any
) -> None:
    """The default, which is removed rather than set when it goes away."""
    from declaro_persistum.abstractions.table_reconstruction import (
        alter_column_default,
    )

    await alter_column_default(connection, table, column, value)


# Whether this is the default, which is the one property that can be removed.
_ALTER_ONE_PROPERTY: dict[bool, Callable[..., Awaitable[None]]] = {
    True: _set_or_remove_the_default,
    False: _set_the_property,
}


@helper
async def _rebuild_the_table(
    connection: DbapiConnection,
    table: str,
    columns: dict[str, Any],
    _operation: Operation,
    _single: tuple[str, Any] | None,
) -> None:
    """More than one property changed, or none SQLite can alter in place."""
    from declaro_persistum.abstractions.table_reconstruction import reconstruct_table

    await reconstruct_table(
        connection,
        table,
        columns,
        preserve_data=NOTHING,
        manage_foreign_keys=NOTHING,
    )


# WHETHER ONE ALTER DOES IT, OR THE TABLE IS REBUILT. Both entries run the
# operation and return None.
_WHEN_ONE_PROPERTY: dict[
    bool,
    Callable[
        [DbapiConnection, str, dict[str, Any], Operation, tuple[str, Any] | None],
        Awaitable[None],
    ],
] = {
    True: _alter_one_property,
    False: _rebuild_the_table,
}


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


# `generate_enum_check(column, enum)` and `generate_column_sql(name, col,
# enums=None)` stood here, 57 lines implementing a CHECK-constraint enum:
#
#     CHECK (status IN ('active', 'inactive', 'pending'))
#
# THAT IS THE DESIGN THIS PACKAGE ABANDONED. A `Literal[...]` column becomes a
# `_dp_enum_` lookup table plus a foreign key precisely to avoid the CHECK
# portability problems, and adding a value to a CHECK means rebuilding the
# table on SQLite.
#
# Both were unreachable. `generate_column_sql` had no caller anywhere in
# `src/`, `generate_enum_check` was called only by it, and no loader has ever
# produced the `enum:name` column type they both keyed on. The only thing
# holding them alive was two tests in `test_enums.py`, a file the hollow-file
# ratchet listed as empty -- so the tests could not run and the code could not
# be reached, and each looked like the other's justification.
#
# The live enum path is `insert_enum_values`, tested in
# `test_a_declared_enum_value_is_accepted.py` against all three engines.
# `generate_create_trigger(table, trigger)` stood here. Nothing in `src/`
# called it -- the only other mention is the line in this module's own
# docstring listing what the class used to hold -- and triggers are not a
# feature of this package: `types.Trigger` is declared, nothing exports it,
# the loader cannot read one, the differ emits no trigger operation, and
# `_SQL_GENERATORS` has none of the 16 either.
#
# It and `tests/unit/test_triggers.py` were each other's only reason to exist,
# the same pair `generate_enum_check` formed with `test_enums.py` earlier
# today, and a hollow test file is what kept both invisible.
@helper
def generate_create_view(view: View) -> list[str]:
    """
    Generate SQLite CREATE VIEW SQL.

    For materialized views, uses table-based emulation with metadata tracking.

    Args:
        view: View definition

    Returns:
        SQL statement (or semicolon-separated statements for materialized views)
    """
    return _CREATE_VIEW[bool(view.get("materialized", False))](view)


@helper
def _plain_view_statements(view: View) -> list[str]:
    """A plain view is only a query, so one statement creates it."""
    return [f'CREATE VIEW IF NOT EXISTS "{view["name"]}" AS\n{view["query"]}']


# WHAT EACH POSTGRESQL REFRESH STRATEGY IS CALLED HERE. A strategy this table
# does not name keeps the name it arrived with.
_SQLITE_REFRESH: dict[str, str] = {
    "on_demand": "manual",
    "on_commit": "manual",
}

# The strategies that watch a source table and refresh when it changes.
_TRIGGERED_REFRESH = frozenset(("trigger", "hybrid"))


@helper
def _sqlite_refresh(refresh: str) -> str:
    """The SQLite name for a declared refresh strategy."""
    renamed = [_SQLITE_REFRESH[key] for key in [refresh] if key in _SQLITE_REFRESH]
    return [*renamed, refresh][0]


@helper
def _materialized_view_statements(view: View) -> list[str]:
    """A materialized view here is a table, plus the triggers that refresh it."""
    from declaro_persistum.abstractions.materialized_views import (
        create_matview_sql,
        generate_refresh_trigger_sql,
    )

    name = view["name"]
    query = view["query"]
    refresh = _sqlite_refresh(view.get("refresh", "manual"))

    statements = create_matview_sql(
        name=name,
        query=query,
        refresh_strategy=refresh,  # type: ignore[arg-type]
        depends_on=view.get("depends_on"),
    )

    # THE TRIGGERS, WRITTEN ONLY FOR A STRATEGY THAT USES THEM. A strategy
    # outside the table above, or a view naming no source table, leaves this
    # list empty.
    triggers = [
        statement
        for source in (view.get("trigger_sources") or [])
        if refresh in _TRIGGERED_REFRESH
        for statement in generate_refresh_trigger_sql(
            matview_name=name,
            source_table=source,
            query=query,
            events=NOTHING,
        )
    ]

    return [*statements, *triggers]


_CREATE_VIEW: dict[bool, Callable[[View], list[str]]] = {
    True: _materialized_view_statements,
    False: _plain_view_statements,
}


@helper
def generate_drop_view(name: str, materialized: bool) -> list[str]:
    """The statements that remove a view, or the table standing in for one.

    A MATERIALIZED VIEW HERE IS A TABLE. The emulation builds one with CREATE
    TABLE, so `DROP VIEW` could not remove it: the object survived, its
    registry row survived, and the schema reported a view the database no
    longer had.

    THE NAME IS QUOTED. It was interpolated bare, so a view called `order` or
    `Select` produced a syntax error while every other generator here quoted.

    `materialized` IS REQUIRED AND HAS NO DEFAULT. The caller reads it off the
    operation the differ emitted, so it is always known, and a default would
    let a forgetful caller take the plain branch and leave a table behind.

    `drop_matview_sql` ALREADY BUILT THE MATERIALIZED HALF. An earlier draft
    of this wrote its own DROP TABLE and DELETE, then had to invent a CREATE
    IF NOT EXISTS for a registry that need not exist, which is machinery for a
    problem the existing function does not have: it only runs when a
    materialized view is being dropped, and that implies the registry.

    WHAT THIS DOES NOT DO. A materialized view refreshed by trigger has three
    triggers for each watched table, and the drop operation carries no record
    of which tables were watched, so they are left behind and fire against a
    table that is gone. `drop_refresh_triggers_sql` builds those statements
    and needs the source list, which the differ does not put in the details.
    """
    return _DROP_VIEW[materialized](name)


@helper
def _drop_materialized_view(name: str) -> list[str]:
    """The table standing in for the view, and its registry row."""
    from declaro_persistum.abstractions.materialized_views import drop_matview_sql

    return drop_matview_sql(name)


@helper
def _drop_plain_view(name: str) -> list[str]:
    """A plain view is only a query, so one statement removes it."""
    return [f'DROP VIEW IF EXISTS "{name}"']


_DROP_VIEW: dict[bool, Callable[[str], list[str]]] = {
    True: _drop_materialized_view,
    False: _drop_plain_view,
}


# WHAT THE DISPATCH TABLES READ OFF THIS MODULE. `APPLIERS` takes `apply`
# and `SQL_GENERATORS` takes `generate_operation_sql`, which nothing here
# calls, so a linter reads it as an unused import and removing it breaks
# `declaro generate` on SQLite.
__all__ = [
    "apply",
    "generate_create_view",
    "generate_drop_view",
    "generate_operation_sql",
]
