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

from typing import Literal

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


@boundary
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
    # THE VISIBLE RESOLVE. A default would hide this inside the
    # signature, where no reader sees it happen and no test reaches it.
    dry_run = resolved(dry_run, False)
    if dry_run:
        return dry_run_preview(operations, execution_order)

    executed: list[str] = []

    # WHAT THE CALLER HAD, so it can be given back. Read BEFORE anything else
    # runs, because the first statement of the loop opens a transaction and
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
        for op_idx in execution_order:
            operation = operations[op_idx]

            try:
                if requires_reconstruction(operation):
                    # Execute with reconstruction
                    await _execute_with_reconstruction(connection, operation)
                    executed.append(f"Table reconstruction for {operation['table']}")
                else:
                    # Direct SQL execution
                    # THE LIST IS EXECUTED AS IT WAS BUILT. This took a joined
                    # string and cut it with `split(";")`, which is wrong inside a
                    # trigger body and inside any string literal holding one.
                    statements = statements_for(operation)
                    for statement in statements:
                        await connection.execute(statement)
                    executed.extend(statements)

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
                    "Failed to execute operation",
                    operation=operation,
                    original_error=e, sql=NOTHING) from e

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
        # The outer twin of the guard inside the loop. Without it the KeyError
        # escapes the per-operation wrapper and is caught here instead, coming
        # back as `MigrationError: Migration failed: 'changes'` -- the same
        # misattribution one frame further out.
        await connection.rollback()
        await _restore_enforcement(connection, entry_enforcement)
        raise
    except Exception as e:
        await connection.rollback()
        await _restore_enforcement(connection, entry_enforcement)
        raise MigrationError(
            f"Migration failed: {e}",
            original_error=e, operation=NOTHING, sql=NOTHING) from e


@helper
async def _execute_with_reconstruction(
    connection: DbapiConnection, operation: Operation
) -> None:
    """
    Execute an operation using table reconstruction (async).

    Fresh introspection is performed before each reconstruction to ensure
    we have the latest schema state. Uses specialized functions for
    single-property changes when possible.
    """
    from declaro_persistum.abstractions.table_reconstruction import (
        _get_full_table_schema,
        alter_column_default,
        alter_column_nullability,
        alter_column_type,
        reconstruct_table,
    )

    table = operation["table"]

    # Fresh introspection for current state (includes FKs + unique constraints)
    columns = await _get_full_table_schema(connection, table)

    # Apply reconstruction changes (pure)
    columns = apply_reconstruction_changes(columns, operation)

    # Use specialized functions for single-property alter_column changes
    single = single_change_property(operation)
    if single is not None:
        change_key, val = single
        column = operation["details"]["column"]
        _SPECIALIZED = {
            "nullable": alter_column_nullability,
            "type": alter_column_type,
            "default": alter_column_default,
        }
        handler = _SPECIALIZED.get(change_key)
        if handler is not None:
            await handler(connection, table, column, val)
            return

    # General reconstruction
    await reconstruct_table(connection, table, columns, preserve_data=NOTHING, manage_foreign_keys=NOTHING)






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
    from declaro_persistum.abstractions.materialized_views import (
        create_matview_sql,
        generate_refresh_trigger_sql,
    )

    name = view["name"]
    query = view["query"]
    materialized = view.get("materialized", False)

    if not materialized:
        return [f'CREATE VIEW IF NOT EXISTS "{name}" AS\n{query}']

    # Use table-based emulation for materialized views
    refresh = view.get("refresh", "manual")
    # Map PostgreSQL strategies to SQLite strategies
    if refresh in ("on_demand", "on_commit"):
        refresh = "manual"

    depends_on = view.get("depends_on")
    trigger_sources = view.get("trigger_sources")

    statements = create_matview_sql(
        name=name,
        query=query,
        refresh_strategy=refresh,  # type: ignore[arg-type]
        depends_on=depends_on,
    )

    # Add triggers if trigger-based refresh
    if refresh in ("trigger", "hybrid") and trigger_sources:
        for source in trigger_sources:
            statements.extend(
                generate_refresh_trigger_sql(
                    matview_name=name,
                    source_table=source,
                    query=query, events=NOTHING)
            )

    return statements


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
    from declaro_persistum.abstractions.materialized_views import drop_matview_sql

    if materialized:
        return drop_matview_sql(name)
    return [f'DROP VIEW IF EXISTS "{name}"']


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
