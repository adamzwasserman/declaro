"""Apply migration operations to Turso. Functions, not a class.

Turso aims at SQLite compatibility but is a separate Rust engine, so this is
not the SQLite applier pointed elsewhere: unsupported operations are skipped
rather than attempted, reconstruction uses temporary names that cannot collide
with sqlite_autoindex, and each operation runs in its own transaction.

SQL generation is shared with SQLite through `applier.shared`; only the
execution differs.

PER-OPERATION TRANSACTIONS, and that is a deliberate trade. Each operation gets
its own BEGIN/COMMIT, and a failure is logged and skipped rather than aborting
the batch, so one unsupported operation such as ADD FOREIGN KEY does not block
a valid one such as ADD COLUMN. The cost is that a partly-applied batch is a
real outcome here, which it is not on PostgreSQL.
"""

from collections.abc import Awaitable, Callable
from functools import reduce
from typing import Any

from declaro_persistum.applier.facts import FactName, fact_of
from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    dry_run_preview,
    generate_operation_sql,
    requires_reconstruction,
    statements_for,
)

# Turso uses identical view generation to SQLite.
from declaro_persistum.applier.sqlite import generate_create_view, generate_drop_view
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import (
    A_BUG_IN_THIS_PACKAGE,
    MigrationError,
    also_failed,
)
from declaro_persistum.reporting import say
from declaro_persistum.types import (
    NOTHING,
    ApplyResult,
    DbapiConnection,
    Nothing,
    Operation,
    is_nothing,
    resolved,
)


@boundary
async def _restore_enforcement(connection: DbapiConnection, enforced: int) -> int:
    """Put `PRAGMA foreign_keys` back to what the caller had.

    Only meaningful outside a transaction, which is why every call site is
    after a commit or a rollback.

    Hands back the value it wrote, so a caller can count the restores it made
    rather than inferring them from the absence of an exception.
    """
    await connection.execute(f"PRAGMA foreign_keys = {'ON' if enforced else 'OFF'}")
    return enforced


@helper
def migration_succeeded(*, skipped: int) -> bool:
    """A migration is all of it, or it is not done.

    THIS WAS `len(executed) > 0 or len(skipped) == 0`, inline. Measured
    2026-08-17 by evaluating that expression:

        executed=3 skipped=0  -> True
        executed=3 skipped=2  -> True    two operations did not happen
        executed=1 skipped=5  -> True    five did not happen
        executed=0 skipped=2  -> False

    So one operation landing made the whole run a success, whatever else was
    skipped. A caller reading `success` stamps the schema as current, and the
    skip-if-clean check then makes that permanent: the next boot compares
    hashes, matches, and never re-proposes the operations that never ran.

    `skipped` is still returned, so a caller that wants to tolerate skips can
    look and decide. What it must not do is have that decision made for it by
    an expression it cannot see.

    THERE IS NO `executed` PARAMETER, and its absence is the point. The old
    rule consulted it and that is what made one landed operation excuse any
    number of missing ones. Every operation either executes, is skipped, or
    raises, so "nothing was skipped" is the whole question and a count of
    successes cannot change the answer.
    """
    return skipped == 0


@helper
async def apply(
    connection: DbapiConnection,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool | Nothing,
) -> ApplyResult:
    """
    Apply migration operations asynchronously.

    Each operation runs in its own transaction. Failed operations
    are rolled back individually and skipped — remaining operations
    continue. This prevents one unsupported operation from blocking
    the entire migration batch.

    Args:
        connection: TursoAsyncConnection (async wrapper over pyturso)
        operations: List of operations to apply
        execution_order: Order to execute operations
        dry_run: If True, only generate SQL without executing
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
def _leads_its_group(position: int, operation: Operation, ordered: list[Operation]) -> bool:
    """Whether this operation is the one its unit is placed at.

    A rebuild that a table has already had earlier in the batch belongs to
    that earlier unit, so it starts no unit of its own. Everything else does.
    """
    earlier = [
        prior
        for prior in ordered[:position]
        if requires_reconstruction(prior) and prior["table"] == operation["table"]
    ]
    return not (requires_reconstruction(operation) and earlier)


@helper
def _rebuild_group(
    operation: Operation, ordered: list[Operation]
) -> tuple[str, bool, list[Operation]]:
    """Every reconstruction operation on this table, gathered into one unit."""
    table = operation["table"]
    return (
        table,
        True,
        [
            candidate
            for candidate in ordered
            if requires_reconstruction(candidate) and candidate["table"] == table
        ],
    )


@helper
def _single_group(
    operation: Operation, _ordered: list[Operation]
) -> tuple[str, bool, list[Operation]]:
    """An operation Turso runs directly, which stands on its own."""
    return ("", False, [operation])


_GROUP_FOR: dict[
    bool, Callable[[Operation, list[Operation]], tuple[str, bool, list[Operation]]]
] = {
    True: _rebuild_group,
    False: _single_group,
}


@helper
def coalesced_operations(
    operations: list[Operation], execution_order: list[int]
) -> list[tuple[str, bool, list[Operation]]]:
    """The batch as units of work, with each table's rebuilds merged into one.

    Several alter_column operations on one table have to become a single
    reconstruction pass. Creating the temporary table twice collides with
    sqlite_autoindex, so the second pass fails on a table the first one had
    already rewritten.

    A merged unit sits where the table's FIRST reconstruction operation sat,
    which keeps the differ's ordering.
    """
    ordered = [operations[op_idx] for op_idx in execution_order]
    return [
        _GROUP_FOR[requires_reconstruction(operation)](operation, ordered)
        for position, operation in enumerate(ordered)
        if _leads_its_group(position, operation, ordered)
    ]


@helper
def _rebuild_description(table: str, ops: list[Operation]) -> str:
    """What a rebuild is called in the log."""
    return f"reconstruct {table} ({len(ops)} ops)"


@helper
def _operation_description(_table: str, ops: list[Operation]) -> str:
    """What a directly-run operation is called in the log."""
    return f"{ops[0]['op']} on {ops[0].get('table', 'N/A')}"


_DESCRIBED: dict[bool, Callable[[str, list[Operation]], str]] = {
    True: _rebuild_description,
    False: _operation_description,
}


@boundary
async def _rolled_back(connection: DbapiConnection) -> Exception | Nothing:
    """Undo what the failed operation had done, and RETURN what stopped it.

    A rollback that fails leaves the connection holding an open transaction,
    which is the thing the next caller of that connection needs to know. It
    used to be written to a log as a warning and dropped, and the two places
    that wrapped this in `contextlib.suppress` said why: nothing here may
    replace the exception already traveling.

    That was true and it is no longer the only option. The failure comes back
    as a value and rides out as a note on whatever is being raised, so the
    original exception keeps its type and its message and the caller still
    learns the transaction is open.
    """
    try:
        await connection.rollback()
        return NOTHING
    except Exception as rollback_exc:
        return rollback_exc


@boundary
async def _enforcement_restored(
    connection: DbapiConnection, entry_enforcement: list[int]
) -> Exception | Nothing:
    """Put `PRAGMA foreign_keys` back, and RETURN what stopped it.

    THE LIST HOLDS THE CALLER'S VALUE ONLY ONCE IT HAS BEEN READ, and that is
    what makes it a list. This was a plain local assigned inside the
    reconstruction path, so a failure in the very statement that reads the
    pragma left the name unbound and the restore below raised
    UnboundLocalError, which the handler then logged as "could not re-enable
    foreign keys" -- a report about the database for a mistake of ours.
    Nothing had been turned off at that point, so there is nothing to put
    back and the count is zero.

    RETURNS THE FAILURE RATHER THAN LOGGING IT. This cannot raise, because the
    operation's own failure is already traveling and would be replaced. So it
    used to log an error and return a count of 0, and the single caller
    discarded the count. Leaving enforcement off is worse than the original
    failure, since later writes are accepted against constraints nobody is
    checking, and that was reported only to a log no caller can read.
    """
    try:
        _restored = [await _restore_enforcement(connection, value) for value in entry_enforcement]
        return NOTHING
    except Exception as fk_exc:
        return fk_exc


@boundary
async def _run_rebuild(
    connection: DbapiConnection, table: str, ops: list[Operation], op_desc: str
) -> tuple[list[str], list[str]]:
    """Every reconstruction operation on one table, in one pass.

    A rebuild that fails is never skipped. It can leave an orphaned temporary
    table behind, so the batch stops and the caller is told which table.
    """
    entry_enforcement: list[int] = []
    try:
        # WHAT THE CALLER HAD, read before we change it. Restoring
        # means putting this back, not turning enforcement on: a caller
        # who disabled it to do a bulk load has not asked for it to
        # come back underneath them.
        #
        # Measured 2026-08-17 against a local Turso database, before
        # this was recorded:
        #
        #     started 1 -> success=True -> ended 1
        #     started 0 -> success=True -> ended 1   <- not theirs
        cursor = await connection.execute("PRAGMA foreign_keys")
        row = await cursor.fetchone()
        entry_enforcement.append(row[0] if row else 1)

        # PRAGMA foreign_keys must be set OUTSIDE a transaction —
        # setting it inside implicitly commits, breaking atomicity.
        await connection.execute("PRAGMA foreign_keys = OFF")

        await connection.execute("BEGIN")
        await _execute_coalesced_reconstruction(connection, table, ops)
        await connection.commit()
        await _restore_enforcement(connection, entry_enforcement[0])

        say(__name__, "info", f"Applied: {op_desc}")
        return ([f"Table reconstruction for {table}"], [])

    except A_BUG_IN_THIS_PACKAGE as ours:
        # OURS, AND NOTHING RAN. A KeyError here means the operation does
        # not match what the differ emits, so no statement reached the
        # engine and no tmp table was created. Reporting it as "Table
        # reconstruction failed ... Check for orphaned _declaro_tmp tables"
        # sends an operator hunting for tables that cannot exist.
        #
        # Measured 2026-08-17 with an operation missing its `changes` key.
        # The bug we are re-raising matters more than a rollback that also
        # failed, and it still does: the note leaves our exception's type and
        # message untouched, so this carries the rollback failure without
        # replacing anything.
        also_failed(
            ours,
            "rolling back after a bug in this package",
            await _rolled_back(connection),
        )
        raise
    except Exception as e:
        _open_transaction = await _rolled_back(connection)
        _left_unenforced = await _enforcement_restored(connection, entry_enforcement)
        raise also_failed(
            also_failed(
                MigrationError(
                    f"Table reconstruction failed for {table}. "
                    f"Check for orphaned _declaro_tmp tables.",
                    operation=ops[0],
                    original_error=e,
                    sql=NOTHING,
                ),
                f"rolling back after {op_desc!r} failed",
                _open_transaction,
            ),
            f"putting foreign key enforcement back after {op_desc!r} failed",
            _left_unenforced,
        ) from e


@helper
def _the_refusal_alone(op_desc: str, refused: Exception, _rollback: Any) -> str:
    """The engine refused the operation and the rollback worked."""
    return f"{op_desc}: {refused}"


@helper
def _the_refusal_and_an_open_transaction(op_desc: str, refused: Exception, rollback: Any) -> str:
    """The rollback failed too, so the connection is still mid-transaction."""
    return (
        f"{op_desc}: {refused}. Rolling back ALSO failed: "
        f"{type(rollback).__name__}: {rollback}. That connection may still "
        f"hold an open transaction."
    )


# WHETHER THE ROLLBACK ALSO FAILED. Both entries build the whole message, so
# the second is not a suffix bolted onto the first.
_SKIP_MESSAGE: dict[bool, Callable[[str, Exception, Any], str]] = {
    True: _the_refusal_and_an_open_transaction,
    False: _the_refusal_alone,
}


@boundary
async def _run_directly(
    connection: DbapiConnection, _table: str, ops: list[Operation], op_desc: str
) -> tuple[list[str], list[str]]:
    """One operation Turso runs as it stands.

    A failure here is skipped rather than fatal, so an operation this engine
    cannot perform does not block the rest of the batch. The skip is returned
    and reported; it never counts as success.
    """
    try:
        await connection.execute("BEGIN")
        # THE LIST IS EXECUTED AS IT WAS BUILT. This took a joined
        # string and cut it with `split(";")`, which is wrong inside a
        # trigger body and inside any string literal holding one.
        statements = statements_for(ops[0])
        cursors = [await connection.execute(statement) for statement in statements]
        await connection.commit()

        say(__name__, "info", f"Applied: {op_desc}")
        return (statements[: len(cursors)], [])

    except A_BUG_IN_THIS_PACKAGE as ours:
        # The twin of the guard in `_run_rebuild`, for the same reason: our
        # own mistake must not come back as an engine refusal, and a skipped
        # operation is exactly what a bug of ours must never become.
        also_failed(
            ours,
            "rolling back after a bug in this package",
            await _rolled_back(connection),
        )
        raise
    except Exception as e:
        # A ROLLBACK THAT ALSO FAILED GOES INTO THE SKIP MESSAGE. This is the
        # one path here that returns rather than raises, so there is no
        # exception to note it on, and the skip message is what the caller
        # reads. A connection still holding an open transaction belongs in it.
        open_transaction = await _rolled_back(connection)
        skip_msg = _SKIP_MESSAGE[not is_nothing(open_transaction)](op_desc, e, open_transaction)
        say(__name__, "warning", f"Skipped unsupported operation: {skip_msg}")
        return ([], [skip_msg])


_RUN_UNIT: dict[
    bool,
    Callable[
        [DbapiConnection, str, list[Operation], str],
        Awaitable[tuple[list[str], list[str]]],
    ],
] = {
    True: _run_rebuild,
    False: _run_directly,
}


@helper
def _nothing_skipped(_skipped: list[str]) -> str | None:
    """No error, because every operation ran.

    None rather than a count: this feeds `ApplyResult["error"]`, where None
    is what "no error" already means to every reader of that record.
    """
    return None  # noqa: RET501


@helper
def _something_skipped(skipped: list[str]) -> str:
    """Some operations did not run, so the result names every one of them."""
    return f"{len(skipped)} operation(s) skipped: {'; '.join(skipped)}"


_SKIP_SUMMARY: dict[bool, Callable[[list[str]], str | None]] = {
    True: _something_skipped,
    False: _nothing_skipped,
}


@helper
def _partial_note(executed: list[str], skipped: list[str]) -> str:
    """Say how much of the batch landed, when some of it did not."""
    note = f"Migration partial: {len(executed)} applied, {len(skipped)} skipped"
    say(__name__, "info", note)
    return note


@helper
def _no_partial_note(_executed: list[str], _skipped: list[str]) -> str:
    """Either everything ran or nothing did, so there is no partial to report."""
    return ""


_PARTIAL: dict[bool, Callable[[list[str], list[str]], str]] = {
    True: _partial_note,
    False: _no_partial_note,
}


@helper
async def _run_batch(
    connection: DbapiConnection,
    operations: list[Operation],
    execution_order: list[int],
) -> ApplyResult:
    """Each unit in its own transaction, so one refusal does not stop the rest."""
    outcomes = [
        await _RUN_UNIT[is_reconstruction](
            connection, table, ops, _DESCRIBED[is_reconstruction](table, ops)
        )
        for table, is_reconstruction, ops in coalesced_operations(operations, execution_order)
    ]

    executed = [entry for done, _ in outcomes for entry in done]
    skipped = [entry for _, missed in outcomes for entry in missed]

    _PARTIAL[bool(skipped and executed)](executed, skipped)

    return {
        "success": migration_succeeded(skipped=len(skipped)),
        "executed_sql": executed,
        "operations_applied": len(executed),
        "error": _SKIP_SUMMARY[bool(skipped)](skipped),
        "error_operation": None,
    }


_WHEN_DRY_RUN: dict[
    bool,
    Callable[[DbapiConnection, list[Operation], list[int]], Awaitable[ApplyResult]],
] = {
    True: _preview,
    False: _run_batch,
}


@helper
async def _execute_coalesced_reconstruction(
    connection: DbapiConnection, table: str, ops: list[Operation]
) -> None:
    """
    Apply multiple reconstruction ops to the same table in one pass.

    Introspects once, applies all column changes, reconstructs once.
    """
    from declaro_persistum.abstractions.reconstruction import execute_reconstruction_async
    from declaro_persistum.abstractions.table_reconstruction import (
        _get_full_table_schema,
    )

    # THE WHOLE TABLE, NOT ITS COLUMNS. This read `PRAGMA table_info` and
    # built the new table from that alone, and `table_info` returns columns
    # and nothing else, so every rebuild wrote back a table with its UNIQUE,
    # its CHECK and its foreign keys gone. Measured 2026-08-19, changing one
    # unrelated column's type:
    #
    #     before  "email" TEXT UNIQUE, "age" INTEGER CHECK (age > 0),
    #             "pid" INTEGER REFERENCES "parents"("id") ON DELETE CASCADE
    #     after   "email" TEXT, "age" INTEGER, "pid" INTEGER
    #
    # and the migration reported success. Changing a column's type on this
    # engine silently removed the referential integrity and the validation
    # from the whole table, and the next write that should have been refused
    # was accepted.
    #
    # `_get_full_table_schema` is what the SQLite applier has always used for
    # the same job, and it is why SQLite kept the unique and the foreign key.
    # EACH OPERATION IN TURN, EACH ONE STARTING FROM THE LAST ONE'S RESULT.
    # Three alter_columns on one table become one set of column definitions,
    # and the rebuild below writes that set once.
    columns = reduce(
        apply_reconstruction_changes,
        ops,
        await _get_full_table_schema(connection, table),
    )

    await execute_reconstruction_async(
        connection, table, columns, manage_foreign_keys=False, preserve_data=NOTHING
    )


# "TursoApplier" was listed here long after the class was deleted, so a star
# import raised AttributeError while every other check stayed green. mypy's
# name-defined does not read __all__; ruff's F822 does, and now gates it.
__all__ = [
    "apply",
    "generate_create_view",
    "generate_drop_view",
    # RE-EXPORTED, NOT UNUSED. `SQL_GENERATORS` in applier/__init__ reads
    # this name off this module, so nothing here calls it and removing it
    # breaks `declaro generate` on this engine.
    "generate_operation_sql",
]


def _reads(fact: FactName) -> Callable[[], Any]:
    """Bind this engine and one column, keeping a no-argument function."""

    def ask() -> Any:
        return fact_of(_ENGINE, fact)

    return ask


# THIS APPLIER'S OWN ROW, read from the one table rather than written again.
# Both names are bound at module level exactly as before, because the migration
# runner reaches them off the module and `applier/__init__.py` re-exports them.
_ENGINE = "turso"

get_engine = _reads("engine")
get_transaction_mode = _reads("transaction_mode")
