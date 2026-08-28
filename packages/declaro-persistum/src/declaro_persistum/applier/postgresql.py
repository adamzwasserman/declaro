"""Apply migration operations to PostgreSQL. Functions, not a class.

De-classed mechanically: methods lifted out, dedented, `self` removed. Every
statement PostgreSQL is asked to run is byte-for-byte what it was.

PostgreSQL supports transactional DDL, so every operation is wrapped in a
single transaction and the batch is atomic. That is the one thing this applier
can promise which the SQLite and Turso ones cannot.
"""

from collections.abc import Callable
from typing import Any

from declaro_persistum.applier.rendering import (
    statements_for as render_statements,
)
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import (
    NOTHING,
    ApplyResult,
    FetchingConnection,
    Nothing,
    Operation,
    resolved,
)

# Type for SQL generator functions


@helper
def get_dialect() -> str:
    """Return dialect identifier."""
    return "postgresql"


@helper
def get_transaction_mode() -> str:
    """One failure ends the batch, and the whole batch is rolled back.

    THE OTHER TWO APPLIERS HAVE DECLARED THIS ALL ALONG AND THIS ONE DID NOT.
    `usage.md` documents importing it from here, and that import raised
    ImportError. A note in `test_every_cli_command_can_be_called` recorded the
    absence on 2026-08-17, alongside `generate_sql` missing from the other
    two: a five-member Protocol that no module satisfied, deleted rather than
    filled. Deleting it left this hole documented and open.

    `all_or_nothing` is what this module already does. `apply` runs inside
    `connection.transaction()` and re-raises, so an operation that fails takes
    the batch with it and leaves nothing behind. Turso is the odd one and says
    `per_operation`, because it cannot perform several operations the differ
    emits and refusing a batch for one of them would block the rest forever.
    """
    return "all_or_nothing"


@helper
async def apply(
    connection: FetchingConnection,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool | Nothing,
) -> ApplyResult:
    """
    Apply migration operations within a transaction.

    PostgreSQL's transactional DDL means we can safely roll back
    all changes if any operation fails.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    dry_run = resolved(dry_run, False)
    # ONE ENTRY PER OPERATION, each holding that operation's statements. The
    # dry run prints them joined; the run below executes them one at a time,
    # because an operation is sometimes more than one statement.
    per_operation = [statements_for(operations[i]) for i in execution_order]
    sql_statements = [";\n".join(group) for group in per_operation]

    # WHETHER THE STATEMENTS ARE PRINTED OR RUN. Both entries take the same
    # five arguments and hand back the same shape of result.
    return await _WHEN_DRY_RUN[bool(dry_run)](
        connection, operations, execution_order, per_operation, sql_statements
    )


@helper
async def _preview(
    _connection: FetchingConnection,
    _operations: list[Operation],
    _execution_order: list[int],
    _per_operation: list[list[str]],
    sql_statements: list[str],
) -> ApplyResult:
    """A dry run asks the database nothing, so the statements are the result."""
    return {
        "success": True,
        "executed_sql": sql_statements,
        "operations_applied": len(sql_statements),
        "error": None,
        "error_operation": None,
    }


@boundary
async def _executed_statements(connection: FetchingConnection, statements: list[str]) -> list[str]:
    """Run each statement in order and hand back the ones that reached the engine.

    One result comes back per statement, so the count of results is the count
    of statements that ran. A statement the engine refuses takes the
    comprehension with it and nothing comes back at all.
    """
    cursors = [await connection.execute(statement) for statement in statements]
    return statements[: len(cursors)]


@boundary
async def _execute_operation(
    connection: FetchingConnection,
    operations: list[Operation],
    execution_order: list[int],
    per_operation: list[list[str]],
    sql_statements: list[str],
    position: int,
) -> str:
    """Run one operation's statements and hand back the block that ran."""
    sql = sql_statements[position]
    try:
        return ";\n".join(await _executed_statements(connection, per_operation[position]))
    except Exception as e:
        raise MigrationError(
            f"Failed to execute operation {position + 1}/{len(execution_order)}",
            operation=operations[execution_order[position]],
            sql=sql,
            original_error=e,
        ) from e


@boundary
async def _run_batch(
    connection: FetchingConnection,
    operations: list[Operation],
    execution_order: list[int],
    per_operation: list[list[str]],
    sql_statements: list[str],
) -> ApplyResult:
    """Every operation inside one transaction, so the batch is atomic."""
    try:
        # Start transaction
        async with connection.transaction():
            executed = [
                await _execute_operation(
                    connection,
                    operations,
                    execution_order,
                    per_operation,
                    sql_statements,
                    position,
                )
                for position in range(len(execution_order))
            ]

        return {
            "success": True,
            "executed_sql": executed,
            "operations_applied": len(executed),
            "error": None,
            "error_operation": None,
        }

    except MigrationError:
        # Transaction automatically rolled back
        raise
    except Exception as e:
        raise MigrationError(
            f"Migration failed: {e}", original_error=e, operation=NOTHING, sql=NOTHING
        ) from e


_WHEN_DRY_RUN: dict[bool, Callable[..., Any]] = {
    True: _preview,
    False: _run_batch,
}


# `generate_sql` was here and nowhere else, so `declaro generate` worked on
# PostgreSQL and raised AttributeError on SQLite and Turso. The loop it ran is
# dialect-independent, so it now lives once in `applier/__init__.py` beside the
# `SQL_GENERATORS` table, and this module keeps only what varies:
# `generate_operation_sql`.


@helper
def statements_for(operation: Operation) -> list[str]:
    """The statements an operation runs on PostgreSQL, already separated.

    THIS MODULE NO LONGER KEEPS ITS OWN TABLE OF SIXTEEN GENERATORS. The
    engines' statements sit side by side in `applier/templates.py`, where the
    four places PostgreSQL differs from SQLite are visible rather than being
    a difference between two files nobody read together.
    """
    return render_statements(operation, "postgresql")


@helper
def generate_operation_sql(operation: Operation) -> str:
    """The same statements as one block, for a person to read."""
    return ";\n".join(statements_for(operation))


# A `to: None` MEANS THE DEFAULT IS GONE, which is DROP DEFAULT rather than
# `SET DEFAULT None`.


# WHICH PROPERTIES THIS RENDERER WRITES, in the order it writes them. The
# order of the statements is the order of this table.


# WHAT EACH CONSTRAINT TYPE IS WRITTEN AS. Two entries, read by subscript.


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


# HOISTED OUT OF THE FUNCTION. This table was a local variable rebuilt on
# every call, so no gate could read it and it silently fell one operation
# behind the table in `shared.py`. `declaro` emitted `insert_enum_values`, this
# module answered `Unknown operation type`, and the SQLite tests were green.
#
# `insert_enum_values` renders identically on every engine -- it is an INSERT
# of literal values into a two-column table -- so the entry points at the one
# in `shared.py` rather than at a fourth copy.
