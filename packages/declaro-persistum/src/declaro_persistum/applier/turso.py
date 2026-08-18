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

import contextlib
import logging
from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    columns_from_pragma_rows,
    dry_run_preview,
    generate_operation_sql,
    requires_reconstruction,
)

# Turso uses identical view generation to SQLite.
from declaro_persistum.applier.sqlite import generate_create_view, generate_drop_view
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE, MigrationError
from declaro_persistum.types import ApplyResult, Operation

logger = logging.getLogger(__name__)


def get_dialect() -> str:
    """Return dialect identifier."""
    return "turso"


def get_transaction_mode() -> Literal["all_or_nothing", "per_operation"]:
    """Per-operation: each op gets its own transaction."""
    return "per_operation"



async def _restore_enforcement(connection: Any, enforced: int) -> None:
    """Put `PRAGMA foreign_keys` back to what the caller had.

    Only meaningful outside a transaction, which is why every call site is
    after a commit or a rollback.
    """
    await connection.execute(f"PRAGMA foreign_keys = {'ON' if enforced else 'OFF'}")



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


async def apply(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool = False,
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
    if dry_run:
        return dry_run_preview(operations, execution_order)

    executed: list[str] = []
    skipped: list[str] = []

    # Coalesce reconstruction ops by table — multiple alter_column ops
    # on the same table must merge into one reconstruction pass to avoid
    # sqlite_autoindex collisions from creating the temp table twice.
    coalesced_ops: list[tuple[str, bool, list[Operation]]] = []
    reconstruction_groups: dict[str, list[Operation]] = {}

    for op_idx in execution_order:
        operation = operations[op_idx]
        if requires_reconstruction(operation):
            table = operation["table"]
            if table not in reconstruction_groups:
                reconstruction_groups[table] = []
                # Insert placeholder in execution order
                coalesced_ops.append((table, True, reconstruction_groups[table]))
            reconstruction_groups[table].append(operation)
        else:
            coalesced_ops.append(("", False, [operation]))

    for table_or_empty, is_reconstruction, ops in coalesced_ops:
        if is_reconstruction:
            op_desc = f"reconstruct {table_or_empty} ({len(ops)} ops)"
        else:
            op_desc = f"{ops[0]['op']} on {ops[0].get('table', 'N/A')}"

        try:
            if is_reconstruction:
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
                entry_enforcement = row[0] if row else 1

                # PRAGMA foreign_keys must be set OUTSIDE a transaction —
                # setting it inside implicitly commits, breaking atomicity.
                await connection.execute("PRAGMA foreign_keys = OFF")

            await connection.execute("BEGIN")

            if is_reconstruction:
                await _execute_coalesced_reconstruction(
                    connection, table_or_empty, ops
                )
                executed.append(f"Table reconstruction for {table_or_empty}")
            else:
                operation = ops[0]
                sql = generate_operation_sql(operation)
                for statement in sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        await connection.execute(statement)
                executed.append(sql)

            await connection.commit()

            if is_reconstruction:
                await _restore_enforcement(connection, entry_enforcement)

            logger.info(f"Applied: {op_desc}")

        except A_BUG_IN_THIS_PACKAGE:
            # OURS, AND NOTHING RAN. A KeyError here means the operation does
            # not match what the differ emits, so no statement reached the
            # engine and no tmp table was created. Reporting it as "Table
            # reconstruction failed ... Check for orphaned _declaro_tmp tables"
            # sends an operator hunting for tables that cannot exist.
            #
            # Measured 2026-08-17 with an operation missing its `changes` key.
            # The bug we are re-raising matters more than a rollback that
            # also failed, so this suppresses rather than masks: nothing here
            # can replace the exception on its way out.
            with contextlib.suppress(Exception):
                await connection.rollback()
            raise
        except Exception as e:
            try:
                await connection.rollback()
            except Exception as rollback_exc:
                logger.warning(
                    "Rollback failed after '%s' failed: %s. The connection "
                    "may still hold an open transaction.",
                    op_desc, rollback_exc,
                )
            if is_reconstruction:
                try:
                    await _restore_enforcement(connection, entry_enforcement)
                except Exception as fk_exc:
                    # Leaving FK enforcement off silently is worse than the
                    # original failure: later writes would be accepted
                    # against constraints nobody is checking.
                    logger.error(
                        "Could not re-enable foreign keys after '%s' "
                        "failed: %s. THIS CONNECTION NOW HAS FOREIGN KEY "
                        "ENFORCEMENT OFF.",
                        op_desc, fk_exc,
                    )
            # Reconstruction failures are catastrophic (orphaned tables) —
            # never skip them. Only skip non-reconstruction ops.
            if is_reconstruction:
                raise MigrationError(
                    f"Table reconstruction failed for {table_or_empty}. "
                    f"Check for orphaned _declaro_tmp tables.",
                    operation=ops[0],
                    original_error=e,
                ) from e
            skip_msg = f"{op_desc}: {e}"
            skipped.append(skip_msg)
            logger.warning(f"Skipped unsupported operation: {skip_msg}")

    success = migration_succeeded(skipped=len(skipped))
    error_msg = None
    if skipped:
        error_msg = f"{len(skipped)} operation(s) skipped: {'; '.join(skipped)}"
        if executed:
            logger.info(
                f"Migration partial: {len(executed)} applied, {len(skipped)} skipped"
            )

    return {
        "success": success,
        "executed_sql": executed,
        "operations_applied": len(executed),
        "error": error_msg,
        "error_operation": None,
    }


async def _execute_coalesced_reconstruction(
    connection: Any, table: str, ops: list[Operation]
) -> None:
    """
    Apply multiple reconstruction ops to the same table in one pass.

    Introspects once, applies all column changes, reconstructs once.
    """
    from declaro_persistum.abstractions.reconstruction import execute_reconstruction_async

    cursor = await connection.execute(f"PRAGMA table_info('{table}')")
    rows = await cursor.fetchall()
    columns = columns_from_pragma_rows(rows)

    for op in ops:
        columns = apply_reconstruction_changes(columns, op)

    await execute_reconstruction_async(
        connection, table, columns, manage_foreign_keys=False
    )







# "TursoApplier" was listed here long after the class was deleted, so a star
# import raised AttributeError while every other check stayed green. mypy's
# name-defined does not read __all__; ruff's F822 does, and now gates it.
__all__ = ["generate_create_view", "generate_drop_view"]
