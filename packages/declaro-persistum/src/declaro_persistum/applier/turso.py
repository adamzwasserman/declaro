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

import logging
from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    columns_from_pragma_rows,
    dry_run_preview,
    enum_population_sql,
    generate_operation_sql,
    requires_reconstruction,
)

# Turso uses identical view generation to SQLite.
from declaro_persistum.applier.sqlite import generate_create_view, generate_drop_view
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Operation

logger = logging.getLogger(__name__)


def get_dialect() -> str:
    """Return dialect identifier."""
    return "turso"


def get_transaction_mode() -> Literal["all_or_nothing", "per_operation"]:
    """Per-operation: each op gets its own transaction."""
    return "per_operation"


async def apply(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool = False,
    target_schema: Any = None,
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
        target_schema: Target schema (used for enum value population)
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

                for insert_sql in enum_population_sql(operation, target_schema):
                    await connection.execute(insert_sql)
                    executed.append(insert_sql)

            await connection.commit()

            if is_reconstruction:
                await connection.execute("PRAGMA foreign_keys = ON")

            logger.info(f"Applied: {op_desc}")

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
                    await connection.execute("PRAGMA foreign_keys = ON")
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

    success = len(executed) > 0 or len(skipped) == 0
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
