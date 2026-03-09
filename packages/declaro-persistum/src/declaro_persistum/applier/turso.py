"""
Turso (libSQL) migration applier implementation.

Turso uses libSQL which is SQLite-compatible, so the SQL generation
is shared with SQLite via applier.shared module.
Uses _maybe_await to support both sync (pyturso, libsql_experimental raw)
and async (LibSQLAsyncConnection) connections.
"""

import inspect
from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    columns_from_pragma_rows,
    dry_run_preview,
    enum_population_sql,
    generate_operation_sql,
    generate_sql,
    requires_reconstruction,
)
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Operation


async def _maybe_await(value: Any) -> Any:
    """Await value if it's awaitable, otherwise return as-is."""
    if inspect.isawaitable(value):
        return await value
    return value


class TursoApplier:
    """
    Turso implementation of MigrationApplier protocol.

    Turso/libSQL is SQLite-compatible. SQL generation is shared
    with SQLite via applier.shared. Connection handling differs
    (synchronous pyturso API, explicit BEGIN).
    """

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "turso"

    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """Turso (libSQL) supports transactional DDL."""
        return "all_or_nothing"

    async def apply(
        self,
        connection: Any,
        operations: list[Operation],
        execution_order: list[int],
        *,
        dry_run: bool = False,
    ) -> ApplyResult:
        """
        Apply migration operations within a transaction.

        Turso/libSQL is SQLite-compatible with transactional DDL.
        Uses per-operation execution with reconstruction for unsupported operations.
        """
        if dry_run:
            return dry_run_preview(operations, execution_order)

        executed: list[str] = []

        try:
            # Enable foreign keys and start transaction
            await _maybe_await(connection.execute("PRAGMA foreign_keys = ON"))

            # Per-operation execution
            for op_idx in execution_order:
                operation = operations[op_idx]

                try:
                    if requires_reconstruction(operation):
                        await self._execute_with_reconstruction(connection, operation)
                        executed.append(f"Table reconstruction for {operation['table']}")
                    else:
                        sql = generate_operation_sql(operation)
                        for statement in sql.split(";"):
                            statement = statement.strip()
                            if statement:
                                await _maybe_await(connection.execute(statement))
                        executed.append(sql)

                except Exception as e:
                    await _maybe_await(connection.rollback())
                    raise MigrationError(
                        f"Failed to execute operation",
                        operation=operation,
                        original_error=e,
                    ) from e

            await _maybe_await(connection.commit())

            return {
                "success": True,
                "executed_sql": executed,
                "operations_applied": len(executed),
                "error": None,
                "error_operation": None,
            }

        except MigrationError:
            raise
        except Exception as e:
            await _maybe_await(connection.rollback())
            raise MigrationError(
                f"Migration failed: {e}",
                original_error=e,
            ) from e

    async def _execute_with_reconstruction(
        self, connection: Any, operation: Operation
    ) -> None:
        """
        Execute an operation using table reconstruction (async).

        Fresh introspection → pure column transform → async reconstruction.
        """
        from declaro_persistum.abstractions.table_reconstruction import (
            _get_full_table_schema,
            alter_column_default,
            alter_column_nullability,
            alter_column_type,
            reconstruct_table,
        )

        table = operation["table"]
        details = operation["details"]

        # Fresh introspection for current state (includes FKs + unique constraints)
        columns = await _get_full_table_schema(connection, table)

        # Apply reconstruction changes (pure)
        columns = apply_reconstruction_changes(columns, operation)

        # Use specialized functions for single-property alter_column changes
        if operation["op"] == "alter_column" and len(details["changes"]) == 1:
            changes = details["changes"]
            column = details["column"]

            if "nullable" in changes:
                val = changes["nullable"]
                if isinstance(val, dict) and "to" in val:
                    val = val["to"]
                await alter_column_nullability(connection, table, column, val)
                return
            elif "type" in changes:
                val = changes["type"]
                if isinstance(val, dict) and "to" in val:
                    val = val["to"]
                await alter_column_type(connection, table, column, val)
                return
            elif "default" in changes:
                val = changes["default"]
                if isinstance(val, dict) and "to" in val:
                    val = val["to"]
                await alter_column_default(connection, table, column, val)
                return

        # General reconstruction
        await reconstruct_table(connection, table, columns)

    def generate_sql(
        self,
        operations: list[Operation],
        execution_order: list[int],
    ) -> list[str]:
        """Generate SQL statements in execution order."""
        return generate_sql(operations, execution_order)

    def generate_operation_sql(self, operation: Operation) -> str:
        """Generate SQL for a single operation."""
        return generate_operation_sql(operation)


# Turso uses identical view generation to SQLite
from declaro_persistum.applier.sqlite import generate_create_view, generate_drop_view

__all__ = ["TursoApplier", "generate_create_view", "generate_drop_view"]
