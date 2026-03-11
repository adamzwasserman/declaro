"""
Turso (libSQL) migration applier implementation.

Turso uses libSQL which is SQLite-compatible, so the SQL generation
is shared with SQLite via applier.shared module.
Connection handling uses LibSQLAsyncConnection (async wrapper over sync libsql_experimental).
"""

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


class TursoApplier:
    """
    Turso implementation of MigrationApplier protocol.

    Turso/libSQL is SQLite-compatible. SQL generation is shared
    with SQLite via applier.shared. Connection handling uses
    LibSQLAsyncConnection (async wrapper over libsql_experimental).
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
        target_schema: Any = None,
    ) -> ApplyResult:
        """
        Apply migration operations asynchronously.

        Args:
            connection: LibSQLAsyncConnection (async wrapper over libsql_experimental)
            operations: List of operations to apply
            execution_order: Order to execute operations
            dry_run: If True, only generate SQL without executing
            target_schema: Target schema (used for enum value population)
        """
        if dry_run:
            return dry_run_preview(operations, execution_order)

        executed: list[str] = []

        try:
            await connection.execute("BEGIN")

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
                                await connection.execute(statement)
                        executed.append(sql)

                    for insert_sql in enum_population_sql(operation, target_schema):
                        await connection.execute(insert_sql)
                        executed.append(insert_sql)

                except Exception as e:
                    await connection.rollback()
                    raise MigrationError(
                        f"Failed to execute operation",
                        operation=operation,
                        original_error=e,
                    ) from e

            await connection.commit()

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
            await connection.rollback()
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
        from declaro_persistum.abstractions.reconstruction import execute_reconstruction_async

        table = operation["table"]

        cursor = await connection.execute(f"PRAGMA table_info('{table}')")
        rows = await cursor.fetchall()
        columns = columns_from_pragma_rows(rows)

        columns = apply_reconstruction_changes(columns, operation)

        await execute_reconstruction_async(connection, table, columns)

    def apply_sync(
        self,
        connection: Any,
        operations: list[Operation],
        execution_order: list[int],
        *,
        dry_run: bool = False,
        target_schema: Any = None,
    ) -> ApplyResult:
        """
        Apply migration operations synchronously (raw sync connection).

        Args:
            connection: Raw sync libsql_experimental or pyturso connection
            operations: List of operations to apply
            execution_order: Order to execute operations
            dry_run: If True, only generate SQL without executing
            target_schema: Target schema (used for enum value population)
        """
        if dry_run:
            return dry_run_preview(operations, execution_order)

        executed: list[str] = []

        try:
            connection.execute("BEGIN")

            for op_idx in execution_order:
                operation = operations[op_idx]

                try:
                    if requires_reconstruction(operation):
                        self._execute_with_reconstruction_sync(connection, operation)
                        executed.append(f"Table reconstruction for {operation['table']}")
                    else:
                        sql = generate_operation_sql(operation)
                        for statement in sql.split(";"):
                            statement = statement.strip()
                            if statement:
                                connection.execute(statement)
                        executed.append(sql)

                    for insert_sql in enum_population_sql(operation, target_schema):
                        connection.execute(insert_sql)
                        executed.append(insert_sql)

                except Exception as e:
                    connection.rollback()
                    raise MigrationError(
                        f"Failed to execute operation",
                        operation=operation,
                        original_error=e,
                    ) from e

            connection.commit()

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
            connection.rollback()
            raise MigrationError(
                f"Migration failed: {e}",
                original_error=e,
            ) from e

    def _execute_with_reconstruction_sync(
        self, connection: Any, operation: Operation
    ) -> None:
        """Execute table reconstruction synchronously (raw sync connection)."""
        from declaro_persistum.abstractions.reconstruction import execute_reconstruction_sync

        table = operation["table"]

        cursor = connection.execute(f"PRAGMA table_info('{table}')")
        rows = cursor.fetchall()
        columns = columns_from_pragma_rows(rows)

        columns = apply_reconstruction_changes(columns, operation)

        execute_reconstruction_sync(connection, table, columns)

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
