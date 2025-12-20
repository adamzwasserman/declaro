"""
Exception hierarchy for declaro_persistum.

All exceptions inherit from DeclaroError for easy catching.
Exception messages follow the principle: "A programmer should not have to be
an expert stack tracer to figure out where they went wrong."
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from declaro_persistum.types import Ambiguity, Operation


class DeclaroError(Exception):
    """Base exception for all declaro_persistum errors."""

    pass


class SchemaError(DeclaroError):
    """
    Schema definition or validation error.

    Raised when:
    - TOML schema file cannot be parsed
    - Schema contains invalid references
    - Required fields are missing
    """

    def __init__(self, message: str, *, file: str | None = None, line: int | None = None) -> None:
        self.file = file
        self.line = line
        location = ""
        if file:
            location = f"\n\n  File: {file}"
            if line:
                location += f", line {line}"
        super().__init__(f"{message}{location}")


class AmbiguityError(SchemaError):
    """
    Unresolved ambiguous change detected.

    Raised in unattended mode when the diff contains changes that
    could be interpreted multiple ways (e.g., rename vs drop+add).
    """

    def __init__(self, ambiguities: list["Ambiguity"]) -> None:
        self.ambiguities = ambiguities
        messages = []
        for amb in ambiguities:
            messages.append(f"  - {amb['table']}: {amb['message']}")
        ambiguity_list = "\n".join(messages)
        super().__init__(
            f"Unresolved ambiguities detected ({len(ambiguities)} total):\n\n"
            f"{ambiguity_list}\n\n"
            "Run in interactive mode (-i) to resolve, or add migration hints to schema files."
        )


class CycleError(SchemaError):
    """
    Circular dependency in operations.

    Raised when the diff produces operations with circular dependencies,
    making it impossible to determine a valid execution order.
    """

    def __init__(self, cycle: list[str], tables: set[str]) -> None:
        self.cycle = cycle
        self.tables = tables
        cycle_display = "\n    ↓ depends on\n".join(
            f"    {i + 1}. {op}" for i, op in enumerate(cycle)
        )
        cycle_display += "\n    ↓ depends on\n    1. (cycle back)"
        tables_list = ", ".join(sorted(tables))
        super().__init__(
            f"Circular dependency detected in migration operations\n\n"
            f"  The following operations form a cycle:\n\n"
            f"{cycle_display}\n\n"
            f"  This usually indicates a circular reference in your schema.\n\n"
            f"  To resolve:\n"
            f"    - Remove one of the foreign keys\n"
            f"    - Or use a deferred constraint (add manually after migration)\n\n"
            f"  Tables involved: {tables_list}"
        )


class DriftError(DeclaroError):
    """
    Database state differs from expected snapshot.

    Raised when the actual database schema doesn't match the last-applied
    snapshot, indicating someone modified the database directly.
    """

    def __init__(
        self,
        differences: list[dict[str, str]],
        *,
        last_snapshot: str | None = None,
        current_time: str | None = None,
    ) -> None:
        self.differences = differences
        self.last_snapshot = last_snapshot
        self.current_time = current_time

        diff_lines = []
        for diff in differences:
            diff_lines.append(f"    {diff.get('symbol', '~')} {diff.get('description', 'Unknown')}")
        diff_display = "\n".join(diff_lines)

        timestamps = ""
        if last_snapshot and current_time:
            timestamps = f"\n\n  Last snapshot: {last_snapshot}\n  Current time:  {current_time}"

        super().__init__(
            f"Database schema has drifted from expected state\n\n"
            f"  The database does not match the last applied migration snapshot.\n"
            f"  Someone may have modified the database directly.\n\n"
            f"  Differences detected:\n\n"
            f"{diff_display}\n\n"
            f"  Options:\n"
            f"    1. Run 'declaro snapshot' to update snapshot to current DB state\n"
            f"    2. Run 'declaro apply --force' to proceed anyway (may cause errors)\n"
            f"    3. Manually reconcile the differences"
            f"{timestamps}"
        )


class ConnectionError(DeclaroError):
    """
    Database connection failure.

    Raised when:
    - Connection string is invalid
    - Database is unreachable
    - Authentication fails
    """

    def __init__(self, message: str, *, dialect: str | None = None) -> None:
        self.dialect = dialect
        dialect_hint = ""
        if dialect:
            dialect_hint = f"\n\n  Dialect: {dialect}"
        super().__init__(f"{message}{dialect_hint}")


class MigrationError(DeclaroError):
    """
    Migration execution failure.

    Raised when a DDL operation fails during migration execution.
    Contains the failing operation and SQL for debugging.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: "Operation | None" = None,
        sql: str | None = None,
        original_error: Exception | None = None,
    ) -> None:
        self.operation = operation
        self.sql = sql
        self.original_error = original_error

        details = []
        if operation:
            details.append(f"  Operation: {operation['op']} on {operation['table']}")
        if sql:
            # Truncate very long SQL
            sql_display = sql if len(sql) < 500 else sql[:500] + "..."
            details.append(f"  SQL: {sql_display}")
        if original_error:
            details.append(f"  Original error: {type(original_error).__name__}: {original_error}")

        details_str = "\n".join(details)
        if details_str:
            details_str = f"\n\n{details_str}"

        super().__init__(f"{message}{details_str}")


class RollbackError(MigrationError):
    """
    Rollback after failure also failed.

    This is a critical error state - the database may be in an
    inconsistent state and requires manual intervention.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: "Operation | None" = None,
        sql: str | None = None,
        original_error: Exception | None = None,
        rollback_error: Exception | None = None,
    ) -> None:
        self.rollback_error = rollback_error
        rollback_msg = ""
        if rollback_error:
            rollback_msg = (
                f"\n\n  CRITICAL: Rollback also failed!\n"
                f"  Rollback error: {type(rollback_error).__name__}: {rollback_error}\n"
                f"  Manual database inspection required."
            )
        super().__init__(
            f"{message}{rollback_msg}",
            operation=operation,
            sql=sql,
            original_error=original_error,
        )


class ValidationError(SchemaError):
    """
    Schema validation error.

    Raised when schema references are invalid:
    - Foreign key references non-existent table/column
    - Index references non-existent column
    - Circular table dependencies (without deferrable)
    """

    def __init__(
        self,
        message: str,
        *,
        table: str | None = None,
        column: str | None = None,
        reference: str | None = None,
    ) -> None:
        self.table = table
        self.column = column
        self.reference = reference

        context = []
        if table:
            context.append(f"Table: {table}")
        if column:
            context.append(f"Column: {column}")
        if reference:
            context.append(f"Reference: {reference}")

        context_str = ""
        if context:
            context_str = "\n  " + "\n  ".join(context)

        super().__init__(f"{message}{context_str}")


class LoaderError(SchemaError):
    """
    Schema file loading error.

    Raised when:
    - TOML file has syntax errors
    - File cannot be read
    - Directory structure is invalid
    """

    def __init__(self, message: str, *, path: str | None = None) -> None:
        self.path = path
        path_str = ""
        if path:
            path_str = f"\n\n  Path: {path}"
        super().__init__(f"{message}{path_str}")


# =============================================================================
# Connection Pool Exceptions
# =============================================================================


class PoolError(DeclaroError):
    """Base exception for connection pool errors."""

    pass


class PoolClosedError(PoolError):
    """
    Pool has been closed.

    Raised when attempting to acquire a connection from a pool
    that has already been closed.
    """

    pass


class PoolExhaustedError(PoolError):
    """
    No connections available (acquire timeout).

    Raised when the pool cannot provide a connection within
    the configured acquire_timeout period.
    """

    pass


class PoolConnectionError(PoolError):
    """
    Failed to create a connection.

    Raised when:
    - The database is unreachable
    - Authentication fails
    - The connection string is invalid
    """

    pass
