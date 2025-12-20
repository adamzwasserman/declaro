"""
Migration applier protocol definition.

Defines the interface that all dialect-specific appliers must implement.
"""

from typing import Any, Literal, Protocol

from declaro_persistum.types import ApplyResult, Operation


class MigrationApplier(Protocol):
    """
    Protocol for applying schema migrations.

    Implementations exist for each supported dialect:
    - PostgreSQL (postgresql.py)
    - SQLite (sqlite.py)
    - Turso (turso.py)
    """

    def get_dialect(self) -> str:
        """
        Return dialect identifier.

        Returns:
            One of: "postgresql", "sqlite", "turso"
        """
        ...

    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """
        Return transaction behavior for this dialect.

        Returns:
            - "all_or_nothing": Single transaction wraps all operations
            - "per_operation": Each operation in separate transaction

        PostgreSQL, SQLite, and Turso all support transactional DDL,
        so they use "all_or_nothing" for maximum safety.
        """
        ...

    async def apply(
        self,
        connection: Any,
        operations: list[Operation],
        execution_order: list[int],
        *,
        dry_run: bool = False,
    ) -> ApplyResult:
        """
        Apply migration operations to database.

        This executes the DDL operations in the specified order,
        wrapped in a transaction for safety.

        Args:
            connection: Database connection object (dialect-specific)
            operations: List of DDL operations to execute
            execution_order: Topologically sorted operation indices
            dry_run: If True, generate SQL without executing

        Returns:
            ApplyResult with success status and executed SQL

        Raises:
            MigrationError: If any operation fails
            RollbackError: If rollback after failure also fails

        Example:
            >>> applier = PostgreSQLApplier()
            >>> result = await applier.apply(conn, operations, order)
            >>> if result["success"]:
            ...     print(f"Applied {result['operations_applied']} operations")
        """
        ...

    def generate_sql(
        self,
        operations: list[Operation],
        execution_order: list[int],
    ) -> list[str]:
        """
        Generate SQL statements without executing.

        Useful for previewing migrations or generating scripts.

        Args:
            operations: List of DDL operations
            execution_order: Topologically sorted operation indices

        Returns:
            List of SQL statements in execution order
        """
        ...

    def generate_operation_sql(self, operation: Operation) -> str:
        """
        Generate SQL for a single operation.

        Args:
            operation: The DDL operation

        Returns:
            SQL statement string
        """
        ...


def create_applier(dialect: str) -> MigrationApplier:
    """
    Factory function to create the appropriate applier for a dialect.

    Args:
        dialect: One of "postgresql", "sqlite", "turso"

    Returns:
        MigrationApplier implementation for the specified dialect

    Raises:
        ValueError: If dialect is not supported
    """
    if dialect == "postgresql":
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        return PostgreSQLApplier()
    elif dialect == "sqlite":
        from declaro_persistum.applier.sqlite import SQLiteApplier

        return SQLiteApplier()
    elif dialect == "turso":
        from declaro_persistum.applier.turso import TursoApplier

        return TursoApplier()
    else:
        raise ValueError(
            f"Unsupported dialect: {dialect}. Supported dialects: postgresql, sqlite, turso"
        )
