"""
Bulk data loaders for cross-database transfer.

Provides protocol + implementations for high-performance row reading/writing:
- PostgreSQLBulkLoader: Uses asyncpg copy_records_to_table() for 10-100x faster writes
- GenericBulkLoader: Uses executemany() for SQLite/Turso

Usage:
    loader = create_bulk_loader("postgresql")
    await loader.load_rows(conn, "users", columns, rows)
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, Sequence

logger = logging.getLogger(__name__)


class BulkLoader(Protocol):
    """Protocol for bulk data loading across database backends."""

    async def read_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[tuple[Any, ...]]:
        """
        Read rows from a table in stable order.

        Args:
            conn: Database connection
            table: Table name
            columns: Column names to read
            offset: Row offset for pagination
            limit: Maximum rows to return (None = all)

        Returns:
            List of row tuples in stable order
        """
        ...

    async def load_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        rows: Sequence[tuple[Any, ...]],
    ) -> int:
        """
        Load rows into a table.

        Args:
            conn: Database connection
            table: Table name
            columns: Column names matching row tuple positions
            rows: Sequence of row tuples to insert

        Returns:
            Number of rows inserted
        """
        ...

    async def count_rows(self, conn: Any, table: str) -> int:
        """
        Count total rows in a table.

        Args:
            conn: Database connection
            table: Table name

        Returns:
            Row count
        """
        ...

    async def delete_rows(self, conn: Any, table: str) -> int:
        """
        Delete all rows from a table.

        Args:
            conn: Database connection
            table: Table name

        Returns:
            Number of rows deleted
        """
        ...

    async def disable_fk_checks(self, conn: Any) -> None:
        """Disable foreign key constraint checking."""
        ...

    async def enable_fk_checks(self, conn: Any) -> None:
        """Re-enable foreign key constraint checking."""
        ...








def create_bulk_loader(dialect: str) -> BulkLoader:
    """
    Factory function to create the appropriate bulk loader for a dialect.

    Args:
        dialect: One of "postgresql", "sqlite", "turso"

    Returns:
        BulkLoader implementation for the specified dialect

    Raises:
        ValueError: If dialect is not supported
    """
    if dialect == "postgresql":
        return PostgreSQLBulkLoader()  # type: ignore[return-value]
    elif dialect in ("sqlite", "turso"):
        return GenericBulkLoader()  # type: ignore[return-value]
    else:
        raise ValueError(
            f"Unsupported dialect for bulk loading: {dialect}. "
            f"Supported: postgresql, sqlite, turso"
        )
