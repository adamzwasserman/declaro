"""
Bulk data loaders for cross-database transfer.

Provides protocol + implementations for high-performance row reading/writing:
- PostgreSQLBulkLoader: Uses asyncpg copy_records_to_table() for 10-100x faster writes
- GenericBulkLoader: Uses executemany() for SQLite/Turso/LibSQL

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


class PostgreSQLBulkLoader:
    """
    Bulk loader for PostgreSQL using asyncpg's optimized COPY protocol.

    Uses copy_records_to_table() for writes (10-100x faster than INSERT)
    and ORDER BY ctid for stable row ordering on reads.
    """

    async def read_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[tuple[Any, ...]]:
        col_list = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {col_list} FROM "{table}" ORDER BY ctid'
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset:
            sql += f" OFFSET {offset}"
        rows = await conn.fetch(sql)
        return [tuple(_normalize_pg_value(v) for v in row.values()) for row in rows]

    async def load_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        rows: Sequence[tuple[Any, ...]],
    ) -> int:
        if not rows:
            return 0
        await conn.copy_records_to_table(
            table,
            columns=columns,
            records=rows,
        )
        return len(rows)

    async def count_rows(self, conn: Any, table: str) -> int:
        row = await conn.fetchval(f'SELECT COUNT(*) FROM "{table}"')
        return int(row)

    async def delete_rows(self, conn: Any, table: str) -> int:
        result = await conn.execute(f'DELETE FROM "{table}"')
        # asyncpg returns "DELETE N"
        return int(result.split()[-1]) if result else 0

    async def disable_fk_checks(self, conn: Any) -> None:
        await conn.execute("SET session_replication_role = 'replica'")

    async def enable_fk_checks(self, conn: Any) -> None:
        await conn.execute("SET session_replication_role = 'origin'")


class GenericBulkLoader:
    """
    Bulk loader for SQLite, Turso, and LibSQL using executemany().

    Uses ORDER BY rowid for stable row ordering on reads.
    Works with any DB-API 2.0 compatible async connection wrapper.
    """

    async def read_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[tuple[Any, ...]]:
        col_list = ", ".join(f'"{c}"' for c in columns)
        sql = f'SELECT {col_list} FROM "{table}" ORDER BY rowid'
        if limit is not None:
            sql += f" LIMIT {limit}"
        if offset:
            sql += f" OFFSET {offset}"
        cursor = await conn.execute(sql, ())
        rows = await cursor.fetchall()
        return [tuple(row) for row in rows]

    async def load_rows(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        rows: Sequence[tuple[Any, ...]],
    ) -> int:
        if not rows:
            return 0
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(f'"{c}"' for c in columns)
        sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'
        await conn.executemany(sql, list(rows))
        return len(rows)

    async def count_rows(self, conn: Any, table: str) -> int:
        cursor = await conn.execute(f'SELECT COUNT(*) FROM "{table}"', ())
        row = await cursor.fetchone()
        return int(row[0]) if row else 0

    async def delete_rows(self, conn: Any, table: str) -> int:
        count = await self.count_rows(conn, table)
        await conn.execute(f'DELETE FROM "{table}"', ())
        return count

    async def disable_fk_checks(self, conn: Any) -> None:
        await conn.execute("PRAGMA foreign_keys = OFF", ())

    async def enable_fk_checks(self, conn: Any) -> None:
        await conn.execute("PRAGMA foreign_keys = ON", ())


def _normalize_pg_value(value: Any) -> Any:
    """
    Normalize PostgreSQL-specific types to portable Python types.

    Converts UUID objects to strings so they can be inserted into
    SQLite/Turso targets without type errors.
    """
    import uuid

    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def create_bulk_loader(dialect: str) -> BulkLoader:
    """
    Factory function to create the appropriate bulk loader for a dialect.

    Args:
        dialect: One of "postgresql", "sqlite", "turso", "libsql"

    Returns:
        BulkLoader implementation for the specified dialect

    Raises:
        ValueError: If dialect is not supported
    """
    if dialect == "postgresql":
        return PostgreSQLBulkLoader()  # type: ignore[return-value]
    elif dialect in ("sqlite", "turso", "libsql"):
        return GenericBulkLoader()  # type: ignore[return-value]
    else:
        raise ValueError(
            f"Unsupported dialect for bulk loading: {dialect}. "
            f"Supported: postgresql, sqlite, turso, libsql"
        )
