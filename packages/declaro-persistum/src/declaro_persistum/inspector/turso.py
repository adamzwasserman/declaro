"""
Turso (libSQL) database inspector implementation.

Turso is SQLite-compatible, so this shares logic with SQLite via
inspector.shared module. Uses pragma_compat abstraction for PRAGMA
calls that may not be natively supported by Turso Database (Rust).
"""

from typing import Any

from declaro_persistum.abstractions.pragma_compat import (
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Index, Schema, Table, View


class TursoInspector:
    """
    Turso implementation of DatabaseInspector protocol.

    Uses pragma_compat wrappers for PRAGMA calls (try native, fall back
    to emulation for unsupported PRAGMAs like foreign_key_list).
    """

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "turso"

    async def introspect(
        self,
        connection: Any,
        *,
        _schema_name: str = "main",
        include_views: bool = False,
    ) -> Schema | tuple[Schema, dict[str, View]]:
        """
        Introspect Turso database schema.

        Uses pragma_compat for PRAGMA calls that may need emulation.
        """
        try:
            cursor = await connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                  AND name NOT LIKE '_litestream_%'
                ORDER BY name
                """
            )
            tables = await cursor.fetchall()

            schema: Schema = {}

            for row in tables:
                table_name = row[0]
                schema[table_name] = await self._introspect_table(connection, table_name)

            if include_views:
                views = await self.introspect_views(connection)
                return schema, views

            return schema

        except Exception as e:
            error_msg = str(e).lower()
            if "network" in error_msg or "connection" in error_msg or "http" in error_msg:
                raise DeclaroConnectionError(
                    f"Failed to connect to Turso database: {e}",
                    dialect="turso",
                ) from e
            raise

    async def _introspect_table(
        self,
        connection: Any,
        table_name: str,
    ) -> Table:
        """Introspect a single table's structure."""
        columns = await self._get_columns(connection, table_name)
        indexes = await self._get_indexes(connection, table_name)
        foreign_keys = await self._get_foreign_keys(connection, table_name)

        return assemble_table(columns, indexes, foreign_keys)

    async def _get_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Column]:
        """Get column definitions for a table."""
        rows = await pragma_table_info(connection, table_name)

        columns = columns_from_pragma_rows(rows)

        unique_cols = await self._get_unique_columns(connection, table_name)
        apply_unique_columns(columns, unique_cols)

        return columns

    async def _get_unique_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> set[str]:
        """Get columns with unique constraints (single-column only)."""
        index_rows = await pragma_index_list(connection, table_name)

        index_info: dict[str, list[tuple]] = {}
        for idx_row in index_rows:
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]
            if is_unique and origin != "pk":
                index_info[idx_name] = await pragma_index_info(connection, idx_name)

        return unique_cols_from_index_rows(index_rows, index_info)

    async def _get_indexes(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Index]:
        """Get non-auto indexes for a table."""
        index_rows = await pragma_index_list(connection, table_name)

        index_info: dict[str, list[tuple]] = {}
        index_sql: dict[str, str | None] = {}

        for idx_row in index_rows:
            idx_name = idx_row[1]
            origin = idx_row[3]

            if origin in ("pk", "u"):
                continue

            index_info[idx_name] = await pragma_index_info(connection, idx_name)

            sql_cursor = await connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (idx_name,),
            )
            sql_row = await sql_cursor.fetchone()
            index_sql[idx_name] = sql_row[0] if sql_row else None

        return indexes_from_rows(index_rows, index_info, index_sql)

    async def _get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
    ) -> list[dict[str, str]]:
        """Get foreign key constraints for a table."""
        rows = await pragma_foreign_key_list(connection, table_name)
        return fk_list_from_pragma_rows(rows)

    async def table_exists(
        self,
        connection: Any,
        table_name: str,
    ) -> bool:
        """Check if a table exists."""
        cursor = await connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
        result = await cursor.fetchone()
        return result is not None

    async def get_table_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Any]:
        """Get column definitions for a specific table."""
        if not await self.table_exists(connection, table_name):
            from declaro_persistum.exceptions import DeclaroError

            raise DeclaroError(f"Table '{table_name}' does not exist")

        return await self._get_columns(connection, table_name)

    async def introspect_views(
        self,
        connection: Any,
    ) -> dict[str, View]:
        """Introspect views from Turso/libSQL."""
        cursor = await connection.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'view'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '_litestream_%'
            ORDER BY name
            """
        )
        rows = await cursor.fetchall()

        return views_from_rows(rows)
