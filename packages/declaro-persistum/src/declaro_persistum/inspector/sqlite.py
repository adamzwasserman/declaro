"""
SQLite database inspector implementation.

Uses PRAGMA statements for metadata extraction.
Shared logic with Turso via inspector.shared module.
"""

import json
from typing import Any

from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    normalize_fk_action,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Index, Schema, Table, View

# Re-export for any external consumers
_normalize_fk_action = normalize_fk_action


class SQLiteInspector:
    """SQLite implementation of DatabaseInspector protocol."""

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "sqlite"

    async def introspect(
        self,
        connection: Any,
        *,
        _schema_name: str = "main",
        include_views: bool = False,
    ) -> Schema | tuple[Schema, dict[str, View]]:
        """
        Introspect SQLite database schema.

        Uses PRAGMA table_info and related pragmas for metadata.
        """
        try:
            cursor = await connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                  AND name NOT LIKE '_declaro_%'
                ORDER BY name
                """
            )
            tables = await cursor.fetchall()

            schema: Schema = {}

            for (table_name,) in tables:
                schema[table_name] = await self._introspect_table(connection, table_name)

            if include_views:
                views = await self.introspect_views(connection)
                return schema, views

            return schema

        except Exception as e:
            if "database" in str(e).lower() or "connection" in str(e).lower():
                raise DeclaroConnectionError(
                    f"Failed to introspect database: {e}",
                    dialect="sqlite",
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
        cursor = await connection.execute(f"PRAGMA table_info('{table_name}')")
        rows = await cursor.fetchall()

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
        cursor = await connection.execute(f"PRAGMA index_list('{table_name}')")
        index_rows = await cursor.fetchall()

        index_info: dict[str, list[tuple]] = {}
        for idx_row in index_rows:
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]
            if is_unique and origin != "pk":
                cursor = await connection.execute(f"PRAGMA index_info('{idx_name}')")
                index_info[idx_name] = await cursor.fetchall()

        return unique_cols_from_index_rows(index_rows, index_info)

    async def _get_indexes(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Index]:
        """Get non-auto indexes for a table."""
        cursor = await connection.execute(f"PRAGMA index_list('{table_name}')")
        index_rows = await cursor.fetchall()

        index_info: dict[str, list[tuple]] = {}
        index_sql: dict[str, str | None] = {}

        for idx_row in index_rows:
            idx_name = idx_row[1]
            origin = idx_row[3]

            if origin in ("pk", "u"):
                continue

            cursor = await connection.execute(f"PRAGMA index_info('{idx_name}')")
            index_info[idx_name] = await cursor.fetchall()

            cursor = await connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (idx_name,),
            )
            sql_row = await cursor.fetchone()
            index_sql[idx_name] = sql_row[0] if sql_row else None

        return indexes_from_rows(index_rows, index_info, index_sql)

    async def _get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
    ) -> list[dict[str, str]]:
        """Get foreign key constraints for a table."""
        cursor = await connection.execute(f"PRAGMA foreign_key_list('{table_name}')")
        rows = await cursor.fetchall()
        return fk_list_from_pragma_rows(rows)

    async def table_exists(
        self,
        connection: Any,
        table_name: str,
    ) -> bool:
        """Check if a table exists."""
        cursor = await connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
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
        """
        Introspect views from SQLite.

        Detects both regular views and emulated materialized views
        (tables with metadata in _dp_materialized_views).
        """
        cursor = await connection.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'view'
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
        rows = await cursor.fetchall()

        views = views_from_rows(rows)

        # Check for emulated materialized views (tables with metadata)
        matview_metadata = await self._get_matview_metadata(connection)

        for name, metadata in matview_metadata.items():
            view: View = {
                "name": name,
                "query": metadata["query"],
                "materialized": True,
            }

            if metadata.get("refresh_strategy"):
                view["refresh"] = metadata["refresh_strategy"]

            if metadata.get("depends_on"):
                try:
                    depends_on = json.loads(metadata["depends_on"])
                    if isinstance(depends_on, list):
                        view["depends_on"] = depends_on
                except (json.JSONDecodeError, TypeError):
                    pass

            views[name] = view

        return views

    async def _get_matview_metadata(
        self,
        connection: Any,
    ) -> dict[str, dict[str, Any]]:
        """Get metadata for emulated materialized views."""
        from declaro_persistum.abstractions.materialized_views import (
            MATVIEW_METADATA_TABLE,
        )

        cursor = await connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (MATVIEW_METADATA_TABLE,),
        )
        if not await cursor.fetchone():
            return {}

        cursor = await connection.execute(
            f"""
            SELECT name, query, refresh_strategy, depends_on, last_refreshed_at
            FROM {MATVIEW_METADATA_TABLE}
            """
        )
        rows = await cursor.fetchall()

        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            result[row[0]] = {
                "query": row[1],
                "refresh_strategy": row[2],
                "depends_on": row[3],
                "last_refreshed_at": row[4],
            }

        return result
