"""
Turso (libSQL) database inspector implementation.

Turso is SQLite-compatible, so this largely delegates to SQLite logic.
Uses async connection API matching aiosqlite patterns.

Note: Turso Database (Rust) has limited PRAGMA support, so we use
the pragma_compat abstraction layer for index_list, index_info, and foreign_key_list.
"""

import re
from typing import Any, Literal

from declaro_persistum.abstractions.pragma_compat import (
    pragma_table_info,
    pragma_index_list,
    pragma_index_info,
    pragma_foreign_key_list,
)
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.types import Column, Index, Schema, Table, View

# Type for FK actions
FKAction = Literal["cascade", "set null", "restrict", "no action"]


def _normalize_fk_action(action: str | None) -> FKAction | None:
    """Normalize FK action string to proper Literal type."""
    if action is None or action == "NO ACTION":
        return None
    normalized = action.lower().replace(" ", "_")
    action_map = {
        "cascade": "cascade",
        "set_null": "set null",
        "restrict": "restrict",
        "no_action": "no action",
    }
    return action_map.get(normalized)  # type: ignore[return-value]


class TursoInspector:
    """
    Turso implementation of DatabaseInspector protocol.

    Turso uses libSQL which is SQLite-compatible, so the introspection
    logic is nearly identical to SQLite. Uses async connection API.
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

        Uses the same PRAGMA-based approach as SQLite since Turso
        is libSQL (SQLite-compatible).

        Args:
            connection: libsql connection object
            schema_name: Ignored for Turso (always uses "main")
            include_views: If True, also introspect views and return as second element

        Returns:
            Schema dict, or tuple of (Schema, views dict) if include_views=True
        """
        try:
            # Get all tables (excluding sqlite internal tables)
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

        # Merge foreign key info into columns
        for fk in foreign_keys:
            col_name = fk["from"]
            if col_name in columns:
                columns[col_name]["references"] = f"{fk['table']}.{fk['to']}"
                on_delete = _normalize_fk_action(fk.get("on_delete"))
                if on_delete:
                    columns[col_name]["on_delete"] = on_delete
                on_update = _normalize_fk_action(fk.get("on_update"))
                if on_update:
                    columns[col_name]["on_update"] = on_update

        table: Table = {"columns": columns}

        # Check for composite primary key
        pk_columns = [name for name, col in columns.items() if col.get("primary_key")]
        if len(pk_columns) > 1:
            table["primary_key"] = pk_columns
            for col_name in pk_columns:
                del columns[col_name]["primary_key"]

        if indexes:
            table["indexes"] = indexes

        return table

    async def _get_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Column]:
        """Get column definitions for a table."""
        rows = await pragma_table_info(connection, table_name)

        columns: dict[str, Column] = {}

        for row in rows:
            # row format: (cid, name, type, notnull, dflt_value, pk)
            col_name = row[1]
            col_type = row[2]
            not_null = bool(row[3])
            default = row[4]
            is_pk = bool(row[5])

            col: Column = {"type": self._normalize_type(col_type)}

            if not_null:
                col["nullable"] = False

            if default is not None:
                col["default"] = default

            if is_pk:
                col["primary_key"] = True

            columns[col_name] = col

        # Get unique constraints from indexes
        unique_cols = await self._get_unique_columns(connection, table_name)
        for col_name in unique_cols:
            if col_name in columns and not columns[col_name].get("primary_key"):
                columns[col_name]["unique"] = True

        return columns

    def _normalize_type(self, col_type: str) -> str:
        """Normalize SQLite/libSQL type to canonical form."""
        if not col_type:
            return "blob"

        col_type = col_type.lower().strip()

        if "int" in col_type:
            return "integer"
        elif "char" in col_type or "clob" in col_type or "text" in col_type:
            return "text"
        elif "blob" in col_type or col_type == "":
            return "blob"
        elif "real" in col_type or "floa" in col_type or "doub" in col_type:
            return "real"
        else:
            return "numeric"

    async def _get_unique_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> set[str]:
        """Get columns with unique constraints (single-column only)."""
        index_rows = await pragma_index_list(connection, table_name)

        unique_cols: set[str] = set()

        for idx_row in index_rows:
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]

            if is_unique and origin != "pk":
                idx_info = await pragma_index_info(connection, idx_name)

                if len(idx_info) == 1:
                    unique_cols.add(idx_info[0][2])

        return unique_cols

    async def _get_indexes(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Index]:
        """Get non-auto indexes for a table."""
        index_rows = await pragma_index_list(connection, table_name)

        indexes: dict[str, Index] = {}

        for idx_row in index_rows:
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]

            if origin in ("pk", "u"):
                continue

            idx_info = await pragma_index_info(connection, idx_name)
            columns = [row[2] for row in idx_info]

            index: Index = {"columns": columns}

            if is_unique:
                index["unique"] = True

            # Check for partial index
            sql_cursor = await connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (idx_name,),
            )
            sql_row = await sql_cursor.fetchone()
            if sql_row and sql_row[0]:
                sql = sql_row[0]
                if " WHERE " in sql.upper():
                    where_idx = sql.upper().index(" WHERE ")
                    index["where"] = sql[where_idx + 7 :].strip()

            indexes[idx_name] = index

        return indexes

    async def _get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
    ) -> list[dict[str, str]]:
        """Get foreign key constraints for a table."""
        rows = await pragma_foreign_key_list(connection, table_name)

        return [
            {
                "from": row[3],
                "table": row[2],
                "to": row[4],
                "on_update": row[5],
                "on_delete": row[6],
            }
            for row in rows
        ]

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
        """
        Introspect views from Turso/libSQL.

        Turso/libSQL is SQLite-compatible, so views work the same way.
        Materialized views are not supported.

        Args:
            connection: libsql connection object
            schema_name: Ignored for Turso (always uses "main")

        Returns:
            Dict mapping view names to View definitions
        """
        views: dict[str, View] = {}

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

        for row in rows:
            name, sql = row[0], row[1]
            query = _extract_view_query(sql)
            views[name] = {
                "name": name,
                "query": query,
                "materialized": False,
            }

        return views


def _extract_view_query(create_statement: str) -> str:
    """Extract SELECT query from CREATE VIEW statement."""
    match = re.search(r"\bAS\s+(.+)$", create_statement, re.IGNORECASE | re.DOTALL)
    if match:
        return " ".join(match.group(1).split())
    return ""
