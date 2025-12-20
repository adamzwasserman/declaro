"""
SQLite database inspector implementation.

Uses PRAGMA statements for metadata extraction.
"""

import re
from typing import Any, Literal

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
        The schema_name parameter is accepted for API compatibility
        but SQLite only has "main" schema.

        Args:
            connection: aiosqlite connection object
            schema_name: Ignored for SQLite (always uses "main")
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
            # Remove primary_key from individual columns for composite PKs
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
        cursor = await connection.execute(f"PRAGMA table_info('{table_name}')")
        rows = await cursor.fetchall()

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
        """Normalize SQLite type to canonical form."""
        if not col_type:
            return "blob"  # SQLite default for no type

        col_type = col_type.lower().strip()

        # SQLite type affinity rules
        if "int" in col_type:
            return "integer"
        elif "char" in col_type or "clob" in col_type or "text" in col_type:
            return "text"
        elif "blob" in col_type or col_type == "":
            return "blob"
        elif "real" in col_type or "floa" in col_type or "doub" in col_type:
            return "real"
        else:
            # NUMERIC affinity
            return "numeric"

    async def _get_unique_columns(
        self,
        connection: Any,
        table_name: str,
    ) -> set[str]:
        """Get columns with unique constraints (single-column only)."""
        cursor = await connection.execute(f"PRAGMA index_list('{table_name}')")
        indexes = await cursor.fetchall()

        unique_cols: set[str] = set()

        for idx_row in indexes:
            # idx_row format: (seq, name, unique, origin, partial)
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]

            # Only consider unique indexes, not primary keys
            if is_unique and origin != "pk":
                cursor = await connection.execute(f"PRAGMA index_info('{idx_name}')")
                idx_cols = await cursor.fetchall()

                # Only single-column unique constraints go on the column
                if len(idx_cols) == 1:
                    unique_cols.add(idx_cols[0][2])  # column name

        return unique_cols

    async def _get_indexes(
        self,
        connection: Any,
        table_name: str,
    ) -> dict[str, Index]:
        """Get non-auto indexes for a table."""
        cursor = await connection.execute(f"PRAGMA index_list('{table_name}')")
        indexes_list = await cursor.fetchall()

        indexes: dict[str, Index] = {}

        for idx_row in indexes_list:
            idx_name = idx_row[1]
            is_unique = bool(idx_row[2])
            origin = idx_row[3]

            # Skip auto-created indexes (primary keys, unique constraints)
            if origin in ("pk", "u"):
                continue

            cursor = await connection.execute(f"PRAGMA index_info('{idx_name}')")
            idx_cols = await cursor.fetchall()
            columns = [row[2] for row in idx_cols]  # column name is at index 2

            index: Index = {"columns": columns}

            if is_unique:
                index["unique"] = True

            # Check for partial index
            cursor = await connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (idx_name,),
            )
            sql_row = await cursor.fetchone()
            if sql_row and sql_row[0]:
                sql = sql_row[0]
                if " WHERE " in sql.upper():
                    # Extract WHERE clause
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
        cursor = await connection.execute(f"PRAGMA foreign_key_list('{table_name}')")
        rows = await cursor.fetchall()

        # row format: (id, seq, table, from, to, on_update, on_delete, match)
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

        Args:
            connection: aiosqlite connection object

        Returns:
            Dict mapping view names to View definitions
        """
        import json

        views: dict[str, View] = {}

        # Get regular views
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

        for name, sql in rows:
            query = _extract_view_query(sql)
            views[name] = {
                "name": name,
                "query": query,
                "materialized": False,
            }

        # Check for emulated materialized views (tables with metadata)
        matview_metadata = await self._get_matview_metadata(connection)

        for name, metadata in matview_metadata.items():
            view: View = {
                "name": name,
                "query": metadata["query"],
                "materialized": True,
            }

            # Add refresh strategy
            if metadata.get("refresh_strategy"):
                view["refresh"] = metadata["refresh_strategy"]

            # Parse depends_on from JSON
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
        """
        Get metadata for emulated materialized views.

        Returns empty dict if metadata table doesn't exist.
        """
        from declaro_persistum.abstractions.materialized_views import (
            MATVIEW_METADATA_TABLE,
        )

        # Check if metadata table exists
        cursor = await connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (MATVIEW_METADATA_TABLE,),
        )
        if not await cursor.fetchone():
            return {}

        # Query all matview metadata
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


def _extract_view_query(create_statement: str) -> str:
    """Extract SELECT query from CREATE VIEW statement."""
    # Format: CREATE VIEW name AS SELECT ...
    match = re.search(r"\bAS\s+(.+)$", create_statement, re.IGNORECASE | re.DOTALL)
    if match:
        return " ".join(match.group(1).split())
    return ""
