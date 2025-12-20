"""
PostgreSQL database inspector implementation.

Uses information_schema and pg_catalog for complete metadata extraction.
"""

from typing import Any, Literal

from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.types import Column, Index, Schema, Table, View

# Type for FK actions
FKAction = Literal["cascade", "set null", "restrict", "no action"]


def _normalize_fk_action(action: str | None) -> FKAction | None:
    """Normalize FK action string to proper Literal type."""
    if action is None:
        return None
    normalized = action.lower().replace(" ", "_")
    action_map = {
        "cascade": "cascade",
        "set_null": "set null",
        "restrict": "restrict",
        "no_action": "no action",
    }
    return action_map.get(normalized)  # type: ignore[return-value]


class PostgreSQLInspector:
    """PostgreSQL implementation of DatabaseInspector protocol."""

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "postgresql"

    async def introspect(
        self,
        connection: Any,
        *,
        schema_name: str = "public",
        include_views: bool = False,
    ) -> Schema | tuple[Schema, dict[str, View]]:
        """
        Introspect PostgreSQL database schema.

        Uses information_schema for standard metadata and pg_catalog
        for PostgreSQL-specific features (partial indexes, etc.).

        Args:
            connection: asyncpg connection object
            schema_name: PostgreSQL schema to introspect (default: "public")
            include_views: If True, also introspect views and return as second element

        Returns:
            Schema dict, or tuple of (Schema, views dict) if include_views=True
        """
        try:
            # Get all tables in schema
            tables = await connection.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = $1
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                schema_name,
            )

            schema: Schema = {}

            for table_row in tables:
                table_name = table_row["table_name"]
                schema[table_name] = await self._introspect_table(
                    connection, table_name, schema_name
                )

            if include_views:
                views = await self.introspect_views(connection, schema_name=schema_name)
                return schema, views

            return schema

        except Exception as e:
            if "connection" in str(e).lower():
                raise DeclaroConnectionError(
                    f"Failed to introspect database: {e}",
                    dialect="postgresql",
                ) from e
            raise

    async def _introspect_table(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> Table:
        """Introspect a single table's structure."""
        columns = await self._get_columns(connection, table_name, schema_name)
        primary_key = await self._get_primary_key(connection, table_name, schema_name)
        indexes = await self._get_indexes(connection, table_name, schema_name)
        foreign_keys = await self._get_foreign_keys(connection, table_name, schema_name)

        # Merge foreign key info into columns
        for fk in foreign_keys:
            col_name = fk["column_name"]
            if col_name in columns:
                columns[col_name]["references"] = f"{fk['foreign_table']}.{fk['foreign_column']}"
                on_delete = _normalize_fk_action(fk.get("delete_rule"))
                if on_delete:
                    columns[col_name]["on_delete"] = on_delete
                on_update = _normalize_fk_action(fk.get("update_rule"))
                if on_update:
                    columns[col_name]["on_update"] = on_update

        table: Table = {"columns": columns}

        if primary_key and len(primary_key) > 1:
            # Only set composite PK if multiple columns
            table["primary_key"] = primary_key
            # For single-column PK, it's already in the column definition

        if indexes:
            table["indexes"] = indexes

        return table

    async def _get_columns(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> dict[str, Column]:
        """Get column definitions for a table."""
        rows = await connection.fetch(
            """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema_name,
            table_name,
        )

        columns: dict[str, Column] = {}

        for row in rows:
            col_name = row["column_name"]
            col: Column = {"type": self._normalize_type(row)}

            if row["is_nullable"] == "NO":
                col["nullable"] = False

            if row["column_default"] is not None:
                # Clean up default value
                default = row["column_default"]
                # Remove type casts like ::text
                if "::" in default:
                    default = default.split("::")[0]
                col["default"] = default

            columns[col_name] = col

        # Get primary key info to mark PK columns
        pk_columns = await self._get_primary_key(connection, table_name, schema_name)
        if pk_columns and len(pk_columns) == 1:
            pk_col = pk_columns[0]
            if pk_col in columns:
                columns[pk_col]["primary_key"] = True

        # Get unique constraints
        unique_cols = await self._get_unique_columns(connection, table_name, schema_name)
        for col_name in unique_cols:
            if col_name in columns:
                columns[col_name]["unique"] = True

        return columns

    def _normalize_type(self, row: dict[str, Any]) -> str:
        """Normalize PostgreSQL type to canonical form."""
        data_type = row["data_type"]
        udt_name = row["udt_name"]

        # Map PostgreSQL internal types to canonical names
        type_map = {
            "character varying": "varchar",
            "character": "char",
            "timestamp without time zone": "timestamp",
            "timestamp with time zone": "timestamptz",
            "time without time zone": "time",
            "time with time zone": "timetz",
            "double precision": "float8",
            "real": "float4",
            "integer": "integer",
            "bigint": "bigint",
            "smallint": "smallint",
            "boolean": "boolean",
            "text": "text",
            "uuid": "uuid",
            "jsonb": "jsonb",
            "json": "json",
            "bytea": "bytea",
        }

        if data_type in type_map:
            base_type = type_map[data_type]
        elif data_type == "ARRAY":
            # Array type - use udt_name without leading underscore
            base_type = f"{udt_name[1:]}[]" if udt_name.startswith("_") else f"{udt_name}[]"
        elif data_type == "USER-DEFINED":
            # Enum or custom type
            base_type = udt_name
        else:
            base_type = data_type

        # Add precision/length for applicable types
        if base_type in ("varchar", "char") and row["character_maximum_length"]:
            return f"{base_type}({row['character_maximum_length']})"
        elif base_type == "numeric" and row["numeric_precision"]:
            if row["numeric_scale"]:
                return f"numeric({row['numeric_precision']},{row['numeric_scale']})"
            return f"numeric({row['numeric_precision']})"

        return base_type

    async def _get_primary_key(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> list[str]:
        """Get primary key columns for a table."""
        rows = await connection.fetch(
            """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
              AND n.nspname = $1
              AND c.relname = $2
            ORDER BY array_position(i.indkey, a.attnum)
            """,
            schema_name,
            table_name,
        )
        return [row["column_name"] for row in rows]

    async def _get_unique_columns(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> set[str]:
        """Get columns with unique constraints (single-column only)."""
        rows = await connection.fetch(
            """
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisunique
              AND NOT i.indisprimary
              AND array_length(i.indkey, 1) = 1
              AND n.nspname = $1
              AND c.relname = $2
            """,
            schema_name,
            table_name,
        )
        return {row["column_name"] for row in rows}

    async def _get_indexes(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> dict[str, Index]:
        """Get non-primary-key indexes for a table."""
        rows = await connection.fetch(
            """
            SELECT
                i.relname as index_name,
                array_agg(a.attname ORDER BY k.n) as columns,
                ix.indisunique as is_unique,
                pg_get_expr(ix.indpred, ix.indrelid) as predicate,
                am.amname as index_method
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE NOT ix.indisprimary
              AND n.nspname = $1
              AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indpred, ix.indrelid, am.amname
            ORDER BY i.relname
            """,
            schema_name,
            table_name,
        )

        indexes: dict[str, Index] = {}

        for row in rows:
            index: Index = {"columns": row["columns"]}

            if row["is_unique"]:
                index["unique"] = True
            if row["predicate"]:
                index["where"] = row["predicate"]
            if row["index_method"] != "btree":
                index["using"] = row["index_method"]

            indexes[row["index_name"]] = index

        return indexes

    async def _get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
        schema_name: str,
    ) -> list[dict[str, str]]:
        """Get foreign key constraints for a table."""
        rows = await connection.fetch(
            """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column,
                rc.delete_rule,
                rc.update_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints rc
                ON rc.constraint_name = tc.constraint_name
                AND rc.constraint_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = $1
              AND tc.table_name = $2
            """,
            schema_name,
            table_name,
        )

        return [dict(row) for row in rows]

    async def table_exists(
        self,
        connection: Any,
        table_name: str,
        *,
        schema_name: str = "public",
    ) -> bool:
        """Check if a table exists."""
        result = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1
                  AND table_name = $2
                  AND table_type = 'BASE TABLE'
            )
            """,
            schema_name,
            table_name,
        )
        return bool(result)

    async def get_table_columns(
        self,
        connection: Any,
        table_name: str,
        *,
        schema_name: str = "public",
    ) -> dict[str, Any]:
        """Get column definitions for a specific table."""
        if not await self.table_exists(connection, table_name, schema_name=schema_name):
            from declaro_persistum.exceptions import DeclaroError

            raise DeclaroError(f"Table '{table_name}' does not exist in schema '{schema_name}'")

        return await self._get_columns(connection, table_name, schema_name)

    async def introspect_views(
        self,
        connection: Any,
        *,
        schema_name: str = "public",
    ) -> dict[str, View]:
        """
        Introspect views and materialized views from PostgreSQL.

        Args:
            connection: asyncpg connection object
            schema_name: PostgreSQL schema to introspect (default: "public")

        Returns:
            Dict mapping view names to View definitions
        """
        views: dict[str, View] = {}

        # Regular views
        rows = await connection.fetch(
            """
            SELECT viewname as name, definition as query
            FROM pg_views
            WHERE schemaname = $1
            """,
            schema_name,
        )

        for row in rows:
            views[row["name"]] = {
                "name": row["name"],
                "query": _normalize_view_query(row["query"]),
                "materialized": False,
            }

        # Materialized views
        rows = await connection.fetch(
            """
            SELECT matviewname as name, definition as query
            FROM pg_matviews
            WHERE schemaname = $1
            """,
            schema_name,
        )

        for row in rows:
            views[row["name"]] = {
                "name": row["name"],
                "query": _normalize_view_query(row["query"]),
                "materialized": True,
            }

        return views

    async def get_materialized_view_indexes(
        self,
        connection: Any,
        view_name: str,
        *,
        schema_name: str = "public",
    ) -> list[dict[str, Any]]:
        """
        Get indexes on a materialized view.

        Args:
            connection: asyncpg connection object
            view_name: Name of the materialized view
            schema_name: PostgreSQL schema (default: "public")

        Returns:
            List of index info dicts with keys: index_name, is_unique, columns
        """
        rows = await connection.fetch(
            """
            SELECT
                i.relname as index_name,
                ix.indisunique as is_unique,
                array_agg(a.attname ORDER BY k.n) as columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class m ON m.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = m.relnamespace
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
            JOIN pg_attribute a ON a.attrelid = m.oid AND a.attnum = k.attnum
            WHERE m.relkind = 'm'
              AND n.nspname = $1
              AND m.relname = $2
            GROUP BY i.relname, ix.indisunique
            """,
            schema_name,
            view_name,
        )
        return [dict(row) for row in rows]

    async def has_unique_index(
        self,
        connection: Any,
        view_name: str,
        *,
        schema_name: str = "public",
    ) -> bool:
        """
        Check if a materialized view has a unique index.

        Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.

        Args:
            connection: asyncpg connection object
            view_name: Name of the materialized view
            schema_name: PostgreSQL schema (default: "public")

        Returns:
            True if view has at least one unique index
        """
        indexes = await self.get_materialized_view_indexes(
            connection, view_name, schema_name=schema_name
        )
        return any(idx.get("is_unique") for idx in indexes)


def _normalize_view_query(query: str) -> str:
    """Normalize view query for consistent comparison."""
    return " ".join(query.split())
