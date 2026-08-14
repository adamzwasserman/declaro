"""Read a PostgreSQL database's shape back out of it. Functions, not a class.

`PostgreSQLInspector` was stateless — no `__init__`, no fields, methods that
took a connection and returned data. What follows is the same SQL against
information_schema and pg_catalog, with the class removed and `self` gone. The
queries are byte-for-byte the ones that were there: this was a de-classing, not
a rewrite, so nothing about what PostgreSQL is asked has changed.
"""

"""
PostgreSQL database inspector implementation.

Uses information_schema and pg_catalog for complete metadata extraction.
"""

from typing import Any

from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import normalize_fk_action as _normalize_fk_action
from declaro_persistum.types import Column, Index, Schema, Table, View


def get_dialect() -> str:
    """Return dialect identifier."""
    return "postgresql"


async def introspect(
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
              AND table_name NOT LIKE '_declaro_%'
            ORDER BY table_name
            """,
            schema_name,
        )

        schema: Schema = {}

        for table_row in tables:
            table_name = table_row["table_name"]
            schema[table_name] = await _introspect_table(
                connection, table_name, schema_name
            )

        if include_views:
            views = await introspect_views(connection, schema_name=schema_name)
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
    connection: Any,
    table_name: str,
    schema_name: str,
) -> Table:
    """Introspect a single table's structure."""
    columns = await _get_columns(connection, table_name, schema_name)
    primary_key = await _get_primary_key(connection, table_name, schema_name)
    indexes = await _get_indexes(connection, table_name, schema_name)
    foreign_keys = await _get_foreign_keys(connection, table_name, schema_name)

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
        col: Column = {"type": _normalize_type(row)}

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
    pk_columns = await _get_primary_key(connection, table_name, schema_name)
    if pk_columns and len(pk_columns) == 1:
        pk_col = pk_columns[0]
        if pk_col in columns:
            columns[pk_col]["primary_key"] = True

    # Get unique constraints
    unique_cols = await _get_unique_columns(connection, table_name, schema_name)
    for col_name in unique_cols:
        if col_name in columns:
            columns[col_name]["unique"] = True

    return columns


def _normalize_type(row: dict[str, Any]) -> str:
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
    connection: Any,
    table_name: str,
    schema_name: str,
) -> dict[str, Index]:
    """Get non-primary-key, non-constraint-backed indexes for a table.

    Indexes that implement a PRIMARY KEY, UNIQUE or EXCLUDE constraint
    are owned by the constraint, not independently declared, so they are
    excluded here. A model declares those via ``unique: True`` on the
    column and never as an index entry; surfacing them as indexes would
    put them in the differ's ``current - target`` set and schedule a
    ``drop_index`` for a constraint the model still declares.

    Standalone indexes built with CREATE [UNIQUE] INDEX have no owning
    constraint and are still reported — those are genuinely declared.
    """
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
        LEFT JOIN pg_constraint c
          ON c.conindid = ix.indexrelid
          AND c.contype IN ('p', 'u', 'x')
        WHERE NOT ix.indisprimary
          AND c.oid IS NULL
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




async def introspect_views(
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




