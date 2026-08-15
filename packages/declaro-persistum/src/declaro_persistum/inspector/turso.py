"""Read a Turso database's shape back out of it. Functions, not a class.

`TursoInspector` was stateless, like the others. Turso aims at SQLite
compatibility but is a separate Rust engine, so every PRAGMA goes through
`abstractions/pragma_compat.py`, which emulates the ones the engine does not
implement natively. That indirection is the whole reason this is not simply
the SQLite inspector pointed at a different connection.

Logic that does not depend on the PRAGMA layer is shared with SQLite through
`inspector.shared`.

THIS FILE USED TO SAY "Turso (libSQL)". Turso Database is a Rust rewrite, not
libSQL and not a libSQL fork, and the difference is the reason pragma_compat
exists: SQLite compatibility is the aim, not a guarantee.
"""

from typing import Any

from declaro_persistum.abstractions.materialized_views import (
    MATVIEW_METADATA_TABLE,
)
from declaro_persistum.abstractions.pragma_compat import (
    _maybe_await,
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


def get_dialect() -> str:
    """Return dialect identifier."""
    return "turso"


async def introspect(
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
        cursor = await _maybe_await(connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '_litestream_%'
              AND name NOT LIKE '_declaro_%'
              AND name NOT LIKE '_dp_materialized_views'
              AND name NOT LIKE '__turso_%'
              AND name NOT LIKE 'turso_%'
            ORDER BY name
            """
        ))
        tables = await _maybe_await(cursor.fetchall())

        # A materialized view is a real TABLE on this engine too, and must
        # not be reported as one. See the same guard in inspector/sqlite.py.
        views_ = await introspect_views(connection)
        backing = {n for n, v in views_.items() if v.get("materialized")}

        schema: Schema = {}

        for row in tables:
            table_name = row[0]
            if table_name in backing:
                continue
            schema[table_name] = await _introspect_table(connection, table_name)

        if include_views:
            return schema, views_

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
    connection: Any,
    table_name: str,
) -> Table:
    """Introspect a single table's structure."""
    columns = await _get_columns(connection, table_name)
    indexes = await _get_indexes(connection, table_name)
    foreign_keys = await _get_foreign_keys(connection, table_name)

    return assemble_table(columns, indexes, foreign_keys)


async def _get_columns(
    connection: Any,
    table_name: str,
) -> dict[str, Column]:
    """Get column definitions for a table."""
    rows = await pragma_table_info(connection, table_name)

    columns = columns_from_pragma_rows(rows)

    unique_cols = await _get_unique_columns(connection, table_name)
    apply_unique_columns(columns, unique_cols)

    return columns


async def _get_unique_columns(
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

        sql_cursor = await _maybe_await(connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (idx_name,),
        ))
        sql_row = await _maybe_await(sql_cursor.fetchone())
        index_sql[idx_name] = sql_row[0] if sql_row else None

    return indexes_from_rows(index_rows, index_info, index_sql)


async def _get_foreign_keys(
    connection: Any,
    table_name: str,
) -> list[dict[str, str]]:
    """Get foreign key constraints for a table."""
    rows = await pragma_foreign_key_list(connection, table_name)
    return fk_list_from_pragma_rows(rows)


async def table_exists(
    connection: Any,
    table_name: str,
) -> bool:
    """Check if a table exists."""
    cursor = await _maybe_await(connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ))
    result = await _maybe_await(cursor.fetchone())
    return result is not None


async def get_table_columns(
    connection: Any,
    table_name: str,
) -> dict[str, Any]:
    """Get column definitions for a specific table."""
    if not await table_exists(connection, table_name):
        from declaro_persistum.exceptions import DeclaroError

        raise DeclaroError(f"Table '{table_name}' does not exist")

    return await _get_columns(connection, table_name)


async def introspect_views(
    connection: Any,
) -> dict[str, View]:
    """Introspect views from Turso/libSQL."""
    cursor = await _maybe_await(connection.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'view'
          AND name NOT LIKE 'sqlite_%'
          AND name NOT LIKE '_litestream_%'
        ORDER BY name
        """
    ))
    rows = await _maybe_await(cursor.fetchall())

    # The emulated materialized views live in a metadata table, not in
    # sqlite_master. See `views_from_rows`.
    #
    # ABSENCE IS ASKED, NOT CAUGHT — the same fix as `sqlite.matviews_of`,
    # and for the same reason. `except Exception: matviews = {}` gave one
    # answer to two questions: "none have been created" and "this database
    # would not answer me". The second one, reported as the first, tells the
    # differ the user declared no materialized views and every one in the
    # database becomes a drop.
    cursor = await _maybe_await(
        connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            f"AND name = '{MATVIEW_METADATA_TABLE}'"
        )
    )
    matviews: dict[str, tuple[str, str]] = {}
    if await _maybe_await(cursor.fetchall()):
        cursor = await _maybe_await(
            connection.execute(
                "SELECT name, query, refresh_strategy "
                f"FROM {MATVIEW_METADATA_TABLE} ORDER BY name"
            )
        )
        matviews = {
            name: (query, refresh)
            for name, query, refresh in await _maybe_await(cursor.fetchall())
        }

    return views_from_rows(rows, matviews)
