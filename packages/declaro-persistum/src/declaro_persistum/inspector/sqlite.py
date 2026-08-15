"""Read a SQLite database's shape back out of it. Functions, not a class.

`SQLiteInspector` was stateless: no `__init__`, no fields, four methods that
each took a connection and returned data, and a `get_dialect()` that returned
the string "sqlite". Instantiating it accomplished nothing — `create_inspector`
built one, called one method, and dropped it.

The pure half of this already lives in `inspector/shared.py`:
`columns_from_pragma_rows`, `indexes_from_rows`, `fk_list_from_pragma_rows`,
`assemble_table`, `views_from_rows`. What is here is the I/O — which PRAGMA to
send and in what order — and nothing else.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.abstractions.materialized_views import (
    MATVIEW_METADATA_TABLE,
)
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    assemble_table,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Schema, Table, View

# Tables neither the user nor the differ should ever see. `sqlite_%` belongs to
# the engine and `_declaro_%` to us; a schema containing either would make the
# differ propose dropping the machinery it runs on.
_USER_TABLES = """
    SELECT name FROM sqlite_master
    WHERE type = 'table'
      AND name NOT LIKE 'sqlite_%'
      AND name NOT LIKE '_declaro_%'
      AND name NOT LIKE '_dp_materialized_views'
      AND name NOT LIKE '__turso_%'
      AND name NOT LIKE 'turso_%'
    ORDER BY name
"""

# The turso patterns are here as well as in the turso inspector because
# `open_turso` runs the Turso engine even for a local database, and that engine
# creates __turso_internal_mvcc_meta. A caller passing dialect="sqlite" for a
# SQLite-compatible file would otherwise have the differ read a system table as
# a user table and emit drop_table for it. The engine refuses the drop, so the
# migration fails outright.


async def _rows(connection: Any, sql: str) -> list[tuple]:
    cursor = await connection.execute(sql)
    return await cursor.fetchall()


async def columns_of(connection: Any, table: str) -> dict[str, Column]:
    """Column definitions, from PRAGMA table_info plus the unique indexes.

    Uniqueness is not in table_info — it lives in index_list — so the two are
    read together here rather than leaving a caller to remember.
    """
    info = await _rows(connection, f"PRAGMA table_info('{table}')")
    columns = columns_from_pragma_rows(info)

    # `unique_cols_from_index_rows` needs the columns of each index as well as
    # the index list, because a UNIQUE index over two columns is a table
    # constraint and NOT a unique column — only single-column ones set the
    # flag. So index_info is read per index before the pure function decides.
    index_rows = await _rows(connection, f"PRAGMA index_list('{table}')")
    index_info = {
        row[1]: await _rows(connection, f"PRAGMA index_info('{row[1]}')")
        for row in index_rows
    }
    for name in unique_cols_from_index_rows(index_rows, index_info):
        if name in columns:
            columns[name]["unique"] = True
    return columns


async def indexes_of(connection: Any, table: str) -> dict[str, Any]:
    """Indexes, from index_list plus each index's columns and its CREATE SQL.

    Three reads, because the pure builder needs three things and none of them
    come from the same place: which indexes exist, which columns each covers,
    and the original SQL — the last only so a partial index keeps its WHERE
    clause, which appears nowhere in the PRAGMAs.

    Indexes whose origin is "pk" or "u" are dropped by the builder: they are
    consequences of a column definition, not objects a caller declared, and
    reporting them would make the differ try to recreate them.
    """
    index_rows = await _rows(connection, f"PRAGMA index_list('{table}')")
    index_info = {
        row[1]: await _rows(connection, f"PRAGMA index_info('{row[1]}')")
        for row in index_rows
    }
    sql_rows = await _rows(
        connection,
        f"SELECT name, sql FROM sqlite_master "
        f"WHERE type = 'index' AND tbl_name = '{table}'",
    )
    index_sql = {name: sql for name, sql in sql_rows}
    return indexes_from_rows(index_rows, index_info, index_sql)


async def foreign_keys_of(connection: Any, table: str) -> list[dict[str, str]]:
    rows = await _rows(connection, f"PRAGMA foreign_key_list('{table}')")
    return fk_list_from_pragma_rows(rows)


async def table_of(connection: Any, table: str) -> Table:
    return assemble_table(
        await columns_of(connection, table),
        await indexes_of(connection, table),
        await foreign_keys_of(connection, table),
    )


async def matviews_of(connection: Any) -> dict[str, tuple[str, str]]:
    """The emulated materialized views, from the metadata table.

    Absent table means none have been created, which is not an error. This is
    the I/O half of the split described in `views_from_rows`.

    ABSENCE IS ASKED, NOT CAUGHT. This was `except Exception: return {}`
    around the SELECT, so a table that had never been created and a table
    that could not be read produced the identical empty answer, and the
    caller went on to report a schema with no materialized views in it. On
    the migration path that reads as "the user declared none" and every
    matview in the database becomes a drop.

    "Has the table been created?" is a question sqlite_master answers
    directly, so it is asked directly. Anything that goes wrong AFTER the
    table is known to exist is a real failure and now raises.
    """
    present = await _rows(
        connection,
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        f"AND name = '{MATVIEW_METADATA_TABLE}'",
    )
    if not present:
        return {}
    rows = await _rows(
        connection,
        "SELECT name, query, refresh_strategy "
        f"FROM {MATVIEW_METADATA_TABLE} ORDER BY name",
    )
    return {name: (query, refresh) for name, query, refresh in rows}


async def views_of(connection: Any) -> dict[str, View]:
    rows = await _rows(
        connection,
        "SELECT name, sql FROM sqlite_master WHERE type = 'view' ORDER BY name",
    )
    return views_from_rows(rows, await matviews_of(connection))


async def introspect(
    connection: Any,
    *,
    _schema_name: str = "main",
    include_views: bool = False,
) -> Schema | tuple[Schema, dict[str, View]]:
    """Read the whole schema back.

    A connection failure is raised as a typed `ConnectionError` carrying the
    dialect, so a caller can tell "the database is unreachable" from "the
    database says no" without reading the message (Rule 8).
    """
    try:
        # A MATERIALIZED VIEW IS A REAL TABLE HERE, and it must not be
        # reported as one. SQLite has no materialized views, so the emulation
        # creates a table plus a metadata row. Introspection returned that
        # table like any other, the models file never declares it, and the
        # differ proposed dropping it on every migration — which would have
        # taken the view's contents with it. Measured 2026-08-14: a schema
        # with one materialized view re-applied 3 operations on an unchanged
        # re-run, where the answer is 0.
        backing = await matviews_of(connection)
        tables = await _rows(connection, _USER_TABLES)
        schema: Schema = {
            name: await table_of(connection, name)
            for (name,) in tables
            if name not in backing
        }
        if include_views:
            return schema, await views_of(connection)
        return schema
    except Exception as e:
        text = str(e).lower()
        if "database" in text or "connection" in text:
            raise DeclaroConnectionError(
                f"Failed to introspect database: {e}", dialect="sqlite"
            ) from e
        raise


async def table_exists(connection: Any, table: str) -> bool:
    rows = await _rows(
        connection,
        f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table}'",
    )
    return bool(rows)
