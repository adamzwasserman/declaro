"""PRAGMA access.

persistum reads a database's shape through these four PRAGMAs: the columns of a
table, the indexes on it, the columns those indexes cover, and the foreign keys
declared. Every supported backend answers all four natively.

Errors are not caught. A backend that cannot answer fails at the point of the
missing capability.
"""

from __future__ import annotations

import inspect
from typing import Any

from declaro_persistum.boundary import boundary, helper

__all__ = [
    "pragma_table_info",
    "pragma_index_list",
    "pragma_index_info",
    "pragma_foreign_key_list",
]


async def _maybe_await(value: Any) -> Any:
    """Await a value if it is awaitable.

    The drivers disagree: some return a cursor, some a coroutine yielding one.
    """
    if inspect.isawaitable(value):
        return await value
    return value


@boundary
async def _rows(conn: Any, query: str) -> list[tuple]:
    cursor = await _maybe_await(conn.execute(query))
    return list(await _maybe_await(cursor.fetchall()) or [])


@helper
def as_identifier(name: str) -> str:
    """An identifier as a quoted name, double quotes doubled.

    DEFINED HERE RATHER THAN BESIDE `as_literal` IN `inspector/shared`,
    because importing the inspector from an abstraction is a cycle: the
    inspector imports this module.

    Every PRAGMA below drops a name into a double-quoted string, so a legal
    name holding a double quote closes it and the engine reports a syntax
    error naming a fragment. The sibling `as_literal` escapes for the single
    quoting the sqlite_master lookups use. Two quotings, two escapes, and
    using either for both leaves half the names broken.
    """
    return name.replace('"', '""')


@helper
async def pragma_table_info(conn: Any, table: str) -> list[tuple]:
    """Columns of `table`."""
    return await _rows(conn, f'PRAGMA table_info("{as_identifier(table)}")')


@helper
async def pragma_index_list(conn: Any, table: str) -> list[tuple]:
    """Indexes on `table`."""
    return await _rows(conn, f'PRAGMA index_list("{as_identifier(table)}")')


@helper
async def pragma_index_info(conn: Any, index_name: str) -> list[tuple]:
    """Columns covered by `index_name`."""
    return await _rows(conn, f'PRAGMA index_info("{as_identifier(index_name)}")')


@helper
async def pragma_foreign_key_list(conn: Any, table: str) -> list[tuple]:
    """Foreign keys declared on `table`."""
    return await _rows(conn, f'PRAGMA foreign_key_list("{as_identifier(table)}")')


# =============================================================================
# Readers built on the pragmas above
# =============================================================================
#
# ONE DEFINITION EACH, AND THERE WERE TWO. `abstractions/reconstruction.py` and
# `abstractions/table_reconstruction.py` each carried its own copy, named
# `_get_table_columns_async` / `_get_table_columns` and
# `_get_table_indexes_async` / `_get_table_indexes`. Measured 2026-08-17 the
# bodies were character-for-character identical; only the name and one word of
# docstring differed, and both were `async def` so the `_async` suffix
# distinguished nothing.
#
# A fix to one copy misses the other in silence. This package has already paid
# that: the `Literal` `_map_type` crash needed the same fix in three files.
#
# They live here because this is where the pragmas they read live.

@helper
async def table_column_names(connection: Any, table_name: str) -> list[str]:
    """Get list of column names for a table ."""
    rows = await pragma_table_info(connection, table_name)
    return [row[1] for row in rows]  # Column name is at index 1


@boundary
async def create_index_statements(connection: Any, table_name: str) -> list[str]:
    """
    Get CREATE INDEX statements for all indexes on a table .

    Returns list of CREATE INDEX SQL statements.
    """
    index_list = await pragma_index_list(connection, table_name)
    index_sqls = []

    for row in index_list:
        # row format: (seq, name, unique, origin, partial)
        index_name = row[1]
        origin = row[3]

        # Skip auto-generated indexes (UNIQUE/PRIMARY KEY constraints)
        if origin in ("u", "pk"):
            continue

        # Get CREATE INDEX statement from sqlite_master
        cursor = await connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (index_name,),
        )
        result = await cursor.fetchone()

        if result and result[0]:
            index_sqls.append(result[0])

    return index_sqls
