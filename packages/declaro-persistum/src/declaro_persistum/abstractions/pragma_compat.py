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


async def _rows(conn: Any, query: str) -> list[tuple]:
    cursor = await _maybe_await(conn.execute(query))
    return list(await _maybe_await(cursor.fetchall()) or [])


async def pragma_table_info(conn: Any, table: str) -> list[tuple]:
    """Columns of `table`."""
    return await _rows(conn, f'PRAGMA table_info("{table}")')


async def pragma_index_list(conn: Any, table: str) -> list[tuple]:
    """Indexes on `table`."""
    return await _rows(conn, f'PRAGMA index_list("{table}")')


async def pragma_index_info(conn: Any, index_name: str) -> list[tuple]:
    """Columns covered by `index_name`."""
    return await _rows(conn, f'PRAGMA index_info("{index_name}")')


async def pragma_foreign_key_list(conn: Any, table: str) -> list[tuple]:
    """Foreign keys declared on `table`."""
    return await _rows(conn, f'PRAGMA foreign_key_list("{table}")')
