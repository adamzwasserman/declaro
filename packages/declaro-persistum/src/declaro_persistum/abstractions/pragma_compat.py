"""PRAGMA access.

persistum reads a database's shape through these four PRAGMAs: the columns of a
table, the indexes on it, the columns those indexes cover, and the foreign keys
declared. Every supported backend answers all four natively.

Errors are not caught. A backend that cannot answer fails at the point of the
missing capability.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.types import NOTHING, Nothing

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
    # One driver hands back a cursor and another a coroutine yielding one, so
    # awaiting a list of nought or one covers both without a branch.
    awaited = [await v for v in [value] if inspect.isawaitable(v)]
    return next(iter(awaited), value)


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


# WHAT EACH READER ASKS FOR, one row per pragma. These were four functions
# with one body between them: name a pragma, quote the identifier, hand the
# rows back. The pragma was the only thing that differed, and it is data now.
#
# THE DOCSTRING IS IN THE TABLE TOO, because it was the second thing that
# differed and a reader looking for "what does index_info give me" should find
# the answer beside the pragma rather than in a function that no longer exists.
_PRAGMAS: dict[str, tuple[str, str]] = {
    "pragma_table_info": ("table_info", "Columns of `table`."),
    "pragma_index_list": ("index_list", "Indexes on `table`."),
    "pragma_index_info": ("index_info", "Columns covered by `index_name`."),
    "pragma_foreign_key_list": (
        "foreign_key_list",
        "Foreign keys declared on `table`.",
    ),
}


def _reader(pragma: str, purpose: str) -> Callable[[Any, str], Awaitable[list[tuple]]]:
    """Build the reader for one pragma.

    THE QUOTING IS WHY THIS IS ONE FUNCTION AND NOT FOUR. A pragma takes a
    quoted identifier, a double quote inside a name has to be doubled, and
    four copies of that rule are four chances for one of them to drift. There
    is one copy now, on the line below.
    """

    @helper
    async def read(conn: Any, subject: str) -> list[tuple]:
        return await _rows(conn, f'PRAGMA {pragma}("{as_identifier(subject)}")')

    read.__name__ = f"pragma_{pragma}"
    read.__qualname__ = read.__name__
    read.__doc__ = purpose
    return read


pragma_table_info = _reader(*_PRAGMAS["pragma_table_info"])
pragma_index_list = _reader(*_PRAGMAS["pragma_index_list"])
pragma_index_info = _reader(*_PRAGMAS["pragma_index_info"])
pragma_foreign_key_list = _reader(*_PRAGMAS["pragma_foreign_key_list"])


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

    # Origin "u" and "pk" are indexes SQLite made for a UNIQUE or PRIMARY KEY
    # constraint. They belong to the constraint, not to a declaration anybody
    # wrote, so they are not read back.
    declared = [row[1] for row in index_list if row[3] not in ("u", "pk")]

    statements = [
        await _maybe_await(
            (
                await _maybe_await(
                    connection.execute(
                        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                        (name,),
                    )
                )
            ).fetchone()
        )
        for name in declared
    ]

    # A partial index and a dropped one both read back as no SQL, and neither
    # is a statement to return.
    return [row[0] for row in statements if row and row[0]]


@boundary
async def enforcement_restored(connection: Any) -> Exception | Nothing:
    """Turn `PRAGMA foreign_keys` back on, and RETURN whatever stopped it.

    ONE DEFINITION FOR BOTH REBUILD PATHS. `reconstruction.py` and
    `table_reconstruction.py` each carried their own copy, character for
    character the same, and the gate that watches those two modules caught
    them the moment they converged. A fix to one would have missed the other,
    which is what the `_map_type` crash cost this package three times over.

    IT DOES NOT RAISE AND IT DOES NOT LOG. It runs in an `except`, so raising
    would replace the rebuild failure already travelling and the caller would
    never learn what actually went wrong. Both copies wrote an error to a log
    instead and returned a value every caller discarded, so a connection left
    with enforcement OFF reached nobody at all. The failure comes back as a
    value and the caller puts it on the exception going out.
    """
    try:
        await _maybe_await(connection.execute("PRAGMA foreign_keys = ON"))
        return NOTHING
    except Exception as fk_exc:
        return fk_exc
