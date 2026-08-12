"""Introspection: one function per dialect, chosen by a dispatch table.

`create_inspector(dialect)` was already a dispatch table. It dispatched on
CLASSES and instantiated the winner on the way out:

    INSPECTORS = {"postgresql": PostgreSQLInspector, ...}
    return INSPECTORS[dialect]()

Every class in it was stateless — no `__init__`, no fields, methods taking a
connection and returning data, and a `get_dialect()` returning a string
literal. So the table is unchanged and the values are now the functions the
table should always have held.

    schema = await introspect(conn, "sqlite")

An unknown dialect raises `ValueError` naming the ones that exist, rather than
returning None and failing later somewhere that cannot say why.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.inspector import postgresql, sqlite, turso
from declaro_persistum.types import Schema, View


class _Inspectors(dict):
    """A dispatch table that explains itself when asked for something absent.

    A bare dict raises `KeyError: 'mysql'`, which tells a caller what they
    asked for and not what was available. Subclassing to override one method
    is the smallest thing that turns the failure into an answer.
    """

    def __missing__(self, dialect: str) -> Any:
        raise ValueError(
            f"Unsupported dialect: {dialect}. "
            f"Supported dialects: {', '.join(sorted(self))}"
        )


INSPECTORS = _Inspectors(
    {
        "postgresql": postgresql.introspect,
        "sqlite": sqlite.introspect,
        "turso": turso.introspect,
    }
)

TABLE_EXISTS = _Inspectors(
    {
        "postgresql": postgresql.table_exists,
        "sqlite": sqlite.table_exists,
        "turso": turso.table_exists,
    }
)


async def introspect(
    connection: Any,
    dialect: str,
    *,
    include_views: bool = False,
) -> Schema | tuple[Schema, dict[str, View]]:
    """Read a database's schema back out of it."""
    return await INSPECTORS[dialect](connection, include_views=include_views)


async def table_exists(connection: Any, dialect: str, table: str) -> bool:
    return await TABLE_EXISTS[dialect](connection, table)


__all__ = ["INSPECTORS", "TABLE_EXISTS", "introspect", "table_exists"]
