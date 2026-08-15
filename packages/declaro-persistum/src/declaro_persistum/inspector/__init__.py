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

from typing import Any, cast

from declaro_persistum.inspector import postgresql, sqlite, turso
from declaro_persistum.types import Dialect, Schema, View


class _Inspectors(dict):
    """A dispatch table that explains itself when asked for something absent.

    A bare dict raises `KeyError: 'mysql'`, which tells a caller what they
    asked for and not what was available. Subclassing to override one method
    is the smallest thing that turns the failure into an answer.
    """

    def __missing__(self, dialect: Dialect) -> Any:
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


async def introspect(connection: Any, dialect: Dialect) -> Schema:
    """Read a database's tables back out of it."""
    result = await INSPECTORS[dialect](connection, include_views=False)
    return cast(Schema, result)


async def introspect_with_views(
    connection: Any, dialect: Dialect
) -> tuple[Schema, dict[str, View]]:
    """Read a database's tables AND its views back out of it.

    TWO FUNCTIONS, NOT A FLAG. This was `introspect(conn, dialect,
    include_views=False)` returning `Schema | tuple[Schema, dict[str, View]]`,
    and the union is what makes it two functions wearing one name: the caller
    cannot know which shape it holds without knowing the argument it passed.

    That is not theoretical. `migrations.py` unpacks the pair, and a union of
    "a dict" and "a pair" unpacks silently as the dict's KEYS, so mypy read
    the schema as `str` and reported four errors downstream that all traced
    back here. A caller writing `schema = await introspect(conn, dialect,
    include_views=True)` gets a tuple and no complaint at all.

    The flag was also an implicit default (Rule 14): `False` decided for every
    caller who never knew there was a choice.
    """
    result = await INSPECTORS[dialect](connection, include_views=True)
    return cast(tuple[Schema, dict[str, View]], result)


async def table_exists(connection: Any, dialect: Dialect, table: str) -> bool:
    return await TABLE_EXISTS[dialect](connection, table)


__all__ = [
    "INSPECTORS",
    "TABLE_EXISTS",
    "introspect",
    "introspect_with_views",
    "table_exists",
]
