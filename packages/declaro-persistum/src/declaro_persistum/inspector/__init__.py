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
from declaro_persistum.types import Dialect, Schema, View, for_dialect

# PLAIN DICTS. Both tables were `_Inspectors(dict)`, a class whose only member
# was `__missing__`. See `for_dialect` in `types.py`: the lookup is the thing
# that varied, so the lookup is a function and the tables are data.
INSPECTORS: dict[str, Any] = {
    "postgresql": postgresql.introspect,
    "sqlite": sqlite.introspect,
    "turso": turso.introspect,
}

TABLE_EXISTS: dict[str, Any] = {
    "postgresql": postgresql.table_exists,
    "sqlite": sqlite.table_exists,
    "turso": turso.table_exists,
}


async def introspect(connection: Any, dialect: Dialect) -> Schema:
    """Read a database's tables back out of it."""
    inspector = for_dialect(INSPECTORS, dialect, "Inspectors")
    result = await inspector(connection, include_views=False)
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
    inspector = for_dialect(INSPECTORS, dialect, "Inspectors")
    result = await inspector(connection, include_views=True)
    return cast(tuple[Schema, dict[str, View]], result)


# THE SAME SHAPE AS `TABLE_EXISTS`, and for the same reason. `usage.md` has
# documented `get_table_columns` as the inspector's since before it existed on
# more than one engine: it was defined in `turso.py` alone, so the documented
# call worked on one of three backends and could not be written portably. That
# is exactly how `declaro generate` came to raise on SQLite.
TABLE_COLUMNS: dict[str, Any] = {
    "postgresql": postgresql.get_table_columns,
    "sqlite": sqlite.get_table_columns,
    "turso": turso.get_table_columns,
}


async def get_table_columns(
    connection: Any, dialect: Dialect, table: str
) -> dict[str, Any]:
    """One table's columns, or a refusal naming the table."""
    reader = for_dialect(TABLE_COLUMNS, dialect, "Table-column readers")
    return cast(dict[str, Any], await reader(connection, table))


async def table_exists(connection: Any, dialect: Dialect, table: str) -> bool:
    exists = for_dialect(TABLE_EXISTS, dialect, "Table-exists checks")
    return cast(bool, await exists(connection, table))


__all__ = [
    "INSPECTORS",
    "TABLE_COLUMNS",
    "TABLE_EXISTS",
    "get_table_columns",
    "introspect",
    "introspect_with_views",
    "table_exists",
]
