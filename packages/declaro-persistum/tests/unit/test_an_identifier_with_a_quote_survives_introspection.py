"""A legal table name must not break the queries that ask about it.

MEASURED 2026-08-18, creating a table named `od'd` and introspecting:

    sqlite  OperationalError: near "d": syntax error
    turso   DatabaseError:    near "d": syntax error

Not for that table. For the whole database. `introspect` is the first thing
every migration does, so `declaro diff` against a database holding one such
table fails entirely, and the error names a fragment of the name rather than
the name.

WHERE IT COMES FROM. Ten places build a query by dropping an identifier into
a string:

    PRAGMA table_info('{table}')            single-quoted, four sites
    PRAGMA index_list("{table}")            double-quoted, four sites
    SELECT ... WHERE name='{table}'         inside a string literal, three

An apostrophe closes the literal in the first and third forms; a double quote
closes it in the second. Both appear in real names, and the same shape was
fixed in `table_exists` earlier today, where a bound parameter was the answer.
A PRAGMA takes no bound parameters, so the identifier has to be escaped
instead, and the two quotings need different escapes.

THIS IS THE INJECTION HOLE SEEN FROM THE SAFE SIDE. Introspection is handed
names by the database rather than by a user, so the exposure is a database
this package did not create, which is exactly what `declaro diff` is pointed
at. The test asserts the answer rather than the absence of an exploit.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.inspector import introspect

pytestmark = [pytest.mark.precommit]

# Names that close each of the two quotings, plus one that closes neither.
AWKWARD = ["od'd", 'quo"ted', "plain_one"]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "q.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "q.db"))


ENGINES = {"sqlite": _sqlite, "turso": _turso}


def _quoted(name: str) -> str:
    """The name as SQL spells it, with the double quote doubled."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


@pytest.mark.parametrize("name", AWKWARD)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_table_with_an_awkward_name_is_introspected(dialect: str, name: str) -> None:
    """The measured crash. One such table broke the whole introspection."""
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(f"CREATE TABLE {_quoted(name)} (id INTEGER PRIMARY KEY, v TEXT)")
        await connection.commit()

        actual = await introspect(connection, dialect)

        assert name in actual, sorted(actual)
    finally:
        await connection.close()


@pytest.mark.parametrize("name", AWKWARD)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_its_columns_come_back_too(dialect: str, name: str) -> None:
    """The floor. A table reported with no columns is a table reported wrong.

    Every PRAGMA that reads a column, an index or a foreign key takes the
    table name the same way, so a fix that only repaired the catalogue query
    would leave the table present and empty.
    """
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(f"CREATE TABLE {_quoted(name)} (id INTEGER PRIMARY KEY, v TEXT)")
        await connection.commit()

        actual = await introspect(connection, dialect)

        assert sorted(actual[name]["columns"]) == ["id", "v"], actual[name]
    finally:
        await connection.close()


@pytest.mark.parametrize("name", AWKWARD)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_its_index_comes_back_too(dialect: str, name: str) -> None:
    """`index_list` and the sqlite_master index query both take the name."""
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(f"CREATE TABLE {_quoted(name)} (id INTEGER PRIMARY KEY, v TEXT)")
        await connection.execute(f'CREATE INDEX "ix_v" ON {_quoted(name)} (v)')
        await connection.commit()

        actual = await introspect(connection, dialect)

        assert "ix_v" in actual[name].get("indexes", {}), actual[name]
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_one_awkward_table_does_not_hide_the_others(dialect: str) -> None:
    """The consequence that makes this worse than one bad row.

    Introspection reads every table in one pass, so a name that breaks the
    query breaks the answer for the whole database.
    """
    connection = await ENGINES[dialect]()
    try:
        for name in ("ordinary", "od'd"):
            await connection.execute(f"CREATE TABLE {_quoted(name)} (id INTEGER PRIMARY KEY)")
        await connection.commit()

        actual = await introspect(connection, dialect)

        assert {"ordinary", "od'd"} <= set(actual), sorted(actual)
    finally:
        await connection.close()
