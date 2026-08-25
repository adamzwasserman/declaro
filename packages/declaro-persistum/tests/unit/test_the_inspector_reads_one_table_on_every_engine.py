"""`get_table_columns` is documented as the inspector's and exists on one.

`docs/usage.md:755` shows it as part of the inspector interface, beside
`table_exists` and `get_dialect`:

    columns = await inspector.get_table_columns(conn, "users")

MEASURED 2026-08-18. It is defined in `inspector/turso.py` and nowhere else.
`inspector/sqlite.py` has `columns_of`, which does the same job under another
name; `inspector/postgresql.py` has only the private `_get_columns`. There is
no dispatch table entry and no package-level function, so the documented call
works on one of the three engines and cannot be written portably at all.

THIS IS THE SHAPE THAT MADE `declaro generate` RAISE ON SQLITE. That loop
lived in `applier/postgresql.py` alone while the docs described it as the
package's, and the fix was to move it up and give it a dispatch table.
`table_exists` in this same package already has that shape: three
implementations, a `TABLE_EXISTS` table, and one package-level function that
takes a dialect. This follows it.

Coverage reported `get_table_columns` dark, which is how it was found: a
documented function that nothing in the package calls and no test ran.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.exceptions import DeclaroError
from declaro_persistum.inspector import get_table_columns
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.precommit]

PROBE_URL = probe_url()

CREATE = 'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)'


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "i.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "i.db"))


async def _postgresql() -> Any:
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)
    await connection.execute('DROP TABLE IF EXISTS "widgets" CASCADE')
    return connection


CONNECT = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}
DIALECTS = sorted(CONNECT)


async def _prepared(dialect: str) -> Any:
    connection = await CONNECT[dialect]()
    await connection.execute(CREATE)
    if dialect != "postgresql":
        await connection.commit()
    return connection


async def _done(connection: Any, dialect: str) -> None:
    if dialect == "postgresql":
        await connection.execute('DROP TABLE IF EXISTS "widgets" CASCADE')
    await connection.close()


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_it_names_every_column(dialect: str) -> None:
    connection = await _prepared(dialect)
    try:
        columns = await get_table_columns(connection, dialect, "widgets")

        assert sorted(columns) == ["id", "name"], columns
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_it_carries_each_column_s_type(dialect: str) -> None:
    """The floor. A dict of empty definitions would satisfy the test above."""
    connection = await _prepared(dialect)
    try:
        columns = await get_table_columns(connection, dialect, "widgets")

        assert columns["name"].get("type"), columns["name"]
        assert columns["id"].get("primary_key") is True, columns["id"]
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_table_that_is_not_there_is_refused_by_name(dialect: str) -> None:
    """What `usage.md` promises: it raises rather than returning an empty dict.

    An empty dict is what an absent table and a table with no columns would
    both look like, and only one of those is possible.
    """
    connection = await _prepared(dialect)
    try:
        with pytest.raises(DeclaroError) as caught:
            await get_table_columns(connection, dialect, "no_such_table")

        assert "no_such_table" in str(caught.value), caught.value
    finally:
        await _done(connection, dialect)


def test_an_unknown_dialect_is_refused_and_the_known_ones_named() -> None:
    """The same refusal every other dispatch table in this package gives."""
    import asyncio

    # `asyncio.run`, NOT a loop made by hand. This built one with
    # `new_event_loop()` and never closed it, so the loop was finalised
    # whenever the collector next ran and its `__del__` raised
    # `ValueError: Invalid file descriptor: -1` -- reported as an unraisable
    # exception against whichever unrelated test happened to be running at the
    # time. It came and went between runs, which is what the Slop Audit sees
    # as test determinism 4/5. `asyncio.run` closes what it opens.
    with pytest.raises(ValueError) as caught:
        asyncio.run(get_table_columns(None, "mysql", "widgets"))

    message = str(caught.value)
    assert "mysql" in message and "sqlite" in message, message
