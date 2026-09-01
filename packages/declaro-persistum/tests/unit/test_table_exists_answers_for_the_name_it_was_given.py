"""`table_exists` must answer the question the engine would answer.

WRITTEN BEFORE THE FIX. `table_exists` is public API with a dispatch table
across all three engines, and coverage over the whole suite showed every one
of the three implementations dark. Two of them are wrong and the third is
the only one that binds its parameter.

MEASURED 2026-08-18, creating `"UserProfile"` and `"od'd"` on each engine and
asking `table_exists`:

                    lower    UserProfile   name with a quote
    sqlite          True     True          OperationalError
    turso           True     False         -
    postgresql      True     True          True

TWO DEFECTS.

`sqlite.table_exists` interpolates the name into the SQL where the other two
bind it, so a legal table name containing an apostrophe raises instead of
answering, and any caller passing a name it did not choose is injectable.

`turso.table_exists` compares `sqlite_master.name` with `=`, and Turso folds
that column, so a table that exists answers False. It is the same folding
that made a capitalised table loop forever, in a second place: the fix in
`introspect` did not reach here.

WHAT COUNTS AS THE SAME TABLE IS NOT THE SAME ON ALL THREE, and this is the
part a uniform expectation would get wrong. Measured by creating
`"UserProfile"` and then trying the folded spelling:

    sqlite      SELECT "userprofile" works; CREATE "userprofile" refused
    turso       SELECT "userprofile" works; CREATE "userprofile" refused
    postgresql  SELECT "userprofile" raises; CREATE "userprofile" allowed

So on SQLite and Turso they are one table and `table_exists` must say True for
either spelling. On PostgreSQL a quoted name is case-sensitive, they are two
tables, and saying True for the wrong one would be the defect rather than the
fix.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.inspector import table_exists
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.precommit]

PROBE_URL = probe_url()

# Whether the engine treats a quoted name as case-insensitive. Measured, not
# assumed: PostgreSQL is the one that does not, and this package quotes every
# identifier it writes.
FOLDS_NAMES = {"sqlite": True, "turso": True, "postgresql": False}

MADE = ["users", "UserProfile"]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "e.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "e.db"))


async def _postgresql() -> Any:
    import asyncpg

    try:
        return await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)


CONNECT = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}
ENGINES = sorted(CONNECT)


async def _execute(connection: Any, engine: str, sql: str) -> None:
    if engine == "postgresql":
        await connection.execute(sql)
        return
    await connection.execute(sql)
    await connection.commit()


async def _prepared(engine: str, names: list[str]) -> Any:
    connection = await CONNECT[engine]()
    for name in names:
        if engine == "postgresql":
            await _execute(connection, engine, f'DROP TABLE IF EXISTS "{name}" CASCADE')
        await _execute(connection, engine, f'CREATE TABLE "{name}" (id INTEGER)')
    return connection


async def _done(connection: Any, engine: str, names: list[str]) -> None:
    if engine == "postgresql":
        for name in names:
            await _execute(connection, engine, f'DROP TABLE IF EXISTS "{name}" CASCADE')
    await connection.close()


@pytest.mark.parametrize("engine", ENGINES)
async def test_a_table_that_is_there_answers_true(engine: str) -> None:
    connection = await _prepared(engine, MADE)
    try:
        assert await table_exists(connection, engine, "users") is True
    finally:
        await _done(connection, engine, MADE)


@pytest.mark.parametrize("engine", ENGINES)
async def test_a_table_that_is_not_there_answers_false(engine: str) -> None:
    """The floor. A function that always says True would pass the test above."""
    connection = await _prepared(engine, MADE)
    try:
        assert await table_exists(connection, engine, "no_such_table") is False
    finally:
        await _done(connection, engine, MADE)


@pytest.mark.parametrize("engine", ENGINES)
async def test_a_capitalised_table_answers_true_for_its_own_spelling(
    engine: str,
) -> None:
    """The Turso defect. The table is there and it said it was not."""
    connection = await _prepared(engine, MADE)
    try:
        assert await table_exists(connection, engine, "UserProfile") is True
    finally:
        await _done(connection, engine, MADE)


@pytest.mark.parametrize("engine", ENGINES)
async def test_the_folded_spelling_answers_what_the_engine_would(
    engine: str,
) -> None:
    """Where the three engines genuinely differ, and the answer follows them."""
    connection = await _prepared(engine, MADE)
    try:
        answer = await table_exists(connection, engine, "userprofile")

        assert answer is FOLDS_NAMES[engine], (
            f"{engine} answered {answer} for the folded spelling of a table "
            f"created as UserProfile; the engine treats them as "
            f"{'one table' if FOLDS_NAMES[engine] else 'two tables'}"
        )
    finally:
        await _done(connection, engine, MADE)


@pytest.mark.parametrize("engine", ENGINES)
async def test_a_name_with_an_apostrophe_is_answered_not_raised(
    engine: str,
) -> None:
    """A legal table name must not break the query that asks about it.

    This is the injection hole seen from the safe side: the name reaches the
    SQL as a value on two engines and as text on the third.
    """
    names = [*MADE, "od'd"]
    connection = await _prepared(engine, names)
    try:
        assert await table_exists(connection, engine, "od'd") is True
    finally:
        await _done(connection, engine, names)


@pytest.mark.parametrize("engine", ENGINES)
async def test_a_name_carrying_sql_is_a_name_and_nothing_else(
    engine: str,
) -> None:
    """The same hole seen from the other side.

    No table is called this, so the answer is False. What matters is that it
    is answered at all: interpolating it closes the string literal and runs
    what follows.
    """
    connection = await _prepared(engine, MADE)
    try:
        answer = await table_exists(connection, engine, "x' OR '1'='1")

        assert answer is False, (
            "a name that closes the quote matched something, so it was "
            "interpolated rather than bound"
        )
    finally:
        await _done(connection, engine, MADE)
