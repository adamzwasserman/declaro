"""The query executor had never executed a query against a database.

MEASURED 2026-08-18 with `coverage json` over the whole suite. Of the 47
unexecuted statements in `query/executor.py`, these are all of them that
matter, and together they are the entire per-dialect execution surface:

    _execute_fetch        asyncpg and DB-API branches   both dark
    _execute_fetch_one    asyncpg and DB-API branches   both dark
    _execute_fetch_scalar asyncpg and DB-API branches   both dark
    _execute_update       asyncpg and DB-API branches   both dark
    _convert_to_asyncpg   :name -> $1                   dark
    _convert_to_turso     :name -> ?                    dark
    the four "Unsupported connection type" raises       dark

So every test of this layer had asserted on the SQL it builds and none had
handed that SQL to an engine. A builder that produces text no driver accepts
passes every one of them, which is the failure this package has already
shipped twice: `COUNT(*)` came back as the string "COUNT(*)", and `update`
dropped its WHERE bindings.

A WRITE HAS TO BE RUN AS A BATCH OF ONE. `_execute_update` is reachable from
`execute_many` and from nowhere else, so the only way to run a single INSERT,
UPDATE or DELETE built by this package is to hand `execute_many` a list
holding one empty dict:

    await execute_many(insert("people", {...}), connection, [{}])

The builder offers `insert`, `update` and `delete`; the executor offers no way
to run one of them once. That is how the writes are exercised below, because
it is the only way a caller has. It is worth a named function and this file
does not add one.

THE THREE ENGINES TAKE THREE DIFFERENT PARAMETER SPELLINGS, which is why a
single-engine test proves so little here. asyncpg wants `$1` positionally,
aiosqlite takes `:name` as it stands, and pyturso wants `?` in the order the
names appear in the SQL. The converter for two of those three had never run.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.query.builder import delete, insert, raw, select, update
from declaro_persistum.query.executor import (
    execute,
    execute_many,
    execute_one,
    execute_scalar,
)

pytestmark = [pytest.mark.precommit, pytest.mark.integration]

PROBE_URL = os.environ.get(
    "TEST_POSTGRESQL_URL", "postgresql://postgres:postgres@localhost/declaro_probe"
)

ROWS = [(1, "alice", 30), (2, "bob", 25), (3, "carol", 35)]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "q.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "q.db"))


async def _postgresql() -> Any:
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    await connection.execute('DROP TABLE IF EXISTS "people" CASCADE')
    return connection


CONNECT = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}
DIALECTS = sorted(CONNECT)


async def _seeded(dialect: str) -> Any:
    connection = await CONNECT[dialect]()
    await connection.execute(
        'CREATE TABLE "people" (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'
    )
    for row in ROWS:
        if dialect == "postgresql":
            await connection.execute(
                'INSERT INTO "people" (id, name, age) VALUES ($1, $2, $3)', *row
            )
        else:
            await connection.execute(
                'INSERT INTO "people" (id, name, age) VALUES (?, ?, ?)', row
            )
    if dialect != "postgresql":
        await connection.commit()
    return connection


async def _done(connection: Any, dialect: str) -> None:
    if dialect == "postgresql":
        await connection.execute('DROP TABLE IF EXISTS "people" CASCADE')
    await connection.close()


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_select_returns_every_row(dialect: str) -> None:
    connection = await _seeded(dialect)
    try:
        rows = await execute(select("*", from_table="people"), connection)

        assert len(rows) == len(ROWS), rows
        assert {r["name"] for r in rows} == {"alice", "bob", "carol"}
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_named_parameter_binds_on_every_driver(dialect: str) -> None:
    """The converters. Three engines, three parameter spellings, one query."""
    connection = await _seeded(dialect)
    try:
        rows = await execute(
            select(
                "*",
                from_table="people",
                where="age > :floor",
                params={"floor": 28},
            ),
            connection,
        )

        assert {r["name"] for r in rows} == {"alice", "carol"}, rows
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_the_same_parameter_used_twice_binds_once(dialect: str) -> None:
    """asyncpg numbers positionally, so a repeated name gets one number.

    THE REUSE GUARD IS AN OPTIMISATION, NOT A CORRECTNESS REQUIREMENT, and
    this test cannot tell the difference. Removing `if name not in param_map`
    still passes: the converter uses `str.replace`, which rewrites every
    occurrence to the number assigned last, and the value sitting at that
    index is the right one. The duplicates are wasted slots and nothing more.
    Measured as a surviving mutant rather than assumed.

    What this does check is that a repeated name reaches the driver as a
    statement it accepts at all, on each of the three.
    """
    connection = await _seeded(dialect)
    try:
        rows = await execute(
            select(
                "*",
                from_table="people",
                where="age > :n AND age < :n + 100",
                params={"n": 28},
            ),
            connection,
        )

        assert {r["name"] for r in rows} == {"alice", "carol"}, rows
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_execute_one_returns_a_single_row(dialect: str) -> None:
    connection = await _seeded(dialect)
    try:
        row = await execute_one(
            select(
                "*", from_table="people", where="name = :n", params={"n": "bob"}
            ),
            connection,
        )

        assert row is not None and row["age"] == 25, row
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_execute_one_returns_none_when_nothing_matches(
    dialect: str,
) -> None:
    """The floor. A function that always returns the first row would pass above."""
    connection = await _seeded(dialect)
    try:
        row = await execute_one(
            select(
                "*", from_table="people", where="name = :n", params={"n": "nobody"}
            ),
            connection,
        )

        assert row is None, row
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_scalar_comes_back_as_a_value(dialect: str) -> None:
    """`COUNT(*)` came back as the string "COUNT(*)" once. It is a number."""
    connection = await _seeded(dialect)
    try:
        total = await execute_scalar(
            select("COUNT(*)", from_table="people"), connection
        )

        assert total == len(ROWS), repr(total)
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_an_insert_lands_and_is_counted(dialect: str) -> None:
    connection = await _seeded(dialect)
    try:
        affected = await execute_many(
            insert("people", {"id": 4, "name": "dave", "age": 40}),
            connection,
            [{}],
        )

        assert affected == 1, affected
        assert (
            await execute_scalar(select("COUNT(*)", from_table="people"), connection)
            == len(ROWS) + 1
        )
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_an_update_changes_only_the_matching_rows(dialect: str) -> None:
    """`update` dropped its WHERE bindings once, which changed every row."""
    connection = await _seeded(dialect)
    try:
        await execute_many(
            update("people", {"age": 99}, where="name = :n", params={"n": "bob"}),
            connection,
            [{}],
        )

        rows = await execute(select("*", from_table="people"), connection)
        by_name = {r["name"]: r["age"] for r in rows}

        assert by_name == {"alice": 30, "bob": 99, "carol": 35}, by_name
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_delete_removes_only_the_matching_rows(dialect: str) -> None:
    connection = await _seeded(dialect)
    try:
        await execute_many(
            delete("people", where="name = :n", params={"n": "bob"}),
            connection,
            [{}],
        )

        rows = await execute(select("*", from_table="people"), connection)

        assert {r["name"] for r in rows} == {"alice", "carol"}, rows
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_raw_sql_carries_its_parameters(dialect: str) -> None:
    connection = await _seeded(dialect)
    try:
        rows = await execute(
            raw('SELECT name FROM "people" WHERE age = :a', {"a": 25}), connection
        )

        assert [r["name"] for r in rows] == ["bob"], rows
    finally:
        await _done(connection, dialect)
