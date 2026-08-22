"""The four executor functions, none of which any test had ever called.

MEASURED 2026-08-17. `execute`, `execute_one`, `execute_many` and
`execute_scalar` are how every consumer query reaches a database, and the
reachability scan found all four called by nothing in the suite.

Running them found two defects.

FIRST, `execute_scalar` raises on SQLite:

    row = await cursor.fetchone()
    return row[0] if row else None
    KeyError: 0

The connection's row factory hands back a dict, so `row[0]` is a key lookup,
not the first column. `COUNT(*)` is the commonest scalar query there is, and
the shipped example app works around this by calling `execute_one` and then
`list(result.values())[0]` -- which is the same workaround, written at the call
site, by someone who hit the same wall.

SECOND, `detect_dialect` guesses:

    detect_dialect(SomeOtherDriver()) -> 'postgresql'
    detect_dialect(None)              -> 'postgresql'

The last line is `return "postgresql"  # Default`. Parameter binding differs
per driver -- $1, ?, :name -- so an unrecognised connection gets PostgreSQL
placeholders and fails somewhere downstream with a message about SQL syntax.
`declared_type` had the same shape and now refuses; so does this.

EVERY QUERY BELOW RUNS AGAINST A REAL SQLITE FILE. A test that asserts the SQL
string cannot see either defect: both are in what the driver does with it.
"""

from __future__ import annotations

import os
import tempfile

import aiosqlite
import pytest

from declaro_persistum.query.builder import insert, select
from declaro_persistum.query.executor import (
    detect_dialect,
    execute,
    execute_many,
    execute_one,
    execute_scalar,
)
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


@pytest.fixture
async def conn():
    path = tempfile.mktemp(suffix=".db")
    connection = await aiosqlite.connect(path)
    await connection.execute(
        "CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, n INTEGER)"
    )
    await connection.execute("INSERT INTO t VALUES ('1','a',10),('2','b',20)")
    await connection.commit()
    yield connection
    await connection.close()
    os.unlink(path)


# ---------------------------------------------------------------------------
# The three that worked
# ---------------------------------------------------------------------------


async def test_execute_returns_every_matching_row(conn) -> None:
    rows = await execute(
        select("id", "name", from_table="t", where={"n": {"gt": 5}}, order_by=["id"], params=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
        conn,
    )

    assert rows == [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]


async def test_execute_one_returns_the_first_row(conn) -> None:
    row = await execute_one(select("id", from_table="t", order_by=["id"], where=NOTHING, params=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert row == {"id": "1"}


async def test_execute_one_returns_none_when_nothing_matches(conn) -> None:
    """None, not an empty dict, so `if row:` means what a caller expects."""
    row = await execute_one(select("id", from_table="t", where={"n": {"gt": 999}}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert row is None


async def test_execute_returns_an_empty_list_when_nothing_matches(conn) -> None:
    rows = await execute(select("id", from_table="t", where={"n": {"gt": 999}}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert rows == []


async def test_a_row_factory_transforms_every_row(conn) -> None:
    names = await execute(
        select("name", from_table="t", order_by=["name"], where=NOTHING, params=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
        conn,
        row_factory=lambda r: r["name"].upper(),
    )

    assert names == ["A", "B"]


async def test_execute_many_reports_how_many_it_wrote(conn) -> None:
    written = await execute_many(
        insert("t", {"id": ":id", "name": ":name", "n": ":n"}, returning=NOTHING, on_conflict=NOTHING),
        conn,
        [{"id": "3", "name": "c", "n": 30}, {"id": "4", "name": "d", "n": 40}],
    )
    await conn.commit()

    assert written == 2
    assert len(await execute(select("id", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)) == 4


async def test_a_where_clause_binds_rather_than_inlines(conn) -> None:
    """An inlined value is an injection, and an apostrophe ends the literal."""
    await conn.execute("INSERT INTO t VALUES ('5', \"it's\", 50)")
    await conn.commit()

    rows = await execute(select("n", from_table="t", where={"name": "it's"}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert rows == [{"n": 50}]


# ---------------------------------------------------------------------------
# The one that did not
# ---------------------------------------------------------------------------


async def test_execute_scalar_returns_the_single_value(conn) -> None:
    """The measured defect. `row[0]` on a dict row is a key lookup."""
    total = await execute_scalar(select("COUNT(*) AS c", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert total == 2


async def test_execute_scalar_returns_none_when_nothing_matches(conn) -> None:
    value = await execute_scalar(
        select("id", from_table="t", where={"n": {"gt": 999}}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn
    )

    assert value is None


async def test_execute_scalar_reads_the_first_column_not_a_key_named_zero(
    conn,
) -> None:
    """The floor. Returning `row.get(0)` would satisfy the None case above.

    A single named column proves the value comes from the column and not from
    a lookup that happens to miss.
    """
    name = await execute_scalar(
        select("name", from_table="t", where={"id": "1"}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn
    )

    assert name == "a"


# ---------------------------------------------------------------------------
# The dialect it guesses
# ---------------------------------------------------------------------------


async def test_a_known_driver_is_recognised(conn) -> None:
    assert detect_dialect(conn) == "sqlite"


def test_an_unknown_connection_is_refused_rather_than_assumed() -> None:
    """`return "postgresql"  # Default` was the last line.

    Parameter binding differs per driver, so an unrecognised connection is
    handed PostgreSQL placeholders and fails later with a message about SQL
    rather than about the connection.
    """

    class SomeOtherDriver:
        pass

    with pytest.raises(ValueError) as caught:
        detect_dialect(SomeOtherDriver())

    message = str(caught.value)
    assert "sqlite" in message and "postgresql" in message, (
        f"the error does not name the drivers that are recognised: {message}"
    )


def test_none_is_not_a_postgresql_connection() -> None:
    """It returned 'postgresql' for None, which is the guess at its plainest."""
    with pytest.raises(ValueError):
        detect_dialect(None)


# ---------------------------------------------------------------------------
# State left behind on the caller's connection
# ---------------------------------------------------------------------------
#
# FOUND WHILE CHASING THE `KeyError: 0` ABOVE, and it is what caused it.
#
#     row_factory before execute: None
#     row_factory after execute : <function _dict_factory>
#
# `_fetch_aiosqlite` assigns `connection.row_factory = _dict_factory` and never
# puts it back. The connection belongs to the CALLER: it is passed in, it is
# not owned here, and after one query every raw `conn.execute(...)` they make
# returns dicts instead of tuples for the rest of its life.
#
# That is how the scalar defect stayed hidden. On a fresh connection a row is
# a tuple and `row[0]` works; run any other query first and it is a dict and
# `row[0]` is a lookup for the key `0`. The behaviour depended on what had
# been run before, which is the definition of hidden state.


async def test_a_query_does_not_change_how_the_connection_behaves(conn) -> None:
    """The caller's connection is theirs. Borrow it and give it back."""
    before = conn.row_factory

    await execute(select("id", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert conn.row_factory is before, (
        f"row_factory was {before!r} and is now {conn.row_factory!r}, so every "
        f"raw query the caller makes from here on returns a different shape"
    )


async def test_a_caller_still_gets_tuples_from_their_own_cursor(conn) -> None:
    """The consequence, stated as the caller sees it rather than as an attribute."""
    await execute(select("id", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    cursor = await conn.execute("SELECT COUNT(*) FROM t")
    row = await cursor.fetchone()

    assert isinstance(row, tuple), (
        f"a raw query came back as {type(row).__name__} because a previous "
        f"declaro query changed the connection: {row!r}"
    )


async def test_a_caller_who_set_their_own_factory_keeps_it(conn) -> None:
    """Restoring must restore what was there, not clear it to None."""

    def mine(_cursor, _row):
        # The sqlite3 row_factory signature. Neither argument is read; this
        # stands in for a factory the caller owns, so only its identity matters.
        return {"mine": True}

    conn.row_factory = mine

    await execute(select("id", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert conn.row_factory is mine


async def test_the_rows_are_still_dicts(conn) -> None:
    """The floor. Never setting the factory at all would pass the three above."""
    rows = await execute(select("id", from_table="t", order_by=["id"], where=NOTHING, params=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert rows == [{"id": "1"}, {"id": "2"}]


async def test_a_scalar_works_when_the_caller_uses_a_mapping_factory(conn) -> None:
    """The case that keeps `_first_value` honest rather than defensive.

    FOUND BY A MUTATION CONTROL. Changing `next(iter(row.values()))` to
    `next(iter(row))` -- the key rather than the value -- left every test
    passing. With the row-factory leak fixed, a SQLite row is a tuple again,
    so the mapping branch was never reached by anything in the suite.

    It is still REACHABLE, and this is how: a caller may set their own row
    factory on their own connection, which the executor now preserves rather
    than overwriting. Then a scalar query sees a mapping and `row[0]` would be
    a lookup for the key `0`.

    Without this test the branch is defensive code guarding a case nothing
    demonstrates, which is the kind of guard that should be deleted rather
    than kept. With it, the branch has a caller.
    """
    import sqlite3

    def as_dict(cursor: sqlite3.Cursor, row: tuple) -> dict:
        return {c[0]: row[i] for i, c in enumerate(cursor.description)}

    conn.row_factory = as_dict

    total = await execute_scalar(select("COUNT(*) AS c", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)

    assert total == 2, (
        f"a scalar query returned {total!r} when the caller's connection "
        f"yields mappings, which is the key rather than the value"
    )
