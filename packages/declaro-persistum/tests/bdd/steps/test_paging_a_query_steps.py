"""Steps for query/paging_a_query.feature: the statement is run, not read.

`SELECT ... LIMIT 2 LIMIT 1` is a plausible-looking string. Only the engine
says it is not a statement, so the paging scenarios execute what the builder
produced against a real table of five rows.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.query.builder import with_limit, with_offset

scenarios("../features/query/paging_a_query.feature")

_PAGERS = {"limit": with_limit, "offset": with_offset}
_KEYWORDS = {"limit": " LIMIT ", "offset": " OFFSET "}


class Ctx(TypedDict, total=False):
    path: str
    query: Any
    rows: int


@pytest.fixture
def ctx() -> Ctx:
    return {
        "path": "",
        "query": {
            "sql": 'SELECT id FROM "t" ORDER BY id',
            "params": {"a": 1},
            "dialect": "sqlite",
        },
        "rows": -1,
    }


@given("five rows in a table")
def _given_rows(ctx, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")

    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            await conn.execute('CREATE TABLE "t" (id INTEGER PRIMARY KEY)')
            for i in range(1, 6):
                await conn.execute(f'INSERT INTO "t" (id) VALUES ({i})')
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


@when(parsers.parse("the query is given a {clause} of {value:d}"))
def _when_paged(ctx, clause, value):
    ctx["query"] = _PAGERS[clause](ctx["query"], value)
    ctx["clause"] = clause


@when(parsers.parse("the query is given a {clause} of {first:d} and then {second:d}"))
def _when_paged_twice(ctx, clause, first, second):
    ctx["query"] = _PAGERS[clause](_PAGERS[clause](ctx["query"], first), second)
    ctx["clause"] = clause


@then(parsers.parse("the statement runs and returns {expected:d} rows"))
def _then_runs(ctx, expected):
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            # An OFFSET with no LIMIT is not valid SQLite, so the scenarios
            # that page by offset get the LIMIT the engine requires.
            sql = ctx["query"]["sql"]
            if " LIMIT " not in sql:
                sql = sql.replace(" OFFSET ", " LIMIT -1 OFFSET ")
            cursor = await conn.execute(sql)
            ctx["rows"] = len(await cursor.fetchall())
        finally:
            await conn.close()

    asyncio.run(_go())
    assert ctx["rows"] == expected, f"{ctx['query']['sql']!r} returned {ctx['rows']} rows"


@then(parsers.parse("the statement carries one {clause} clause, not two"))
def _then_one_clause(ctx, clause):
    seen = ctx["query"]["sql"].count(_KEYWORDS[clause])
    assert seen == 1, f"{seen} {clause} clauses in {ctx['query']['sql']!r}"


@then("its parameters and its engine are unchanged")
def _then_carried(ctx):
    assert ctx["query"]["params"] == {"a": 1}, ctx["query"]["params"]
    assert ctx["query"]["dialect"] == "sqlite", ctx["query"]["dialect"]
