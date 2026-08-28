"""Steps for migration/statements_per_engine.feature.

The SQLite statements are RUN, not read. A statement naming a column the table
does not have is a plausible-looking string, and only executing it says so.
Postgres is checked for the statement existing, because running it needs a
server and these are the fast checks.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import migration_sql as ms

scenarios("../features/migration/statements_per_engine.feature")


class Ctx(TypedDict, total=False):
    path: str
    rows: Any
    answer: str
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"path": "", "rows": None, "answer": "", "refusal": None}


@given("a stamp table on a real database")
def _given_stamp_table(ctx, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")

    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            await conn.execute(ms.meta_table_sql())
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


@given(parsers.parse('a table "{table}" that the user made'))
def _given_user_table(ctx, table):
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            await conn.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY)')
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


@when("the same key is stamped twice with different values")
def _when_stamped_twice(ctx):
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            sql = ms.store_hash_sql("sqlite")
            await conn.execute(sql, ("models.py", "first", "2026-01-01"))
            await conn.execute(sql, ("models.py", "second", "2026-01-02"))
            await conn.commit()
            cursor = await conn.execute(ms.stored_hash_sql("sqlite"), ("models.py",))
            ctx["answer"] = (await cursor.fetchone())[0]
            counted = await conn.execute(f'SELECT count(*) FROM "{ms.META_TABLE}"')
            ctx["rows"] = (await counted.fetchone())[0]
        finally:
            await conn.close()

    asyncio.run(_go())


@when("the user-tables query is run")
def _when_user_tables(ctx):
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            cursor = await conn.execute(ms.user_tables_sql("sqlite"))
            ctx["rows"] = [row[0] for row in await cursor.fetchall()]
        finally:
            await conn.close()

    asyncio.run(_go())


@when(parsers.parse('"{builder}" is asked for "{engine}"'))
def _when_asked(ctx, builder, engine):
    try:
        ctx["answer"] = getattr(ms, builder)(engine)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - a refusal is an outcome here
        ctx["answer"] = ""
        ctx["refusal"] = refused


@then("the second value is what is stored")
def _then_second(ctx):
    assert ctx["answer"] == "second", (
        f"the stamp says {ctx['answer']!r}; a second boot left the database reporting the first one"
    )


@then("there is one row, not two")
def _then_one_row(ctx):
    assert ctx["rows"] == 1, ctx["rows"]


@then(parsers.parse('it names "{wanted}" and not the stamp table'))
def _then_names_only_user(ctx, wanted):
    assert wanted in ctx["rows"], ctx["rows"]
    assert ms.META_TABLE not in ctx["rows"], (
        f"our own table is counted as the user's: {ctx['rows']}"
    )


@then("it gives back a statement")
def _then_a_statement(ctx):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert ctx["answer"].strip(), "the builder handed back nothing"


@then("it refuses and names the engines it has")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r}"
    said = str(ctx["refusal"])
    assert "sqlite" in said and "postgresql" in said, said


@then(parsers.parse('it gives back "{expected}"'))
def _then_gives_back(ctx, expected):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert ctx["answer"] == expected, ctx["answer"]
