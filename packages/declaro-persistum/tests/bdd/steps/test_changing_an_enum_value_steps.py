"""Steps for enums/changing_an_enum_value.feature: real tables, real enforcement.

THE INTERESTING HALF IS THE ENGINE'S ANSWER, not the string. A DELETE that
strands a row still points at is refused by the database and by nothing else,
so every scenario here builds the real lookup table, the real parent table and
the real foreign key, and turns enforcement on.
"""

from __future__ import annotations

import asyncio
from typing import TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions import enums

scenarios("../features/enums/changing_an_enum_value.feature")


class Ctx(TypedDict, total=False):
    path: str
    statements: list[str]
    refused: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"path": "", "statements": [], "refused": None}


def _run(ctx: Ctx, *statements: str) -> None:
    """Run statements against the real database, keeping any refusal."""

    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            await conn.execute("PRAGMA foreign_keys = ON")
            for statement in statements:
                await conn.execute(statement)
            await conn.commit()
            ctx["refused"] = None
        except Exception as refused:  # noqa: BLE001 - the refusal is the answer
            ctx["refused"] = refused
        finally:
            await conn.close()

    asyncio.run(_go())


@given(parsers.parse('a table "{table}" whose status is one of "{a}" and "{b}"'))
def _given_table(ctx, table, a, b, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    lookup = enums.enum_table_name(table, "status")
    _run(
        ctx,
        f'CREATE TABLE "{lookup}" (value TEXT PRIMARY KEY)',
        f"INSERT INTO \"{lookup}\" (value) VALUES ('{a}')",
        f"INSERT INTO \"{lookup}\" (value) VALUES ('{b}')",
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, status TEXT '
        f'REFERENCES "{lookup}" (value))',
    )
    assert ctx["refused"] is None, ctx["refused"]
    ctx["table"] = table


@given(parsers.parse('an order whose status is "{value}"'))
def _given_a_row(ctx, value):
    _run(ctx, f"INSERT INTO \"{ctx['table']}\" (id, status) VALUES (1, '{value}')")
    assert ctx["refused"] is None, ctx["refused"]


@when(parsers.parse('"{value}" is added to the statuses'))
def _when_added(ctx, value):
    ctx["statements"] = enums.add_enum_value_sql(ctx.get("table", "orders"), "status", value)
    _run(ctx, *ctx["statements"]) if ctx["path"] else None


@when(parsers.parse('"{value}" is taken out of the statuses'))
def _when_removed(ctx, value):
    ctx["statements"] = enums.remove_enum_value_sql(ctx.get("table", "orders"), "status", value)
    _run(ctx, *ctx["statements"]) if ctx["path"] else None


@then(parsers.parse('a row with status "{value}" is accepted'))
def _then_accepted(ctx, value):
    _run(ctx, f"INSERT INTO \"{ctx['table']}\" (id, status) VALUES (99, '{value}')")
    assert ctx["refused"] is None, (
        f"the column refuses {value!r} after it was added to the lookup: {ctx['refused']}"
    )


@then(parsers.parse('a row with status "{value}" is refused'))
def _then_refused(ctx, value):
    _run(ctx, f"INSERT INTO \"{ctx['table']}\" (id, status) VALUES (98, '{value}')")
    assert ctx["refused"] is not None, (
        f"the column accepted {value!r}, which is not one of its values"
    )


@then("the engine refuses, because a row still points at it")
def _then_refused_in_use(ctx):
    assert ctx["refused"] is not None, (
        "the value was removed while a row still referenced it, and the database did not object"
    )


@then("the statement carries the value with its quote doubled")
def _then_doubled(ctx):
    (statement,) = ctx["statements"]
    assert "it''s new" in statement, (
        f"a quote in the value was not doubled, so the statement ends early: {statement}"
    )
