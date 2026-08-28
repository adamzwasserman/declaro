"""Steps for matview/asking_about_a_matview.feature: run the SQL, do not read it.

READING THE STRING CANNOT SEE THE FAILURE THAT MATTERS. A query naming a
column the registry does not have is still a plausible-looking string. So
every scenario builds the real registry in a real SQLite file and executes
what the function returns.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions import materialized_views as mv

scenarios("../features/matview/asking_about_a_matview.feature")


class Ctx(TypedDict, total=False):
    path: str
    rows: Any


@pytest.fixture
def ctx() -> Ctx:
    return {"path": "", "rows": None}


def _create_from(schema: dict[str, Any]) -> str:
    """A CREATE TABLE for the registry, read off the schema it declares."""
    ((table, definition),) = schema.items()
    columns = ", ".join(
        f'"{name}" {column["type"]}'
        + (" PRIMARY KEY" if column.get("primary_key") else "")
        + ("" if column.get("nullable", True) else " NOT NULL")
        # THE DEFAULTS COME FROM THE DECLARATION TOO. `created_at` is NOT NULL
        # with a default; dropping the default here made every insert fail,
        # which is the registry being built wrong rather than the query.
        + (f" DEFAULT {column['default']}" if "default" in column else "")
        for name, column in definition["columns"].items()
    )
    return f'CREATE TABLE "{table}" ({columns})'


def _registry_holding(ctx: Ctx, *names: str) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            # THE REGISTRY IS BUILT FROM ITS OWN DECLARATION, not from
            # columns typed here. If the declaration gains a column, these
            # scenarios follow it; if a query names one that was removed,
            # they fail, which is the point of running the SQL at all.
            await conn.execute(_create_from(mv.generate_metadata_table_schema()))
            for name in names:
                await conn.execute(
                    f"INSERT INTO {mv.MATVIEW_METADATA_TABLE} "
                    "(name, query, refresh_strategy) VALUES (?, ?, ?)",
                    (name, f"SELECT * FROM {name}_source", "manual"),
                )
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


def _run(ctx: Ctx, sql: str, params: dict[str, Any]) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            cursor = await conn.execute(sql, params)
            ctx["rows"] = list(await cursor.fetchall())
        finally:
            await conn.close()

    asyncio.run(_go())


@given(parsers.parse('a registry holding a view "{name}"'))
def _given_one(ctx, name, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    _registry_holding(ctx, name)


@given(parsers.parse('a registry holding views "{first}" and "{second}"'))
def _given_two(ctx, first, second, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    _registry_holding(ctx, second, first)  # inserted out of order on purpose


@when(parsers.parse('the is-it-one query is run for "{name}"'))
def _when_is_one(ctx, name):
    _run(ctx, mv.is_matview_sql(), {"table_name": name})


@when(parsers.parse('the metadata query is run for "{name}"'))
def _when_metadata(ctx, name):
    _run(ctx, mv.get_matview_metadata_sql(), {"name": name})


@when("the list query is run")
def _when_list(ctx):
    _run(ctx, mv.list_matviews_sql(), {})


@then("it answers yes")
def _then_yes(ctx):
    assert ctx["rows"] == [(1,)], ctx["rows"]


@then("it answers no")
def _then_no(ctx):
    assert ctx["rows"] == [], ctx["rows"]


@then("it gives back the name, the query and how it refreshes")
def _then_metadata(ctx):
    assert len(ctx["rows"]) == 1, ctx["rows"]
    name, query, refresh = ctx["rows"][0][:3]
    assert name == "sales_summary"
    assert query == "SELECT * FROM sales_summary_source"
    assert refresh == "manual"


@then("it names both, in order")
def _then_both(ctx):
    assert [row[0] for row in ctx["rows"]] == ["a_view", "b_view"], ctx["rows"]
