"""Steps for pragma/reading_a_pragma.feature: a real SQLite file, no stand-in.

A pragma is the engine answering a question about itself. There is nothing to
stand in for, and a double that returned rows would be testing the double.
Every scenario opens a real file under `tmp_path`.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions import pragma_compat

scenarios("../features/pragma/reading_a_pragma.feature")

# A NAME WITH A QUOTE IN IT. `as_identifier` doubles it; a reader that forgets
# hands the engine a broken statement.
_AWKWARD = 'we"ird'


class Ctx(TypedDict, total=False):
    path: str
    rows: Any
    raised: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"path": "", "rows": None, "raised": None}


def _on_a_real_database(ctx: Ctx, *statements: str) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            for statement in statements:
                await conn.execute(statement)
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


def _read(ctx: Ctx, reader: str, subject: str) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx["path"])
        try:
            ctx["rows"] = await getattr(pragma_compat, reader)(conn, subject)
            ctx["raised"] = None
        except Exception as raised:  # noqa: BLE001 - whether it raises is the answer
            ctx["rows"] = None
            ctx["raised"] = raised
        finally:
            await conn.close()

    asyncio.run(_go())


@given(parsers.parse('a table "{table}" with columns id and name'))
def _given_table(ctx, table, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    _on_a_real_database(ctx, f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT)')
    ctx["subject"] = table


@given(parsers.parse('a table "{table}" with an index "{index}" over name'))
def _given_indexed(ctx, table, index, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    _on_a_real_database(
        ctx,
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT)',
        f'CREATE INDEX "{index}" ON "{table}" (name)',
    )
    ctx["subject"] = table


@given(parsers.parse('a table "{table}" whose customer references "{parent}"'))
def _given_referencing(ctx, table, parent, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    _on_a_real_database(
        ctx,
        f'CREATE TABLE "{parent}" (id INTEGER PRIMARY KEY)',
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, '
        f'customer INTEGER REFERENCES "{parent}" (id))',
    )
    ctx["subject"] = table


@given("a table named with a quote in it")
def _given_awkward(ctx, tmp_path):
    ctx["path"] = str(tmp_path / "t.db")
    doubled = _AWKWARD.replace('"', '""')
    _on_a_real_database(ctx, f'CREATE TABLE "{doubled}" (id INTEGER PRIMARY KEY)')
    ctx["subject"] = _AWKWARD


@when("its columns are read back")
def _when_columns(ctx):
    _read(ctx, "pragma_table_info", ctx["subject"])


@when("its indexes are read back")
def _when_indexes(ctx):
    _read(ctx, "pragma_index_list", ctx["subject"])


@when(parsers.parse('the columns of "{index}" are read back'))
def _when_index_columns(ctx, index):
    _read(ctx, "pragma_index_info", index)


@when("its foreign keys are read back")
def _when_keys(ctx):
    _read(ctx, "pragma_foreign_key_list", ctx["subject"])


@when(parsers.parse('"{reader}" is asked about it'))
def _when_awkward_read(ctx, reader):
    _read(ctx, reader, ctx["subject"])


@then("the answer names id and name")
def _then_names_columns(ctx):
    names = [row[1] for row in ctx["rows"]]
    assert names == ["id", "name"], names


@then(parsers.parse('the answer names "{wanted}"'))
def _then_names(ctx, wanted):
    flat = [str(cell) for row in ctx["rows"] for cell in row]
    assert wanted in flat, f"{wanted!r} is not in {ctx['rows']}"


@then("the answer names name")
def _then_names_name(ctx):
    flat = [str(cell) for row in ctx["rows"] for cell in row]
    assert "name" in flat, ctx["rows"]


@then("it answers without raising")
def _then_no_raise(ctx):
    assert ctx["raised"] is None, (
        f"a name carrying a double quote broke the statement: {ctx['raised']}"
    )
    assert ctx["rows"] is not None
