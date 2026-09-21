"""Steps for schema_inspect.feature.

Merged on 2026-09-21 from test_reading_a_pragma_steps.py, test_recovering_what_turso_folded_steps.py.
One feature, one step file: pytest-bdd resolves a step to whichever module
imported last when two bind the same file, so exactly one is the rule.
Each part keeps its own context type and fixture, under its own name,
because all of them were called Ctx and ctx and the last would have won.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions import pragma_compat
from declaro_persistum.inspector import turso as inspector

scenarios("../features/schema/schema_inspect.feature")

# ---- from test_reading_a_pragma_steps.py ----------------------------


# A NAME WITH A QUOTE IN IT. `as_identifier` doubles it; a reader that forgets
# hands the engine a broken statement.
_AWKWARD = 'we"ird'


class Ctx_reading_a_pragma(TypedDict, total=False):
    path: str
    rows: Any
    raised: Exception | None


@pytest.fixture
def ctx_reading_a_pragma() -> Ctx_reading_a_pragma:
    return {"path": "", "rows": None, "raised": None}


def _on_a_real_database(ctx_reading_a_pragma: Ctx_reading_a_pragma, *statements: str) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx_reading_a_pragma["path"])
        try:
            for statement in statements:
                await conn.execute(statement)
            await conn.commit()
        finally:
            await conn.close()

    asyncio.run(_go())


def _read(ctx_reading_a_pragma: Ctx_reading_a_pragma, reader: str, subject: str) -> None:
    async def _go() -> None:
        import aiosqlite

        conn = await aiosqlite.connect(ctx_reading_a_pragma["path"])
        try:
            ctx_reading_a_pragma["rows"] = await getattr(pragma_compat, reader)(conn, subject)
            ctx_reading_a_pragma["raised"] = None
        except Exception as raised:  # noqa: BLE001 - whether it raises is the answer
            ctx_reading_a_pragma["rows"] = None
            ctx_reading_a_pragma["raised"] = raised
        finally:
            await conn.close()

    asyncio.run(_go())


@given(parsers.parse('a table "{table}" with columns id and name'))
def _given_table(ctx_reading_a_pragma, table, tmp_path):
    ctx_reading_a_pragma["path"] = str(tmp_path / "t.db")
    _on_a_real_database(
        ctx_reading_a_pragma, f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT)'
    )
    ctx_reading_a_pragma["subject"] = table


@given(parsers.parse('a table "{table}" with an index "{index}" over name'))
def _given_indexed(ctx_reading_a_pragma, table, index, tmp_path):
    ctx_reading_a_pragma["path"] = str(tmp_path / "t.db")
    _on_a_real_database(
        ctx_reading_a_pragma,
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT)',
        f'CREATE INDEX "{index}" ON "{table}" (name)',
    )
    ctx_reading_a_pragma["subject"] = table


@given(parsers.parse('a table "{table}" whose customer references "{parent}"'))
def _given_referencing(ctx_reading_a_pragma, table, parent, tmp_path):
    ctx_reading_a_pragma["path"] = str(tmp_path / "t.db")
    _on_a_real_database(
        ctx_reading_a_pragma,
        f'CREATE TABLE "{parent}" (id INTEGER PRIMARY KEY)',
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, '
        f'customer INTEGER REFERENCES "{parent}" (id))',
    )
    ctx_reading_a_pragma["subject"] = table


@given("a table named with a quote in it")
def _given_awkward(ctx_reading_a_pragma, tmp_path):
    ctx_reading_a_pragma["path"] = str(tmp_path / "t.db")
    doubled = _AWKWARD.replace('"', '""')
    _on_a_real_database(ctx_reading_a_pragma, f'CREATE TABLE "{doubled}" (id INTEGER PRIMARY KEY)')
    ctx_reading_a_pragma["subject"] = _AWKWARD


@when("its columns are read back")
def _when_columns(ctx_reading_a_pragma):
    _read(ctx_reading_a_pragma, "pragma_table_info", ctx_reading_a_pragma["subject"])


@when("its indexes are read back")
def _when_indexes(ctx_reading_a_pragma):
    _read(ctx_reading_a_pragma, "pragma_index_list", ctx_reading_a_pragma["subject"])


@when(parsers.parse('the columns of "{index}" are read back'))
def _when_index_columns(ctx_reading_a_pragma, index):
    _read(ctx_reading_a_pragma, "pragma_index_info", index)


@when("its foreign keys are read back")
def _when_keys(ctx_reading_a_pragma):
    _read(ctx_reading_a_pragma, "pragma_foreign_key_list", ctx_reading_a_pragma["subject"])


@when(parsers.parse('"{reader}" is asked about it'))
def _when_awkward_read(ctx_reading_a_pragma, reader):
    _read(ctx_reading_a_pragma, reader, ctx_reading_a_pragma["subject"])


@then("the answer names id and name")
def _then_names_columns(ctx_reading_a_pragma):
    names = [row[1] for row in ctx_reading_a_pragma["rows"]]
    assert names == ["id", "name"], names


@then(parsers.parse('the answer names "{wanted}"'))
def _then_names(ctx_reading_a_pragma, wanted):
    flat = [str(cell) for row in ctx_reading_a_pragma["rows"] for cell in row]
    assert wanted in flat, f"{wanted!r} is not in {ctx_reading_a_pragma['rows']}"


@then("the answer names name")
def _then_names_name(ctx_reading_a_pragma):
    flat = [str(cell) for row in ctx_reading_a_pragma["rows"] for cell in row]
    assert "name" in flat, ctx_reading_a_pragma["rows"]


@then("it answers without raising")
def _then_no_raise(ctx_reading_a_pragma):
    assert ctx_reading_a_pragma["raised"] is None, (
        f"a name carrying a double quote broke the statement: {ctx_reading_a_pragma['raised']}"
    )
    assert ctx_reading_a_pragma["rows"] is not None


# ---- from test_recovering_what_turso_folded_steps.py ----------------


# WHAT A PRAGMA HANDS BACK, and nothing more. If a rewrite drops a key, the
# differ sees a column that changed and rebuilds a table that did not.
_FROM_A_PRAGMA: dict[str, Any] = {
    "type": "integer",
    "nullable": False,
    "primary_key": False,
    "default": None,
}


class Ctx_recovering_what_turso_folded(TypedDict, total=False):
    given: dict[str, Any]
    before: dict[str, Any]
    result: dict[str, Any]


@pytest.fixture
def ctx_recovering_what_turso_folded() -> Ctx_recovering_what_turso_folded:
    return {"given": {}, "before": {}, "result": {}}


@given("a column read back from a pragma")
def _given_column(ctx_recovering_what_turso_folded):
    ctx_recovering_what_turso_folded["given"] = dict(_FROM_A_PRAGMA)
    ctx_recovering_what_turso_folded["before"] = dict(_FROM_A_PRAGMA)


@when(parsers.parse('the reference "{target}" is put back on it'))
def _when_reference(ctx_recovering_what_turso_folded, target):
    ctx_recovering_what_turso_folded["result"] = inspector._referring_to(
        ctx_recovering_what_turso_folded["given"], target
    )


@when(parsers.parse('the check "{expression}" is put back on it'))
def _when_check(ctx_recovering_what_turso_folded, expression):
    ctx_recovering_what_turso_folded["result"] = inspector._with_check(
        ctx_recovering_what_turso_folded["given"], expression
    )


@then(parsers.parse('it carries reference "{target}"'))
def _then_reference(ctx_recovering_what_turso_folded, target):
    assert ctx_recovering_what_turso_folded["result"]["references"] == target, (
        ctx_recovering_what_turso_folded["result"]
    )


@then(parsers.parse('it carries check "{expression}"'))
def _then_check(ctx_recovering_what_turso_folded, expression):
    assert ctx_recovering_what_turso_folded["result"]["check"] == expression, (
        ctx_recovering_what_turso_folded["result"]
    )


@then("everything the pragma said is still there")
def _then_kept(ctx_recovering_what_turso_folded):
    for key, value in _FROM_A_PRAGMA.items():
        assert ctx_recovering_what_turso_folded["result"][key] == value, (
            f"{key} came back as {ctx_recovering_what_turso_folded['result'].get(key)!r} and the pragma "
            f"said {value!r}; the differ would read that as a change"
        )


@then("the column it was given is unchanged")
def _then_not_mutated(ctx_recovering_what_turso_folded):
    assert (
        ctx_recovering_what_turso_folded["given"] == ctx_recovering_what_turso_folded["before"]
    ), (
        "the column handed in was modified in place; a caller still holding "
        "it now sees a reference it never asked for"
    )
