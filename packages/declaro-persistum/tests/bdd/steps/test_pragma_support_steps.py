"""Steps for pragma_support.feature.

Real engines, no doubles. A claim about what a backend supports can only be
checked by asking that backend.
"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.abstractions.pragma_compat import (
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)

scenarios("../features/pragma_support.feature")


class Ctx(TypedDict, total=False):
    conn: Any
    answers: dict[str, list]
    rows: list
    error: Exception | None
    index_name: str


@pytest.fixture
def ctx() -> Ctx:
    return {}


class _SyncConn:
    """A real sqlite3 connection behind the async shape the wrappers expect."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def execute(self, sql: str) -> Any:
        return self._conn.execute(sql)


class _Withdrawn:
    def execute(self, sql: str) -> Any:
        raise sqlite3.OperationalError(f"not supported: {sql}")


@given("a real SQLite database with a table, an index and a foreign key")
def _given_real_db(ctx, tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)")
    conn.execute(
        "CREATE TABLE child ("
        "  id INTEGER PRIMARY KEY,"
        "  parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE,"
        "  note TEXT)"
    )
    conn.execute("CREATE INDEX ix_child_parent ON child(parent_id)")
    conn.commit()
    ctx["conn"] = _SyncConn(conn)


@given("a connection whose PRAGMA support has been withdrawn")
def _given_withdrawn(ctx):
    ctx["conn"] = _Withdrawn()


@when("each PRAGMA persistum depends on is asked of it")
def _when_all_pragmas(ctx):
    async def run():
        return {
            "table_info": await pragma_table_info(ctx["conn"], "child"),
            "index_list": await pragma_index_list(ctx["conn"], "child"),
            "foreign_key_list": await pragma_foreign_key_list(ctx["conn"], "child"),
            "index_info": await pragma_index_info(ctx["conn"], "ix_child_parent"),
        }

    ctx["answers"] = asyncio.run(run())


@then("every one is answered by the engine")
def _then_all_native(ctx):
    for name, rows in ctx["answers"].items():
        assert rows, f"PRAGMA {name} returned nothing on a real database"


@when("a PRAGMA is asked of it")
def _when_pragma_on_withdrawn(ctx):
    try:
        asyncio.run(pragma_table_info(ctx["conn"], "anything"))
        ctx["error"] = None
    except Exception as e:  # noqa: BLE001 - the raise IS the behaviour
        ctx["error"] = e


@then("the error reaches the caller")
def _then_raises(ctx):
    assert ctx["error"] is not None, (
        "a backend that cannot answer a PRAGMA was silently absorbed, so the "
        "caller cannot tell an answer from a substitute"
    )


@when("table_info is read for that table")
def _when_table_info(ctx):
    ctx["rows"] = asyncio.run(pragma_table_info(ctx["conn"], "child"))


@then("it lists every column of the table")
def _then_lists_columns(ctx):
    names = {r[1] for r in ctx["rows"]}
    assert names == {"id", "parent_id", "note"}, (
        f"table_info reported {names}, not the three declared columns"
    )


@when("index_list is read and then index_info for one of its indexes")
def _when_index_pair(ctx):
    listed = asyncio.run(pragma_index_list(ctx["conn"], "child"))
    assert listed, "index_list found no index on a table that has one"
    ctx["index_name"] = listed[0][1]
    ctx["rows"] = asyncio.run(pragma_index_info(ctx["conn"], ctx["index_name"]))


@then("the index named by the first is described by the second")
def _then_index_pair_agrees(ctx):
    assert ctx["rows"], (
        f"index_info returned nothing for {ctx['index_name']}, which "
        f"index_list had just reported"
    )
    covered = {r[2] for r in ctx["rows"]}
    assert "parent_id" in covered, (
        f"index_info says {ctx['index_name']} covers {covered}, but it was "
        f"created on parent_id"
    )


@when("foreign_key_list is read for the referencing table")
def _when_fk_list(ctx):
    ctx["rows"] = asyncio.run(pragma_foreign_key_list(ctx["conn"], "child"))


@then("it names the referenced table")
def _then_names_referenced(ctx):
    assert ctx["rows"], "foreign_key_list found no key on a table that has one"
    assert any("parent" in str(r) for r in ctx["rows"]), (
        f"the referenced table is not named: {ctx['rows']}"
    )
