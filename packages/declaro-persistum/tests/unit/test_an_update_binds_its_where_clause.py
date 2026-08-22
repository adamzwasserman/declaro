"""An UPDATE with a dict WHERE renders a placeholder and drops its value.

MEASURED 2026-08-17:

    update("t", {"a": 1}, where={"id": "x"})
    sql    'UPDATE "t" SET "a" = :set_a WHERE id = :w_0_id'
    params {'set_a': 1}

The `w_0_id` binding is gone, so every such call raises at the driver:

    sqlite3.ProgrammingError: You did not supply a value for binding
    parameter :w_0_id.

FOUND BY COMPARING THE FOUR BUILDERS SIDE BY SIDE, which is what made it a
one-line defect rather than a mystery:

    select  dict-where   params {'w_0_id': 'x'}    correct
    delete  dict-where   params {'w_0_id': 'x'}    correct
    update  str-where    params {'set_a': 1, 'id': 'x'}   correct
    update  dict-where   params {'set_a': 1}       WRONG

Only one of the four is wrong, and only for one of its two WHERE forms. The
cause:

    where_sql, params = _clause(where, params, "w")   # merged, into a local
    ...
    return {"sql": ..., "params": all_params}          # which is not that one

`_clause` returns the merged parameters, `update` binds them to `params`, and
then returns `all_params` -- built from `set_values` and the caller's original
`params` before `_clause` ran. `delete` returns what `_clause` gave it, which
is why it works.

A str-WHERE hides this: the caller passes `params=` themselves, so
`all_params` already has the binding and never needed the merge.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from declaro_persistum.query import delete, select, update
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit


def _rendered(query) -> tuple[str, dict]:
    return query["sql"], query["params"]


def test_an_update_binds_every_placeholder_it_renders() -> None:
    """The measured defect. A rendered `:name` with no value cannot run."""
    sql, params = _rendered(update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING))

    missing = [
        token
        for token in sql.replace(",", " ").split()
        if token.startswith(":") and token[1:] not in params
    ]
    assert not missing, f"{sql} renders {missing} and binds {sorted(params)}"


def test_the_where_value_is_the_one_that_was_passed() -> None:
    """Binding something is not the same as binding the right thing."""
    _sql, params = _rendered(update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING))

    assert "x" in params.values(), params


def test_the_set_values_survive_the_merge() -> None:
    """The obvious way to fix this is to return `_clause`'s dict and lose SET."""
    _sql, params = _rendered(update("t", {"a": 1, "b": 2}, where={"id": "x"}, params=NOTHING, returning=NOTHING))

    assert params["set_a"] == 1 and params["set_b"] == 2, params


def test_a_string_where_still_works() -> None:
    """The form that hid the defect, which must keep working."""
    _sql, params = _rendered(
        update("t", {"a": 1}, where="id = :id", params={"id": "x"}, returning=NOTHING)
    )

    assert params == {"set_a": 1, "id": "x"}


@pytest.mark.parametrize(
    "builder",
    [
        lambda: select("a", from_table="t", where={"id": "x"}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
        lambda: delete("t", where={"id": "x"}, params=NOTHING, returning=NOTHING),
        lambda: update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING),
    ],
    ids=["select", "delete", "update"],
)
def test_every_builder_binds_what_it_renders(builder) -> None:
    """The property, over all three, so a fourth cannot drift alone."""
    sql, params = _rendered(builder())

    missing = [
        token
        for token in sql.replace(",", " ").split()
        if token.startswith(":") and token[1:] not in params
    ]
    assert not missing, f"{sql} renders {missing} and binds {sorted(params)}"


def test_an_update_runs_against_a_real_engine() -> None:
    """The floor. A params dict that satisfies a string check can still fail.

    This is the assertion the driver makes, and it is the one that raised.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id TEXT, a INTEGER)")
    conn.execute("INSERT INTO t VALUES ('x', 0), ('y', 0)")

    q = update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING)
    conn.execute(q["sql"], q["params"])

    assert conn.execute("SELECT a FROM t WHERE id = 'x'").fetchone()[0] == 1
    assert conn.execute("SELECT a FROM t WHERE id = 'y'").fetchone()[0] == 0, (
        "the WHERE clause did not narrow the update"
    )


def test_a_multi_condition_where_binds_all_of_them() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id TEXT, n INTEGER, a INTEGER)")
    conn.execute("INSERT INTO t VALUES ('x', 1, 0), ('x', 2, 0)")

    q = update("t", {"a": 9}, where={"id": "x", "n": 2}, params=NOTHING, returning=NOTHING)
    conn.execute(q["sql"], q["params"])

    assert conn.execute("SELECT a FROM t WHERE n = 2").fetchone()[0] == 9
    assert conn.execute("SELECT a FROM t WHERE n = 1").fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_an_update_through_the_executor_lands() -> None:
    """End to end, which is how the example app's toggle route reaches this."""
    import aiosqlite

    from declaro_persistum.query import execute

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.execute("CREATE TABLE t (id TEXT, a INTEGER)")
        await conn.execute("INSERT INTO t VALUES ('x', 0)")
        await conn.commit()

        await execute(update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING), conn, row_factory=NOTHING)
        await conn.commit()

        cursor = await conn.execute("SELECT a FROM t")
        assert (await cursor.fetchone())[0] == 1
    finally:
        await conn.close()
        os.unlink(path)
