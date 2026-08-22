"""Declaring `Literal["draft", "live"]` must let you store "draft".

WRITTEN BEFORE THE CODE, against a measured rejection of valid data.

A `Literal` column becomes a lookup table plus a foreign key, which is how this
package avoids CHECK-constraint portability problems. The lookup table has to
hold the declared values, or the foreign key rejects every one of them.

MEASURED 2026-08-17, from `expand_schema_enums` through `diff` to `apply`, on a
real SQLite file:

    tables after expansion: ['_dp_enum_posts_status', 'posts']
      _dp_enum_posts_status: _enum_values=['draft', 'live']
    ops: ['create_table', 'create_table']
    ROWS IN _dp_enum_posts_status: []
    insert of a VALID value FAILED: FOREIGN KEY constraint failed

So a schema that declares two legal values produces a column that accepts
neither. With foreign keys off, it accepts anything, which is the same defect
wearing the opposite mask.

THE ROOT IS THAT THE VALUES WERE LEFT OUT OF THE OPERATION.

    the values live in the SCHEMA        expanded[table]["_enum_values"]
    the applier receives OPERATIONS      [{"op": "create_table", ...}]

An applier cannot populate what it was not given. Something noticed this and
added a `target_schema` parameter to `turso.apply` so the schema could be
passed alongside -- and no caller ever passed it, so `enum_population_sql`
returned `[]` on every run for the life of the parameter, and the other two
appliers never called it at all.

Passing the schema beside the operations is the wrong shape, because it makes
one operation's meaning depend on an argument the other operations ignore. The
differ knows the values and knows it is creating the table, so it emits the
INSERTs as operations of their own and every applier runs them the way it runs
everything else.
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import apply
from declaro_persistum.differ import diff
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

SCHEMA = {
    "posts": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "status": {"type": "text", "literal_values": ["draft", "live"]},
        }
    }
}


def _plan(dialect: str):
    """Expansion and diff, from the functions that do them in production."""
    expanded = expand_schema_enums(SCHEMA)
    return expanded, diff({}, expanded, dialect=dialect, decisions=NOTHING)


async def _migrated(path) -> aiosqlite.Connection:
    expanded, plan = _plan("sqlite")
    conn = await aiosqlite.connect(str(path))
    await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
    return conn


def test_the_differ_emits_the_values_alongside_the_table() -> None:
    """The fix, stated as the shape rather than as the outcome.

    Without this the applier is handed a CREATE TABLE and no values, and the
    only way to populate it is an out-of-band argument, which is what was tried
    and never wired up.
    """
    _, plan = _plan("sqlite")
    ops = [o["op"] for o in plan["operations"]]

    assert "insert_enum_values" in ops, (
        f"the differ creates the lookup table and never fills it: {ops}"
    )


def test_the_values_are_the_ones_declared() -> None:
    """An operation that carries the wrong values passes the test above."""
    _, plan = _plan("sqlite")
    inserts = [o for o in plan["operations"] if o["op"] == "insert_enum_values"]

    assert len(inserts) == 1, inserts
    assert inserts[0]["details"]["values"] == ["draft", "live"], inserts[0]


def test_the_insert_is_ordered_after_the_table_it_fills() -> None:
    """Inserting into a table that does not exist yet fails."""
    _, plan = _plan("sqlite")
    ops = plan["operations"]

    positions = {}
    for rank, index in enumerate(plan["execution_order"]):
        op = ops[index]
        if op["op"] in ("create_table", "insert_enum_values") and op[
            "table"
        ].startswith("_dp_enum_"):
            positions[op["op"]] = rank

    assert positions["create_table"] < positions["insert_enum_values"], positions


async def test_the_lookup_table_holds_the_declared_values(tmp_path) -> None:
    """Executed. The rows have to be in the database, not in the plan."""
    conn = await _migrated(tmp_path / "enum.db")
    try:
        cursor = await conn.execute(
            'SELECT value FROM "_dp_enum_posts_status" ORDER BY value'
        )
        assert await cursor.fetchall() == [("draft",), ("live",)]
    finally:
        await conn.close()


async def test_a_declared_value_can_be_stored(tmp_path) -> None:
    """The measured rejection, in one assertion.

    Foreign keys ON, because that is the whole point of the lookup table and
    the state a correct migration leaves the database in.
    """
    conn = await _migrated(tmp_path / "enum.db")
    try:
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("INSERT INTO posts (id, status) VALUES ('1', 'draft')")
        await conn.commit()

        cursor = await conn.execute("SELECT status FROM posts")
        assert await cursor.fetchall() == [("draft",)]
    finally:
        await conn.close()


async def test_a_value_that_was_not_declared_is_still_rejected(tmp_path) -> None:
    """The floor. Populating the table with everything would pass the test above.

    The lookup table exists to refuse `pending`. A fix that makes `draft` work
    by loosening the constraint has removed the feature.
    """
    conn = await _migrated(tmp_path / "enum.db")
    try:
        await conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "INSERT INTO posts (id, status) VALUES ('2', 'pending')"
            )
    finally:
        await conn.close()


async def test_a_value_with_a_quote_in_it_survives(tmp_path) -> None:
    """The INSERTs are built as SQL text, so the escaping is load-bearing."""
    schema = {
        "posts": {
            "columns": {
                "id": {"type": "text", "primary_key": True},
                "status": {"type": "text", "literal_values": ["it's live", "done"]},
            }
        }
    }
    expanded = expand_schema_enums(schema)
    plan = diff({}, expanded, dialect="sqlite", decisions=NOTHING)

    conn = await aiosqlite.connect(str(tmp_path / "quote.db"))
    try:
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        cursor = await conn.execute(
            'SELECT value FROM "_dp_enum_posts_status" ORDER BY value'
        )
        assert await cursor.fetchall() == [("done",), ("it's live",)]
    finally:
        await conn.close()


async def test_running_the_migration_twice_does_not_duplicate(tmp_path) -> None:
    """A second start must not double the rows or fail on the primary key."""
    path = tmp_path / "twice.db"
    conn = await _migrated(path)
    await conn.close()

    expanded, _ = _plan("sqlite")
    conn = await aiosqlite.connect(str(path))
    try:
        plan = diff(expanded, expanded, dialect="sqlite", decisions=NOTHING)
        assert plan["operations"] == [], (
            f"an unchanged schema still emits operations: {plan['operations']}"
        )
    finally:
        await conn.close()


def test_every_operation_the_differ_emits_can_be_rendered() -> None:
    """The vocabulary gate, which adding an operation is the way to break.

    15 declared and 15 handled before today. A sixteenth that the differ emits
    and no generator renders raises `Unknown operation type` mid-migration,
    after earlier operations have already run.
    """
    from declaro_persistum.applier.shared import _SQL_GENERATORS

    _, plan = _plan("sqlite")
    emitted = {o["op"] for o in plan["operations"]}
    missing = sorted(emitted - set(_SQL_GENERATORS))

    assert not missing, (
        f"the differ emits {missing} and no SQL generator handles it, so the "
        f"migration dies partway through"
    )


# ---------------------------------------------------------------------------
# The vocabulary, checked in every place that has to know it
# ---------------------------------------------------------------------------
#
# THE GATE ABOVE PASSED WHILE POSTGRESQL RAISED. It read
# `shared._SQL_GENERATORS` and there is a SECOND table with the same fifteen
# keys, rebuilt on every call inside `postgresql.generate_operation_sql`:
#
#     ValueError: Unknown operation type: insert_enum_values
#
# measured against a real PostgreSQL server after the SQLite tests were green.
# Two tables with one vocabulary drift the moment either is edited, and the
# second one was invisible to any instrument because it was a local variable
# inside a function rather than a module-level table.
#
# So the vocabulary is asserted per DIALECT, not per table. A sixteenth
# operation now has to reach all three or this fails.

RENDERERS = ["postgresql", "sqlite", "turso"]


@pytest.mark.parametrize("dialect", RENDERERS)
def test_every_dialect_renders_every_operation_the_differ_emits(
    dialect: str,
) -> None:
    """One vocabulary, checked where each engine actually reads it."""
    from declaro_persistum.applier import SQL_GENERATORS

    _, plan = _plan(dialect)
    render = SQL_GENERATORS[dialect]

    unrenderable = []
    for index in plan["execution_order"]:
        operation = plan["operations"][index]
        try:
            render(operation)
        except ValueError as exc:
            unrenderable.append(f"{operation['op']}: {exc}")

    assert not unrenderable, (
        f"the differ emits operations the {dialect} renderer refuses, so a "
        f"migration dies partway through: {unrenderable}"
    )


def test_the_two_dispatch_tables_hold_the_same_vocabulary() -> None:
    """The drift itself, rather than the one operation that revealed it.

    `postgresql.OPERATION_SQL` was a dict built inside the function on every
    call. Hoisting it to module level is what lets this be asked at all.
    """
    from declaro_persistum.applier.postgresql import OPERATION_SQL
    from declaro_persistum.applier.shared import _SQL_GENERATORS

    only_shared = sorted(set(_SQL_GENERATORS) - set(OPERATION_SQL))
    only_pg = sorted(set(OPERATION_SQL) - set(_SQL_GENERATORS))

    assert not only_shared and not only_pg, (
        f"the two operation tables disagree. Only in shared: {only_shared}. "
        f"Only in postgresql: {only_pg}"
    )
