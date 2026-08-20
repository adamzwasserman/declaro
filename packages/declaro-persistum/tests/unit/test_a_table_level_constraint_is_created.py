"""A declared table-level constraint was never created, then failed forever.

MEASURED 2026-08-19, declaring one and deploying twice on SQLite:

    first deploy   success=True
    stored DDL     CREATE TABLE "t" ("a" INTEGER PRIMARY KEY, "lo" INTEGER,
                                     "hi" INTEGER)
    introspected   constraints: None
    second deploy  add_constraint

There is no CHECK in that DDL. `create_table_sql` reads `columns` and
`primary_key` out of the operation's details and ignores `constraints`, so the
constraint is dropped on the floor and the migration reports success. Every
deploy afterwards sees it missing and emits `add_constraint`, which SQLite and
Turso refuse by name, so every deploy after the first fails.

THIS IS THE SAME DEFECT AS `add_index`, FIXED THIS MORNING. That one read:
"`create_table`'s details carry `indexes` and `create_table_sql` never looks
at them, so a fresh migration created the table and none of its indexes."
Same function, same operation, the next key along.

IT IS FIXED DIFFERENTLY, and the difference is the point. An index became a
separate `add_index` operation beside the create, because `CREATE INDEX` is a
statement every engine has. A table-level constraint is not: SQLite and Turso
have no ADD CONSTRAINT at all, so the only moment one can exist is inside the
CREATE TABLE. It is rendered inline.

A COLUMN-LEVEL CHECK IS A DIFFERENT THING and already worked. `{"check":
"lo > 0"}` on a column renders in that column's definition. What was lost is
the table-level form, which is the only one that can span two columns.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.applier.shared import create_table_sql
from declaro_persistum.differ.core import diff

pytestmark = [pytest.mark.precommit]

DECLARED: dict[str, Any] = {
    "t": {
        "columns": {
            "a": {"type": "integer", "primary_key": True},
            "lo": {"type": "integer"},
            "hi": {"type": "integer"},
        },
        "constraints": {"ck_order": {"type": "check", "expression": "lo < hi"}},
    }
}


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "c.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "c.db"))


ENGINES = {"sqlite": _sqlite, "turso": _turso}


def test_the_renderer_puts_the_constraint_in_the_create() -> None:
    """The only moment it can exist on these engines.

    SQLite and Turso have no ADD CONSTRAINT, so a constraint not written here
    can never be added later.
    """
    rendered = create_table_sql("t", {**DECLARED["t"], "primary_key": None})

    assert "CHECK" in rendered.upper(), rendered
    assert "lo < hi" in rendered, rendered


def test_the_constraint_is_named_in_the_create() -> None:
    """An unnamed constraint cannot be dropped by name afterwards."""
    rendered = create_table_sql("t", {**DECLARED["t"], "primary_key": None})

    assert "ck_order" in rendered, rendered


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_constraint_reaches_the_database(dialect: str) -> None:
    connection = await ENGINES[dialect]()
    try:
        plan = diff({}, DECLARED, dialect=dialect)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            dialect,
            dry_run=False,
        )

        # TWO CURSOR SHAPES. pyturso holds results on a cursor object it
        # hands you; aiosqlite returns a cursor FROM execute. Third time this
        # session I have written the same line for both.
        select = "SELECT sql FROM sqlite_master WHERE name='t'"
        if dialect == "turso":
            cursor = connection.cursor()
            await cursor.execute(select)
            rows = await cursor.fetchall()
        else:
            cursor = await connection.execute(select)
            rows = await cursor.fetchall()
        stored = rows[0][0]

        assert "CHECK" in stored.upper(), stored
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_constraint_is_enforced(dialect: str) -> None:
    """The floor. A CHECK that is written and not enforced reads as correct.

    A row violating it must be refused, which is the whole reason to declare
    one.
    """
    connection = await ENGINES[dialect]()
    try:
        plan = diff({}, DECLARED, dialect=dialect)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            dialect,
            dry_run=False,
        )

        cursor = connection.cursor() if dialect == "turso" else connection
        await cursor.execute('INSERT INTO "t" (a, lo, hi) VALUES (1, 1, 2)')
        await connection.commit()

        with pytest.raises(Exception):
            await cursor.execute('INSERT INTO "t" (a, lo, hi) VALUES (2, 9, 3)')
            await connection.commit()
    finally:
        await connection.close()


async def test_a_table_with_no_constraints_is_unchanged() -> None:
    """The control. Rendering an empty constraints map must add nothing."""
    plain = {"columns": {"a": {"type": "integer", "primary_key": True}}}

    rendered = create_table_sql("t", {**plain, "primary_key": None})

    assert "CHECK" not in rendered.upper(), rendered
