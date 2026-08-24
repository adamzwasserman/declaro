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

CREATING IT AND NOT READING IT BACK IS THE SAME LOOP. Once the renderer
wrote it, the inspectors still reported no constraints, so the second deploy
still emitted `add_constraint`. Both inspectors read the named table-level
constraints out of the stored CREATE TABLE now, beside the column-level
checks they already read from there.

AND THE COMPARISON HAD TO BE NORMALISED. `_diff_constraints` compared the two
definition dicts raw. Turso rewrites `<>` to `!=` in the DDL it stores, so a
declared `lo <> hi` came back as `lo != hi` and every deploy proposed
dropping and re-adding it. Measured across five expression shapes, and that
was the only one that differed: running one shape would have missed it, which
is how this package shipped two CHECK loops before.

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
from declaro_persistum.applier.shared import statements_for
from declaro_persistum.differ.core import diff
from declaro_persistum.types import NOTHING
from tests.conftest import probe_url

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


PROBE_URL = probe_url()


async def _postgresql() -> Any:
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    await connection.execute('DROP TABLE IF EXISTS "t" CASCADE')
    return connection


ENGINES = {"sqlite": _sqlite, "turso": _turso}

# PostgreSQL creates and reads the constraint back, and does not COMPARE it.
# See `_diff_constraints`: that engine rewrites the expression in at least two
# ways, so comparing there proposed dropping and re-adding a constraint that
# had not changed. The tests that need a comparison run on the two engines
# that store the expression verbatim; the ones that only need the constraint
# to exist run on all three.
ALL_ENGINES = {**ENGINES, "postgresql": _postgresql}


def test_the_renderer_puts_the_constraint_in_the_create() -> None:
    """The only moment it can exist on these engines.

    SQLite and Turso have no ADD CONSTRAINT, so a constraint not written here
    can never be added later.
    """
    statements = statements_for(
        {"op": "create_table", "table": "t",
         "details": {**DECLARED["t"], "primary_key": None}}
    )
    # ONE CREATE TABLE IS ONE STATEMENT. The generator returns a list because
    # some operations are several; this one is not.
    rendered = "\n".join(statements)

    assert "CHECK" in rendered.upper(), rendered
    assert "lo < hi" in rendered, rendered


def test_the_constraint_is_named_in_the_create() -> None:
    """An unnamed constraint cannot be dropped by name afterwards."""
    statements = statements_for(
        {"op": "create_table", "table": "t",
         "details": {**DECLARED["t"], "primary_key": None}}
    )
    # ONE CREATE TABLE IS ONE STATEMENT. The generator returns a list because
    # some operations are several; this one is not.
    rendered = "\n".join(statements)

    assert "ck_order" in rendered, rendered


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_constraint_reaches_the_database(dialect: str) -> None:
    connection = await ENGINES[dialect]()
    try:
        plan = diff({}, DECLARED, dialect=dialect, decisions=NOTHING)
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
        plan = diff({}, DECLARED, dialect=dialect, decisions=NOTHING)
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

    rendered = "\n".join(
        statements_for(
            {"op": "create_table", "table": "t",
             "details": {**plain, "primary_key": None}}
        )
    )

    assert "CHECK" not in rendered.upper(), rendered


# ---------------------------------------------------------------------------
# Convergence, across the shapes the engines spell differently
# ---------------------------------------------------------------------------

# `lo <> hi` is the one that mattered: Turso stores it as `lo != hi`. The
# others are here because finding it took running all of them.
EXPRESSIONS = [
    "lo < hi",
    "lo <> hi",
    "lo != hi",
    "lo > 0 AND hi > 0",
    "lo IN (1, 2, 3)",
]


@pytest.mark.parametrize("expression", EXPRESSIONS)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_declared_constraint_converges(dialect: str, expression: str) -> None:
    """Declare it, apply it, read it back, diff again. Zero operations."""
    from declaro_persistum.inspector import introspect

    declared = {
        "t": {
            "columns": {
                "a": {"type": "integer", "primary_key": True},
                "lo": {"type": "integer"},
                "hi": {"type": "integer"},
            },
            "constraints": {"ck": {"type": "check", "expression": expression}},
        }
    }
    connection = await ENGINES[dialect]()
    try:
        plan = diff({}, declared, dialect=dialect, decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            dialect,
            dry_run=False,
        )

        actual = await introspect(connection, dialect)
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect=dialect, decisions=NOTHING)["operations"]

        assert remaining == [], (
            f"{expression!r} on {dialect} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in remaining]}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_changed_constraint_is_still_noticed(dialect: str) -> None:
    """The control on the normalisation.

    Folding `<>` to `!=` must not fold two genuinely different expressions
    together, or a real change would migrate nothing and report success.
    """
    from declaro_persistum.inspector import introspect

    def declared(expression: str) -> dict:
        return {
            "t": {
                "columns": {
                    "a": {"type": "integer", "primary_key": True},
                    "lo": {"type": "integer"},
                    "hi": {"type": "integer"},
                },
                "constraints": {"ck": {"type": "check", "expression": expression}},
            }
        }

    connection = await ENGINES[dialect]()
    try:
        plan = diff({}, declared("lo < hi"), dialect=dialect, decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            dialect,
            dry_run=False,
        )

        actual = await introspect(connection, dialect)
        changed = diff(
            {n: actual[n] for n in actual},
            declared("lo > hi"),
            dialect=dialect, decisions=NOTHING)["operations"]

        assert [o["op"] for o in changed] == ["drop_constraint", "add_constraint"], (
            changed
        )
    finally:
        await connection.close()


async def test_postgresql_creates_the_constraint_too() -> None:
    """PostgreSQL keeps its own `_create_table_sql` and ignored the key.

    The shared renderer was given the constraints an hour before this one, so
    the fix landed on two engines of three and PostgreSQL still dropped a
    declared constraint on the floor. That is the shape this module's own
    comment records about `generate_sql` living in one dialect module: a
    function copied per dialect goes missing from the copies.
    """
    from declaro_persistum.inspector import introspect

    connection = await _postgresql()
    try:
        declared = {
            "t": {
                "columns": {
                    "a": {"type": "integer", "primary_key": True},
                    "lo": {"type": "integer"},
                    "hi": {"type": "integer"},
                },
                "constraints": {
                    "ck_order": {"type": "check", "expression": "lo < hi"}
                },
            }
        }
        plan = diff({}, declared, dialect="postgresql", decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            "postgresql",
            dry_run=False,
        )

        actual = await introspect(connection, "postgresql")
        assert actual["t"].get("constraints"), actual["t"]

        with pytest.raises(Exception):
            await connection.execute('INSERT INTO "t" (a, lo, hi) VALUES (1, 9, 3)')
    finally:
        await connection.execute('DROP TABLE IF EXISTS "t" CASCADE')
        await connection.close()


async def test_postgresql_does_not_propose_a_constraint_it_already_has() -> None:
    """The loop this restriction avoids, asserted rather than assumed.

    Reading the constraint back is what stops `add_constraint` firing on every
    deploy. Comparing it is what would fire drop and add instead, and that is
    switched off there.
    """
    from declaro_persistum.inspector import introspect

    connection = await _postgresql()
    try:
        declared = {
            "t": {
                "columns": {
                    "a": {"type": "integer", "primary_key": True},
                    "lo": {"type": "integer"},
                    "hi": {"type": "integer"},
                },
                "constraints": {
                    "ck_order": {"type": "check", "expression": "lo > 0 AND hi > 0"}
                },
            }
        }
        plan = diff({}, declared, dialect="postgresql", decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            "postgresql",
            dry_run=False,
        )

        actual = await introspect(connection, "postgresql")
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect="postgresql", decisions=NOTHING)["operations"]

        assert remaining == [], [(o["op"], o["details"]) for o in remaining]
    finally:
        await connection.execute('DROP TABLE IF EXISTS "t" CASCADE')
        await connection.close()
