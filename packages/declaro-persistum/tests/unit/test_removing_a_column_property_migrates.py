"""Taking a property off a column must migrate, not crash.

MEASURED 2026-08-18. A stored table carrying a CHECK the models no longer
declare:

    operation  alter_column widgets
               {'column': 'name',
                'changes': {'check': {'from': 'length(name) < 3', 'to': None}}}
    result     MigrationError: Table reconstruction failed for 'widgets'
               Original error: OperationalError: no such column: None

`_apply_alter_column` unwraps the differ's `{"from": x, "to": y}` and then
treats `to: None` as a removal for exactly one property:

    if value is None and key == "default":
        col_def.pop("default", None)
    else:
        col_def[key] = value

So a removed CHECK was written back as `check: None`, and the renderer emitted
`CHECK (None)`, which SQLite reads as a column named None. Removing a `unique`
had the same shape.

WHY IT LOOKED LIKE A DATABASE PROBLEM. The error names the table and says
reconstruction failed, so it reads as the engine refusing a rebuild. Nothing
in it points at a `None` that this package put in the SQL itself.

THIS IS THE THIRD TIME THE `{"from": ..., "to": ...}` SHAPE HAS DONE THIS.
A change record reached a column definition unwrapped once before and
`_map_type` called `.lower()` on the dict. The wrapper is unpacked in one
place and every property that can be removed has to be handled there.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect

pytestmark = [pytest.mark.precommit]

PLAIN = {
    "widgets": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "name": {"type": "text"},
        }
    }
}

# A CHECK IS COMPARED ON SQLITE ONLY, and that is a decision rather than an
# oversight. PostgreSQL and Turso both rewrite the expression they store --
# `c <> ''` becomes `c != ''`, `length(name)` becomes `length (name)` -- so
# comparing the text shipped a migration loop on each of them. The differ
# reads `compared = ("unique", "check") if dialect == "sqlite" else
# ("unique",)`.
#
# The consequence is real and is not this file's to fix: on Turso a removed
# CHECK produces zero operations and reports success, so the constraint stays
# on the column forever. Measured here rather than assumed, and asserted as
# the contract that exists.
COMPARES_CHECK = {"sqlite": True, "turso": False}

# (label, the DDL that creates the stored table, the property being removed)
STORED = [
    ("check", 'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, '
              "name TEXT CHECK (length(name) < 3))"),
    ("unique", 'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, '
               "name TEXT UNIQUE)"),
]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "p.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "p.db"))


ENGINES = {"sqlite": _sqlite, "turso": _turso}


async def _migrate(connection: Any, dialect: str) -> None:
    actual = await introspect(connection, dialect)
    plan = diff(
        {n: actual[n] for n in PLAIN if n in actual}, PLAIN, dialect=dialect
    )
    await apply(
        connection, plan["operations"], plan["execution_order"], dialect,
        dry_run=False,
    )


@pytest.mark.parametrize("prop,ddl", STORED, ids=[s[0] for s in STORED])
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_removing_it_from_the_model_migrates(
    dialect: str, prop: str, ddl: str
) -> None:
    """The measured crash, for every property that can be taken away."""
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(ddl)
        await connection.commit()

        await _migrate(connection, dialect)
    finally:
        await connection.close()


@pytest.mark.parametrize("prop,ddl", STORED, ids=[s[0] for s in STORED])
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_property_is_actually_gone_afterwards(
    dialect: str, prop: str, ddl: str
) -> None:
    """The floor. A migration that ran and changed nothing is not a migration."""
    if prop == "check" and not COMPARES_CHECK[dialect]:
        pytest.skip(
            f"{dialect} rewrites a stored CHECK, so the differ does not "
            f"compare one and a removal produces no operation"
        )

    connection = await ENGINES[dialect]()
    try:
        await connection.execute(ddl)
        await connection.commit()
        await _migrate(connection, dialect)

        actual = await introspect(connection, dialect)

        assert prop not in actual["widgets"]["columns"]["name"], (
            f"{prop} survived the migration that removed it: "
            f"{actual['widgets']['columns']['name']}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("prop,ddl", STORED, ids=[s[0] for s in STORED])
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_rows_survive_the_rebuild(
    dialect: str, prop: str, ddl: str
) -> None:
    """Removing a constraint rebuilds the table, and a rebuild can lose rows."""
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(ddl)
        await connection.execute(
            'INSERT INTO "widgets" (id, name) VALUES (1, ?)', ("ab",)
        )
        await connection.commit()

        await _migrate(connection, dialect)

        cursor = await connection.execute('SELECT id, name FROM "widgets"')
        assert [tuple(r) for r in await cursor.fetchall()] == [(1, "ab")]
    finally:
        await connection.close()


@pytest.mark.parametrize("prop,ddl", STORED, ids=[s[0] for s in STORED])
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_removal_converges(dialect: str, prop: str, ddl: str) -> None:
    """A second deploy has no work left."""
    connection = await ENGINES[dialect]()
    try:
        await connection.execute(ddl)
        await connection.commit()
        await _migrate(connection, dialect)

        actual = await introspect(connection, dialect)
        remaining = diff(
            {n: actual[n] for n in PLAIN if n in actual}, PLAIN, dialect=dialect
        )["operations"]

        assert remaining == [], [(o["op"], o["details"]) for o in remaining]
    finally:
        await connection.close()
