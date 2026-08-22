"""A table created with an index must have that index when the migration ends.

MEASURED 2026-08-17. Take a database with an index, introspect it, migrate
that schema into a fresh database:

    inspector reports    {"i_spare": {"columns": ["spare"]}}
    operations           ['create_table']
    SQL                  CREATE TABLE "t" ("id" TEXT PRIMARY KEY NOT NULL,
                                           "spare" TEXT)
    indexes afterwards   []

The differ puts the indexes in the operation -- `create_table`'s details carry
`indexes` -- and `create_table_sql` never looks at them. `grep indexes` over
`applier/shared.py` returns nothing.

IT SELF-HEALS ON THE NEXT DEPLOY, which is what kept it quiet. The second diff
notices the missing index and emits `add_index`, so the shape is right after
two migrations. A fresh deployment runs without the indexes it declared until
somebody migrates again, and `declaro apply` reports success both times.

THE FIX IS THE ONE THE ENUM DEFECT GOT THIS MORNING, and for the same reason:
data the operation needed was sitting in the schema where the applier could
not reach it. `insert_enum_values` carries its values; `add_index` carries its
definition and already exists in the vocabulary. The differ emits one beside
every `create_table` that declares an index, and every applier runs it the way
it runs everything else.
"""

from __future__ import annotations

import os
import tempfile

import aiosqlite
import pytest

from declaro_persistum.applier import apply, generate_sql
from declaro_persistum.differ import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit

DECLARED = {
    "t": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "spare": {"type": "text"},
            "email": {"type": "text"},
        },
        "indexes": {
            "i_spare": {"columns": ["spare"]},
            "i_email": {"columns": ["email"], "unique": True},
        },
    }
}

DIALECTS = ["postgresql", "sqlite", "turso"]


@pytest.mark.parametrize("dialect", DIALECTS)
def test_the_differ_emits_an_index_beside_the_table(dialect: str) -> None:
    """The fix as a shape. Without it the applier is handed no way to know."""
    ops = [o["op"] for o in diff({}, DECLARED, dialect=dialect, decisions=NOTHING)["operations"]]

    assert ops.count("add_index") == 2, ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_each_index_is_ordered_after_the_table_it_indexes(dialect: str) -> None:
    """CREATE INDEX on a table that does not exist yet fails.

    THIS MEASURES TOPOSORT, NOT THE EMISSION ORDER, and saying so is the
    point. A mutation control that inserted the `add_index` operations at
    position 0 -- before their own `create_table` -- left every test here
    passing, because `execution_order` is computed by dependency and not by
    the order the differ happened to append in.

    So the property holds for a reason that has nothing to do with the change
    this file is about, and a reader who took this test as evidence that the
    differ emits them in the right order would be wrong.
    """
    plan = diff({}, DECLARED, dialect=dialect, decisions=NOTHING)
    ranks = {}
    for rank, index in enumerate(plan["execution_order"]):
        ranks.setdefault(plan["operations"][index]["op"], rank)

    assert ranks["create_table"] < ranks["add_index"], ranks


@pytest.mark.parametrize("dialect", DIALECTS)
def test_the_generated_sql_creates_both_indexes(dialect: str) -> None:
    plan = diff({}, DECLARED, dialect=dialect, decisions=NOTHING)

    sql = "\n".join(generate_sql(plan["operations"], plan["execution_order"], dialect))

    assert "i_spare" in sql, sql
    assert "i_email" in sql, sql
    assert "UNIQUE" in sql.upper(), "the unique index lost its uniqueness"


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_index_is_there_after_one_migration() -> None:
    """The measured defect, executed. Not after two."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, DECLARED, dialect="sqlite", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False
        )

        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
        )
        names = {row[0] for row in await cursor.fetchall()}

        assert names == {"i_spare", "i_email"}, names
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_one_migration_is_enough_to_converge() -> None:
    """The property the defect broke: a second run must have nothing to do.

    Before the fix the second diff emitted `add_index`, so a fresh deployment
    reached its declared shape only on the deploy after the one that reported
    success.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, DECLARED, dialect="sqlite", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False
        )

        actual = await introspect(conn, "sqlite")
        second = diff({"t": actual["t"]}, DECLARED, dialect="sqlite", decisions=NOTHING)

        assert second["operations"] == [], (
            f"the declared shape needed a second migration: "
            f"{[(o['op'], o['details']) for o in second['operations']]}"
        )
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_unique_index_actually_constrains() -> None:
    """The floor. A CREATE INDEX that drops UNIQUE passes every name check."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, DECLARED, dialect="sqlite", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False
        )

        await conn.execute("INSERT INTO t (id, email) VALUES ('1', 'a@b')")
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute("INSERT INTO t (id, email) VALUES ('2', 'a@b')")
    finally:
        await conn.close()
        os.unlink(path)


def test_a_table_with_no_indexes_emits_none() -> None:
    """The floor under the floor. Emitting add_index always would pass above."""
    plain = {"t": {"columns": {"id": {"type": "text", "primary_key": True}}}}

    ops = [o["op"] for o in diff({}, plain, dialect="sqlite", decisions=NOTHING)["operations"]]

    assert ops == ["create_table"], ops
