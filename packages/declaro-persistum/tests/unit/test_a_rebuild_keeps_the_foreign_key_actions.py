"""A table rebuild dropped the constraints of the table it rebuilt.

SQLite and Turso have no ALTER COLUMN, so changing a column's type rebuilds
the whole table: create a new one, copy the rows, drop the old, rename. Every
constraint on that table has to be written out again from the schema the
inspector read back, and the lines that write ON DELETE and ON UPDATE during a
rebuild had never run. Measured 2026-08-19 with `coverage json` over the whole
suite, in both reconstruction modules.

THEY DID NOT SURVIVE. Measured 2026-08-19 on a table carrying a UNIQUE, a
CHECK and a foreign key, rebuilt because an unrelated column changed type:

    before  "email" TEXT UNIQUE, "age" INTEGER CHECK (age > 0),
            "pid" INTEGER REFERENCES "parents"("id") ON DELETE CASCADE
    turso   "email" TEXT, "age" INTEGER, "pid" INTEGER
    sqlite  "email" TEXT UNIQUE, "age" INTEGER, "pid" ... ON DELETE CASCADE

Turso lost all three and reported success. SQLite lost the CHECK.

The two paths read different things. SQLite reconstructs from
`_get_full_table_schema`, which reads the columns, the unique indexes and the
foreign keys. Turso reconstructed from `PRAGMA table_info` alone, which
returns columns and nothing else, so it wrote back a table with every
constraint gone. Neither read the CHECK, which is in no PRAGMA and has to come
out of the stored DDL.

WHAT IT COSTS. Changing one column's type on Turso silently removes the
referential integrity and the validation from the whole table. Nothing
reports it, the migration succeeds, and the next write that should have been
refused is accepted.

THE DECLARATION IS NOT THE ENFORCEMENT, which is why this does not stop at
introspecting the rebuilt table. `PRAGMA foreign_key_list` reports what the
DDL says; whether the engine acts on it depends on `PRAGMA foreign_keys`,
which is a no-op inside a transaction and was silently off during migrations
until 2026-08-17. A CASCADE that is declared and not enforced reads as correct
from every angle except the one that matters, so a parent row is deleted here
and the child is checked to have gone with it.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit]


def _schema(note_type: str, **fk: Any) -> dict[str, Any]:
    return {
        "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "children": {
            # A TABLE-LEVEL CONSTRAINT TOO, added 2026-08-19. It was the third
            # constraint kind this rebuild dropped, after the unique and the
            # foreign key, and for the same reason: the rebuild is handed
            # columns and a table-level constraint is not on a column.
            "constraints": {"ck_age": {"type": "check", "expression": "age > 0"}},
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id", **fk},
                "email": {"type": "text", "unique": True},
                "age": {"type": "integer", "check": "age > 0"},
                "note": {"type": note_type},
            },
        },
    }


ACTIONS = [
    ("on_delete cascade", {"on_delete": "cascade"}),
    ("on_update cascade", {"on_update": "cascade"}),
    ("both", {"on_delete": "cascade", "on_update": "cascade"}),
    # `set null` WITH A SPACE. `types.py` declares the vocabulary as
    # Literal["cascade", "set null", "restrict", "no action"], and the first
    # version of this file wrote `set_null`. The renderer accepts it, because
    # it does `.replace("_", " ")`, so the table is written correctly and the
    # comparison never matches: every deploy rebuilds the table forever. That
    # is a defect of its own and is refused now, in
    # test_a_foreign_key_action_outside_the_vocabulary.py.
    ("on_delete set null", {"on_delete": "set null"}),
]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "fk.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "fk.db"))


ENGINES = {"sqlite": _sqlite, "turso": _turso}


async def _apply(connection: Any, dialect: str, current: dict, target: dict) -> None:
    plan = diff(current, target, dialect=dialect, decisions=NOTHING)
    await apply(connection, plan["operations"], plan["execution_order"], dialect, dry_run=False)


@pytest.mark.parametrize("label,fk", ACTIONS, ids=[a[0].replace(" ", "_") for a in ACTIONS])
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_actions_survive_a_rebuild(
    dialect: str,
    label: str,  # noqa: ARG001 - parametrize unpacks it; it names the case
    fk: dict,
) -> None:
    """The rebuild is triggered by a column the foreign key has nothing to do
    with, which is what makes it a rebuild rather than a change to the key."""
    connection = await ENGINES[dialect]()
    try:
        await _apply(connection, dialect, {}, _schema("text", **fk))
        actual = await introspect(connection, dialect)

        await _apply(connection, dialect, actual, _schema("integer", **fk))

        rebuilt = await introspect(connection, dialect)
        column = rebuilt["children"]["columns"]["pid"]

        assert column.get("references") == "parents.id", column
        for action, value in fk.items():
            assert column.get(action) == value, column
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_rebuild_converges(dialect: str) -> None:
    """A rebuild that loses an action migrates forever afterwards."""
    connection = await ENGINES[dialect]()
    try:
        declared = _schema("integer", on_delete="cascade")
        await _apply(connection, dialect, {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, dialect)
        await _apply(connection, dialect, actual, declared)

        rebuilt = await introspect(connection, dialect)
        remaining = diff(
            {n: rebuilt[n] for n in declared if n in rebuilt},
            declared,
            dialect=dialect,
            decisions=NOTHING,
        )["operations"]

        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        await connection.close()


async def test_the_cascade_is_still_enforced_after_a_rebuild() -> None:
    """The floor, and the only check that separates declared from enforced.

    `PRAGMA foreign_key_list` reports what the DDL says. Whether the engine
    acts on it depends on `PRAGMA foreign_keys`, which is a no-op inside a
    transaction and was silently off during migrations until 2026-08-17.
    """
    import aiosqlite

    path = os.path.join(tempfile.mkdtemp(), "e.db")
    connection = await aiosqlite.connect(path)
    try:
        await _apply(connection, "sqlite", {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, "sqlite")
        await _apply(connection, "sqlite", actual, _schema("integer", on_delete="cascade"))

        await connection.execute("PRAGMA foreign_keys = ON")
        await connection.execute('INSERT INTO "parents" (id) VALUES (1)')
        await connection.execute('INSERT INTO "children" (id, pid, note) VALUES (1, 1, 0)')
        await connection.commit()

        await connection.execute('DELETE FROM "parents" WHERE id = 1')
        await connection.commit()

        cursor = await connection.execute('SELECT count(*) FROM "children"')
        assert (await cursor.fetchone())[0] == 0, (
            "the parent was deleted and the child stayed: the cascade is declared but not enforced"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_unique_survives_a_rebuild(dialect: str) -> None:
    """Turso reconstructed from `PRAGMA table_info`, which does not report it."""
    connection = await ENGINES[dialect]()
    try:
        await _apply(connection, dialect, {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, dialect)
        await _apply(connection, dialect, actual, _schema("integer", on_delete="cascade"))

        rebuilt = await introspect(connection, dialect)

        assert rebuilt["children"]["columns"]["email"].get("unique") is True, rebuilt["children"][
            "columns"
        ]["email"]
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_check_survives_a_rebuild(dialect: str) -> None:
    """A CHECK is in no PRAGMA, so neither path read one before this."""
    connection = await ENGINES[dialect]()
    try:
        await _apply(connection, dialect, {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, dialect)
        await _apply(connection, dialect, actual, _schema("integer", on_delete="cascade"))

        rebuilt = await introspect(connection, dialect)

        assert rebuilt["children"]["columns"]["age"].get("check"), rebuilt["children"]["columns"][
            "age"
        ]
    finally:
        await connection.close()


async def test_the_unique_is_still_enforced_after_a_rebuild() -> None:
    """Declared is not enforced, the same distinction the cascade test makes."""
    import aiosqlite

    connection = await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "u.db"))
    try:
        await _apply(connection, "sqlite", {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, "sqlite")
        await _apply(connection, "sqlite", actual, _schema("integer", on_delete="cascade"))

        await connection.execute(
            'INSERT INTO "children" (id, email, age, note) VALUES (1, ?, 5, 0)',
            ("a@b.c",),
        )
        await connection.commit()

        with pytest.raises(aiosqlite.IntegrityError):
            await connection.execute(
                'INSERT INTO "children" (id, email, age, note) VALUES (2, ?, 5, 0)',
                ("a@b.c",),
            )
            await connection.commit()
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_table_level_constraint_survives_a_rebuild(dialect: str) -> None:
    """The third kind. The rebuild reads the whole table now, not part of it."""
    connection = await ENGINES[dialect]()
    try:
        await _apply(connection, dialect, {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, dialect)
        await _apply(connection, dialect, actual, _schema("integer", on_delete="cascade"))

        rebuilt = await introspect(connection, dialect)

        assert rebuilt["children"].get("constraints", {}).get("ck_age"), rebuilt["children"]
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_table_level_constraint_is_still_enforced(dialect: str) -> None:
    """Declared is not enforced, the same floor the cascade and unique have."""
    connection = await ENGINES[dialect]()
    try:
        await _apply(connection, dialect, {}, _schema("text", on_delete="cascade"))
        actual = await introspect(connection, dialect)
        await _apply(connection, dialect, actual, _schema("integer", on_delete="cascade"))

        cursor = connection.cursor() if dialect == "turso" else connection
        # NOT `pytest.raises(Exception)` ALONE. That passes on a bug of ours
        # as readily as on the engine refusing, which is the misattribution
        # `A_BUG_IN_THIS_PACKAGE` exists to name. The type cannot be pinned
        # to one class here because three engines each raise their own, so
        # the claim is the one that matters: something refused, and it was
        # not us.
        with pytest.raises(Exception) as refused:  # noqa: B017 - checked below
            await cursor.execute(
                'INSERT INTO "children" (id, pid, email, age, note) '
                "VALUES (1, NULL, 'a@b.c', -5, 0)"
            )
            await connection.commit()

        assert not isinstance(refused.value, A_BUG_IN_THIS_PACKAGE), (
            f"the engine was supposed to refuse this; instead a bug of ours "
            f"raised {type(refused.value).__name__}: {refused.value}"
        )
    finally:
        await connection.close()
