"""Repointing a foreign key and changing an index had never been run.

MEASURED 2026-08-19 with `coverage json` over the whole suite. Two branches in
the differ had never executed, and both emit a drop followed by an add:

    a foreign key whose target changed   drop_foreign_key + add_foreign_key
    an index whose definition changed    drop_index + add_index

Adding and removing each were covered. Changing one was not, and change is
the case where the order of the pair matters and where the old object has to
be gone before the new one can exist.

WHY THESE ARE NOT THE SAME AS AN ADD FOLLOWED BY A DROP. On SQLite and Turso
both foreign-key operations go through table reconstruction, so a changed
reference rebuilds the table twice unless the pair is coalesced. On any
engine, an index dropped after its replacement is created is a name collision
rather than a change.

CONVERGENCE IS NOT ENOUGH HERE, and finding that out took a mutation.
Dropping only the convention-named constraint on PostgreSQL leaves the
original in place, because a key created inline by CREATE TABLE carries
PostgreSQL's own auto-name. Measured 2026-08-19 after repointing:

    keys on children  ['children_pid_fkey', 'fk_children_pid_others']

Introspection reports one `references` per column, so it names the new
target, the diff converges, and every test that stops there passes. The
column is still constrained by BOTH: a row valid under the new target and not
the old one is refused, silently, by a key the package cannot see.

So the count is asserted, and a row that only the new target permits is
inserted.

TWO NAMES, AND EACH NEEDS ITS OWN CASE. A key created inline by CREATE TABLE
carries PostgreSQL's auto-name; one created by `add_foreign_key` carries this
package's convention. So repointing once exercises the first and repointing
twice exercises the second, and a test that only repoints once passes with
the convention clause removed.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.precommit]

PROBE_URL = probe_url()

TOUCHED = ("children", "parents", "others", "widgets")


def _fk_schema(target_table: str) -> dict[str, Any]:
    return {
        "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "others": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": f"{target_table}.id"},
            }
        },
    }


def _index_schema(columns: list[str]) -> dict[str, Any]:
    return {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "a": {"type": "text"},
                "b": {"type": "text"},
            },
            "indexes": {"ix": {"columns": columns}},
        }
    }


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "c.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "c.db"))


async def _postgresql() -> Any:
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)
    for table in TOUCHED:
        await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    return connection


ENGINES = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}
DIALECTS = sorted(ENGINES)


async def _done(connection: Any, dialect: str) -> None:
    if dialect == "postgresql":
        for table in TOUCHED:
            await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    await connection.close()


async def _deploy(connection: Any, dialect: str, declared: dict) -> None:
    actual = await introspect(connection, dialect)
    plan = diff(
        {n: actual[n] for n in declared if n in actual}, declared, dialect=dialect, decisions=NOTHING)
    await apply(
        connection, plan["operations"], plan["execution_order"], dialect, dry_run=False
    )


def test_a_changed_reference_emits_a_drop_and_an_add() -> None:
    """The branch itself, before any engine sees it."""
    operations = diff(
        _fk_schema("parents"), _fk_schema("others"), dialect="sqlite", decisions=NOTHING)["operations"]

    assert [o["op"] for o in operations] == ["drop_foreign_key", "add_foreign_key"]


def test_the_drop_names_the_reference_being_replaced() -> None:
    """Without it, a rebuild cannot tell which key it is removing."""
    operations = diff(
        _fk_schema("parents"), _fk_schema("others"), dialect="sqlite", decisions=NOTHING)["operations"]

    assert operations[0]["details"]["references"] == "parents.id", operations[0]
    assert operations[1]["details"]["references"] == "others.id", operations[1]


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_repointed_reference_applies_and_converges(dialect: str) -> None:
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, _fk_schema("parents"))

        await _deploy(connection, dialect, _fk_schema("others"))

        actual = await introspect(connection, dialect)
        declared = _fk_schema("others")
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect=dialect, decisions=NOTHING)["operations"]

        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_the_repointed_reference_points_where_it_was_told(
    dialect: str,
) -> None:
    """The floor. Converging on a key that still points at the old table
    would satisfy a diff that never looked at the target."""
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, _fk_schema("parents"))
        await _deploy(connection, dialect, _fk_schema("others"))

        actual = await introspect(connection, dialect)

        assert actual["children"]["columns"]["pid"]["references"] == "others.id"
    finally:
        await _done(connection, dialect)


def test_a_changed_index_emits_a_drop_and_an_add() -> None:
    operations = diff(
        _index_schema(["a"]), _index_schema(["a", "b"]), dialect="sqlite", decisions=NOTHING)["operations"]

    assert [o["op"] for o in operations] == ["drop_index", "add_index"]


def test_the_drop_comes_before_the_add() -> None:
    """Creating the replacement first is a name collision, not a change."""
    plan = diff(_index_schema(["a"]), _index_schema(["a", "b"]), dialect="sqlite", decisions=NOTHING)
    ordered = [plan["operations"][i]["op"] for i in plan["execution_order"]]

    assert ordered.index("drop_index") < ordered.index("add_index"), ordered


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_changed_index_applies_and_converges(dialect: str) -> None:
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, _index_schema(["a"]))

        await _deploy(connection, dialect, _index_schema(["a", "b"]))

        actual = await introspect(connection, dialect)
        declared = _index_schema(["a", "b"])
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect=dialect, decisions=NOTHING)["operations"]

        assert actual["widgets"]["indexes"]["ix"]["columns"] == ["a", "b"]
        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        await _done(connection, dialect)


def test_an_unchanged_index_emits_nothing() -> None:
    """The control on both. A differ emitting drop-and-add for everything
    would satisfy every test above."""
    assert diff(_index_schema(["a"]), _index_schema(["a"]), dialect="sqlite", decisions=NOTHING)[
        "operations"
    ] == []
    assert diff(_fk_schema("parents"), _fk_schema("parents"), dialect="sqlite", decisions=NOTHING)[
        "operations"
    ] == []


async def test_the_old_reference_is_gone_not_merely_unreported() -> None:
    """One key on the column, not two.

    `references` is one value per column, so introspection cannot report a
    stale key beside the new one. Counting them in the catalogue is the only
    way to see it, and a mutation dropping only the convention name passed
    every other test in this file.
    """
    connection = await ENGINES["postgresql"]()
    try:
        await _deploy(connection, "postgresql", _fk_schema("parents"))
        await _deploy(connection, "postgresql", _fk_schema("others"))

        rows = await connection.fetch(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'children'::regclass AND contype = 'f'"
        )

        assert len(rows) == 1, [r[0] for r in rows]
    finally:
        await _done(connection, "postgresql")


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_row_the_new_target_permits_is_accepted(dialect: str) -> None:
    """The floor, and it is the enforcement rather than the catalogue.

    A stale foreign key refuses rows the declaration allows. Here `others`
    has the row and `parents` does not, so anything still checking `parents`
    rejects it.
    """
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, _fk_schema("parents"))
        await _deploy(connection, dialect, _fk_schema("others"))

        if dialect == "postgresql":
            await connection.execute('INSERT INTO "others" (id) VALUES (7)')
            await connection.execute(
                'INSERT INTO "children" (id, pid) VALUES (1, 7)'
            )
        else:
            cursor = connection.cursor() if dialect == "turso" else connection
            await cursor.execute("PRAGMA foreign_keys = ON")
            await cursor.execute('INSERT INTO "others" (id) VALUES (7)')
            await cursor.execute('INSERT INTO "children" (id, pid) VALUES (1, 7)')
            await connection.commit()
    finally:
        await _done(connection, dialect)


async def test_a_reference_repointed_twice_leaves_one_key() -> None:
    """The convention name, which one repoint never reaches.

    The first repoint drops PostgreSQL's inline auto-name and adds a key
    under this package's convention. The second has to drop THAT, and a
    version dropping only the auto-name passes every test that stops at one
    repoint.
    """
    connection = await ENGINES["postgresql"]()
    try:
        await _deploy(connection, "postgresql", _fk_schema("parents"))
        await _deploy(connection, "postgresql", _fk_schema("others"))

        await _deploy(connection, "postgresql", _fk_schema("parents"))

        rows = await connection.fetch(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'children'::regclass AND contype = 'f'"
        )

        assert len(rows) == 1, [r[0] for r in rows]
    finally:
        await _done(connection, "postgresql")


async def test_a_reference_repointed_twice_points_where_it_was_told() -> None:
    """And it points back at the first target, not at the middle one."""
    connection = await ENGINES["postgresql"]()
    try:
        await _deploy(connection, "postgresql", _fk_schema("parents"))
        await _deploy(connection, "postgresql", _fk_schema("others"))
        await _deploy(connection, "postgresql", _fk_schema("parents"))

        actual = await introspect(connection, "postgresql")

        assert actual["children"]["columns"]["pid"]["references"] == "parents.id"
    finally:
        await _done(connection, "postgresql")
