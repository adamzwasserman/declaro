"""Changing a PostgreSQL table that already exists had never been done.

MEASURED 2026-08-18 with `coverage json` over the whole suite. Unrun in
`applier/postgresql.py`:

    ALTER COLUMN ... DROP NOT NULL   and   SET NOT NULL
    ALTER COLUMN ... DROP DEFAULT    and   SET DEFAULT
    ADD CONSTRAINT ... UNIQUE
    ON DELETE and ON UPDATE on an added foreign key

Every gate before this one created its tables from nothing. Creating is not
altering: a CREATE TABLE renders one statement and an ALTER renders a
different one per property, and only the create side had ever run against a
server.

These are the ordinary migrations. Making a column nullable, giving it a
default, taking the default away, adding a unique constraint to a table with
rows in it: that is what a schema change usually is, and none of it had been
executed.

CONVERGENCE IS THE ORACLE, and the rows are checked beside it. An ALTER that
rewrites a table can succeed and lose what was in it, which no diff would
show.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING
from tests.conftest import probe_url

pytestmark = [pytest.mark.precommit, pytest.mark.postgresql, pytest.mark.integration]

PROBE_URL = probe_url()

TOUCHED = ("children", "widgets", "parents")


def _widgets(**column_overrides: Any) -> dict[str, Any]:
    columns: dict[str, Any] = {
        "id": {"type": "integer", "primary_key": True},
        "name": {"type": "text"},
    }
    columns["name"] = {**columns["name"], **column_overrides}
    return {"widgets": {"columns": columns}}


# (label, the schema to arrive at first, the schema to change to)
STEPS: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
    ("make a column NOT NULL", _widgets(), _widgets(nullable=False)),
    ("make a column nullable", _widgets(nullable=False), _widgets()),
    ("give a column a default", _widgets(), _widgets(default="'unnamed'")),
    ("take a default away", _widgets(default="'unnamed'"), _widgets()),
    ("change a default", _widgets(default="'a'"), _widgets(default="'b'")),
    ("make a column unique", _widgets(), _widgets(unique=True)),
    (
        "add a foreign key with ON DELETE",
        {
            "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
            "children": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "pid": {"type": "integer"},
                }
            },
        },
        {
            "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
            "children": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "pid": {
                        "type": "integer",
                        "references": "parents.id",
                        "on_delete": "cascade",
                    },
                }
            },
        },
    ),
]


async def _connection() -> Any:
    """A database this run owns, so two runners cannot collide.

    EVERY TEST HERE DROPS AND CREATES THE SAME THREE TABLE NAMES. Two runs at
    once interleave those drops and creates, and on 2026-08-22 three agents
    working in parallel each saw `DuplicateTableError: relation "widgets"
    already exists` on a different test, then each spent a run proving their
    own change innocent. The names are fixed on purpose -- the tests are about
    what the differ does to a named table -- so the boundary has to be the
    database, which is what `conftest.postgresql_schema` already does for the
    rest of the suite.
    """
    import asyncpg

    owned = f"{PROBE_URL}_{uuid.uuid4().hex[:12]}"
    server, _, name = owned.rpartition("/")
    try:
        admin = await asyncpg.connect(f"{server}/postgres")
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    _MADE.append(owned)
    return await asyncpg.connect(owned)


# The databases this module made, dropped when it is done with them.
_MADE: list[str] = []


@pytest.fixture(autouse=True, scope="module")
def _drop_what_this_module_made() -> Any:
    """Every database `_connection` created, removed after the last test."""
    yield
    asyncio.run(_drop_them())


async def _drop_them() -> None:
    import asyncpg

    server, _, _ = PROBE_URL.rpartition("/")
    admin = await asyncpg.connect(f"{server}/postgres")
    try:
        [
            await admin.execute(f'DROP DATABASE IF EXISTS "{url.rpartition("/")[2]}"')
            for url in _MADE
        ]
    finally:
        await admin.close()


async def _clean(connection: Any) -> None:
    for name in TOUCHED:
        await connection.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


async def _deploy(connection: Any, declared: dict[str, Any]) -> None:
    actual = await introspect(connection, "postgresql")
    plan = diff(
        {n: actual[n] for n in declared if n in actual},
        declared,
        dialect="postgresql", decisions=NOTHING)
    await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        "postgresql",
        dry_run=False,
    )


@pytest.mark.parametrize(
    "_label,before,after", STEPS, ids=[s[0].replace(" ", "_") for s in STEPS]
)
async def test_the_change_applies(
    _label: str, before: dict[str, Any], after: dict[str, Any]
) -> None:
    """The floor. A statement the server refuses cannot converge."""
    connection = await _connection()
    try:
        await _clean(connection)
        await _deploy(connection, before)

        await _deploy(connection, after)
    finally:
        await _clean(connection)
        await connection.close()


@pytest.mark.parametrize(
    "label,before,after", STEPS, ids=[s[0].replace(" ", "_") for s in STEPS]
)
async def test_the_change_converges(
    label: str, before: dict[str, Any], after: dict[str, Any]
) -> None:
    connection = await _connection()
    try:
        await _clean(connection)
        await _deploy(connection, before)
        await _deploy(connection, after)

        actual = await introspect(connection, "postgresql")
        remaining = diff(
            {n: actual[n] for n in after if n in actual},
            after,
            dialect="postgresql", decisions=NOTHING)["operations"]

        assert remaining == [], (
            f"{label} migrates on every deploy: "
            f"{[(o['op'], o['table'], o['details']) for o in remaining]}"
        )
    finally:
        await _clean(connection)
        await connection.close()


@pytest.mark.parametrize(
    "label,before,after", STEPS, ids=[s[0].replace(" ", "_") for s in STEPS]
)
async def test_the_rows_are_still_there_afterwards(
    label: str, before: dict[str, Any], after: dict[str, Any]
) -> None:
    """An ALTER that rewrites a table can succeed and empty it.

    No diff would show that, because a diff compares shapes.
    """
    connection = await _connection()
    try:
        await _clean(connection)
        await _deploy(connection, before)
        # EVERY COLUMN GETS A VALUE. Inserting only `id` leaves `name` NULL,
        # and a migration to NOT NULL then fails for the right reason: the
        # engine is protecting rows that would violate the new constraint.
        # The first version of this test did that and reported the engine's
        # correct refusal as a defect.
        for table in after:
            columns = after[table]["columns"]
            names = ", ".join(f'"{c}"' for c in columns)
            values = ", ".join(
                "1" if columns[c]["type"] == "integer" else "'x'" for c in columns
            )
            await connection.execute(
                f'INSERT INTO "{table}" ({names}) VALUES ({values})'
            )

        await _deploy(connection, after)

        for table in after:
            count = await connection.fetchval(f'SELECT count(*) FROM "{table}"')
            assert count == 1, f"{label} left {count} rows in {table}"
    finally:
        await _clean(connection)
        await connection.close()
