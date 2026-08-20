"""An inline multi-column UNIQUE was invisible, and a rebuild destroyed it.

MEASURED 2026-08-19 against a table this package did not create:

    CREATE TABLE "t" ("a" INTEGER PRIMARY KEY, "c" TEXT, "d" TEXT,
                      UNIQUE ("c","d"))

    introspect  indexes: None
                columns: {'c': {'type': 'text'}, 'd': {'type': 'text'}}

Nothing. Not an index, not a property on either column. The constraint exists
in the database and this package cannot see it, so a rebuild writes the table
back without it and the duplicate rows it was refusing start being accepted.

WHY IT WAS DROPPED, AND WHY THAT REASONING ONLY COVERS HALF THE CASES.
`indexes_from_rows` skips every index whose origin is "pk" or "u", with the
note: "they are consequences of a column definition, not objects a caller
declared, and reporting them would make the differ try to recreate them."

That is right for a single-column unique. SQLite creates one automatically
for `"email" TEXT UNIQUE`, and the inspector folds it back onto the column as
`unique: True`, so the column carries the fact and the index does not need to.

A multi-column unique has no column to fold onto. It is a table-level
constraint that somebody wrote by hand, and skipping it loses the fact
entirely rather than moving it somewhere better.

WHAT REPORTING IT CHANGES. A `declaro diff` against an existing database will
now propose dropping a multi-column unique the declaration does not declare.
That is the correct proposal: the declaration is the source of truth and it
does not mention the constraint. Silence was the wrong answer, not a safer
one.

Nothing this package writes is affected. It creates a multi-column unique as
a named `CREATE UNIQUE INDEX`, whose origin is "c" and which was always
reported.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.inspector import introspect

pytestmark = [pytest.mark.precommit]

INLINE_MULTI = (
    'CREATE TABLE "t" ("a" INTEGER PRIMARY KEY, "c" TEXT, "d" TEXT, '
    'UNIQUE ("c","d"))'
)
INLINE_SINGLE = 'CREATE TABLE "t" ("a" INTEGER PRIMARY KEY, "email" TEXT UNIQUE)'
NAMED_INDEX = [
    'CREATE TABLE "t" ("a" INTEGER PRIMARY KEY, "c" TEXT, "d" TEXT)',
    'CREATE UNIQUE INDEX "ix_cd" ON "t" ("c", "d")',
]


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "u.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "u.db"))


PROBE_URL = os.environ.get(
    "TEST_POSTGRESQL_URL", "postgresql://postgres:postgres@localhost/declaro_probe"
)


async def _postgresql() -> Any:
    """PostgreSQL hides it for the same reason and by a different mechanism.

    It has no `origin` column. `_get_indexes` excludes any index with an
    owning constraint of type p, u or x, so a UNIQUE constraint's index is
    dropped whether it covers one column or four.
    """
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    await connection.execute('DROP TABLE IF EXISTS "t" CASCADE')
    return connection


ENGINES = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}


async def _built(dialect: str, statements: list[str]) -> Any:
    connection = await ENGINES[dialect]()
    for statement in statements:
        await connection.execute(statement)
    if dialect != "postgresql":
        await connection.commit()
    return connection


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_an_inline_multi_column_unique_is_reported(dialect: str) -> None:
    """The measured hole: it was reported as nothing at all."""
    connection = await _built(dialect, [INLINE_MULTI])
    try:
        actual = await introspect(connection, dialect)

        indexes = actual["t"].get("indexes") or {}
        assert any(
            set(index.get("columns", [])) == {"c", "d"} and index.get("unique")
            for index in indexes.values()
        ), actual["t"]
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_single_column_unique_is_still_folded_onto_the_column(
    dialect: str,
) -> None:
    """The half the old reasoning covered, and it must not change.

    SQLite creates the index automatically for `"email" TEXT UNIQUE`, and
    reporting it as an index as well as a column property would make the
    differ try to recreate something it never declared.
    """
    connection = await _built(dialect, [INLINE_SINGLE])
    try:
        actual = await introspect(connection, dialect)

        assert actual["t"]["columns"]["email"].get("unique") is True
        indexes = actual["t"].get("indexes") or {}
        assert not any(
            index.get("columns") == ["email"] for index in indexes.values()
        ), indexes
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_named_unique_index_is_unaffected(dialect: str) -> None:
    """The form this package itself writes. It was always reported."""
    connection = await _built(dialect, NAMED_INDEX)
    try:
        actual = await introspect(connection, dialect)

        assert actual["t"]["indexes"]["ix_cd"]["columns"] == ["c", "d"]
        assert actual["t"]["indexes"]["ix_cd"]["unique"] is True
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_primary_key_index_is_still_not_reported(dialect: str) -> None:
    """The other origin the builder skips, and it stays skipped.

    A composite primary key produces a `pk`-origin index, and the key is
    already carried on the table as `primary_key`.
    """
    connection = await _built(
        dialect,
        ['CREATE TABLE "t" ("a" INTEGER, "b" INTEGER, PRIMARY KEY ("a","b"))'],
    )
    try:
        actual = await introspect(connection, dialect)

        assert actual["t"].get("primary_key") == ["a", "b"], actual["t"]
        assert not (actual["t"].get("indexes") or {}), actual["t"]
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# The form this package writes, on every engine
# ---------------------------------------------------------------------------

DECLARED = {
    "t": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "c": {"type": "text"},
            "d": {"type": "text"},
        },
        "indexes": {"ix_cd": {"columns": ["c", "d"], "unique": True}},
    }
}


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_declared_multi_column_unique_converges(dialect: str) -> None:
    """The control on the whole change, run against a real engine.

    Reporting a constraint-backed index that the model does not declare is
    correct. Reporting one it DOES declare twice, or under a name the model
    never used, would make every deploy propose dropping and recreating it.
    Three engines, and the second diff has to be empty on each.
    """
    from declaro_persistum.applier import apply
    from declaro_persistum.differ.core import diff

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

        actual = await introspect(connection, dialect)
        remaining = diff(
            {n: actual[n] for n in DECLARED if n in actual},
            DECLARED,
            dialect=dialect,
        )["operations"]

        assert actual["t"]["indexes"]["ix_cd"]["columns"] == ["c", "d"]
        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        if dialect == "postgresql":
            await connection.execute('DROP TABLE IF EXISTS "t" CASCADE')
        await connection.close()
