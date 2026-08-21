"""Two tests both create a table called `t`, and neither sees the other's.

TWENTY-FOUR TEST FILES SHARED ONE POSTGRESQL DATABASE and collided by name:
`t` is created by 83 different places in this suite, `users` by 22, `orders`
by 8. Each depended on the others cleaning up, so the suite passed in the
order it happened to run in. Measured 2026-08-21, when `umbra audit` ran the
files in its own order and three tests failed that pass under
`-p no:randomly`.

A test that passes only in one order is reading state it never declared. The
`postgresql_schema` fixture gives each test a schema named for itself, so
these two create two different tables.

BOTH TESTS HERE WOULD PASS ON A SHARED DATABASE IF THEY ONLY CHECKED THEIR OWN
ROWS, so each also asserts the shape of `t`. On a shared database the second
test to run would find the first one's columns, or fail to create the table at
all.
"""

import asyncpg
import pytest

pytestmark = pytest.mark.postgresql


async def _columns(url: str, schema: str, table: str) -> list[str]:
    """QUALIFIED BY SCHEMA. Asking `information_schema` for a table by name
    alone finds every schema's copy, including the `public.t` that other
    tests in this suite leave behind, which is the very sharing under test."""
    conn = await asyncpg.connect(url)
    try:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position",
            schema,
            table,
        )
        return [row["column_name"] for row in rows]
    finally:
        await conn.close()


async def test_one_test_creates_t_with_a_single_column(postgresql_schema) -> None:
    url, name = postgresql_schema
    conn = await asyncpg.connect(url)
    try:
        await conn.execute('CREATE TABLE "t" ("only_here" INTEGER)')
    finally:
        await conn.close()

    assert await _columns(url, name, "t") == ["only_here"]


async def test_another_test_creates_t_with_different_columns(
    postgresql_schema,
) -> None:
    """The same table name, and it must not already exist.

    `CREATE TABLE` without IF NOT EXISTS is the assertion: on a shared
    database whichever of these two ran second would raise
    DuplicateTableError.
    """
    url, name = postgresql_schema
    conn = await asyncpg.connect(url)
    try:
        await conn.execute('CREATE TABLE "t" ("different" TEXT, "again" TEXT)')
    finally:
        await conn.close()

    assert await _columns(url, name, "t") == ["different", "again"]


async def test_the_schema_is_gone_afterwards(postgresql_schema) -> None:
    """The fixture cleans up, so a rerun of the suite starts clean.

    Without this the schemas accumulate, one per test per run, and the
    database fills with them.
    """
    url, name = postgresql_schema
    conn = await asyncpg.connect(url)
    try:
        await conn.execute('CREATE TABLE "t" ("x" INTEGER)')
        leftovers = await conn.fetch(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 't\\_%' AND schema_name <> $1",
            name,
        )
    finally:
        await conn.close()

    assert not leftovers, (
        f"{len(leftovers)} schemas from earlier tests were never dropped: "
        f"{[r['schema_name'] for r in leftovers][:5]}"
    )
