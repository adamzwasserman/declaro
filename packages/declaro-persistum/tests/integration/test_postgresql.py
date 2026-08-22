"""PostgreSQL, executed. The backend this package documents as production.

WRITTEN AGAINST A FILE THAT CONTAINED NO TESTS.

`tests/integration/test_postgresql.py` was 40 lines: a module docstring, a
`skipif` on `TEST_POSTGRESQL_URL`, a fixture that creates and drops a schema,
and then nothing. It READ as a suite waiting for a server. There was nothing
behind the guard to wait, so setting the variable changed the collected count
from zero to zero.

MEASURED 2026-08-17: two PostgreSQL-marked tests in a suite of 1241, neither of
which opens a connection. Every claim this package makes about PostgreSQL --
type mapping, expression defaults, enum lookup tables, transactional DDL --
rested on SQLite tests and on reading.

THE SKIP IS NARROW AND SAYS WHAT IT SKIPPED. A test that cannot reach a server
is not the same as a passing test, and the old guard could not tell "no server"
from "no tests". This one names the URL it could not reach, so a skip in the
summary is actionable rather than decorative.

WHY A DEDICATED DATABASE. These tests create and drop tables. `declaro_probe`
exists for that; `declarotodo` holds data. A diff-driven migration is a
destructive operation and has already dropped eight tables from the wrong
database on this machine once.
"""

from __future__ import annotations

import os

import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import apply, generate_sql
from declaro_persistum.differ import diff
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING
from tests.conftest import probe_url

pytestmark = [pytest.mark.asyncio, pytest.mark.postgresql, pytest.mark.integration]

URL = os.environ.get(
    "TEST_POSTGRESQL_URL",
    probe_url(),
)


async def _connect():
    """A connection, or a skip that names what could not be reached."""
    import asyncpg

    try:
        return await asyncpg.connect(URL)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no PostgreSQL at {URL}: {type(exc).__name__}: {exc}")


async def _clean(conn, *tables: str) -> None:
    quoted = ", ".join(f'"{t}"' for t in tables)
    await conn.execute(f"DROP TABLE IF EXISTS {quoted} CASCADE")


# ---------------------------------------------------------------------------
# The floor: the server is really there
# ---------------------------------------------------------------------------


async def test_the_server_is_reachable_and_is_postgresql() -> None:
    """Everything below is meaningless if this is talking to something else."""
    conn = await _connect()
    try:
        version = await conn.fetchval("SELECT version()")
        assert "PostgreSQL" in version, version
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Migrate, introspect, and agree with yourself
# ---------------------------------------------------------------------------

WIDGETS = {
    "widgets": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "name": {"type": "text", "nullable": False},
            "count": {"type": "integer", "default": 0},
        }
    }
}


async def test_a_migration_creates_the_table_it_planned() -> None:
    conn = await _connect()
    try:
        await _clean(conn, "widgets")
        plan = diff({}, WIDGETS, dialect="postgresql", decisions=NOTHING)

        result = await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        assert result["success"] is True, result
        assert await conn.fetchval(
            "SELECT to_regclass('widgets') IS NOT NULL"
        ) is True
    finally:
        await _clean(conn, "widgets")
        await conn.close()


async def test_introspection_reads_back_what_was_written() -> None:
    """The round trip. A differ that disagrees with the inspector migrates forever."""
    conn = await _connect()
    try:
        await _clean(conn, "widgets")
        plan = diff({}, WIDGETS, dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        actual = await introspect(conn, "postgresql")

        assert "widgets" in actual, sorted(actual)
        assert set(actual["widgets"]["columns"]) == {"id", "name", "count"}
    finally:
        await _clean(conn, "widgets")
        await conn.close()


async def test_migrating_twice_is_a_no_op() -> None:
    """The property that makes every deployment safe to repeat.

    If introspection and the schema disagree on any detail, the second diff
    emits operations and the table is rebuilt on every start.
    """
    conn = await _connect()
    try:
        await _clean(conn, "widgets")
        plan = diff({}, WIDGETS, dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        actual = await introspect(conn, "postgresql")

        # SCOPED TO THE TABLE THIS TEST DECLARES. Diffing the whole database
        # against a one-table schema reports every OTHER table as a drop, and
        # `declaro_probe` carries leftovers from earlier runs. The first
        # version of this reported ten operations, eight of which were mine.
        second = diff({"widgets": actual["widgets"]}, WIDGETS, dialect="postgresql", decisions=NOTHING)

        assert second["operations"] == [], (
            f"an unchanged schema still migrates: "
            f"{[o['op'] for o in second['operations']]}"
        )
    finally:
        await _clean(conn, "widgets")
        await conn.close()


async def test_a_dry_run_changes_nothing() -> None:
    """`dry_run` is required now, so both directions have to be real."""
    conn = await _connect()
    try:
        await _clean(conn, "widgets")
        plan = diff({}, WIDGETS, dialect="postgresql", decisions=NOTHING)

        result = await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=True,
        )

        assert result["executed_sql"], result
        assert await conn.fetchval("SELECT to_regclass('widgets') IS NULL") is True
    finally:
        await _clean(conn, "widgets")
        await conn.close()


# ---------------------------------------------------------------------------
# Enum lookup tables, which were broken on every engine until 2026-08-17
# ---------------------------------------------------------------------------

POSTS = {
    "posts": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "status": {"type": "text", "literal_values": ["draft", "live"]},
        }
    }
}


async def test_a_declared_enum_value_can_be_stored() -> None:
    """Measured broken here before the fix, exactly as on SQLite."""
    conn = await _connect()
    try:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        expanded = expand_schema_enums(POSTS)
        plan = diff({}, expanded, dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        rows = await conn.fetch(
            'SELECT value FROM "_dp_enum_posts_status" ORDER BY value'
        )
        assert [r[0] for r in rows] == ["draft", "live"]

        await conn.execute("INSERT INTO posts (id, status) VALUES ('1', 'draft')")
        assert await conn.fetchval("SELECT status FROM posts") == "draft"
    finally:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        await conn.close()


async def test_an_undeclared_enum_value_is_refused() -> None:
    """The floor. Populating the table with everything passes the test above."""
    import asyncpg

    conn = await _connect()
    try:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        plan = diff({}, expand_schema_enums(POSTS), dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        with pytest.raises(asyncpg.exceptions.ForeignKeyViolationError):
            await conn.execute(
                "INSERT INTO posts (id, status) VALUES ('2', 'pending')"
            )
    finally:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        await conn.close()


async def test_an_enum_value_containing_an_apostrophe_survives() -> None:
    """The INSERTs are built as SQL text, so the escaping is load-bearing."""
    conn = await _connect()
    try:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        schema = {
            "posts": {
                "columns": {
                    "id": {"type": "text", "primary_key": True},
                    "status": {
                        "type": "text",
                        "literal_values": ["it's live", "done"],
                    },
                }
            }
        }
        plan = diff({}, expand_schema_enums(schema), dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        rows = await conn.fetch(
            'SELECT value FROM "_dp_enum_posts_status" ORDER BY value'
        )
        assert [r[0] for r in rows] == ["done", "it's live"]
    finally:
        await _clean(conn, "posts", "_dp_enum_posts_status")
        await conn.close()


# ---------------------------------------------------------------------------
# Expression defaults, which emitted unparseable SQL on the other engines
# ---------------------------------------------------------------------------


async def test_postgresql_keeps_its_own_expression_defaults() -> None:
    """`now()` is PostgreSQL's spelling and must not be translated away.

    The translation table exists because `now()` is a syntax error on SQLite.
    Running it in the direction that needs no translation is how a fix for one
    engine breaks another.
    """
    conn = await _connect()
    try:
        await _clean(conn, "stamped")
        schema = {
            "stamped": {
                "columns": {
                    "id": {"type": "text", "primary_key": True},
                    "made_at": {"type": "timestamptz", "default": "now()"},
                }
            }
        }
        plan = diff({}, schema, dialect="postgresql", decisions=NOTHING)
        await apply(
            conn, plan["operations"], plan["execution_order"], "postgresql",
            dry_run=False,
        )

        await conn.execute("INSERT INTO stamped (id) VALUES ('1')")
        assert await conn.fetchval("SELECT made_at FROM stamped") is not None
    finally:
        await _clean(conn, "stamped")
        await conn.close()


async def test_generated_sql_is_sql_this_server_accepts() -> None:
    """`generate_sql` renders without running. The server is the judge of it."""
    conn = await _connect()
    try:
        await _clean(conn, "widgets")
        plan = diff({}, WIDGETS, dialect="postgresql", decisions=NOTHING)

        statements = generate_sql(
            plan["operations"], plan["execution_order"], "postgresql"
        )

        assert statements
        for statement in statements:
            await conn.execute(statement)
    finally:
        await _clean(conn, "widgets")
        await conn.close()


async def test_a_failed_operation_rolls_the_whole_migration_back() -> None:
    """PostgreSQL has transactional DDL, which is why the applier uses one.

    Two operations where the second cannot run: neither table may survive, or
    a failed deploy leaves a database half-migrated and the next diff computes
    from a shape nobody designed.
    """
    conn = await _connect()
    try:
        await _clean(conn, "good", "bad")
        plan = diff({}, {"good": {"columns": {"id": {"type": "text"}}}},
                    dialect="postgresql", decisions=NOTHING)
        broken = list(plan["operations"]) + [
            {
                "op": "create_table",
                "table": "bad",
                "details": {
                    "columns": {"id": {"type": "text", "references": "nowhere.id"}},
                    "primary_key": None,
                    "indexes": {},
                    "constraints": {},
                },
            }
        ]

        # NOT `pytest.raises(Exception)`. A blind catch passes when the
        # applier raises TypeError because this test built the operation
        # wrongly, which is the difference between proving a rollback and
        # proving that something went wrong somewhere.
        with pytest.raises(MigrationError):
            await apply(conn, broken, [0, 1], "postgresql", dry_run=False)

        assert await conn.fetchval(
            "SELECT to_regclass('good') IS NULL"
        ) is True, "the first table survived a failed migration"
    finally:
        await _clean(conn, "good", "bad")
        await conn.close()
