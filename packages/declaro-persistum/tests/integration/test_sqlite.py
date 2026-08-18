"""SQLite, executed. The backend a developer runs before any other.

WRITTEN AGAINST A FILE THAT CONTAINED NO TESTS. It was 24 lines: a docstring,
an `aiosqlite` fixture yielding an in-memory connection, and nothing else. It
was the last entry on the hollow-file list.

SQLite is exercised by 74 marked tests elsewhere, so this is not the gap
`test_postgresql.py` was. What it adds is the WHOLE PATH in one place --
declare, migrate, introspect, diff again -- on a real file rather than
in memory, because the two differ in ways this package cares about.

ON DISK, NOT `:memory:`. The fixture that was here yielded an in-memory
connection, and several of the properties below cannot be observed there: a
file survives being closed and reopened, which is what makes the
migrate-twice test mean anything, and `PRAGMA foreign_keys` state is per
connection.

Every defect this file would have caught was found on 2026-08-17 by writing
these checks against PostgreSQL and Turso first, and then asking whether
SQLite agreed. Twice it did not, and both were real:

    a boolean default translated to 0 and read back as "0", so an
    unchanged schema migrated on every deploy

    `declaro generate -d sqlite` emitted `DEFAULT gen_random_uuid()`,
    which SQLite refuses with `near "(": syntax error`
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import apply, generate_sql
from declaro_persistum.differ import diff
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.inspector import introspect

pytestmark = [pytest.mark.asyncio, pytest.mark.sqlite, pytest.mark.integration]

WIDGETS = {
    "widgets": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "name": {"type": "text", "nullable": False},
            "count": {"type": "integer", "default": 0},
            "live": {"type": "boolean", "default": "false"},
            "made_at": {"type": "timestamptz", "default": "now()"},
        }
    }
}


@pytest.fixture
async def database(tmp_path):
    """A real file. See the module docstring for why not `:memory:`."""
    path = str(tmp_path / "integration.db")
    conn = await aiosqlite.connect(path)
    try:
        yield path, conn
    finally:
        await conn.close()


async def _migrate(conn, schema) -> None:
    plan = diff({}, schema, dialect="sqlite")
    await apply(
        conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False
    )


# ---------------------------------------------------------------------------
# Migrate, introspect, and agree with yourself
# ---------------------------------------------------------------------------


async def test_a_migration_creates_the_table_it_planned(database) -> None:
    _path, conn = database

    await _migrate(conn, WIDGETS)

    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='widgets'"
    )
    assert await cursor.fetchone() is not None


async def test_introspection_reads_back_what_was_written(database) -> None:
    _path, conn = database
    await _migrate(conn, WIDGETS)

    actual = await introspect(conn, "sqlite")

    assert "widgets" in actual, sorted(actual)
    assert set(actual["widgets"]["columns"]) == set(WIDGETS["widgets"]["columns"])


async def test_migrating_twice_is_a_no_op(database) -> None:
    """The property that makes every deployment safe to repeat.

    A boolean default broke this on 2026-08-17: declared "false", written
    `DEFAULT 0`, read back "0", and an `alter_column` proposed forever.
    """
    _path, conn = database
    await _migrate(conn, WIDGETS)

    actual = await introspect(conn, "sqlite")
    second = diff({"widgets": actual["widgets"]}, WIDGETS, dialect="sqlite")

    assert second["operations"] == [], (
        f"an unchanged schema still migrates: "
        f"{[(o['op'], o['details']) for o in second['operations']]}"
    )


async def test_the_schema_survives_closing_and_reopening(database) -> None:
    """What a file gives you that `:memory:` cannot: a second process.

    A deployment migrates, exits, and starts again. In memory the database
    dies with the connection, so a migrate-twice test there proves nothing
    about the case it exists for.
    """
    path, conn = database
    await _migrate(conn, WIDGETS)
    await conn.commit()
    await conn.close()

    reopened = await aiosqlite.connect(path)
    try:
        actual = await introspect(reopened, "sqlite")
        assert diff({"widgets": actual["widgets"]}, WIDGETS, dialect="sqlite")[
            "operations"
        ] == []
    finally:
        await reopened.close()


async def test_a_dry_run_changes_nothing(database) -> None:
    _path, conn = database
    plan = diff({}, WIDGETS, dialect="sqlite")

    result = await apply(
        conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=True
    )

    assert result["executed_sql"], result
    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='widgets'"
    )
    assert await cursor.fetchone() is None


# ---------------------------------------------------------------------------
# Defaults, which emitted unparseable SQL here until 2026-08-17
# ---------------------------------------------------------------------------


async def test_every_declared_default_produces_a_value(database) -> None:
    """`DEFAULT now()` was a syntax error on this engine and exited 0."""
    _path, conn = database
    await _migrate(conn, WIDGETS)

    await conn.execute("INSERT INTO widgets (id, name) VALUES ('1', 'a')")
    cursor = await conn.execute("SELECT count, live, made_at FROM widgets")
    count, live, made_at = await cursor.fetchone()

    assert count == 0
    assert live in (0, "false", "FALSE"), repr(live)
    assert made_at is not None, "the timestamp default produced NULL"


async def test_a_default_sqlite_cannot_spell_is_refused_not_emitted(
    database,
) -> None:
    """A refusal is better than DDL the engine rejects halfway through.

    `gen_random_uuid()` has no SQLite equivalent, and a random hex string is
    not a v4 UUID, so translating it would swap a loud error for a quiet
    wrong value.
    """
    _path, conn = database
    schema = {
        "t": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True,
                       "default": "gen_random_uuid()"}
            }
        }
    }

    with pytest.raises(Exception, match="gen_random_uuid"):
        await _migrate(conn, schema)


# ---------------------------------------------------------------------------
# Enum lookup tables, empty on every engine until 2026-08-17
# ---------------------------------------------------------------------------

POSTS = {
    "posts": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "status": {"type": "text", "literal_values": ["draft", "live"]},
        }
    }
}


async def test_a_declared_enum_value_can_be_stored(database) -> None:
    _path, conn = database
    await _migrate(conn, expand_schema_enums(POSTS))

    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("INSERT INTO posts (id, status) VALUES ('1', 'draft')")

    cursor = await conn.execute("SELECT status FROM posts")
    assert (await cursor.fetchone())[0] == "draft"


async def test_an_undeclared_enum_value_is_refused(database) -> None:
    """The floor. Filling the lookup table with everything passes above."""
    _path, conn = database
    await _migrate(conn, expand_schema_enums(POSTS))

    await conn.execute("PRAGMA foreign_keys = ON")
    with pytest.raises(aiosqlite.IntegrityError):
        await conn.execute("INSERT INTO posts (id, status) VALUES ('2', 'nope')")


# ---------------------------------------------------------------------------
# What SQLite does differently from the other two
# ---------------------------------------------------------------------------


async def test_a_migration_leaves_foreign_keys_as_it_found_them(database) -> None:
    """`PRAGMA foreign_keys` is per connection and a no-op inside a
    transaction, so a migration that toggles it has to restore it after the
    commit or the caller's setting is silently dropped.
    """
    _path, conn = database
    await conn.execute("PRAGMA foreign_keys = ON")

    await _migrate(conn, WIDGETS)

    cursor = await conn.execute("PRAGMA foreign_keys")
    assert (await cursor.fetchone())[0] == 1, (
        "the migration left foreign keys off on a connection that had them on"
    )


async def test_generated_sql_is_sql_this_engine_accepts(database) -> None:
    """`generate_sql` renders without running. The engine judges it."""
    path, _conn = database
    plan = diff({}, WIDGETS, dialect="sqlite")

    statements = generate_sql(plan["operations"], plan["execution_order"], "sqlite")

    assert statements
    fresh = await aiosqlite.connect(path)
    try:
        for statement in statements:
            await fresh.execute(statement)
    finally:
        await fresh.close()


async def test_a_failed_operation_leaves_no_half_migrated_table(database) -> None:
    """SQLite has transactional DDL, which is why the applier uses one.

    THE SECOND OPERATION HAS TO BE ONE SQLITE ACTUALLY REFUSES. My first
    version referenced a missing table, copying the PostgreSQL version of this
    test, and SQLite ACCEPTS a forward foreign-key reference at CREATE TABLE
    time -- measured -- so nothing failed and the test asserted a rollback
    that never had to happen. Two primary keys is refused:

        CREATE TABLE bad (a TEXT PRIMARY KEY, b TEXT PRIMARY KEY)
        OperationalError: table "bad" has more than one primary key
    """
    _path, conn = database
    plan = diff({}, {"good": {"columns": {"id": {"type": "text"}}}}, dialect="sqlite")
    broken = list(plan["operations"]) + [
        {
            "op": "create_table",
            "table": "bad",
            "details": {
                "columns": {
                    "a": {"type": "text", "primary_key": True},
                    "b": {"type": "text", "primary_key": True},
                },
                "primary_key": None,
                "indexes": {},
                "constraints": {},
            },
        }
    ]

    with pytest.raises(MigrationError):
        await apply(conn, broken, [0, 1], "sqlite", dry_run=False)

    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='good'"
    )
    assert await cursor.fetchone() is None, (
        "the first table survived a failed migration"
    )
