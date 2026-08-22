"""The three appliers answer a failure differently, and only one was tested.

Each declares its own transaction mode and means it:

    sqlite      all_or_nothing    one failure rolls the batch back and raises
    turso       per_operation     a refused operation is skipped, batch goes on
    postgresql  all_or_nothing    one failure raises

MEASURED 2026-08-19 with `coverage json`. The Turso path was covered the day
before this file; the SQLite and PostgreSQL `except` blocks had never run, so
the behaviour that distinguishes them from Turso was asserted nowhere.

WHY THEY DIFFER AT ALL, which is the part a reader needs. Turso is the engine
that cannot do several things the differ emits -- no ADD CONSTRAINT, no DROP
CONSTRAINT -- so refusing the whole batch for one of them would block every
other change forever. SQLite can do more and PostgreSQL can do all of it, so
a failure there means something is wrong rather than unsupported, and stopping
is the right answer.

THE CONSEQUENCE IS THE STAMP. A caller that reads success writes the schema
hash, and a hash written over a partial migration is never re-proposed. So
"partial" must be reachable only where it is deliberate, and it must never be
spelled `success`.

The failures are real. A batch is made to fail by asking each engine for
something it refuses, and nothing is patched.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.applier import postgresql as pg_applier
from declaro_persistum.applier import sqlite as sqlite_applier
from declaro_persistum.applier import turso as turso_applier
from declaro_persistum.exceptions import MigrationError
from tests.conftest import probe_url

pytestmark = [pytest.mark.precommit]

PROBE_URL = probe_url()

MODES = {
    "sqlite": "all_or_nothing",
    "turso": "per_operation",
    "postgresql": "all_or_nothing",
}


def _create(table: str) -> dict[str, Any]:
    return {
        "op": "create_table",
        "table": table,
        "details": {
            "columns": {"id": {"type": "integer", "primary_key": True}},
            "primary_key": None,
            "indexes": {},
            "constraints": {},
        },
    }


async def _sqlite() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "e.db"))


async def _turso() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "e.db"))


async def _postgresql() -> Any:
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    for table in ("first", "second"):
        await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    return connection


CONNECT = {"sqlite": _sqlite, "turso": _turso, "postgresql": _postgresql}
DIALECTS = sorted(CONNECT)


async def _done(connection: Any, dialect: str) -> None:
    if dialect == "postgresql":
        for table in ("first", "second"):
            await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    await connection.close()


async def _apply(connection: Any, dialect: str, operations: list[dict]) -> Any:
    return await apply(
        connection,
        operations,
        list(range(len(operations))),
        dialect,
        dry_run=False,
    )


@pytest.mark.parametrize("dialect", DIALECTS)
def test_each_module_declares_the_mode_it_behaves_as(dialect: str) -> None:
    """The declaration and the behaviour are checked together, below.

    A module whose declared mode disagreed with what it does would mislead
    every reader of the dispatch table.
    """
    module = {
        "sqlite": sqlite_applier,
        "turso": turso_applier,
        "postgresql": pg_applier,
    }[dialect]

    assert module.get_transaction_mode() == MODES[dialect]
    assert module.get_dialect() == dialect


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_batch_of_good_operations_succeeds(dialect: str) -> None:
    """The control on every test below."""
    connection = await CONNECT[dialect]()
    try:
        result = await _apply(
            connection, dialect, [_create("first"), _create("second")]
        )

        assert result["success"] is True, result
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_a_failure_is_never_reported_as_success(dialect: str) -> None:
    """The one thing all three must agree on.

    A caller reading `success` writes the schema hash, and a hash written over
    a migration that did not finish is never re-proposed.
    """
    connection = await CONNECT[dialect]()
    try:
        await _apply(connection, dialect, [_create("first")])

        # The same table again. Every engine refuses it.
        try:
            result = await _apply(connection, dialect, [_create("first")])
        except MigrationError:
            return

        assert result["success"] is False, result
    finally:
        await _done(connection, dialect)


@pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
async def test_an_all_or_nothing_applier_raises(dialect: str) -> None:
    """It does not hand back a result saying part of the batch worked."""
    connection = await CONNECT[dialect]()
    try:
        await _apply(connection, dialect, [_create("first")])

        with pytest.raises(MigrationError) as caught:
            await _apply(connection, dialect, [_create("first")])

        assert "first" in str(caught.value), caught.value
    finally:
        await _done(connection, dialect)


async def test_an_all_or_nothing_applier_leaves_nothing_behind() -> None:
    """The rollback, which is what `all_or_nothing` means.

    The second operation of the batch fails, so the first must be gone too.

    THIS DOES NOT DISTINGUISH THE ROLLBACK CALL FROM SQLITE'S OWN BEHAVIOUR,
    and the difference is worth stating rather than implying. Substituting
    `commit()` for `rollback()` in the applier's failure block leaves the
    outcome identical, measured directly: the table created before the
    failure is gone either way, because SQLite discards a transaction its own
    statement aborted. The explicit rollback is belt and braces on this engine
    for this failure shape, and no test here can tell it from the engine.

    A control mutating that line also has to hit the RIGHT line. `sqlite.py`
    has four `await connection.rollback()` calls in three different `except`
    blocks, and the first attempt at this control replaced the first one,
    which is in the bug-re-raise path that this test never reaches.
    """
    import aiosqlite

    path = os.path.join(tempfile.mkdtemp(), "r.db")
    connection = await aiosqlite.connect(path)
    try:
        await _apply(connection, "sqlite", [_create("first")])

        with pytest.raises(MigrationError):
            await _apply(
                connection, "sqlite", [_create("second"), _create("first")]
            )
    finally:
        await connection.close()

    reader = await aiosqlite.connect(path)
    try:
        cursor = await reader.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='second'"
        )
        assert not await cursor.fetchall(), (
            "the operation before the failure survived the rollback"
        )
    finally:
        await reader.close()


async def test_the_per_operation_applier_keeps_going() -> None:
    """Turso, and the reason it differs.

    It cannot do several things the differ emits, so refusing a whole batch
    for one of them would block every other change forever.
    """
    connection = await _turso()
    try:
        result = await _apply(
            connection,
            "turso",
            [
                _create("first"),
                {
                    "op": "add_constraint",
                    "table": "first",
                    "details": {
                        "constraint": "ck_id",
                        "definition": {"type": "check", "expression": "id > 0"},
                    },
                },
                _create("second"),
            ],
        )

        cursor = connection.cursor()
        await cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        made = [row[0] for row in await cursor.fetchall()]

        assert "second" in made, made
        assert result["success"] is False, result
    finally:
        await connection.close()
