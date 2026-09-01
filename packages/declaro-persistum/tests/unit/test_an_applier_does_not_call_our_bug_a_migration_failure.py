"""A malformed operation is not a migration that failed.

Both appliers wrap every per-operation exception as

    MigrationError("Failed to execute operation", operation=..., original_error=...)

which is right when the engine refuses the statement: the table, the SQL and
the driver's own error are all in there.

IT IS WRONG WHEN THE OPERATION ITSELF IS MALFORMED. Measured 2026-08-17 with a
hand-built operation missing its `changes` key:

    RAISED MigrationError: Failed to execute operation
      cause: KeyError: 'changes'

The database was never asked anything. `apply_reconstruction_changes` reached
for a key that was not there, and the report says the migration failed. An
operator reads "failed to execute operation", opens the database, and finds
nothing wrong with it.

A `KeyError` here means the operation handed to the applier does not match what
the differ emits, which is a bug in whoever built it. That is worth telling
apart from "the engine refused this DDL", because the two are fixed in
completely different places.

THIS IS THE FIFTH PLACE TODAY WITH THIS SHAPE. `bulk_transfer`, the foreign key
toggle, the replication push and the write queue all absorbed `Exception` in a
per-item loop, which is right for one item the engine refuses and wrong for a
bug of ours that fails all of them.
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.differ import diff
from declaro_persistum.exceptions import MigrationError

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


async def _seeded(path):
    conn = await aiosqlite.connect(path)
    await conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, note TEXT)")
    await conn.execute("INSERT INTO child VALUES (1, 'x')")
    await conn.commit()
    return conn


def _real_ops():
    """From the differ, which is the only thing that builds these correctly."""
    actual = {
        "child": {
            "columns": {
                "id": {"type": "INTEGER", "primary_key": True},
                "note": {"type": "TEXT", "nullable": True},
            }
        }
    }
    target = {
        "child": {
            "columns": {
                "id": {"type": "INTEGER", "primary_key": True},
                "note": {"type": "TEXT", "nullable": False},
            }
        }
    }
    return [
        o
        for o in diff(actual, target, engine="sqlite", decisions={})["operations"]
        if o["op"] == "alter_column"
    ]


async def test_a_real_operation_still_applies(tmp_path) -> None:
    """The baseline. Every refusal below must not cost this."""
    conn = await _seeded(tmp_path / "ok.db")
    try:
        ops = _real_ops()
        result = await apply_operations(conn, ops, list(range(len(ops))), "sqlite", dry_run=False)
        assert result["success"] is True, result.get("error")
    finally:
        await conn.close()


async def test_a_malformed_operation_is_not_reported_as_a_failed_migration(
    tmp_path,
) -> None:
    """The measured misattribution, in one assertion.

    `details` with no `changes`, which is what a hand-built operation looks
    like. The differ emits `{"column": ..., "changes": {...}}`.
    """
    conn = await _seeded(tmp_path / "bad.db")
    try:
        malformed = [
            {
                "op": "alter_column",
                "table": "child",
                "details": {"column": "note", "nullable": False},
            }
        ]

        with pytest.raises(KeyError):
            await apply_operations(conn, malformed, [0], "sqlite", dry_run=False)
    finally:
        await conn.close()


async def test_an_engine_refusal_is_still_a_migration_failure(tmp_path) -> None:
    """The behaviour that must survive, and the reason the wrapper exists.

    An unparseable type is a well-formed operation the engine refuses. That is
    a migration failure and the report should carry the operation and the
    driver's error.
    """
    conn = await _seeded(tmp_path / "refuse.db")
    try:
        refused = [
            {
                "op": "alter_column",
                "table": "child",
                "details": {
                    "column": "note",
                    "changes": {"type": {"from": "TEXT", "to": "BAD TYPE ("}},
                },
            }
        ]

        with pytest.raises(MigrationError) as caught:
            await apply_operations(conn, refused, [0], "sqlite", dry_run=False)

        assert caught.value.__cause__ is not None, (
            "the driver's own error was discarded, so the report cannot say "
            "what the engine objected to"
        )
        assert not isinstance(caught.value.__cause__, KeyError), (
            "a KeyError reached the wrapper, which means the operation was "
            "malformed rather than refused"
        )
    finally:
        await conn.close()


async def test_the_rollback_still_happens_when_our_bug_escapes(tmp_path) -> None:
    """Raising must not leave a transaction open behind it.

    The malformed operation is detected mid-flight, so whatever it had already
    done has to be undone before the exception leaves.
    """
    conn = await _seeded(tmp_path / "rb.db")
    try:
        malformed = [
            {
                "op": "alter_column",
                "table": "child",
                "details": {"column": "note", "nullable": False},
            }
        ]

        with pytest.raises(KeyError):
            await apply_operations(conn, malformed, [0], "sqlite", dry_run=False)

        cursor = await conn.execute("SELECT id, note FROM child")
        assert await cursor.fetchall() == [(1, "x")], (
            "the table changed despite the operation never running"
        )
    finally:
        await conn.close()
