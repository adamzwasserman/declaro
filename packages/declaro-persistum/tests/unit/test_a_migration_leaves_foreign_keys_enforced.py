"""A migration must hand back a connection that still enforces its constraints.

MEASURED 2026-08-17 through `applier.apply`, which is the path `declaro apply`
takes:

    before apply       PRAGMA foreign_keys = 1
    apply succeeds     success=True
    after apply        PRAGMA foreign_keys = 0

A reconstruction turns enforcement off so it can drop a table other rows still
reference. It is supposed to put it back, and it cannot: SQLite IGNORES the
pragma while a transaction is open, and it does not raise, so the `try/except`
around it catches nothing and the code reads as if it worked.

    inside an open transaction   PRAGMA foreign_keys = ON  ->  still 0
    after the transaction ends   ->  1

`reconstruct_table` runs inside the applier's transaction and must not commit,
because the applier commits once for all operations. So the restore belongs to
whoever commits, and that is the applier.

`applier/turso.py` already got this right on its failure path: it rolls back
first and then restores, so its pragma lands. The success path in both appliers
did not.

WHAT IT COSTS. A long-lived connection that has run one migration accepts every
later write against constraints nobody is checking, silently. The migration
reports success, because it succeeded.
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.differ import diff
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

ACTUAL = {
    "child": {
        "columns": {
            "id": {"type": "INTEGER", "primary_key": True},
            "pid": {"type": "INTEGER"},
            "note": {"type": "TEXT", "nullable": True},
        }
    }
}
TARGET = {
    "child": {
        "columns": {
            "id": {"type": "INTEGER", "primary_key": True},
            "pid": {"type": "INTEGER"},
            "note": {"type": "TEXT", "nullable": False},
        }
    }
}


async def _enforcement(conn) -> int:
    cursor = await conn.execute("PRAGMA foreign_keys")
    return (await cursor.fetchone())[0]


async def _seeded(path):
    conn = await aiosqlite.connect(path)
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    await conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id), note TEXT)"
    )
    await conn.execute("INSERT INTO parent VALUES (1)")
    await conn.execute("INSERT INTO child VALUES (1, 1, 'x')")
    await conn.commit()
    return conn


def _reconstruction_ops():
    """From the differ, not by hand.

    A hand-built operation is how this investigation wasted a run: mine said
    `details: {"column": ..., "nullable": False}` and the real shape is
    `details: {"column": ..., "changes": {...}}`, so the applier raised
    `KeyError: 'changes'` and reported it as "Failed to execute operation".
    """
    plan = diff(ACTUAL, TARGET, engine="sqlite", decisions={})
    ops = [o for o in plan["operations"] if o["op"] == "alter_column"]
    assert ops, "the differ no longer emits alter_column for a nullability change"
    return ops


async def test_a_successful_migration_leaves_enforcement_on(tmp_path) -> None:
    """The defect, in one assertion."""
    conn = await _seeded(tmp_path / "m.db")
    try:
        assert await _enforcement(conn) == 1, "the fixture did not start enforcing"

        ops = _reconstruction_ops()
        result = await apply_operations(conn, ops, list(range(len(ops))), "sqlite", dry_run=False)
        assert result["success"] is True, result.get("error")

        assert await _enforcement(conn) == 1, (
            "the migration succeeded and left foreign key enforcement OFF. "
            "Every later write on this connection is accepted against "
            "constraints nobody is checking, and nothing logged, because the "
            "pragma does not raise when a transaction causes it to be ignored."
        )
    finally:
        await conn.close()


async def test_the_data_survives_the_migration(tmp_path) -> None:
    """The restore must not be bought by breaking the thing being restored."""
    conn = await _seeded(tmp_path / "d.db")
    try:
        ops = _reconstruction_ops()
        await apply_operations(conn, ops, list(range(len(ops))), "sqlite", dry_run=False)

        cursor = await conn.execute("SELECT id, pid, note FROM child")
        assert await cursor.fetchall() == [(1, 1, "x")]
    finally:
        await conn.close()


async def test_enforcement_is_real_after_the_migration(tmp_path) -> None:
    """Reading the pragma is not the same as the engine honoring it.

    A restore that sets the flag without the engine acting on it would satisfy
    the test above and nothing else. This writes a row whose parent does not
    exist and requires the engine to refuse it.
    """
    conn = await _seeded(tmp_path / "e.db")
    try:
        ops = _reconstruction_ops()
        await apply_operations(conn, ops, list(range(len(ops))), "sqlite", dry_run=False)

        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute("INSERT INTO child VALUES (99, 12345, 'orphan')")
            await conn.commit()
    finally:
        await conn.close()


async def test_a_connection_that_was_not_enforcing_is_left_alone(tmp_path) -> None:
    """Restoring means putting it back, not turning it on.

    A caller who deliberately disabled enforcement before migrating has not
    asked for it to be switched on underneath them.
    """
    conn = await _seeded(tmp_path / "off.db")
    try:
        await conn.execute("PRAGMA foreign_keys = OFF")
        assert await _enforcement(conn) == 0

        ops = _reconstruction_ops()
        await apply_operations(conn, ops, list(range(len(ops))), "sqlite", dry_run=False)

        assert await _enforcement(conn) == 0, (
            "the migration switched enforcement on for a caller who had turned it off"
        )
    finally:
        await conn.close()


async def test_a_failed_migration_also_leaves_enforcement_as_it_found_it(
    tmp_path,
) -> None:
    """The caller catches the error and carries on. Its connection must be sound.

    A reconstruction that dies part-way has already turned enforcement off, and
    the rollback does not put it back. Without this, a handled migration
    failure silently converts the connection into one that checks nothing.
    """
    from declaro_persistum.exceptions import MigrationError

    conn = await _seeded(tmp_path / "fail.db")
    try:
        assert await _enforcement(conn) == 1

        # AN INVALID TYPE, NOT AN UNKNOWN COLUMN, and the difference decides
        # whether this test asserts anything. My first version named a column
        # the table does not have, and the reconstruction refuses that BEFORE
        # it ever turns enforcement off. Traced: `before recon 1, after recon
        # 1, final 1`. Removing the restore entirely changed nothing and the
        # control could not fire.
        #
        # An unparseable type fails at the CREATE TABLE, which is after the
        # disable, so the restore is the only thing that puts it back.
        bad = [
            {
                "op": "alter_column",
                "table": "child",
                "details": {
                    "column": "note",
                    "changes": {"type": {"from": "TEXT", "to": "BAD TYPE ("}},
                },
            }
        ]

        with pytest.raises(MigrationError):
            await apply_operations(conn, bad, [0], "sqlite", dry_run=False)

        assert await _enforcement(conn) == 1, (
            "a failed migration left enforcement off, so a caller that handles "
            "the error and continues writes against unchecked constraints"
        )
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Turso, which had the placement right and the meaning wrong
# ---------------------------------------------------------------------------
#
# `applier/turso.py` restores AFTER its commit, which is correct and is why its
# pragma lands where sqlite's did not. But it wrote `PRAGMA foreign_keys = ON`
# unconditionally, so a caller who had enforcement off got it switched on.
#
# MEASURED 2026-08-17 against a real local Turso database:
#
#     started 1 -> success=True -> ended 1
#     started 0 -> success=True -> ended 1   <- not what the caller had
#
# Less dangerous than sqlite's old behavior, which ended at 0 and accepted
# unchecked writes. Still wrong: a caller who disabled enforcement to do a bulk
# load finds it back on underneath them.


async def _turso_seeded(tmp_path):
    """A local Turso database with the tables, but no pragma set yet.

    THE PRAGMA IS SET IN THE SAME BLOCK IT IS READ IN. `writing(db)` yields a
    connection and `PRAGMA foreign_keys` is per-connection, so setting it in one
    block and reading it in the next reads a different connection's default. My
    first version did that and reported "the fixture did not take".
    """
    from declaro_persistum import open_turso, writing

    db = await open_turso(
        str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING
    )
    async with writing(db) as conn:
        await conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        await conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, "
            "pid INTEGER REFERENCES parent(id), note TEXT)"
        )
        await conn.execute("INSERT INTO parent VALUES (1)")
        await conn.execute("INSERT INTO child VALUES (1, 1, 'x')")
        await conn.commit()
    return db


@pytest.mark.turso
@pytest.mark.parametrize("start_enforcing", [True, False])
async def test_turso_leaves_enforcement_as_it_found_it(tmp_path, start_enforcing: bool) -> None:
    """Both directions, because forcing ON passes a one-direction test."""
    from declaro_persistum import close as close_db
    from declaro_persistum import writing

    expected = 1 if start_enforcing else 0
    db = await _turso_seeded(tmp_path)
    try:
        async with writing(db) as conn:
            await conn.execute(f"PRAGMA foreign_keys = {'ON' if start_enforcing else 'OFF'}")
            assert await _enforcement(conn) == expected, "the fixture did not take"

            ops = [
                o
                for o in diff(ACTUAL, TARGET, engine="turso", decisions={})["operations"]
                if o["op"] == "alter_column"
            ]
            result = await apply_operations(
                conn, ops, list(range(len(ops))), "turso", dry_run=False
            )
            assert result["success"] is True, result.get("error")

            assert await _enforcement(conn) == expected, (
                f"the migration changed enforcement from {expected} to "
                f"{await _enforcement(conn)}. Restoring means putting it back, "
                f"not turning it on."
            )
    finally:
        await close_db(db)
