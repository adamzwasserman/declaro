"""Restoring `PRAGMA foreign_keys` only works outside a transaction.

MEASURED 2026-08-17 on aiosqlite:

    inside an open transaction        PRAGMA foreign_keys = ON  -> still 0
    after rollback, then ON           -> 1

SQLite ignores the pragma while a transaction is open. It does not raise, so a
`try/except` around it catches nothing and the code reads as if it worked.

THREE PLACES RESTORE IT AND ONE OF THEM IS RIGHT. `applier/turso.py` rolls back
first and then restores, so its pragma lands. `abstractions/reconstruction.py`
and `abstractions/table_reconstruction.py` restore with the transaction still
open, on BOTH the success and the failure path, so theirs never land.

WHAT THAT COSTS. `reconstruct_table` turns enforcement off to drop a table
other rows still reference, and is supposed to put it back. It does not. The
connection is handed back enforcing nothing, and every later write on it is
accepted against constraints nobody is checking. Nothing logs, because nothing
failed.

The fix is not a better handler. The restore has to happen where it can take
effect, which is after the transaction ends.
"""

from __future__ import annotations

import pathlib

import aiosqlite
import pytest

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


async def _enforcement(conn) -> int:
    cursor = await conn.execute("PRAGMA foreign_keys")
    return (await cursor.fetchone())[0]


async def _seeded(path: pathlib.Path):
    conn = await aiosqlite.connect(path)
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    await conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER "
        "REFERENCES parent(id), note TEXT)"
    )
    await conn.execute("INSERT INTO parent VALUES (1)")
    await conn.execute("INSERT INTO child VALUES (1, 1, 'x')")
    await conn.commit()
    return conn


# ---------------------------------------------------------------------------
# The engine's rule, pinned so the fix rests on a measurement
# ---------------------------------------------------------------------------


async def test_the_pragma_is_ignored_inside_a_transaction(tmp_path) -> None:
    conn = await _seeded(tmp_path / "rule.db")
    try:
        await conn.execute("PRAGMA foreign_keys = OFF")
        await conn.execute("BEGIN")
        await conn.execute("INSERT INTO child VALUES (2, 1, 'y')")

        await conn.execute("PRAGMA foreign_keys = ON")

        assert await _enforcement(conn) == 0, (
            "SQLite now honours the pragma inside a transaction. Every "
            "restore in this package can be simplified; re-measure first."
        )
    finally:
        await conn.close()


async def test_the_pragma_lands_once_the_transaction_ends(tmp_path) -> None:
    conn = await _seeded(tmp_path / "rule2.db")
    try:
        await conn.execute("PRAGMA foreign_keys = OFF")
        await conn.execute("BEGIN")
        await conn.execute("INSERT INTO child VALUES (2, 1, 'y')")
        await conn.rollback()

        await conn.execute("PRAGMA foreign_keys = ON")

        assert await _enforcement(conn) == 1
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# What the package does with that rule
# ---------------------------------------------------------------------------


async def test_a_reconstruction_leaves_the_restore_to_whoever_commits(
    tmp_path,
) -> None:
    """`reconstruct_table` does NOT restore, and that is now the contract.

    It used to try, with `PRAGMA foreign_keys = ON` inside the caller's open
    transaction, where SQLite ignores it and does not raise. The line read as
    though it worked and never did.

    It cannot restore. Committing here would break the applier's
    all-or-nothing transaction, and the pragma needs the transaction closed.
    So the restore belongs to whoever commits, and
    `test_a_migration_leaves_foreign_keys_enforced.py` holds the appliers to
    it.

    A caller using this function directly owns the restore. This test states
    that plainly rather than leaving them to discover it from a connection
    that quietly stopped checking anything.
    """
    from declaro_persistum.abstractions.table_reconstruction import reconstruct_table

    conn = await _seeded(tmp_path / "recon.db")
    try:
        assert await _enforcement(conn) == 1, "the fixture did not start enforcing"

        await reconstruct_table(
            conn,
            "child",
            {
                "id": {"type": "INTEGER", "primary_key": True},
                "pid": {"type": "INTEGER"},
                "note": {"type": "TEXT", "nullable": False},
            },
        )

        assert await _enforcement(conn) == 0, (
            "`reconstruct_table` restored enforcement on its own. If it can "
            "now do that, the appliers' post-commit restore is redundant and "
            "this comment is out of date -- check both before removing either."
        )

        # And the caller, who owns the transaction, can put it back.
        await conn.commit()
        await conn.execute("PRAGMA foreign_keys = ON")
        assert await _enforcement(conn) == 1, (
            "even the transaction owner cannot restore it, which would mean "
            "the rule measured at the top of this file has changed"
        )
    finally:
        await conn.close()


async def test_a_failed_reconstruction_also_leaves_enforcement_on(
    tmp_path,
) -> None:
    """The failure path restores too, and inside the transaction as well.

    A caller that catches the reconstruction error and carries on is exactly
    the caller this matters for.
    """
    from declaro_persistum.abstractions.table_reconstruction import reconstruct_table

    conn = await _seeded(tmp_path / "recon_fail.db")
    try:
        with pytest.raises(Exception):  # noqa: B017 - any failure will do here
            await reconstruct_table(
                conn,
                "child",
                {"id": {"type": "NOT A REAL TYPE ("}},
            )

        assert await _enforcement(conn) == 1, (
            "a failed reconstruction left enforcement off, so a caller that "
            "handles the error and continues writes against unchecked "
            "constraints"
        )
    finally:
        await conn.close()


async def test_a_reconstruction_that_managed_nothing_changes_nothing(
    tmp_path,
) -> None:
    """`manage_foreign_keys=False` means the caller owns the pragma.

    It must be left exactly as it was found, off included.
    """
    from declaro_persistum.abstractions.table_reconstruction import reconstruct_table

    conn = await _seeded(tmp_path / "unmanaged.db")
    try:
        await conn.execute("PRAGMA foreign_keys = OFF")
        assert await _enforcement(conn) == 0

        await reconstruct_table(
            conn,
            "child",
            {
                "id": {"type": "INTEGER", "primary_key": True},
                "pid": {"type": "INTEGER"},
                "note": {"type": "TEXT"},
            },
            manage_foreign_keys=False,
        )

        assert await _enforcement(conn) == 0, (
            "the reconstruction turned enforcement on despite being told not "
            "to manage it"
        )
    finally:
        await conn.close()
