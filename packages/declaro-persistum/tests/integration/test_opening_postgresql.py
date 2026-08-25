"""`open_postgresql`, the entry point every PostgreSQL consumer calls first.

The reachability scan found `open_postgresql` and `connect_postgresql` called
by no test. `open_sqlite` is exercised throughout the suite; the production
backend's opener was not exercised at all, which is of a piece with
PostgreSQL having had 2 marked tests in a suite of 1241 this morning.

What an opener has to get right is not the connection -- asyncpg does that --
but the `Database` it hands back. Every later call reads that record: which
dialect to spell SQL for, whether writers are serialised, what happens at
shutdown. A field that is wrong here is wrong everywhere afterwards, and
silently, because nothing downstream re-derives it.
"""

from __future__ import annotations

import os

import pytest

from declaro_persistum import close, open_postgresql, reading, writing
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.asyncio, pytest.mark.postgresql, pytest.mark.integration]

DSN = os.environ.get(
    "TEST_POSTGRESQL_URL",
    probe_url(),
)


async def _open():
    """A database, or a skip naming what could not be reached."""
    import asyncpg

    try:
        return await open_postgresql(
            DSN, shutdown="exit_immediately", busy_timeout_s=10.0
        )
    except (OSError, asyncpg.PostgresError) as exc:
        refuse_without_postgresql(DSN, exc)


async def test_it_opens_and_closes() -> None:
    """The floor, and the only test here that would pass on a broken record."""
    db = await _open()
    await close(db)


async def test_the_record_says_which_engine_it_is() -> None:
    """Everything downstream spells SQL from this field."""
    db = await _open()
    try:
        assert db["dialect"] == "postgresql"
    finally:
        await close(db)


async def test_the_journal_mode_says_what_is_true() -> None:
    """PostgreSQL has no journal mode a client sets.

    The field is required, so it says "server" rather than borrowing SQLite's
    vocabulary and claiming a WAL that is not there.
    """
    db = await _open()
    try:
        assert db["journal_mode"] == "server"
    finally:
        await close(db)


async def test_writers_are_not_serialised() -> None:
    """PostgreSQL does multi-writer MVCC server-side.

    A lock here would take away what the server provides, so this asserts the
    absence of one rather than its presence.
    """
    db = await _open()
    try:
        assert db.get("write_lock") is None, db.get("write_lock")
    finally:
        await close(db)


async def test_a_write_block_can_write() -> None:
    """`writing(db)` is how a consumer reaches the database at all."""
    db = await _open()
    try:
        async with writing(db) as conn:
            await conn.execute("DROP TABLE IF EXISTS opened CASCADE")
            await conn.execute("CREATE TABLE opened (id TEXT PRIMARY KEY)")
            await conn.execute("INSERT INTO opened VALUES ('1')")

        async with reading(db) as conn:
            assert await conn.fetchval("SELECT COUNT(*) FROM opened") == 1
    finally:
        async with writing(db) as conn:
            await conn.execute("DROP TABLE IF EXISTS opened CASCADE")
        await close(db)


async def test_a_read_block_sees_a_committed_write() -> None:
    """The two blocks are separate, so a write that never commits is invisible.

    This is what a `writing` block is FOR, and asserting it here is the
    difference between "the opener returns something" and "the record works".
    """
    db = await _open()
    try:
        async with writing(db) as conn:
            await conn.execute("DROP TABLE IF EXISTS opened CASCADE")
            await conn.execute("CREATE TABLE opened (id TEXT)")
            await conn.execute("INSERT INTO opened VALUES ('a')")

        async with reading(db) as conn:
            rows = await conn.fetch("SELECT id FROM opened")

        assert [r[0] for r in rows] == ["a"]
    finally:
        async with writing(db) as conn:
            await conn.execute("DROP TABLE IF EXISTS opened CASCADE")
        await close(db)


async def test_a_replicating_shutdown_is_refused() -> None:
    """A database with no primary cannot replicate at shutdown.

    `shutdown` is required on all three openers precisely so this is a
    decision the caller states, and PostgreSQL accepts only one answer.
    """
    with pytest.raises(ValueError) as caught:
        await open_postgresql(
            DSN, shutdown="replicate_then_exit", busy_timeout_s=10.0
        )

    assert "exit_immediately" in str(caught.value), caught.value
