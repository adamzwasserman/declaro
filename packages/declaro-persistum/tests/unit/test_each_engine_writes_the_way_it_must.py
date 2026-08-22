"""Each engine's write takes a different transaction, and two had never run.

MEASURED 2026-08-18 with `coverage json` over the whole suite: none of the
three had ever run. My first reading of that said `turso_write_one` had, and
it was wrong: the coverage run already included this file.

The three are not spellings of one thing. Turso opens BEGIN CONCURRENT, which
is what lets a crew write in parallel; SQLite has no such statement and opens
BEGIN IMMEDIATE, which takes the write lock up front so a busy database fails
at the start rather than half way; asyncpg owns the transaction itself, takes
positional parameters, and has no `commit()` to call.

So a copy of the Turso writer used for SQLite would open a statement SQLite
does not have, and used for PostgreSQL would call a method asyncpg does not
provide. Only the one that had a test was known to work.

WHAT IS ASSERTED IS THE ROW, NOT THE CALL. Checking that `conn.execute` was
reached says nothing about whether the row is in the database, and a write
path that opens the wrong transaction can execute and roll back. Each test
reads the row afterwards through a separate connection where the engine
allows one, so an uncommitted write is not mistaken for a committed one.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from declaro_persistum.writers import (
    WRITERS,
    postgres_write_one,
    sqlite_write_one,
    turso_write_one,
)
from tests.conftest import probe_url

pytestmark = [pytest.mark.precommit]

PROBE_URL = probe_url()

CREATE = 'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)'


def test_the_table_holds_one_writer_per_dialect() -> None:
    """The floor. A missing entry would make a whole engine unwritable."""
    assert set(WRITERS) == {"sqlite", "turso", "postgresql"}, sorted(WRITERS)


def test_each_dialect_gets_its_own_writer() -> None:
    """Three engines, three transaction statements, three functions.

    A table pointing two dialects at one function would pass every behavioural
    test below on the engine that function was written for.
    """
    assert WRITERS["sqlite"] is sqlite_write_one
    assert WRITERS["turso"] is turso_write_one
    assert WRITERS["postgresql"] is postgres_write_one


async def test_sqlite_writes_a_row_that_is_still_there_afterwards() -> None:
    """BEGIN IMMEDIATE, then the write, then a commit that has to happen."""
    import aiosqlite

    path = os.path.join(tempfile.mkdtemp(), "w.db")
    connection = await aiosqlite.connect(path)
    try:
        await connection.execute(CREATE)
        await connection.commit()

        await sqlite_write_one(
            connection, 'INSERT INTO "widgets" (id, name) VALUES (?, ?)', (1, "a")
        )
    finally:
        await connection.close()

    # A SECOND CONNECTION, because the first one can see its own uncommitted
    # write and would report a rolled-back transaction as a success.
    reader = await aiosqlite.connect(path)
    try:
        cursor = await reader.execute('SELECT name FROM "widgets" WHERE id = 1')
        assert [tuple(r) for r in await cursor.fetchall()] == [("a",)]
    finally:
        await reader.close()


async def test_turso_writes_a_row_that_is_still_there_afterwards() -> None:
    """BEGIN CONCURRENT NEEDS AN MVCC DATABASE, and that is a journal mode.

    The first version of this test opened a fresh file with
    `turso.aio.connect` and got

        Transaction error: Concurrent transaction mode is only supported
        when MVCC is enabled

    which read like a defect in the writer and is not one. The mode is
    negotiated once at open by `negotiate_journal_mode`, and the writer is
    only ever handed a connection to a database that already granted it. A
    test that skips the negotiation is testing a state the package never
    produces.
    """
    import turso.aio

    from declaro_persistum.turso_database import negotiate_journal_mode

    path = os.path.join(tempfile.mkdtemp(), "w.db")
    granted = await negotiate_journal_mode(path, "mvcc")
    if granted != "mvcc":
        pytest.skip(f"this build granted {granted!r} rather than mvcc")
    connection = await turso.aio.connect(path)
    try:
        cursor = connection.cursor()
        await cursor.execute(CREATE)
        await connection.commit()

        await turso_write_one(
            connection, 'INSERT INTO "widgets" (id, name) VALUES (?, ?)', (1, "a")
        )
    finally:
        await connection.close()

    reader = await turso.aio.connect(path)
    try:
        cursor = reader.cursor()
        await cursor.execute('SELECT name FROM "widgets" WHERE id = 1')
        assert [tuple(r) for r in await cursor.fetchall()] == [("a",)]
    finally:
        await reader.close()


@pytest.mark.postgresql
@pytest.mark.integration
async def test_postgresql_writes_a_row_that_is_still_there_afterwards() -> None:
    """asyncpg owns the transaction, so there is no commit to forget."""
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")

    try:
        await connection.execute('DROP TABLE IF EXISTS "widgets" CASCADE')
        await connection.execute(CREATE)

        await postgres_write_one(
            connection,
            'INSERT INTO "widgets" (id, name) VALUES ($1, $2)',
            (1, "a"),
        )
    except BaseException:
        await connection.close()
        raise

    reader = await asyncpg.connect(PROBE_URL)
    try:
        rows = await reader.fetch('SELECT name FROM "widgets" WHERE id = 1')
        assert [tuple(r) for r in rows] == [("a",)]
    finally:
        await reader.execute('DROP TABLE IF EXISTS "widgets" CASCADE')
        await reader.close()
        await connection.close()


async def test_a_failed_sqlite_write_leaves_no_row() -> None:
    """The floor under the transaction.

    A writer that never opened one would leave the first statement of a failed
    pair committed. Here the write itself fails, so what matters is that the
    table is untouched and the connection is usable afterwards.
    """
    import aiosqlite

    path = os.path.join(tempfile.mkdtemp(), "w.db")
    connection = await aiosqlite.connect(path)
    try:
        await connection.execute(CREATE)
        await connection.commit()

        # The driver's own error type, not a bare Exception. A blind catch
        # here would pass on any failure at all, including one raised before
        # the write was attempted.
        with pytest.raises(aiosqlite.Error):
            await sqlite_write_one(
                connection,
                'INSERT INTO "no_such_table" (id) VALUES (?)',
                (1,),
            )
        await connection.rollback()

        cursor = await connection.execute('SELECT count(*) FROM "widgets"')
        assert (await cursor.fetchone())[0] == 0
    finally:
        await connection.close()
