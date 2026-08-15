"""A crew larger than one, and the failure that hid inside it.

`start_crew(size=N)` for N above 1 lost every write it was given. Measured
2026-08-14, pyturso 0.7.2: at size 4 with 12 writes it hung on the first
`collect` once and returned 0 of 12 the next time.

TWO FAULTS, AND THE SECOND IS WHY THE FIRST WAS INVISIBLE.

The journal mode was negotiated on EVERY connect. The first connection to
request MVCC has to bootstrap the MV store, and that bootstrap is not
concurrency-safe, so N drainers opening at the same instant raced and most
lost. One fresh database per row, 4 concurrent opens:

    nothing prepared              file=wal   3 of 4 failed
    migrating + DDL               file=wal   4 of 4 failed
    DDL on a normal connection    file=mvcc  0 of 4 failed

Then: the drainer opened its own connection, inside its own task, and nothing
observed the task. A drainer that died at connect time was indistinguishable
from a slow queue, because `collect` waits on a future that only a live
drainer resolves. So an engine error presented as a hang.

WHAT THIS FILE ASSERTS AND WHAT IT DOES NOT. It asserts that N connections
open — the property that was broken. It does not assert throughput, and it
does not have N drainers write concurrently: pyturso is thread-per-connection
with a blocking driver, and two drainers writing one local file on macOS hit
the engine's busy-wait inside a worker thread, which never returns. That is a
property of the laptop, measured, and `test_crew.py` says so at length. The
crew's concurrency numbers come from Render and belong there.

So this is the narrow, checkable half: a crew that cannot open its connections
must say so, to the caller, before it returns.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.crew import start_crew, stop_crew
from declaro_persistum.retry import ON_CONTENTION
from declaro_persistum.turso_database import (
    migrating,
    negotiate_journal_mode,
    open_turso,
)
from declaro_persistum.write_queue import new_room

pytestmark = pytest.mark.turso

IDLE_S = 0.01


async def _migrated(tmp_path, name="t.db"):
    db = await open_turso(str(tmp_path / name), shutdown="exit_immediately")
    async with migrating(db) as conn:
        await conn.execute("CREATE TABLE t (v INTEGER)")
        await conn.commit()
    return db


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [2, 4, 8, 16])
async def test_connections_open_concurrently_on_a_database_nobody_prepared(
    tmp_path, n: int
) -> None:
    """THE REGRESSION, with nothing else in the way.

    No migration, no warm-up, no crew. Just a database straight from
    `open_turso` and N connections opening at once, which is what a crew does.
    Under the old shape each of them negotiated the journal mode on the way in
    and they raced to bootstrap the MV store: 3 of 4 failed here.

    This is the version of the assertion that bites. Routing it through
    `migrating` hides the defect, because `migrating` now restores the mode on
    exit and that restore is serial — it does the bootstrap the crew would
    otherwise race for. A test that cannot fail is worse than no test, and the
    first draft of this file could not: mutating the fix away left all seven
    passing.
    """
    db = await open_turso(str(tmp_path / f"n{n}.db"), shutdown="exit_immediately")

    conns = await asyncio.gather(
        *(db["connect"](db) for _ in range(n)), return_exceptions=True
    )
    failed = [c for c in conns if isinstance(c, BaseException)]
    try:
        assert not failed, f"{len(failed)} of {n} concurrent opens failed: {failed[0]}"
        for conn in conns:
            cursor = await conn.execute("PRAGMA journal_mode")
            assert (await cursor.fetchone())[0] == db["journal_mode"]
    finally:
        for conn in conns:
            if not isinstance(conn, BaseException):
                await db["close_connection"](conn)


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [2, 4, 8])
async def test_every_drainer_has_a_connection_before_the_crew_returns(
    tmp_path, size: int
) -> None:
    """The same property, through the door a caller actually uses."""
    db = await _migrated(tmp_path, f"t{size}.db")
    crew = await start_crew(new_room(), db, size, ON_CONTENTION, IDLE_S)
    try:
        assert crew["size"] == size
        assert len(crew["tasks"]) == size
        # A drainer that failed to open would already be done, with an
        # exception nobody read. Every one of them must be alive and waiting.
        await asyncio.sleep(0)
        assert not [t for t in crew["tasks"] if t.done()], (
            "a drainer finished before doing any work, which is what a failed "
            "open looks like from outside"
        )
    finally:
        await stop_crew(crew)


@pytest.mark.asyncio
async def test_a_crew_that_cannot_open_raises_instead_of_starting(tmp_path) -> None:
    """The shape that made the bug silent, asserted directly.

    The caller finds out. Before this, the open lived inside the drainer task
    and the caller got a running crew that would never drain.
    """
    db = await _migrated(tmp_path, "refuse.db")
    opened: list[object] = []

    async def refuse_the_third(_db):
        if len(opened) >= 2:
            raise OSError("no more connections")
        conn = await _real_connect(_db)
        opened.append(conn)
        return conn

    _real_connect = db["connect"]
    db["connect"] = refuse_the_third

    with pytest.raises(OSError, match="no more connections"):
        await start_crew(new_room(), db, 4, ON_CONTENTION, IDLE_S)


@pytest.mark.asyncio
async def test_a_refused_crew_leaves_no_connection_behind(tmp_path) -> None:
    """A partial crew must not exist, and neither must its connections."""
    db = await _migrated(tmp_path, "partial.db")
    opened: list[object] = []
    closed: list[object] = []
    real_connect, real_close = db["connect"], db["close_connection"]

    async def open_two_then_fail(_db):
        if len(opened) >= 2:
            raise OSError("no more connections")
        conn = await real_connect(_db)
        opened.append(conn)
        return conn

    async def record_close(conn):
        closed.append(conn)
        await real_close(conn)

    db["connect"] = open_two_then_fail
    db["close_connection"] = record_close

    with pytest.raises(OSError):
        await start_crew(new_room(), db, 4, ON_CONTENTION, IDLE_S)

    assert len(opened) == 2
    assert closed == opened, "the connections opened before the failure leaked"


@pytest.mark.asyncio
async def test_migrating_gives_the_journal_mode_back(tmp_path) -> None:
    """The mode is a property of the FILE, so borrowing it means returning it.

    `migrating` forces WAL for DDL, which is right and is why it exists. Left
    that way it silently undid the negotiation done at open: the Database went
    on saying mvcc while every later connection got wal, and a crew started
    after a migration raced for the MV store bootstrap all over again.
    """
    db = await open_turso(str(tmp_path / "restore.db"), shutdown="exit_immediately")
    declared = db["journal_mode"]

    async with migrating(db) as conn:
        await conn.execute("CREATE TABLE t (v INTEGER)")
        await conn.commit()
        cursor = await conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "wal", "DDL must be on WAL"

    # Asking for what the file already is returns it unchanged, so this reads
    # the file rather than setting it.
    assert await negotiate_journal_mode(db["path"], declared) == declared


@pytest.mark.asyncio
async def test_the_declared_mode_is_the_one_the_engine_granted(tmp_path) -> None:
    """Rule 14's cousin: a field that says mvcc over a wal file is a lie.

    `_open_local` used to hard-code `journal_mode="mvcc"` whether or not the
    engine agreed. The value now records what came back.
    """
    db = await open_turso(str(tmp_path / "granted.db"), shutdown="exit_immediately")
    async with migrating(db) as conn:
        cursor = await conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "wal"

    conn = await db["connect"](db)
    try:
        cursor = await conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == db["journal_mode"], (
            "the Database declares a mode the file does not have"
        )
    finally:
        await db["close_connection"](conn)
