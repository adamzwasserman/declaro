"""Eight concurrent writes on a real cloud replica all reach the primary.

THE DEFECT THIS COVERS, tracked as declaro-p39 and declaro-eer. Concurrent
writers on a synced replica used to strand: 17 writes reported ok, 15 were
durable locally, 4 reached the primary, and it plateaued at 4 with no
convergence after 348 seconds. A separate run reported 8 ok, kept 6, and
raised nothing at all -- silent loss before any push.

WHY IT HAPPENED, from Turso's own COMPAT.md rather than inference: under
experimental MVCC every statement on a connection shares one transaction, so
a write that finishes while a sibling is still running defers its commit, and
a write that reported success is silently rolled back if the transaction then
ends abnormally. Concurrent writers are sibling statements. The package now
opens one connection and serialises writes through it, and MVCC is local-only.

NOTHING COVERED THIS AND THE SUITE WAS GREEN ANYWAY, which is declaro-06c:
every remote_url in tests/ was the fake https://example.turso.io. A stranding
defect against a fake primary cannot strand.

SO THIS READS THE PRIMARY, NOT THE REPLICA. Counting rows in the replica that
wrote them proves only that the writes were durable locally, which was true
in every failing run above. A second, empty replica is opened afterwards and
sees only what actually crossed.

Measured by hand before it was written: 8 local, 8 on the primary.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile

import pytest

pytestmark = [pytest.mark.network, pytest.mark.turso]

TABLE = "_test_concurrent_writes_reach"
WRITERS = 8


def _the_primary() -> tuple[str, str]:
    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    assert url and token, "TEST_TURSO_URL and TEST_TURSO_AUTH_TOKEN must be set"
    return url, token


def _a_fresh_replica_path() -> str:
    return str(pathlib.Path(tempfile.mkdtemp()) / "app.db")


async def test_every_concurrent_write_crosses_to_the_primary() -> None:
    """Eight overlapping writes, and the primary ends up with all eight."""
    from declaro_persistum import close, open_turso
    from declaro_persistum.database import writing

    url, token = _the_primary()
    db = await open_turso(
        _a_fresh_replica_path(), primary=url, token=token, shutdown="exit_immediately"
    )
    peak = 0
    try:
        async with writing(db) as conn:
            await conn.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            await conn.execute(f'CREATE TABLE "{TABLE}" (id INTEGER PRIMARY KEY)')

        # WRITERS IN FLIGHT, COUNTED BY THIS TEST. It used to count writers
        # blocked on the serialise lock, which proved overlap only while the
        # lock existed and made the test unable to survive its removal. A test
        # that can only pass under the design it was written beside is pinning
        # that design rather than measuring anything.
        in_flight = 0

        async def one(i: int) -> None:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            try:
                async with writing(db) as conn:
                    await conn.execute(f'INSERT INTO "{TABLE}" (id) VALUES (?)', (i,))
            finally:
                in_flight -= 1

        await asyncio.gather(*(one(i) for i in range(WRITERS)))

        async with writing(db) as conn:
            cursor = await conn.execute(f'SELECT count(*) FROM "{TABLE}"')
            local = (await cursor.fetchone())[0]
    finally:
        # `close` flushes, retrying until the primary takes everything.
        await close(db)

    # THE WRITES HAVE TO HAVE OVERLAPPED or this measures nothing. Eight
    # writes that politely queued would reach the primary under the broken
    # design too.
    assert peak > 1, (
        f"only {peak} write was ever in flight, so these {WRITERS} writes did "
        f"not overlap and this test proves nothing about concurrency"
    )
    assert local == WRITERS, f"{local} of {WRITERS} writes were durable even locally"

    reader = await open_turso(
        _a_fresh_replica_path(), primary=url, token=token, shutdown="exit_immediately"
    )
    try:
        async with writing(reader) as conn:
            cursor = await conn.execute(f'SELECT count(*) FROM "{TABLE}"')
            crossed = (await cursor.fetchone())[0]
            await conn.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
    finally:
        await close(reader)

    assert crossed == WRITERS, (
        f"{crossed} of {WRITERS} concurrent writes reached the primary. "
        f"{local} were durable locally, so the rest are stranded: reported "
        f"successful, held on a replica, and never crossing."
    )


async def test_a_replica_that_never_pushed_leaves_the_primary_empty() -> None:
    """The companion. Without it, "8 of 8 crossed" cannot be told from "the
    reader was looking at the wrong copy".

    The test above passes if the second replica reads the primary, and it also
    passes if the second replica somehow reads local state. Those are the same
    green. So this writes eight rows, releases the connection WITHOUT flushing,
    and asserts the primary never heard of the table.

    Measured: 8 rows durable in the writing replica, table absent on the
    primary. That is the stranding shape declaro-p39 recorded, produced here
    on purpose.
    """
    from declaro_persistum import close, open_turso
    from declaro_persistum.database import writing

    url, token = _the_primary()
    unpushed = "_test_never_pushed"
    db = await open_turso(
        _a_fresh_replica_path(), primary=url, token=token, shutdown="exit_immediately"
    )
    try:
        async with writing(db) as conn:
            await conn.execute(f'CREATE TABLE IF NOT EXISTS "{unpushed}" (id INTEGER PRIMARY KEY)')
            for i in range(WRITERS):
                await conn.execute(f'INSERT INTO "{unpushed}" (id) VALUES (?)', (i,))
            cursor = await conn.execute(f'SELECT count(*) FROM "{unpushed}"')
            local = (await cursor.fetchone())[0]
    finally:
        # NOT `close`, which flushes. Releasing directly is the whole point:
        # these writes are durable locally and have never crossed.
        await db["release"](db)

    assert local == WRITERS, f"only {local} of {WRITERS} writes were durable locally"

    reader = await open_turso(
        _a_fresh_replica_path(), primary=url, token=token, shutdown="exit_immediately"
    )
    try:
        async with writing(reader) as conn:
            cursor = await conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE name = ?", (unpushed,)
            )
            on_the_primary = (await cursor.fetchone())[0]
    finally:
        await close(reader)

    assert on_the_primary == 0, (
        "the primary already has a table that was never pushed, so the reader "
        "in the test above is not reading the primary and its result means nothing"
    )
