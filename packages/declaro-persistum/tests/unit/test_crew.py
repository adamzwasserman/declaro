"""The crew, against a real local Turso database.

A caller deposits a write and gets a ticket. A crew of drainers empties the
queue, each holding ONE connection for its whole life. That connection reuse
is the larger of the two throughput levers — 6.01x on its own, 18.87x
compounded with MVCC — and a connection per write throws it away.

CREW SIZE IS 1 HERE, DELIBERATELY. What this file tests is the MACHINERY:
deposit returns a ticket at once, a drainer picks the write up, executes it on
its held connection, and the receipt comes back. None of that needs two
drainers. Size above 1 is `test_a_crew_opens_before_it_runs.py`.

"TWO DOES NOT WORK ON A LAPTOP" WAS WRITTEN HERE AND IS FALSE. It said pyturso
is thread-per-connection with a blocking driver, so two drainers writing one
local file on macOS hit the engine's busy-wait inside a worker thread and the
await never returns. That WAS the observed behaviour, and it was a symptom of
a defect rather than a property of the machine: the journal mode was
negotiated on every connect, so N drainers raced to bootstrap the MV store and
the survivors ran on WAL. Two WAL writers on one file is the busy-wait. The
negotiation now happens once, at open. Measured 2026-08-14 on this MacBook,
200 writes per run, two runs each:

    crew  1   200/200 landed   2,036 and 2,181 w/s
    crew  4   200/200 landed   2,795 and 2,706 w/s
    crew  8   200/200 landed   2,930 and 3,023 w/s
    crew 16   200/200 landed   2,836 and 5,757 w/s

Nothing lost, nothing hung, and every row on disk.

The SIZING still does not come from here. Crew 16 gave 4,721 writes/s on
Render with knees at 16 and 64 on two tiers, and a laptop number is not a
server number. What changed is that "a laptop cannot run two" is no longer a
reason for anything.

DDL goes through `migrating`, which is WAL. A table created on an MVCC
connection is invisible to every other connection.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.crew import start_crew, stop_crew
from declaro_persistum.database import reading, writing
from declaro_persistum.retry import ON_CONTENTION
from declaro_persistum.turso_database import migrating, open_turso
from declaro_persistum.write_queue import collect, deposit, new_room

pytestmark = pytest.mark.turso

CREW = 1   # machinery, not concurrency — see the module docstring
WRITES = 12



async def _write_one(conn, sql, params):
    """A writer for a test double: no engine, no transaction statement."""
    await conn.execute(sql, params)


async def _database_with_table(tmp_path):
    db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately")
    async with migrating(db) as conn:
        await conn.execute("CREATE TABLE t (v INTEGER)")
        await conn.commit()
    return db


async def _count(db) -> int:
    async with reading(db) as conn:
        cur = await conn.execute("SELECT count(*) FROM t")
        return (await cur.fetchone())[0]


@pytest.mark.asyncio
async def test_every_deposited_write_lands(tmp_path):
    """The property that matters. Not throughput — completeness."""
    db = await _database_with_table(tmp_path)
    room = new_room()
    crew = await start_crew(room, db, size=CREW, retry=ON_CONTENTION, idle_s=0.01)

    tickets = [
        deposit(room, {"sql": "INSERT INTO t VALUES (?)", "params": (i,)})
        for i in range(WRITES)
    ]
    receipts = await asyncio.gather(*(collect(room, t) for t in tickets))
    await stop_crew(crew)

    assert all(r["ok"] for r in receipts), [r for r in receipts if not r["ok"]]
    assert await _count(db) == WRITES


@pytest.mark.asyncio
async def test_a_ticket_comes_back_before_the_write_is_durable(tmp_path):
    """`deposit` returns immediately. That is what makes the queue async.

    If deposit blocked until the write landed there would be no queue, only a
    slower `writing(db)`.
    """
    db = await _database_with_table(tmp_path)
    room = new_room()
    crew = await start_crew(room, db, size=CREW, retry=ON_CONTENTION, idle_s=0.01)

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (?)", "params": (1,)})
    assert isinstance(ticket, str) and ticket

    receipt = await collect(room, ticket)
    assert receipt["ok"] is True
    await stop_crew(crew)


@pytest.mark.asyncio
async def test_an_empty_queue_does_not_spin(tmp_path):
    """A crew with nothing to do must idle, and must still stop promptly."""
    db = await _database_with_table(tmp_path)
    room = new_room()
    crew = await start_crew(room, db, size=CREW, retry=ON_CONTENTION, idle_s=0.01)

    await asyncio.sleep(0.05)
    await asyncio.wait_for(stop_crew(crew), timeout=2.0)
    assert await _count(db) == 0


@pytest.mark.asyncio
async def test_a_crew_refuses_a_replicated_database(tmp_path):
    """The rule, enforced rather than documented.

    A replicated database gets no write concurrency from either journal mode,
    so N drainers there would queue behind one lock while looking like
    parallelism.
    """
    from declaro_persistum.database import new_database

    async def unused(*a, **k):
        raise AssertionError("a refused crew must not touch the database")

    replicated = new_database(
        path=str(tmp_path / "r.db"),
        dialect="sqlite",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary="https://example.turso.io",
        token="t",
        connect=unused,
        close_connection=unused,
        for_ddl=writing,
        serialise=asyncio.Lock(),
        replicate_once=unused,
        refresh_once=unused,
        release=unused,
        sleep=asyncio.sleep,
        retry_delay_s=0.1,
        shutdown="exit_immediately",
        write_one=_write_one,
    )

    with pytest.raises(ValueError, match="local databases only"):
        await start_crew(
            new_room(), replicated, size=2, retry=ON_CONTENTION, idle_s=0.01
        )
