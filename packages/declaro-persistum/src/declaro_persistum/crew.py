"""The crew: N drainers over one queue, each holding its own connection.

This is where local write concurrency comes from, and it is the third time the
package has needed the lesson written down:

    declaro-dna    a connection per write on a replicated database took a
                   consumer's box down at 20 concurrent signups
    the replicated path   fixed by holding ONE connection
    the local path        `writing(db)` opens one per write, which is correct
                          for a single writer and is NOT how you get many

A connection per write throws away the larger of the two throughput levers.
Measured on Render, crew 16, 2000 writes, three retries, journal mode asserted
on every connection:

    WAL  + one-and-done     250 writes/s   1629/2000 landed   371 LOST
    WAL  + persistent      1505 writes/s   1812/2000 landed   216 LOST
    MVCC + one-and-done     426 writes/s   2000/2000 landed     0 lost
    MVCC + persistent      4721 writes/s   2000/2000 landed     0 lost

    reuse alone      6.01x
    MVCC alone       1.70x
    both            18.87x   <- they COMPOUND

Connection REUSE removes the per-write OS worker thread and is the bigger
lever. MVCC and `BEGIN CONCURRENT` are what let a crew write in PARALLEL and
are what make it correct. Neither alone gets there.

THOSE NUMBERS ARE FROM A RENDER INSTANCE, NOT A LAPTOP. macOS file locking
behaves differently enough that a Mac measurement of concurrent local writers
says nothing about the server — a fact this package has had to learn twice.
Do not re-derive crew sizing from a MacBook.

WHO THIS IS FOR: local databases only. A replicated database gets no write
concurrency from either journal mode — under MVCC no second connection opens
once anything is written, and under WAL the engine rejects the second writer —
so a crew there would be N drainers queueing behind one lock.

DDL DOES NOT GO THROUGH THE CREW. A table created on an MVCC connection is
invisible to every other connection (measured 2026-08-12: "Parse error: no
such table"). Migration takes its own WAL connection — `turso_database
.migrating` — and must complete before a crew starts.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, TypedDict

from declaro_persistum.database import Database, is_replicated
from declaro_persistum.retry import Retry
from declaro_persistum.write_queue import PendingWrite, Room, drain

logger = logging.getLogger(__name__)

__all__ = ["Crew", "start_crew", "stop_crew", "drainer"]


class Crew(TypedDict):
    """A running crew. Data plus the handles needed to stop it.

    There is no behaviour here: the tasks and the stop signal are facts about
    a running crew, and `stop_crew` is a function that takes them.
    """

    room: Room
    stop: asyncio.Event
    tasks: list[asyncio.Task[None]]
    size: int


async def drainer(
    room: Room,
    db: Database,
    conn: Any,
    stop: asyncio.Event,
    retry: Retry,
    idle_s: float,
) -> None:
    """One drainer, one connection, held for the drainer's whole life.

    It does not know which engine it is on. `db["write_one"]` came from the
    WRITERS table at open; see writers.py.

    THE CONNECTION IS HANDED IN, NOT OPENED HERE. This used to call
    `db["connect"](db)` as its first act, inside its own task, where nothing
    could observe the result. A connection that failed to open killed the task
    silently and its share of the queue was never drained — `collect` then
    waited on a future no one would resolve, so a dead drainer and a slow
    queue looked identical from outside. Opening at the boundary, in
    `start_crew`, is what makes the failure reach the caller (Rule 4).

    The connection is used for writes ONLY. It never reads, because a
    partially-consumed cursor alive on an MVCC connection when a write commits
    is the documented silent-rollback window, and on pyturso 0.7.2 it panics
    the engine outright (core/mvcc/database/mod.rs:5424, reproducible 3 of 3).
    One connection, one job.
    """
    write_one = db["write_one"]
    try:
        while not stop.is_set():
            # CLEAR BEFORE CHECKING. The reverse order is a lost wakeup: a
            # drainer finds the queue empty, `deposit` appends and sets
            # `arrived`, and then the drainer clears the flag it was just
            # signalled with. Every drainer sleeps, `collect` waits on a
            # future nobody will resolve, and the caller hangs.
            #
            # That is not hypothetical — it is the bug this loop had for one
            # revision, and it presented as a test that passed alone and hung
            # roughly one run in six.
            #
            # Clearing first makes the window harmless. A deposit landing
            # before the check is seen by the check; one landing after leaves
            # `arrived` set, so the wait returns at once.
            room["arrived"].clear()

            if not room["writes"]:
                # `idle_s` still bounds the wait, so a drainer notices `stop`
                # even when nothing is ever deposited.
                waiters = [
                    asyncio.create_task(room["arrived"].wait()),
                    asyncio.create_task(stop.wait()),
                ]
                _done, pending = await asyncio.wait(
                    waiters, timeout=idle_s, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                continue

            async def execute(write: PendingWrite) -> None:
                # The engine's own way to run one write in a transaction,
                # resolved at open from WRITERS and carried on the Database.
                # The drainer knows a write has SQL and parameters; it does not
                # know which statement opens a transaction, whether parameters
                # go as a tuple or positionally, or whether there is a commit.
                await write_one(conn, write["sql"], write["params"])

            drained = await drain(room, execute, retry)
            # A failure whose depositor cancelled its `collect` has no ticket
            # to raise on. `drain` hands it back rather than dropping it, and
            # this is the boundary, so this is where it becomes visible. The
            # write reached the database; that its caller stopped listening
            # does not make the engine's answer uninteresting.
            for orphan in drained["orphaned"]:
                logger.warning(
                    "A write failed after its caller stopped waiting for it. "
                    "Nobody will see this exception except this line.",
                    exc_info=orphan,
                )
    finally:
        await db["close_connection"](conn)


async def start_crew(
    room: Room,
    db: Database,
    size: int,
    retry: Retry,
    idle_s: float,
) -> Crew:
    """Start `size` drainers over one room.

    Every argument is required. `size` in particular: the right crew is a
    property of the machine and the workload, it was measured at 16 on one
    Render tier and 64 on another, and a default here would be a number
    nobody chose applied to a machine nobody measured (Rule 14).
    """
    if is_replicated(db):
        raise ValueError(
            "A crew is for local databases only. A replicated database gets no "
            "write concurrency from either journal mode — under MVCC no second "
            "connection opens once anything is written, and under WAL the "
            "engine rejects the second writer. Use `writing(db)` instead; it "
            "serialises, which is the most a replica can do."
        )

    # EVERY CONNECTION IS OPEN BEFORE A SINGLE DRAINER STARTS. A failure here
    # raises to the caller, where it can be seen and handled, instead of
    # killing one task and leaving the queue with a share nobody drains.
    # Already-opened connections are closed on the way out, so a partial crew
    # never exists.
    conns: list[Any] = []
    try:
        for _ in range(size):
            conns.append(await db["connect"](db))
    except Exception:
        for conn in conns:
            with contextlib.suppress(Exception):
                await db["close_connection"](conn)
        raise

    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(drainer(room, db, conn, stop, retry, idle_s))
        for conn in conns
    ]
    return {"room": room, "stop": stop, "tasks": tasks, "size": size}


async def stop_crew(crew: Crew) -> None:
    """Signal every drainer and wait for it to finish its current write.

    Drainers are not cancelled. A cancelled drainer may be between `execute`
    and `commit`, and the queue would have no way to tell whether that write
    landed — so the stop is cooperative and the wait is unbounded on purpose.
    """
    crew["stop"].set()
    await asyncio.gather(*crew["tasks"], return_exceptions=True)
