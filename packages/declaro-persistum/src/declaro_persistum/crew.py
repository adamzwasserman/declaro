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
from typing import Any

from declaro_persistum.database import Database, is_replicated
from declaro_persistum.retry import Retry
from declaro_persistum.write_queue import Room, drain

__all__ = ["Crew", "start_crew", "stop_crew", "drainer"]


class Crew(dict):
    """A running crew. Data plus the handles needed to stop it.

    A dict rather than an object because there is no behaviour here: the
    tasks and the stop signal are facts about a running crew, and
    `stop_crew` is a function that takes them.
    """


async def drainer(
    room: Room,
    db: Database,
    stop: asyncio.Event,
    retry: Retry,
    idle_s: float,
) -> None:
    """One drainer, one connection, held for the drainer's whole life.

    The connection is opened once and used for writes ONLY. It never reads,
    because a partially-consumed cursor alive on an MVCC connection when a
    write commits is the documented silent-rollback window, and on pyturso
    0.7.2 it panics the engine outright (core/mvcc/database/mod.rs:5424,
    reproducible 3 of 3). One connection, one job.
    """
    conn = await db["connect"](db)
    try:
        while not stop.is_set():
            if not room["writes"]:
                # Nothing to do. Sleeping beats spinning, and the interval is
                # a parameter because the right value depends on how bursty
                # the caller is, which this module cannot know.
                try:
                    await asyncio.wait_for(stop.wait(), timeout=idle_s)
                except TimeoutError:
                    pass
                continue

            async def execute(write: Any) -> None:
                # `drain` hands over the PendingWrite itself, so the drainer
                # never has to know how a write is spelled — only how to run
                # one on its own connection.
                await conn.execute("BEGIN CONCURRENT")
                await conn.execute(write["sql"], write["params"])
                await conn.commit()

            await drain(room, execute, retry)
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

    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(drainer(room, db, stop, retry, idle_s))
        for _ in range(size)
    ]
    return Crew({"room": room, "stop": stop, "tasks": tasks, "size": size})


async def stop_crew(crew: Crew) -> None:
    """Signal every drainer and wait for it to finish its current write.

    Drainers are not cancelled. A cancelled drainer may be between `execute`
    and `commit`, and the queue would have no way to tell whether that write
    landed — so the stop is cooperative and the wait is unbounded on purpose.
    """
    crew["stop"].set()
    await asyncio.gather(*crew["tasks"], return_exceptions=True)
