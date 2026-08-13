"""A waiting room in front of the WAL.

The WAL is already the queue. A write is durable once it is in the log, and
the engine applies it to the main file later. That is what a write-ahead log
is for, and it is why a local commit takes under a millisecond.

So the only job left is to buffer callers who arrive at the same instant and
hand their writes to the log in order. That is all this module does.

It is NOT a claim that the engine takes one writer. Turso supports concurrent
writers through MVCC and BEGIN CONCURRENT, and the pool still opens a write
connection per concurrent caller. This buffers callers; it does not serialise
the database.

Nothing is stored here. The room is empty except during the microseconds when
callers overlap. There is no persistence, because nothing sits in it; and no
pending list surviving a failure, because every write has a caller holding
its ticket.

Contention IS retried, and only contention. This paragraph used to say there
was no retry at all, "because a real error -- a constraint violation -- fails
again and belongs to its caller". That reasoning still holds for constraint
violations and they are still not retried. It does not hold for a write-write
conflict, which is the documented price of MVCC concurrency and means "not
now" rather than "no". `drain` takes a required `Retry` policy; see retry.py
for why it is required rather than defaulted.

    ticket = deposit(room, write)      # returns at once
    ...                                # the caller is free
    receipt = await collect(room, ticket)

`deposit` hands back a ticket immediately and `collect` awaits that ticket,
so a caller can deposit several writes, keep working, and collect when it
actually needs the answer. That is the difference between this and a lock: a
lock makes you wait at the moment of writing.

The ticket is also how a caller designs its own atomicity. A transaction is
a boundary the library imposes -- everything inside it, all or nothing.
Tickets put that choice with the caller, who draws the boundary by choosing
when to collect: deposit three and collect all three, or collect the first
before depositing the one that depends on it, or deposit three and collect
only the one it actually needs. Failure is per ticket too, so a caller can
retry one write and carry on with the rest instead of losing work that was
independently fine.

The appender is `drain`, and the caller runs it. This module never starts a
task of its own::

    async def appender():
        while True:
            await drain(room, execute, NO_RETRY)
            await asyncio.sleep(0)

THE TWO LEVERS. Measured 2026-08-12, Mac, crew 16, 2000 writes, 3 retries,
journal mode asserted on every connection. DO NOT RE-DERIVE THIS.

    WAL  + one-and-done     250 writes/s   1629/2000 landed   371 LOST
    WAL  + persistent      1505 writes/s   1812/2000 landed   216 LOST
    MVCC + one-and-done     426 writes/s   2000/2000 landed     0 lost
    MVCC + persistent      4721 writes/s   2000/2000 landed     0 lost

    reuse alone (WAL)          6.01x
    concurrency alone (MVCC)   1.70x
    both together             18.87x   <- they COMPOUND, they do not add

Connection REUSE removes the per-write OS thread; that is the larger lever.
MVCC plus BEGIN CONCURRENT is what lets a crew write in PARALLEL; that is what
makes the crew CORRECT. Neither alone gets there. The 13,826 writes/sec figure
on pro_ultra requires BOTH.

WAL LOSES WRITES at crew 16 even after three retries. MVCC loses none. So
"WAL plus persistent connections" is not a cheaper safe option, it is a lossy
one. WAL's safe crew is 1, or writers serialised behind a lock.

A REPLICA TAKES ONE REPLICA CONNECTION. That is the constraint, and it
is NOT about MVCC. Measured 2026-08-12 against a real replica, pyturso 0.7.2:

    MVCC on a replica          journal_mode = 'mvcc', 4 of 4 runs
    20 writes, sequential, 1 conn     20 local -> 20 ON PRIMARY, no checkpoint
    8 writes over 8 connections       5 local -> 0 ON PRIMARY, no convergence
    opening a 2nd replica connection     "database tape error: database is busy"
                                      3 of 4 runs failed outright, one with
                                      12 retries over 30s on an IDLE database

So MVCC plus replication is fine for sequential writes. What breaks is more
than one replica connection against one replica, which is what persistum's
one-connection-per-write does the moment nothing serialises it. MVCC is
incidental: it is merely the mode in which `_write_serialisation` stops
taking the lock, and that lock is what has been masking this on WAL.

THIS PARAGRAPH PREVIOUSLY SAID "MVCC IS LOCAL ONLY ... it creates local-only
internal tables the replication engine cannot reconcile." Both halves were wrong.
MVCC runs on a replica, measured repeatedly, and the internal-table
mechanism was asserted from one correlational observation and never proven.
The engine has never refused this combination; persistum's policy did.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypedDict

from declaro_persistum.retry import Retry, delay_before, is_contention


class PendingWrite(TypedDict):
    """One write on its way to the log."""

    sql: str
    params: Any


class Receipt(TypedDict):
    """The answer to one deposited write."""

    id: str      # the ticket returned by deposit
    ok: bool
    error: str   # empty when ok


class Room(TypedDict):
    """Callers waiting for their turn at the log.

    `writes` is the arrival order. `waiting` maps a ticket to the caller
    awaiting it. Both are empty whenever no caller is mid-flight.
    """

    writes: list[tuple[str, PendingWrite]]
    waiting: dict[str, asyncio.Future]
    arrived: asyncio.Event


def new_room() -> Room:
    """An empty waiting room.

    `arrived` is how a drainer learns there is work without polling. A polling
    drainer can be starved by a busy event loop while `collect` waits on a
    future that has no timeout, which is a hang rather than a slowdown.
    """
    return {"writes": [], "waiting": {}, "arrived": asyncio.Event()}


def deposit(room: Room, write: PendingWrite) -> str:
    """Put a write in the room and return its ticket at once.

    Nothing is executed here. Call from inside the event loop.
    """
    ticket = str(uuid.uuid4())
    room["writes"].append((ticket, write))
    room["waiting"][ticket] = asyncio.get_running_loop().create_future()
    # Wake a drainer. Without this the crew has to POLL, and a polling
    # drainer can be starved by a busy event loop while `collect` waits on a
    # future with no timeout — which is exactly how this flaked once in a
    # full-suite run and passed every time it was run alone.
    room["arrived"].set()
    return ticket


async def collect(room: Room, ticket: str) -> Receipt:
    """Wait for one deposited write and return its receipt.

    The ticket is dropped once collected, so the room does not grow.
    """
    receipt = await room["waiting"][ticket]
    room["waiting"].pop(ticket, None)
    return receipt


async def drain(
    room: Room,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
) -> int:
    """Append every waiting write to the log, in arrival order.

    One at a time: the next write starts only after the previous one
    returns, so the order the caller deposited in is the order the log
    receives. Returns how many were appended.

    `retry` is required, not defaulted. Pass `NO_RETRY` to run each write
    once; retry.py says why the choice is forced on the caller.

    Contention is retried, up to `retry["attempts"]` in total. Anything else
    fails on the first attempt. Either way the failure belongs to the caller
    that deposited it, goes back down that caller's ticket, and does not
    stop the queue.

    Retrying is only possible because a deposited write holds its own SQL
    and parameters. `acquire_write` cannot do this -- it never sees the
    statements -- which is the whole reason this module exists.
    """
    appended = 0
    while room["writes"]:
        ticket, write = room["writes"].pop(0)
        receipt = await _append_one(ticket, write, execute, retry)
        waiter = room["waiting"].get(ticket)
        if waiter is not None and not waiter.done():
            waiter.set_result(receipt)
        appended += 1
    return appended


async def _append_one(
    ticket: str,
    write: PendingWrite,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
) -> Receipt:
    """Run one write, re-running it while the engine reports contention."""
    for attempt in range(1, retry["attempts"] + 1):
        wait = delay_before(retry, attempt)
        if wait:
            await asyncio.sleep(wait)
        try:
            await execute(write)
            return {"id": ticket, "ok": True, "error": ""}
        except Exception as exc:
            if attempt == retry["attempts"] or not is_contention(exc):
                return {"id": ticket, "ok": False, "error": str(exc)}
    raise AssertionError("unreachable: attempts is at least 1")
