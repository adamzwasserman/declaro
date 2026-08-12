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


def new_room() -> Room:
    """An empty waiting room."""
    return {"writes": [], "waiting": {}}


def deposit(room: Room, write: PendingWrite) -> str:
    """Put a write in the room and return its ticket at once.

    Nothing is executed here. Call from inside the event loop.
    """
    ticket = str(uuid.uuid4())
    room["writes"].append((ticket, write))
    room["waiting"][ticket] = asyncio.get_running_loop().create_future()
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
