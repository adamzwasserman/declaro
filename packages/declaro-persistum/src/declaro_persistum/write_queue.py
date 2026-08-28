"""A waiting room in front of the WAL.

The WAL is already the queue. A write is durable once it is in the log, and
the engine applies it to the main file later. That is what a write-ahead log
is for, and it is why a local commit takes under a millisecond.

So the only job left is to buffer callers who arrive at the same instant and
hand their writes to the log in order. That is all this module does.

It is NOT a claim that the engine takes one writer. Turso supports concurrent
writers through MVCC and BEGIN CONCURRENT, and a writer still opens a
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

FAILURE ARRIVES IN TWO SHAPES, and telling them apart is the caller's whole
reason for reading a receipt. `collect` RAISES the write's own exception for
anything the engine refused outright: a constraint violation, bad SQL, a full
disk. A receipt with `ok: False` says only that the engine stayed busy for
the whole retry budget. Both reach exactly one ticket and neither stops the
queue for anyone else.

The appender is `drain`, and the caller runs it. This module never starts a
task of its own::

    async def appender():
        while True:
            drained = await drain(room, execute, NO_RETRY)
            for orphan in drained["orphaned"]:
                logger.warning("write failed, caller gone", exc_info=orphan)
            await asyncio.sleep(0)

`drained["orphaned"]` is not optional politeness. It holds the failures whose
depositor cancelled its `collect`, so there is no ticket left to raise on and
this loop is the last place they can be seen. An appender that writes
`await drain(...)` and discards the result is the silence this module spent a
revision removing.

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

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.retry import Retry, delay_before, is_contention


class PendingWrite(TypedDict):
    """One write on its way to the log."""

    sql: str
    params: Any


class Receipt(TypedDict):
    """The answer to one deposited write that the engine actually answered.

    `ok: False` HAS EXACTLY ONE MEANING: the engine reported contention on
    every attempt the retry budget allowed. It is not the general failure
    channel, and no other failure arrives this way — `collect` raises those.
    """

    id: str  # the ticket returned by deposit
    ok: bool  # False means contention outlasted the budget, nothing else
    error: str  # empty when ok


class Drained(TypedDict):
    """What one pass of `drain` did, and what it could not hand to anyone.

    `orphaned` is the failures with no living caller: the depositor cancelled
    its own `collect`, so its future is gone or already settled and there is
    no ticket left to raise on. The write still reached the database and its
    failure is still a fact about the system, so it leaves here as data.

    It is a list rather than a count because the exception is the useful part.
    A number would say a write failed and refuse to say how.
    """

    attempted: int
    orphaned: list[BaseException]


class Room(TypedDict):
    """Callers waiting for their turn at the log.

    `writes` is the arrival order. `waiting` maps a ticket to the caller
    awaiting it. Both are empty whenever no caller is mid-flight.
    """

    writes: list[tuple[str, PendingWrite]]
    waiting: dict[str, asyncio.Future]
    arrived: asyncio.Event


@helper
def new_room() -> Room:
    """An empty waiting room.

    `arrived` is how a drainer learns there is work without polling. A polling
    drainer can be starved by a busy event loop while `collect` waits on a
    future that has no timeout, which is a hang rather than a slowdown.
    """
    return {"writes": [], "waiting": {}, "arrived": asyncio.Event()}


@helper
def _the_statement(write: PendingWrite) -> PendingWrite:
    """There is SQL here, so the write goes on as it arrived."""
    return write


@helper
def _refuse_an_empty_write(_write: PendingWrite) -> PendingWrite:
    """No SQL, so the caller is told at the door rather than by the engine."""
    raise ValueError(
        "a write needs SQL. `deposit` was given an empty statement, which "
        "the engine would reject after a ticket, a drainer and a round "
        "trip."
    )


# Whether the deposited write carries a statement to run.
_A_WRITE_WITH: dict[bool, Callable[[PendingWrite], PendingWrite]] = {
    True: _the_statement,
    False: _refuse_an_empty_write,
}


@boundary
def deposit(room: Room, write: PendingWrite) -> str:
    """Put a write in the room and return its ticket at once.

    Nothing is executed here. Call from inside the event loop.

    A WRITE WITH NO SQL IS REFUSED HERE, not by the engine. `PendingWrite`
    types `sql` as `str`, and `str` includes the empty string, so the type
    permits a "write" that is not one. Deposited, it took a ticket, woke a
    drainer, occupied a connection and came back as a receipt reading "no SQL
    statements to execute" — a full round trip to learn what the caller could
    have been told at the door.

    This is Rule 13 rather than a guard: the boundary states the contract once
    so the interior may trust it. `turso_write_one` and its siblings then mean
    "a statement" by `sql`, which is what their signatures already imply.
    """
    write = _A_WRITE_WITH[bool(write["sql"].strip())](write)
    ticket = str(uuid.uuid4())
    room["writes"].append((ticket, write))
    room["waiting"][ticket] = asyncio.get_running_loop().create_future()
    # Wake a drainer. Without this the crew has to POLL, and a polling
    # drainer can be starved by a busy event loop while `collect` waits on a
    # future with no timeout — which is exactly how this flaked once in a
    # full-suite run and passed every time it was run alone.
    room["arrived"].set()
    return ticket


@helper
async def collect(room: Room, ticket: str) -> Receipt:
    """Wait for one deposited write, and RAISE whatever it raised.

    The ticket is dropped once collected, so the room does not grow.

    TWO OUTCOMES, AND THEY ARE NOT THE SAME SHAPE.

    A receipt with `ok: False` means one thing only: the engine kept saying
    "not now" until the retry budget ran out. retry.py calls that an ordinary
    outcome of a write, so it is an answer and not an exception.

    ANYTHING ELSE RAISES HERE, in the caller that deposited the write, with
    its own type and its own traceback. This used to come back as
    `{"ok": False, "error": str(exc)}`, which flattened a constraint
    violation, a syntax error and a full disk into a string, and put them in
    the same field as ordinary contention. A caller reading `ok` could not
    tell "the row already exists" from "the engine was busy", and the only
    correct response to those two differs completely.

    A caller that deposits and never collects now gets Python's
    "Future exception was never retrieved" at collection. That is the point:
    an unread failure should be noisy.

    THE `finally` IS THERE BECAUSE I BROKE THIS. When
    `collect` only raised on the happy path this read::

        receipt = await room["waiting"][ticket]
        room["waiting"].pop(ticket, None)

    Once the future could hold an exception, the `await` raised and the `pop`
    below it never ran. Every failed write left its ticket in the room for
    the life of the process, which is precisely the growth the line above
    promises does not happen. The room is emptied whichever way the await
    ends.
    """
    try:
        return await room["waiting"][ticket]
    finally:
        room["waiting"].pop(ticket, None)


@helper
def _is_listening(waiter: asyncio.Future[Any] | None) -> bool:
    """Whether one ticket still has a caller who can be told the answer.

    A depositor that cancelled its `collect` leaves either no future at all or
    a settled one, and both mean the same thing here: nobody to hand this to.
    """
    return waiter is not None and not waiter.done()


@helper
def _hand_the_failure_over(waiter: Any, exc: BaseException) -> list[BaseException]:
    """The depositor is waiting, so its own exception goes down its ticket."""
    waiter.set_exception(exc)
    return []


@helper
def _nobody_holds_the_ticket(_waiter: Any, exc: BaseException) -> list[BaseException]:
    """No caller left, so the failure leaves as data for `drain` to return."""
    return [exc]


@helper
def _hand_the_receipt_over(waiter: Any, receipt: Receipt) -> Receipt:
    """The depositor is waiting, so its receipt goes down its ticket."""
    waiter.set_result(receipt)
    return receipt


@helper
def _a_receipt_nobody_collected(_waiter: Any, receipt: Receipt) -> Receipt:
    """No caller left. The write landed and its receipt has nowhere to go."""
    return receipt


# Whether the depositor of this write is still waiting on its ticket.
_WITH_A_FAILURE: dict[bool, Callable[[Any, BaseException], list[BaseException]]] = {
    True: _hand_the_failure_over,
    False: _nobody_holds_the_ticket,
}

_WITH_A_RECEIPT: dict[bool, Callable[[Any, Receipt], Receipt]] = {
    True: _hand_the_receipt_over,
    False: _a_receipt_nobody_collected,
}


@boundary
async def drain(
    room: Room,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
) -> Drained:
    """Append every waiting write to the log, in arrival order.

    One at a time: the next write starts only after the previous one
    returns, so the order the caller deposited in is the order the log
    receives. Returns how many were ATTEMPTED, and any failure that found
    nobody waiting for it.

    THE FIELD WAS CALLED `appended` AND COUNTED ATTEMPTS. Measured
    2026-08-17: three writes with one raising returned
    `{'appended': 3, 'orphaned': []}`, and two of the three were appended.
    Three tests already relied on the real meaning and said so in their own
    assertion messages -- "every write was attempted, including the two after
    the failure" -- so the count is right and the name was wrong.

    Attempts is also the more useful number: it says the queue did not stop
    at the first failure, which is the property this module exists to have.
    A caller who wants to know which writes landed asks their own ticket.

    `retry` is required, not defaulted. Pass `NO_RETRY` to run each write
    once; retry.py says why the choice is forced on the caller.

    Contention is retried, up to `retry["attempts"]` in total. Anything else
    fails on the first attempt. Either way the failure belongs to the caller
    that deposited it, goes back down that caller's ticket, and does not
    stop the queue.

    Retrying is only possible because a deposited write holds its own SQL
    and parameters. `writing(db)` cannot do this -- it never sees the
    statements -- which is the whole reason this module exists.

    THE `except` BELOW IS A ROUTER, NOT A SWALLOW, and the difference is the
    whole of Rule 8. It does not decide anything about the failure, does not
    read it, and does not summarise it. It carries the exception object
    itself to the one future that owns it, so `collect` can raise it intact
    in the caller that deposited the write.

    It is here rather than around the whole loop because "does not stop the
    queue" is a promise to the OTHER depositors. A drainer runs this inside
    `while not stop.is_set()`; let one write's exception out of `drain` and
    that drainer's task dies, its connection closes, and the crew silently
    shrinks by one for the life of the process.

    A FAILURE WITH NO LIVING CALLER LEAVES IN `orphaned`. The router above
    can only route where someone is listening, and a depositor that cancelled
    its own `collect` has taken its future with it. That branch used to end
    at `if waiter is not None and not waiter.done():` and nothing else, so
    the exception fell off the end of the `if` and was gone.

    Cancelling a `collect` says "I no longer need the answer". It does not
    say "do not tell anyone the database refused this write", and those are
    different sentences. The failure comes back as data, and the boundary
    that runs `drain` decides what to do with it. This module still logs
    nothing and still stores nothing (Rule 4).
    """
    attempted = 0
    orphaned: list[BaseException] = []
    while room["writes"]:
        ticket, write = room["writes"].pop(0)
        waiter = room["waiting"].get(ticket)
        try:
            receipt = await _append_one(ticket, write, execute, retry)
        except A_BUG_IN_THIS_PACKAGE as exc:
            # OURS, SO IT FAILS EVERY WRITE, and handing it to each ticket in
            # turn tells every caller their own SQL was wrong.
            #
            # Measured 2026-08-17, five writes queued and a TypeError from
            # `execute`: appended=5, and all five callers were told their write
            # failed. None of them had written anything wrong.
            #
            # The caller in flight IS told, because it is genuinely waiting and
            # would otherwise never be resolved. Then this raises, so the
            # appender stops instead of grinding the rest of the room through
            # the same broken call. Those writes stay queued, which is
            # recoverable and visible; popped and dropped is neither.
            #
            # Nothing is orphaned here: the exception itself leaves on the
            # `raise` below, so a caller that stopped listening loses nothing
            # this line could have told it.
            _WITH_A_FAILURE[_is_listening(waiter)](waiter, exc)
            raise
        except Exception as exc:
            orphaned.extend(_WITH_A_FAILURE[_is_listening(waiter)](waiter, exc))
            attempted += 1
            continue
        _WITH_A_RECEIPT[_is_listening(waiter)](waiter, receipt)
        attempted += 1
    return {"attempted": attempted, "orphaned": orphaned}


@helper
async def _append_one(
    ticket: str,
    write: PendingWrite,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
) -> Receipt:
    """Run one write, re-running it while the engine reports contention.

    ONLY CONTENTION IS ABSORBED, and only contention has ever been meant to
    be. The old `except Exception` here collapsed two different things into
    one `return`:

        if attempt == retry["attempts"] or not is_contention(exc):
            return {"id": ticket, "ok": False, "error": str(exc)}

    Read the `or`. On the left, contention that outlasted the budget, which
    retry.py rules is an ordinary answer a write can give. On the right,
    EVERY OTHER FAILURE — a constraint violation, a typo in the SQL, a full
    disk, a bug in this library — reduced to its `str()` and posted into the
    same field. The type was gone, the traceback was gone, and the caller was
    handed a receipt that looked exactly like a busy engine.

    Now the two are separate. Contention exhaustion returns a receipt.
    Anything else raises, on the attempt it happened, and `drain` carries it
    to the depositor's ticket.

    THE THREE ANSWERS TO A REFUSAL ARE DECLARED, in `_AFTER_A_REFUSAL`, keyed
    by the two facts that decide between them: was this contention, and was
    that the last attempt the budget allowed. All four combinations are
    written down, so a reader sees every answer this function can give
    without tracing a loop.
    """
    return await _WITH_AN_ATTEMPT[retry["attempts"] >= 1](ticket, write, execute, retry, 1)


@boundary
async def _one_attempt(
    ticket: str,
    write: PendingWrite,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
    attempt: int,
) -> Receipt:
    """Run the write once. A refusal goes to the table that answers refusals.

    ATTEMPTS ARE A CHAIN RATHER THAN A LOOP. Each one either answers or asks
    for the next, and `attempt` counts from 1, so `retry["attempts"]` is still
    the total number of runs a write can get.
    """
    # The first attempt waits for nothing: `delay_before` returns 0.0 for it,
    # and sleeping on a zero delay would hand the event loop a yield point the
    # write never had before.
    delay = delay_before(retry, attempt)
    await _BEFORE_THIS_ATTEMPT[delay > 0](delay)
    try:
        await execute(write)
        return {"id": ticket, "ok": True, "error": ""}
    except Exception as exc:
        return await _AFTER_A_REFUSAL[(is_contention(exc), attempt == retry["attempts"])](
            ticket, write, execute, retry, attempt, exc
        )


@helper
async def _wait_out_the_delay(seconds: float) -> int:
    """The policy asks this attempt to pause first, so it takes the clock."""
    await asyncio.sleep(seconds)
    return 1


@helper
async def _no_delay(_seconds: float) -> int:
    """No pause is due, so the write runs now without touching the clock."""
    return 0


# Whether the policy asks this attempt to wait before it runs.
_BEFORE_THIS_ATTEMPT: dict[bool, Callable[[float], Awaitable[int]]] = {
    True: _wait_out_the_delay,
    False: _no_delay,
}


@helper
async def _no_attempt_at_all(
    _ticket: str,
    _write: PendingWrite,
    _execute: Callable[[PendingWrite], Awaitable[Any]],
    _retry: Retry,
    _attempt: int,
) -> Receipt:
    """A policy allowing no attempts cannot run a write, so it is refused."""
    raise AssertionError("a retry policy runs a write at least once")


# Whether the policy allows the write to run at all.
_WITH_AN_ATTEMPT: dict[
    bool,
    Callable[
        [str, PendingWrite, Callable[[PendingWrite], Awaitable[Any]], Retry, int],
        Awaitable[Receipt],
    ],
] = {
    True: _one_attempt,
    False: _no_attempt_at_all,
}


@helper
async def _belongs_to_the_caller(
    _ticket: str,
    _write: PendingWrite,
    _execute: Callable[[PendingWrite], Awaitable[Any]],
    _retry: Retry,
    _attempt: int,
    exc: Exception,
) -> Receipt:
    """Not "not now" but "no", so it goes back with its type intact."""
    raise exc


@helper
async def _the_budget_is_spent(
    ticket: str,
    _write: PendingWrite,
    _execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
    _attempt: int,
    exc: Exception,
) -> Receipt:
    """The engine stayed busy for every attempt. That is an answer, not a fault."""
    return {
        "id": ticket,
        "ok": False,
        "error": (f"the engine stayed busy for all {retry['attempts']} attempts: {exc}"),
    }


@helper
async def _once_more(
    ticket: str,
    write: PendingWrite,
    execute: Callable[[PendingWrite], Awaitable[Any]],
    retry: Retry,
    attempt: int,
    _exc: Exception,
) -> Receipt:
    """Contention, with budget left, so the write runs again."""
    return await _one_attempt(ticket, write, execute, retry, attempt + 1)


# Keyed by (the engine said "not now", this was the last attempt allowed).
_AFTER_A_REFUSAL: dict[
    tuple[bool, bool],
    Callable[
        [
            str,
            PendingWrite,
            Callable[[PendingWrite], Awaitable[Any]],
            Retry,
            int,
            Exception,
        ],
        Awaitable[Receipt],
    ],
] = {
    (False, False): _belongs_to_the_caller,
    (False, True): _belongs_to_the_caller,
    (True, True): _the_budget_is_spent,
    (True, False): _once_more,
}
