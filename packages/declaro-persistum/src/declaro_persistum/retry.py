"""What to do when the engine says "not now".

MVCC concurrency has a documented price: two transactions that touch
overlapping data race, and one of them is told to roll back and try again.
Turso states this directly -- "one will receive a conflict error and must
roll back and retry" -- and its own examples loop until the commit succeeds
(https://docs.turso.tech/tursodb/concurrent-writes).

Measured on a real cloud replica, 2026-08-10: concurrent writers to one
table under MVCC raise `turso.Error: Write-write conflict` **even for
distinct rows**. So this is not a rare case reachable only by contending
callers; it is the ordinary cost of writing concurrently.

Retry is only possible where the write is held as data. `acquire_write`
cannot retry, because it never sees the caller's statements -- it yields a
connection and the caller runs whatever it likes on it. A deposited write
carries its own SQL and parameters, so `drain` can run it again. That is
the difference between the queue and the database, and it is the reason the
queue exists at all.

THERE ARE TWO RETRIES AND THEY ARE NOT THE SAME. They were conflated here on
2026-08-11 and separated after Adam ruled on the sync path.

1. CONFLICT ABSORPTION. A commit-time conflict is re-run below BOTH the async
   and sync paths. It is NOT a caller argument — the application does not know
   the engine. Its bound and its retryable set are declared once, as data,
   where the engine is known (the dialect layer). A query is already data, so
   this works on the sync path with no queue in sight.

2. DURABILITY RETRY, the `drain` policy below. How long the system keeps trying
   to make a deposited write durable. That IS the caller's business, through
   the ticket, and it exists only on the ASYNCHRONOUS path.

The write queue is what makes ASYNC possible, not what makes retry possible.
If a sync caller wants queuing it builds one in its own I/O — a hidden queue
under a synchronous call would return before the write was durable, which is
exactly what synchronous promises it does not do. Providing one would make the
return value a lie.

A policy is REQUIRED wherever retrying is possible. It is not defaulted,
because a default would swallow the caller's omission: nothing in the
signature would distinguish "I chose one attempt" from "I did not think
about it". `NO_RETRY` is the explicit way to say once and only once.

    await drain(room, execute, NO_RETRY)        # one attempt, as before
    await drain(room, execute, ON_CONTENTION)   # absorb write-write conflicts

Only contention is retried. A constraint violation is not contention: it
will fail identically on the next attempt, and it belongs to the caller
that deposited it.

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

from typing import TypedDict


class Retry(TypedDict):
    """How many attempts a refused write gets, and how long between them.

    `attempts` is the TOTAL, not the number of retries: 1 means run it once
    and hand any failure back. Delays grow exponentially from
    `base_delay_s`, capped at `max_delay_s`.
    """

    attempts: int
    base_delay_s: float
    max_delay_s: float


NO_RETRY: Retry = {"attempts": 1, "base_delay_s": 0.0, "max_delay_s": 0.0}
"""Run the write once. Any failure, contention or not, goes to its caller."""

ON_CONTENTION: Retry = {"attempts": 4, "base_delay_s": 0.01, "max_delay_s": 0.25}
"""Absorb write-write conflicts under MVCC. Anything else still raises.

Four attempts is one try plus THREE RETRIES, then raise (Adam, 2026-08-11).
`attempts` counts the total, so the retry count is `attempts - 1`.

THE BOUND IS DECLARED HERE, never written as a literal in a retry loop. At
real contention it is the bound, not the engine, that decides how many writes
land — so a bound nobody consciously chose is the input-side implicit default
Rule 14 exists to forbid.

THE CONTENTION CURVE, measured 2026-08-11. 20 concurrent one-and-done writers
spread over K rows, `v = v + 1` so every collision is a genuine conflict:

    writers/row   landed   lost updates   first-try
        1         20/20         0            20
        2         20/20         0            10
        4         20/20         0             6
        5         20/20         0             5
       10         18/20         0             3
       20         14/20         0             1

THE KNEE, repeated five times per point, because the single-run version of
this said five and was wrong:

    writers/row   landed across 5 runs      all 20?
        1         20, 20, 20, 20, 20          yes
        2         20, 20, 20, 20, 20          yes
        4         20, 20, 20, 20, 20          yes
        5         19, 20, 20, 20, 20          NO

THREE RETRIES LANDS EVERYTHING UP TO FOUR CONCURRENT WRITERS PER ROW. At
five, one run in five drops a write. Four held across five runs — a bound on
the evidence, not a proof of four.

FIRST-ATTEMPT SUCCESS IS THE LEADING INDICATOR. It falls 20 -> 10 -> 5 -> 4
while landed still reads 20/20, so the retry absorbs a rising load silently
and by the time anything fails the system has been deep into its budget for
some time. Emit first-attempt success SEPARATELY from eventual success; an
implementation that reports only the eventual rate gives an operator no
warning at all.

THIS BOUND WAS VERIFIED AT FOUR CONCURRENT WRITERS PER ROW, five runs, one
machine. A declared bound carries the contention level it was verified at,
or it is a preference rather than a claim anyone can check.

ZERO LOST UPDATES AT EVERY POINT. The sum of counters equals the landed count
on all six. Twenty-way concurrent read-modify-write on one row is the textbook
lost-update test and nothing was lost anywhere on the curve. That is what one
write per connection buys: COMPAT.md's silent-rollback window has zero
duration when no sibling statement can exist.

Retries earn their keep long before failures appear — first-try success falls
20 -> 10 -> 6 -> 5 while landed holds at 20/20.

Beyond that, exhausting the bound is CORRECT. A hot row is an
application-shaped problem; no retry policy fixes one, it only decides how long
you spend discovering that. A retry that never gives up is a hang wearing a
different name. Contention exhaustion is an ORDINARY outcome of a write, not an
exceptional condition, and every consumer surface must carry it as one of the
answers a write can give.

One run per point, one machine, one process. The 20-writers-per-row point read
10/20 on an earlier run and 14/20 here; treat the shape as solid and the
individual numbers as approximate.

Disjoint rows do not need this at all: 20 concurrent writers to 20 distinct
rows in one small table conflicted zero times. Conflict rate tracks the
application's access pattern, not page layout — pyturso detects at row level,
never SQLite's page level."""


def is_contention(error: BaseException) -> bool:
    """True when the engine said "not now" rather than "no".

    The retryable set is `Error::Busy`, `Error::BusySnapshot`, and anything
    reporting a conflict -- a commit-time row conflict under BEGIN
    CONCURRENT. The replication engine also wraps contention as "database tape
    error: database is busy".

    NOTE: the same predicate is carried elsewhere. Two
    definitions of "retryable" WILL drift. Tracked as a task to unify; do
    not add a third.
    """
    text = str(error).lower()
    return "busy" in text or "sqlite_busy" in text or "conflict" in text


def delay_before(retry: Retry, attempt: int) -> float:
    """Seconds to wait before `attempt`, counting the first attempt as 1.

    Pure: no clock, no sleep. The caller decides what to do with the number,
    which is what makes it testable without waiting for anything.
    """
    if attempt <= 1:
        return 0.0
    grown = retry["base_delay_s"] * (2 ** (attempt - 2))
    return min(grown, retry["max_delay_s"])
