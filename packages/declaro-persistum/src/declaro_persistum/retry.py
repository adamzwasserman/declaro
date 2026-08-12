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
the difference between the queue and the pool, and it is the reason the
queue exists at all.

A policy is REQUIRED wherever retrying is possible. It is not defaulted,
because a default would swallow the caller's omission: nothing in the
signature would distinguish "I chose one attempt" from "I did not think
about it". `NO_RETRY` is the explicit way to say once and only once.

    await drain(room, execute, NO_RETRY)        # one attempt, as before
    await drain(room, execute, ON_CONTENTION)   # absorb write-write conflicts

Only contention is retried. A constraint violation is not contention: it
will fail identically on the next attempt, and it belongs to the caller
that deposited it.
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

ON_CONTENTION: Retry = {"attempts": 5, "base_delay_s": 0.01, "max_delay_s": 0.25}
"""Absorb write-write conflicts under MVCC. Anything else still raises."""


def is_contention(error: BaseException) -> bool:
    """True when the engine said "not now" rather than "no".

    The retryable set is `Error::Busy`, `Error::BusySnapshot`, and anything
    reporting a conflict -- a commit-time row conflict under BEGIN
    CONCURRENT. The sync engine also wraps contention as "database tape
    error: database is busy".

    NOTE: `TursoPool._is_busy` in pool.py carries the same predicate. Two
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
