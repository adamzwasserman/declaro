"""Dual-write during a cutover: write to both databases, read from the primary.

Three classes used to wrap
two connections and a pair of booleans. A mirror is two databases and two
policies — that is data, and what is done with it is functions.

    m = mirror(primary=old, replica=new, fail_open=True, compare_on_read=True)

    async with mirror_writing(m) as (a, b):   # both, in parallel
        ...
    async with mirror_reading(m) as conn:     # the primary
        ...

THE TWO POLICIES ARE THE WHOLE DESIGN, and both are required arguments because
neither has an obviously safe value (Rule 14):

`fail_open` — what a MIRROR failure means. True keeps serving from the primary
and records the divergence; False makes the mirror's failure the caller's. A
cutover starts fail-open, because the point is to shadow production without
risking it, and ends fail-closed, because by then the mirror is production.

`compare_on_read` — whether a read is run against both and the answers
compared. It doubles read cost and is the only thing that can tell you the
mirror is actually correct rather than merely accepting writes.

A CUTOVER IS FOUR PHASES: bulk transfer, dual-write with comparison, promote
(the mirror becomes primary), detach (one database again). `promote` and
`detach` are functions returning a new Mirror rather than mutating one,
because a caller holding the old value would otherwise silently be talking to
a different database than it thinks.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, TypedDict

from declaro_persistum.database import Database, reading, writing

__all__ = [
    "Mirror",
    "Divergence",
    "mirror",
    "mirror_writing",
    "mirror_reading",
    "promote",
    "detach",
]


class Divergence(TypedDict):
    """One place the two databases disagreed."""

    sql: str
    primary: Any
    replica: Any


class Mirror(TypedDict):
    """Two databases being kept in step during a cutover."""

    primary: Database
    replica: Database
    fail_open: bool
    compare_on_read: bool
    divergences: list[Divergence]


def mirror(
    primary: Database,
    replica: Database,
    fail_open: bool,
    compare_on_read: bool,
) -> Mirror:
    """Build a mirror. Both policies are required — see the module docstring."""
    return {
        "primary": primary,
        "replica": replica,
        "fail_open": fail_open,
        "compare_on_read": compare_on_read,
        "divergences": [],
    }


@asynccontextmanager
async def mirror_writing(m: Mirror) -> AsyncIterator[tuple[Any, Any]]:
    """Yield both write connections. The caller writes the same thing to each.

    Both are yielded rather than one wrapper that forwards, because a caller
    doing a multi-statement transaction needs to control the order and the
    commit on each side, and a forwarding wrapper would have to guess.
    """
    async with writing(m["primary"]) as a, writing(m["replica"]) as b:
        yield a, b


@asynccontextmanager
async def mirror_reading(m: Mirror) -> AsyncIterator[Any]:
    """Yield the primary's read connection.

    Reads come from the primary during a cutover. The replica is not
    authoritative until `promote`, and reading from it would hide exactly the
    divergence the cutover exists to find.
    """
    async with reading(m["primary"]) as conn:
        yield conn


async def compare(m: Mirror, sql: str, params: Any) -> tuple[Any, Mirror]:
    """Run a read against both and record any disagreement.

    Returns the PRIMARY's answer and a new Mirror carrying any divergence.
    The primary's answer is authoritative either way — this reports, it does
    not adjudicate.

    A mirror failure is swallowed when `fail_open`, because the cutover must
    not take production down; it is raised otherwise. Either way the
    divergence list is what tells you whether the mirror is ready.
    """
    async with reading(m["primary"]) as conn:
        cur = await conn.execute(sql, params)
        primary_rows = await cur.fetchall()

    if not m["compare_on_read"]:
        return primary_rows, m

    try:
        async with reading(m["replica"]) as conn:
            cur = await conn.execute(sql, params)
            replica_rows = await cur.fetchall()
    except Exception:
        if not m["fail_open"]:
            raise
        return primary_rows, m

    if primary_rows != replica_rows:
        divergence: Divergence = {
            "sql": sql,
            "primary": primary_rows,
            "replica": replica_rows,
        }
        return primary_rows, {**m, "divergences": [*m["divergences"], divergence]}

    return primary_rows, m


def promote(m: Mirror) -> Mirror:
    """Phase 3: the replica becomes the primary.

    Returns a new Mirror rather than mutating, so a caller still holding the
    old value keeps talking to the database it thinks it is talking to. Both
    are still written; only the authority for reads has moved.
    """
    return {**m, "primary": m["replica"], "replica": m["primary"]}


def detach(m: Mirror) -> Database:
    """Phase 4: one database again. Returns the survivor."""
    return m["primary"]


async def parallel_write(
    m: Mirror, sql: str, params: Any
) -> Mirror:
    """Write to both, at the same time, and report a mirror failure per policy.

    The two writes run concurrently rather than in sequence: a cutover's
    latency is the caller's latency, and doing them one after the other would
    double it for the whole duration of the migration.
    """

    async def to(db: Database) -> None:
        async with writing(db) as conn:
            await conn.execute(sql, params)
            await conn.commit()

    primary_task = asyncio.create_task(to(m["primary"]))
    replica_task = asyncio.create_task(to(m["replica"]))

    # The primary's failure is always the caller's.
    await primary_task

    try:
        await replica_task
    except Exception:
        if not m["fail_open"]:
            raise
        divergence: Divergence = {"sql": sql, "primary": "ok", "replica": "failed"}
        return {**m, "divergences": [*m["divergences"], divergence]}

    return m
