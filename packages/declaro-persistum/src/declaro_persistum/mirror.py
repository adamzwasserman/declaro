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

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, TypedDict

from declaro_persistum.boundary import boundary, helper
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


@helper
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


@helper
@asynccontextmanager
async def mirror_writing(m: Mirror) -> AsyncIterator[tuple[Any, Any]]:
    """Yield both write connections. The caller writes the same thing to each.

    Both are yielded rather than one wrapper that forwards, because a caller
    doing a multi-statement transaction needs to control the order and the
    commit on each side, and a forwarding wrapper would have to guess.
    """
    async with writing(m["primary"]) as a, writing(m["replica"]) as b:
        yield a, b


@helper
@asynccontextmanager
async def mirror_reading(m: Mirror) -> AsyncIterator[Any]:
    """Yield the primary's read connection.

    Reads come from the primary during a cutover. The replica is not
    authoritative until `promote`, and reading from it would hide exactly the
    divergence the cutover exists to find.
    """
    async with reading(m["primary"]) as conn:
        yield conn


# WHAT A MIRROR FAILURE MEANS, AS A TABLE ON `fail_open`. Both entries take
# the same three things and both return, so neither reads as a path nothing
# takes.
@helper
def _kept_to_the_mirror(m: Mirror, primary_rows: Any, _error: Exception) -> tuple[Any, Mirror]:
    """fail_open: the primary's answer stands and the failure goes no further."""
    return primary_rows, m


@helper
def _handed_to_the_caller(_m: Mirror, _primary_rows: Any, error: Exception) -> tuple[Any, Mirror]:
    """fail_closed: by now the mirror is production, so its failure is the caller's."""
    raise error


_WHEN_THE_MIRROR_FAILS: dict[bool, Callable[[Mirror, Any, Exception], tuple[Any, Mirror]]] = {
    True: _kept_to_the_mirror,
    False: _handed_to_the_caller,
}


@helper
def _recorded(m: Mirror, sql: str, primary_rows: Any, replica_rows: Any) -> tuple[Any, Mirror]:
    """The two disagreed, so the disagreement joins the list."""
    divergence: Divergence = {
        "sql": sql,
        "primary": primary_rows,
        "replica": replica_rows,
    }
    return primary_rows, {**m, "divergences": [*m["divergences"], divergence]}


@helper
def _agreed(m: Mirror, _sql: str, primary_rows: Any, _replica_rows: Any) -> tuple[Any, Mirror]:
    """The two answered the same thing, so there is nothing to record."""
    return primary_rows, m


_WHEN_THEY_DIFFER: dict[bool, Callable[[Mirror, str, Any, Any], tuple[Any, Mirror]]] = {
    True: _recorded,
    False: _agreed,
}


@boundary
async def _read(db: Database, sql: str, params: Any) -> Any:
    """One read, on its own connection."""
    async with reading(db) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


@boundary
async def _against_the_replica(
    m: Mirror, sql: str, params: Any, primary_rows: Any
) -> tuple[Any, Mirror]:
    """compare_on_read is on, so the same read runs against the replica."""
    try:
        replica_rows = await _read(m["replica"], sql, params)
    except Exception as error:
        return _WHEN_THE_MIRROR_FAILS[m["fail_open"]](m, primary_rows, error)
    return _WHEN_THEY_DIFFER[primary_rows != replica_rows](m, sql, primary_rows, replica_rows)


@helper
async def _the_primary_alone(
    m: Mirror, _sql: str, _params: Any, primary_rows: Any
) -> tuple[Any, Mirror]:
    """compare_on_read is off, so the primary's answer is the whole answer."""
    return primary_rows, m


_WHEN_COMPARING: dict[bool, Any] = {
    True: _against_the_replica,
    False: _the_primary_alone,
}


@helper
async def compare(m: Mirror, sql: str, params: Any) -> tuple[Any, Mirror]:
    """Run a read against both and record any disagreement.

    Returns the PRIMARY's answer and a new Mirror carrying any divergence.
    The primary's answer is authoritative either way — this reports, it does
    not adjudicate.

    A mirror failure is swallowed when `fail_open`, because the cutover must
    not take production down; it is raised otherwise. Either way the
    divergence list is what tells you whether the mirror is ready.
    """
    primary_rows = await _read(m["primary"], sql, params)
    result: tuple[Any, Mirror] = await _WHEN_COMPARING[m["compare_on_read"]](
        m, sql, params, primary_rows
    )
    return result


@helper
def promote(m: Mirror) -> Mirror:
    """Phase 3: the replica becomes the primary.

    Returns a new Mirror rather than mutating, so a caller still holding the
    old value keeps talking to the database it thinks it is talking to. Both
    are still written; only the authority for reads has moved.
    """
    return {**m, "primary": m["replica"], "replica": m["primary"]}


@helper
def detach(m: Mirror) -> Database:
    """Phase 4: one database again. Returns the survivor."""
    return m["primary"]
