"""A database is a value. Functions open it, read it, write it, close it.

A local database file, sometimes with a primary in the cloud. So it is called a
`Database`, it is a TypedDict, and the operations are functions that take it.

    db = await open_turso(path, shutdown=...)                    local only
    db = await open_turso(path, primary=..., token=..., shutdown=...)

    async with reading(db) as conn: ...
    async with writing(db) as conn: ...

    await flush(db)      block until local writes reach the primary
    await close(db)      final replication, then release everything

THE CONNECTION IS NEVER STORED ON ANYTHING. `reading` and `writing` are context
managers, so a connection exists for the span of the block and not one
statement longer (Rule 12). What `Database` carries is configuration and the
injected callables — never a live handle a caller could reach around and use.

CALLABLES ARE INJECTED. `connect` and `close_connection` come in as arguments,
so the same functions drive SQLite, Turso and PostgreSQL, and a test drives
them against a real database rather than a fake.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal, TypedDict

from declaro_persistum.exceptions import DatabaseClosedError

logger = logging.getLogger(__name__)

__all__ = [
    "Database",
    "ShutdownPolicy",
    "WriteLock",
    "new_database",
    "new_write_lock",
    "writers_waiting",
    "reading",
    "writing",
    "is_replicated",
    "replicate",
    "replicate_if_idle",
    "replication_loop",
    "replicate_on_shutdown",
    "refresh",
    "flush",
    "close",
]


# WHAT HAPPENS TO UNREPLICATED WRITES WHEN THE PROCESS IS SIGNALLED.
# No default, and not for symmetry with Rule 14 for its own sake. persistum's
# usual home has ephemeral disk, where anything not replicated when the process
# dies is gone — so a default would silently pick the losing side of the exact
# failure this exists to prevent, for every caller who never read the changelog.
#
#   "replicate"         trap SIGTERM/SIGINT, replicate to completion, then exit
#   "exit_immediately"  install no handler; exit without replicating
#
# "exit_immediately" is a real answer — a read-only copy, a test, a host that
# already calls close(). Stating it is the difference between a decision and an
# omission, and a default cannot tell those apart.
ShutdownPolicy = Literal["replicate", "exit_immediately"]


class WriteLock(TypedDict):
    """The serialise lock, and the count of writers waiting on it.

    THE COUNT LIVES WITH THE LOCK BECAUSE IT DESCRIBES THE LOCK. Opportunistic
    replication yields when a writer is waiting, and the resource it yields is
    this one: replication goes out on the held connection, and so does every
    write. Counting anything else — CPU, active readers, a queue length — is a
    cheaper signal standing in for the real noun, which is the substitution
    behind every replication incident this package has had.

    A reader is not counted. `reading` takes no lock, so a reader never
    contends with replication and must never defer it.
    """

    lock: Any
    waiting: int


def new_write_lock(lock: Any) -> WriteLock:
    return {"lock": lock, "waiting": 0}


def writers_waiting(db: Database) -> int:
    """How many writers are blocked on the serialise lock right now."""
    serialise = db["serialise"]
    return 0 if serialise is None else serialise["waiting"]


class Database(TypedDict):
    """A database, as data.

    `path` is the local file. `primary` is the cloud database it replicates
    to, or None when there is none — and that single field decides everything
    else, which is why it is a field and not an argument anyone can pass.

    `connect` and `close_connection` are the injected boundary. `serialise` is
    the lock a replicated database needs and a local one does not; it is None
    for local, which is the whole difference expressed as data rather than as
    a branch someone has to remember.
    """

    path: str
    dialect: str
    primary: str | None
    token: str | None
    connect: Callable[..., Any]
    close_connection: Callable[..., Any]
    serialise: WriteLock | None
    shutdown: ShutdownPolicy
    write_one: Callable[..., Any]
    closed: bool
    replicate_once: Callable[..., Any]
    refresh_once: Callable[..., Any]
    release: Callable[..., Any]
    sleep: Callable[..., Any]
    retry_delay_s: float


def new_database(
    path: str,
    dialect: str,
    primary: str | None,
    token: str | None,
    connect: Callable[..., Any],
    close_connection: Callable[..., Any],
    serialise: WriteLock | None,
    shutdown: ShutdownPolicy,
    write_one: Callable[..., Any],
    replicate_once: Callable[..., Any],
    refresh_once: Callable[..., Any],
    release: Callable[..., Any],
    sleep: Callable[..., Any],
    retry_delay_s: float,
) -> Database:
    """Build the value. Every argument is required — see Rule 14.

    No defaults, so a caller who omits `primary` cannot be mistaken for one
    who chose local. The factories in `turso_database.py` and friends are
    where a human-facing signature with sensible arguments belongs; this is
    the shape underneath it.
    """
    return {
        "path": path,
        "dialect": dialect,
        "primary": primary,
        "token": token,
        "connect": connect,
        "close_connection": close_connection,
        "serialise": serialise,
        "shutdown": shutdown,
        "write_one": write_one,
        "closed": False,
        "replicate_once": replicate_once,
        "refresh_once": refresh_once,
        "release": release,
        "sleep": sleep,
        "retry_delay_s": retry_delay_s,
    }


def is_replicated(db: Database) -> bool:
    """True when this database has a primary.

    The one predicate the rest of the library branches on. Measured
    2026-08-12: a replicated database takes ONE connection under MVCC once
    anything has been written, and rejects the second writer under WAL. So
    `is_replicated` is what decides whether writers must be serialised, and
    it is derived from data rather than stored as a second flag that could
    disagree with `primary`.
    """
    return db["primary"] is not None


def _check_open(db: Database) -> None:
    if db["closed"]:
        raise DatabaseClosedError("Database has been closed")


@asynccontextmanager
async def reading(db: Database) -> AsyncIterator[Any]:
    """A connection for reading, for the span of the block.

    Reads never touch the primary — the local file is a full copy — so a
    reader takes no lock and waits for nothing.
    """
    _check_open(db)
    conn = await db["connect"](db)
    try:
        yield conn
    finally:
        await db["close_connection"](conn)


@asynccontextmanager
async def writing(db: Database) -> AsyncIterator[Any]:
    """A connection for writing, for the span of the block.

    On a replicated database the writers are serialised: the engine allows one
    writer, and without the lock 1 of 8 concurrent writes landed and 7 errored
    (measured, real replica, 2026-08-12). On a local database there is no lock
    and no serialisation, because concurrency is the entire reason MVCC is on
    there.
    """
    _check_open(db)
    serialise = db["serialise"]
    if serialise is None:
        conn = await db["connect"](db)
        try:
            yield conn
        finally:
            await db["close_connection"](conn)
        return

    # INCREMENTED AROUND THE AWAIT, not anywhere convenient. A counter that is
    # initialised and never incremented satisfies every test that sets it by
    # hand, which is how machinery that does nothing passes for healthy.
    # Around the await it means exactly: writers currently blocked on the
    # resource replication would take.
    serialise["waiting"] += 1
    try:
        await serialise["lock"].acquire()
    finally:
        serialise["waiting"] -= 1
    try:
        conn = await db["connect"](db)
        try:
            yield conn
        finally:
            await db["close_connection"](conn)
    finally:
        serialise["lock"].release()


# ---------------------------------------------------------------------------
# Replication. Two copies of one database exist, and these functions bring them
# into conformity. "Sync" means synchronous and nothing else.
# ---------------------------------------------------------------------------


async def replicate(db: Database) -> bool:
    """Bring this copy and its primary into conformity. BOTH DIRECTIONS, one pass.

    UP FIRST, THEN DOWN, AND THE ORDER IS LOAD-BEARING. Bringing the primary's
    changes down first could overwrite a local commit that has been waiting
    since the last shutdown. Sending up first makes the local copy the
    primary's problem before the primary becomes the local copy's.

    THIS USED TO SEND ONLY. `replicate` pushed, `refresh` pulled, and the
    comment above them claimed the pair "bring the two copies into conformity"
    — which neither did alone. A caller that only ever called `replicate` had a
    local copy that learned nothing about its primary for the life of the
    process, and nothing said so.

    It also returned True for a local-only database, and its own docstring said
    the value meant nothing: "true by vacuity rather than by success". A caller
    cannot tell that apart from a replication that worked, so it now raises.
    """
    if not is_replicated(db):
        raise ValueError(
            f"cannot replicate {db['path']}: it has no primary. A local-only "
            f"database has nothing to bring into conformity — open it with a "
            f"primary, or do not ask."
        )
    _check_open(db)
    if not await db["replicate_once"](db):
        return False
    await db["refresh_once"](db)
    return True


async def replicate_if_idle(db: Database) -> bool:
    """Replicate only when no writer is waiting. Returns whether a pass ran.

    OPPORTUNISTIC MEANS YIELDING TO REAL WORK. Replication holds the same
    connection every write holds, so a writer waiting on the serialise lock is
    a request paying for the round trip.

    A DEFERRED PASS IS DROPPED, NOT QUEUED. Ten passes skipped under load
    produce one pass when the load clears, not ten — otherwise recovering load
    releases a backlog onto a connection at the moment it can least afford it.
    """
    if db["closed"] or not is_replicated(db):
        return False
    if writers_waiting(db) > 0:
        return False
    return await replicate(db)


async def replication_loop(db: Database, wanted: Any) -> None:
    """Replicate when work arrives and no writer waits. NOTHING IS ON A CLOCK.

    `wanted` is an event a commit sets, and the caller's idleness sets. A timer
    would wake whether or not anything was pending, take the connection to find
    out, and still leave a fresh write sitting for a whole interval.
    """
    while not db["closed"]:
        await wanted.wait()
        # Cleared BEFORE the pass, never after. Clearing afterwards discards a
        # signal that arrived while the pass was running, and that write then
        # waits for an unrelated trigger — a lost wakeup, which is how an
        # eventual-consistency loop quietly stops being eventual.
        wanted.clear()
        if db["closed"]:
            return
        await replicate_if_idle(db)


async def replicate_on_shutdown(db: Database, *, now: Callable[[], float]) -> None:
    """Block until the primary has everything, and SAY SO WHILE IT HAPPENS.

    THE ONE PLACE BLOCKING IS CORRECT, AND THE ONE PLACE POLITENESS IS WRONG.
    On ephemeral disk anything not replicated when the process dies is gone,
    and sustained load is exactly when the most unreplicated data is sitting
    there. So this ignores `writers_waiting` entirely.

    IT LOGS BECAUSE A PROCESS THAT WILL NOT EXIT IS INDISTINGUISHABLE FROM A
    HUNG ONE. Every line carries the path and the elapsed time, so an operator
    can tell work from a stall, and can tell WHICH database is holding up the
    exit when several are open.

    `now` is injected rather than called here, which is what lets a test assert
    the elapsed time appears without sleeping for it.
    """
    if not is_replicated(db):
        return

    started = now()
    logger.info(
        "Shutdown: replicating %s to its primary before exit. The process will "
        "not exit until this completes.",
        db["path"],
    )
    attempt = 0
    while not await db["replicate_once"](db):
        attempt += 1
        logger.warning(
            "Shutdown: replicating %s — attempt %d failed after %.1fs. "
            "Not yet delivered to the primary.",
            db["path"], attempt, now() - started,
        )
        await db["sleep"](db["retry_delay_s"])
    await db["refresh_once"](db)
    logger.info(
        "Shutdown: %s is in conformity with its primary after %.1fs (%d retries).",
        db["path"], now() - started, attempt,
    )


async def refresh(db: Database) -> None:
    """Bring the primary's changes down into the local copy.

    The other direction. Named for what it does to the local file rather
    than "pull", which describes the wire and not the effect.
    """
    if not is_replicated(db):
        return
    _check_open(db)
    await db["refresh_once"](db)


async def flush(db: Database) -> None:
    """Block until every local write has reached the primary.

    The deliberate durability call. Writes reach the primary on a background
    schedule; a caller that needs them there NOW says so here. On ephemeral
    disk, anything not replicated when the process dies is gone.
    """
    if not is_replicated(db):
        return
    # IGNORES LOAD. A caller that awaited this has said it is willing to wait;
    # politeness belongs to the background pass, not here.
    while not await db["replicate_once"](db):
        await db["sleep"](db["retry_delay_s"])
    await db["refresh_once"](db)


async def close(db: Database) -> Database:
    """Replicate whatever is left, then release everything.

    Retries the final replication until it succeeds, because the local file
    may be on disk that does not survive the process. Returns the closed
    value rather than mutating in place, so a caller cannot hold a `Database`
    that silently became unusable.
    """
    if is_replicated(db):
        await flush(db)
    await db["release"](db)
    return {**db, "closed": True}
