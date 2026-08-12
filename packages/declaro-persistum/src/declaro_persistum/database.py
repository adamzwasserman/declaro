"""A database is a value. Functions open it, read it, write it, close it.

THE WORD "POOL" IS ABOLISHED HERE, and not as a rename. There was nothing
pooled left to name. On a replicated database persistum held exactly one write
connection — one is not a pool. On a local database it opened a connection per
write and closed it — nothing retained, so nothing pooled. The class was called
`ConnectionPool` after a mechanism that had already been removed, and the name
went on shaping how the code was reasoned about long after the free list was
gone.

What the thing IS: a local database file, sometimes with a primary in the
cloud. So it is called a `Database`, it is a TypedDict, and the operations are
functions that take it.

    db = await open_turso(path)                      local only
    db = await open_turso(path, primary=..., token=...)   with a primary

    async with reading(db) as conn: ...
    async with writing(db) as conn: ...

    await flush(db)      block until local writes reach the primary
    await close(db)      final flush, then release everything

THE CONNECTION IS NEVER STORED ON ANYTHING. `reading` and `writing` are
context managers, so a connection exists for the span of the block and not one
statement longer (Rule 12). What `Database` carries is configuration and the
injected callables — never a live handle a caller could reach around and use.

CALLABLES ARE INJECTED, following honest-persist's
`open_pool(db_id, connect, classify, close, size)`. `connect` and `close` come
in as arguments, so the same functions drive SQLite, Turso and PostgreSQL, and
a test drives them against a real database rather than a fake.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, TypedDict

from declaro_persistum.exceptions import PoolClosedError

__all__ = [
    "Database",
    "new_database",
    "reading",
    "writing",
    "is_replicated",
    "replicate",
    "refresh",
    "flush",
    "close",
]


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
    primary: str | None
    token: str | None
    connect: Callable[..., Any]
    close_connection: Callable[..., Any]
    serialise: Any
    closed: bool
    replicate_once: Callable[..., Any]
    refresh_once: Callable[..., Any]
    release: Callable[..., Any]
    sleep: Callable[..., Any]
    retry_delay_s: float


def new_database(
    path: str,
    primary: str | None,
    token: str | None,
    connect: Callable[..., Any],
    close_connection: Callable[..., Any],
    serialise: Any,
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
        "primary": primary,
        "token": token,
        "connect": connect,
        "close_connection": close_connection,
        "serialise": serialise,
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
        raise PoolClosedError("Database has been closed")


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
    lock = db["serialise"]
    if lock is None:
        conn = await db["connect"](db)
        try:
            yield conn
        finally:
            await db["close_connection"](conn)
        return

    async with lock:
        conn = await db["connect"](db)
        try:
            yield conn
        finally:
            await db["close_connection"](conn)


# ---------------------------------------------------------------------------
# Replication — and this is where "replicate" IS the correct word.
#
# "pool" was abolished because nothing was pooled. Replication is not a
# euphemism for anything: two copies of one database exist, and these
# functions bring them into conformity. The word stays, and it is the ONLY
# word for it — "sync" now means synchronous and nothing else.
# ---------------------------------------------------------------------------


async def replicate(db: Database) -> bool:
    """Send local commits to the primary. Returns whether they landed.

    A local-only database has no primary, so there is nothing to replicate
    and this is true by vacuity rather than by success.
    """
    if not is_replicated(db):
        return True
    _check_open(db)
    return await db["replicate_once"](db)


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
    while not await replicate(db):
        await db["sleep"](db["retry_delay_s"])


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
