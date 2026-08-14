"""Build a Turso `Database`. The only module that knows pyturso exists.

`database.py` defines the shape and the operations; every callable in it is
injected. This is where they come from, which keeps the driver in one place
and lets the rest of the package — and every test — work against a `Database`
without importing an engine.

THE ENGINE CHOICE IS MADE HERE AND NOWHERE ELSE, from one fact:

    primary set  ->  replicated  ->  WAL,  writers serialised
    no primary   ->  local       ->  MVCC, writers concurrent

WHY, MEASURED 2026-08-12 against a real replica, pyturso 0.7.2, one variable
at a time — 2 journal modes x 4 preludes, every cell run:

    does a SECOND connection open on the same replica?
      wal  / nothing written    OPENED
      wal  / after CREATE       OPENED
      wal  / after commit       OPENED
      wal  / after push         OPENED
      mvcc / nothing written    OPENED
      mvcc / after CREATE       REFUSED  "database tape error: database is busy"
      mvcc / after commit       REFUSED
      mvcc / after push         REFUSED

Under MVCC on a replicated database, once anything has been written no second
connection can open — persistent, not transient: 15 retries over ~35s did not
get one. MVCC's whole benefit is concurrency ACROSS connections, so on a
replicated database it delivers none while still carrying the stranding that
got 0.1.29 yanked.

Under WAL many connections open, but the engine rejects the second WRITER: 8
open with no lock, 1 of 8 writes landed. That is what the serialisation lock
is for, and it costs no concurrency MVCC could have recovered.

So neither mode gives a replicated database concurrent writers, and WAL is the
one that fails safely. Earlier versions of this reasoning claimed MVCC "cannot
run" on a replica (false — it runs) and blamed unreconcilable internal tables
(never proven). Both retracted.

A REPLICATED DATABASE HOLDS ONE CONNECTION. Opening one per write is
declaro-dna, which took a consumer's box down at 20 concurrent signups where
0.1.x had seeded 200.

A LOCAL DATABASE opens one per `writing` block, which is right for a single
writer. CONCURRENCY COMES FROM THE CREW, not from this module: `crew.py` runs
N drainers, each holding its own connection, and that reuse is the larger of
the two levers. Do not read `writing(db)` as the concurrent path — it is the
one-writer path, and reaching for it in a loop is how this package has
arrived at connection-per-write three separate times.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from declaro_persistum.database import (
    Database,
    ShutdownPolicy,
    new_database,
    new_write_lock,
)
from declaro_persistum.writers import WRITERS

__all__ = ["open_turso", "migrating", "replicate_path"]

logger = logging.getLogger(__name__)

# A primary under continuous write can answer "there may be more" forever.
# Reaching this bound means the copy is not converging, which is worth saying
# out loud rather than looping in silence.
_MAX_PULL_BATCHES = 64

# Bound once so the field and the writer cannot disagree. Two literals is two
# chances for a Database to say it is one engine and write like another.
DIALECT = "turso"

# How long a writer waits for the replica lock before failing. A property of
# the engine's own retry behaviour, not of any caller.
_BUSY_TIMEOUT_S = 5.0


async def _pull_until_level(conn: Any) -> bool:
    """Keep pulling until the primary reports nothing further.

    pyturso's `pull()` fetches ONE batch and returns whether it fetched
    anything — "there may be more". Calling it once and discarding the answer
    leaves a copy part-way through and reports it current.

    Measured 2026-08-13 against a real replica: a copy re-claimed after the
    primary moved on shows nothing after connect, gains the missing table on
    pull #1, and pull #2 returns False. One call was never enough.
    """
    fetched_any = False
    for _ in range(_MAX_PULL_BATCHES):
        if not await conn.pull():
            return fetched_any
        fetched_any = True
    logger.warning(
        "Pulled %d batches and the primary still reports more; the local copy "
        "may not be converging",
        _MAX_PULL_BATCHES,
    )
    return fetched_any


async def replicate_path(path: str, *, primary: str, token: str | None) -> None:
    """Bring a PATH into conformity with its primary, creating it if absent.

    THE SAME VERB WHETHER OR NOT A COPY EXISTS YET. A caller should not have to
    know which case it is in to ask for the same outcome: this path and that
    primary, in conformity, when this returns.

      no local copy   the whole database is copied down before returning
      local copy      local commits go up, then the primary's changes come down

    THIS IS THE PROVISIONING VERB and it has no predecessor. A database can be
    prepared and filled before anyone opens it. It takes its own connection and
    releases it, so it neither disturbs nor depends on an open Database.

    Raises whatever the engine raises. A caller that asked for conformity and
    did not get it is told, rather than left to infer it later from a missing
    table.
    """
    if not primary:
        raise ValueError(
            f"cannot replicate {path}: no primary given. A local-only database "
            f"has nothing to bring into conformity."
        )

    import turso.aio.sync

    conn = await turso.aio.sync.connect(path, remote_url=primary, auth_token=token)
    try:
        await conn.push()
        await _pull_until_level(conn)
    finally:
        with contextlib.suppress(Exception):
            await conn.close()

_DEFAULT_RETRY_DELAY_S = 0.1


async def _set_journal_mode(conn: Any, mode: str) -> str:
    """Request a journal mode and report what the engine granted.

    THE CURSOR MUST BE FETCHED. An unfetched PRAGMA is a silent no-op in
    pyturso: the statement is prepared and never stepped, so the mode does not
    change and the caller believes it did. That cost a whole measurement once —
    WAL was measured and reported as MVCC.
    """
    cursor = await conn.execute(f"PRAGMA journal_mode = '{mode}'")
    row = await cursor.fetchone()
    return row[0] if row else "unknown"


async def connect_local(db: Database) -> Any:
    """Open a connection to a local database in the journal mode it declares.

    The mode is requested, not assumed. The engine has the last word, and a
    refusal is not an error — it means WAL, and the writers still work.
    """
    import turso.aio

    conn = await turso.aio.connect(db["path"])
    await _set_journal_mode(conn, db["journal_mode"])
    return conn


async def migrating(db: Database) -> Any:
    """Open a connection for DDL. ALWAYS WAL, even on a local database.

    MVCC IS BLIND TO ANOTHER CONNECTION'S DDL. Measured 2026-08-12, pyturso
    0.7.2, both connections opened before any write:

        wal  : CREATE TABLE on A -> B sees the table, and its rows
        mvcc : CREATE TABLE on A -> B: "Parse error: no such table: t"

    The blind spot is DDL ONLY. With the schema created under WAL first, two
    MVCC connections see each other's committed INSERTs perfectly — A writes,
    B counts 1; B writes, A counts 2. So MVCC is sound for DML across
    connections and unusable for DDL across them.

    That is why migration takes its own WAL connection rather than the mode a
    write would get. A table created on an MVCC connection is invisible to
    every later writer, which presents as "no such table" on a database whose
    migration just reported success.
    """
    import turso.aio

    conn = await turso.aio.connect(db["path"])
    await _set_journal_mode(conn, "wal")
    return conn




async def _close_connection(conn: Any) -> None:
    await conn.close()


async def open_turso(
    path: str,
    primary: str | None = None,
    token: str | None = None,
    *,
    shutdown: ShutdownPolicy,
) -> Database:
    """Open a Turso database, local or replicated.

        db = await open_turso("/tmp/app.db", shutdown="exit_immediately")                     local
        db = await open_turso("/tmp/app.db", primary=..., token=..., shutdown="exit_immediately")

    `primary` decides everything else. There is no engine argument, and adding
    one would put the choice back on a call site that cannot know the
    measurements above.

    `shutdown` IS REQUIRED — see ShutdownPolicy. On ephemeral disk a default
    would silently pick the losing side of the exact failure it exists to
    prevent, and a default cannot tell "chose this" from "never knew there was
    a choice".

    A WARM OPEN DOES NOT REPLICATE. The schema is already on local disk, so
    only data can be behind, and that is the eventual consistency the caller
    asked for. A COLD open copies the whole database before returning, because
    a database with no schema is unusable rather than merely stale — and that
    copy is pyturso's `bootstrap_if_empty`, not persistum's. Measured
    2026-08-13: 2.8s alone, 20-25s for 25 cold opens at once, because they
    serialize.
    """
    db = (
        _open_local(path, shutdown)
        if primary is None
        else await _open_replicated(path, primary, token, shutdown)
    )
    # THE POLICY IS ACTED ON HERE, not left for a caller to remember. A
    # `shutdown` field that is stored and never read is a swallowed argument,
    # and "whoever remembers to call trap_shutdown" is the same shape as
    # "whoever remembers to call close" — which is the shape this exists to
    # remove. trap_shutdown returns immediately for "exit_immediately".
    from declaro_persistum.shutdown import trap_shutdown

    trap_shutdown(db)
    return db


def _open_local(path: str, shutdown: ShutdownPolicy) -> Database:
    """A local database: MVCC, no lock, a connection per `writing` block.

    `writing(db)` is the SINGLE-WRITER door and opening a connection per call
    is correct for it. It is NOT how you get concurrency — that is the crew
    (`crew.py`), where each drainer holds one connection for its whole life,
    which is the larger of the two throughput levers (6.01x reuse, 18.87x
    compounded with MVCC).

    There is no lock here because a local database is where MVCC actually
    pays. A lock would serialise the crew and throw the reason for MVCC away.

    Nothing to replicate, so the replication callables are inert rather than
    absent — `database.replicate` short-circuits on `is_replicated` before it
    reaches them, and a function that is never called is still better than a
    None someone has to check for.
    """

    async def nothing_to_replicate(db: Database) -> bool:
        return True

    async def nothing_to_refresh(db: Database) -> None:
        return None

    async def release(db: Database) -> None:
        return None

    return new_database(
        path=path,
        dialect=DIALECT,
        journal_mode="mvcc",
        busy_timeout_s=_BUSY_TIMEOUT_S,
        primary=None,
        token=None,
        connect=connect_local,
        close_connection=_close_connection,
        serialise=None,  # MVCC: writers run concurrently, which is the point
        shutdown=shutdown,
        write_one=WRITERS[DIALECT],
        replicate_once=nothing_to_replicate,
        refresh_once=nothing_to_refresh,
        release=release,
        sleep=asyncio.sleep,
        retry_delay_s=_DEFAULT_RETRY_DELAY_S,
    )


async def _open_replicated(
    path: str, primary: str, token: str | None, shutdown: ShutdownPolicy
) -> Database:
    """A replicated database: WAL, serialised writers, ONE held connection.

    The connection is opened here and handed to every write, because opening
    one per write is declaro-dna — one sync handshake and one OS worker thread
    per write, which took a consumer's box down at 20 concurrent signups.
    """
    import turso.aio.sync

    held = await turso.aio.sync.connect(path, remote_url=primary, auth_token=token)
    await _set_journal_mode(held, "wal")

    async def connect(db: Database) -> Any:
        return held

    async def close_connection(conn: Any) -> None:
        # The held connection outlives every write. `close(db)` releases it.
        return None

    async def replicate_once(db: Database) -> bool:
        try:
            await held.push()
            return True
        except Exception:
            # A failed push is ordinary — the network is not always there. The
            # caller decides how long to keep trying; `flush` retries until it
            # lands, and the return value is what it loops on.
            return False

    async def refresh_once(db: Database) -> None:
        # KEEPS ASKING UNTIL THE PRIMARY REPORTS NOTHING FURTHER. This called
        # `pull()` exactly once. pyturso's pull fetches ONE batch and returns
        # "there may be more", so a copy more than one batch behind was left
        # part-way and reported current.
        await _pull_until_level(held)

    async def release(db: Database) -> None:
        await held.close()

    return new_database(
        path=path,
        dialect=DIALECT,
        journal_mode="wal",
        busy_timeout_s=_BUSY_TIMEOUT_S,
        primary=primary,
        token=token,
        connect=connect,
        close_connection=close_connection,
        serialise=new_write_lock(asyncio.Lock()),  # WAL: one writer
        shutdown=shutdown,
        write_one=WRITERS[DIALECT],
        replicate_once=replicate_once,
        refresh_once=refresh_once,
        release=release,
        sleep=asyncio.sleep,
        retry_delay_s=_DEFAULT_RETRY_DELAY_S,
    )
