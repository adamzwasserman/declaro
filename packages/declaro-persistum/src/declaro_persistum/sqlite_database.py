"""Opening a SQLite database.

A connection per block, opened and closed by `reading` and `writing`. SQLite
connections are cheap and nothing here holds one: the resource lives in the
block (Rule 12).

WAL, because it is what lets a reader run while a writer holds the write lock.
`busy_timeout` is what makes a concurrent writer wait rather than fail at once,
and it is a required argument: how long a caller is willing to wait for a lock
is a property of that caller's workload, not something this module can know.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.database import (
    Database,
    ShutdownPolicy,
    already_delivered,
    check_shutdown,
    new_database,
    no_passes,
    writing,
)
from declaro_persistum.types import DbapiConnection, Dialect
from declaro_persistum.writers import WRITERS

__all__ = ["open_sqlite", "connect_sqlite"]

DIALECT: Dialect = "sqlite"


@boundary
async def connect_sqlite(db: Database) -> Any:
    """Open one connection to the file this database names."""
    import aiosqlite

    conn = await aiosqlite.connect(db["path"], timeout=db["busy_timeout_s"])
    await conn.execute(f"PRAGMA journal_mode={db['journal_mode']}")
    await conn.execute(f"PRAGMA busy_timeout={int(db['busy_timeout_s'] * 1000)}")
    return conn


@helper
async def _close_connection(conn: DbapiConnection) -> int:
    await conn.close()
    return 1


@helper
async def open_sqlite(
    path: str,
    *,
    shutdown: ShutdownPolicy,
    busy_timeout_s: float,
) -> Database:
    """Open a SQLite database.

        db = await open_sqlite("./app.db", shutdown="exit_immediately",
                               busy_timeout_s=5.0)

    There is no `dialect` argument: this opener knows what it opens, and a
    caller cannot ask for a SQLite database that claims to be something else.

    A SQLite database has no primary, so it never replicates. `replicate(db)`
    raises on it rather than returning a success nobody earned.

    Writers are NOT serialised here by a lock. SQLite takes one writer and
    `BEGIN IMMEDIATE` fails at the start of a transaction rather than part-way
    through it; `busy_timeout_s` is how long a writer waits for the lock before
    that failure. Concurrency comes from the crew (crew.py), which bounds how
    many writes are in flight rather than how many connections exist.
    """
    check_shutdown(shutdown, can_replicate=False, opener="open_sqlite")

    import asyncio

    db = new_database(
        path=path,
        dialect=DIALECT,
        journal_mode="wal",
        primary=None,
        token=None,
        connect=connect_sqlite,
        for_ddl=writing,  # DDL and writes take the same door here
        close_connection=_close_connection,
        serialise=None,
        shutdown=shutdown,
        busy_timeout_s=busy_timeout_s,
        write_one=WRITERS[DIALECT],
        # SQLite writes to a local file: there is no primary.
        replicate_once=already_delivered,
        # No replica to pull from on SQLite.
        refresh_once=no_passes,
        # No primary to replicate to at shutdown on SQLite.
        release=no_passes,
        sleep=asyncio.sleep,
        retry_delay_s=0.001,
    )
    from declaro_persistum.shutdown import trap_shutdown

    trap_shutdown(db)
    return db
