"""Opening a PostgreSQL database.

A connection per block, opened and closed by `reading` and `writing`. Nothing
here holds one: the resource lives in the block (Rule 12).

asyncpg owns its transactions, so `postgres_write_one` uses a transaction
context manager rather than a commit, and parameters go positionally. Those
differences live in `writers.py`, not here.
"""

from __future__ import annotations

import asyncio
from typing import Any

from declaro_persistum.database import Database, ShutdownPolicy, new_database
from declaro_persistum.writers import WRITERS

__all__ = ["open_postgresql", "connect_postgresql"]

DIALECT = "postgresql"


async def connect_postgresql(db: Database) -> Any:
    """Open one connection to the server this database names."""
    import asyncpg

    return await asyncpg.connect(db["path"], timeout=db["busy_timeout_s"])


async def _close_connection(conn: Any) -> None:
    await conn.close()


async def _nothing_to_replicate(db: Database) -> bool:
    return True


async def _nothing_to_refresh(db: Database) -> None:
    return None


async def _release(db: Database) -> None:
    return None


async def open_postgresql(
    dsn: str,
    *,
    shutdown: ShutdownPolicy,
    busy_timeout_s: float,
) -> Database:
    """Open a PostgreSQL database.

        db = await open_postgresql("postgresql://localhost/app",
                                   shutdown="exit_immediately",
                                   busy_timeout_s=10.0)

    There is no `dialect` argument: this opener knows what it opens.

    `journal_mode` is "server": PostgreSQL has no journal mode a client sets,
    and the field is required, so it says what is true rather than borrowing
    SQLite's vocabulary.

    Writers are not serialised. PostgreSQL does multi-writer MVCC server-side,
    so a lock here would take away what the server provides.
    """
    db = new_database(
        path=dsn,
        dialect=DIALECT,
        journal_mode="server",
        busy_timeout_s=busy_timeout_s,
        primary=None,
        token=None,
        connect=connect_postgresql,
        close_connection=_close_connection,
        serialise=None,
        shutdown=shutdown,
        write_one=WRITERS[DIALECT],
        replicate_once=_nothing_to_replicate,
        refresh_once=_nothing_to_refresh,
        release=_release,
        sleep=asyncio.sleep,
        retry_delay_s=0.001,
    )
    from declaro_persistum.shutdown import trap_shutdown

    trap_shutdown(db)
    return db
