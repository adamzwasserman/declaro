"""pyturso driver adapters: a holder, a connection, and a cursor.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). These are DB-API shims over pyturso, not pooling
— the pools use them, nothing here knows a pool exists.

`_TursoConnectionHolder` owns the pyturso connection and supports both
modes: async via turso.aio / turso.aio.sync, and the blocking API on a
dedicated executor thread for the sync pools.

Worth knowing while reading this: pyturso's async API is a worker THREAD
per connection wrapping the blocking driver (see turso/lib_aio.py), not
native async I/O. `await` frees the event loop; it does not make the
engine concurrent. Concurrency comes from separate connections. The cost
numbers are in pool.py's module docstring.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)


class _TursoConnectionHolder:
    """
    Holds a pyturso connection.

    Supports two modes:
    - Sync (legacy): turso.connect() on a dedicated executor thread.
      Used by SyncTursoPool. All operations go through sync methods.
    - Async (new): turso.aio / turso.aio.sync for natively async I/O.
      Used by TursoPool. No executor or threading needed.

    When remote_url is provided, connect_async() uses turso.aio.sync
    to enable push()/pull() cloud synchronisation.
    """

    def __init__(self, database_path: str, remote_url: str | None = None, auth_token: str | None = None) -> None:
        self.database_path = database_path
        self._remote_url = remote_url
        self._auth_token = auth_token
        self.conn: Any = None

    # ------------------------------------------------------------------
    # Async path (turso.aio / turso.aio.sync) — used by TursoPool
    # ------------------------------------------------------------------

    async def connect_async(self) -> None:
        """Open async connection with optional cloud sync."""
        if self._remote_url:
            import turso.aio.sync

            self.conn = await turso.aio.sync.connect(
                self.database_path,
                remote_url=self._remote_url,
                auth_token=self._auth_token,
            )
        else:
            import turso.aio

            self.conn = await turso.aio.connect(self.database_path)

    async def push(self) -> None:
        """Push local commits to Turso Cloud."""
        if hasattr(self.conn, "push"):
            await self.conn.push()

    async def pull(self) -> None:
        """Pull remote changes into local."""
        if hasattr(self.conn, "pull"):
            await self.conn.pull()

    # ------------------------------------------------------------------
    # Sync path (turso.connect) — used by SyncTursoPool
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open connection (must be called on executor thread)."""
        import turso

        self.conn = turso.connect(self.database_path)

    def execute(self, sql: str, parameters: tuple[Any, ...]) -> tuple[list[Any], Any, int]:
        cursor = self.conn.cursor()
        cursor.execute(sql, parameters)
        try:
            rows = cursor.fetchall()
        except Exception:
            rows = []
        return (rows, cursor.description, cursor.rowcount)

    def executemany(self, sql: str, parameters: list[Any]) -> int:
        cursor = self.conn.cursor()
        cursor.executemany(sql, parameters)
        return cursor.rowcount

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def sync(self) -> None:
        """Sync the database (pyturso-specific operation)."""
        if hasattr(self.conn, "sync"):
            self.conn.sync()

    def close(self) -> None:
        """Close and release connection (must be on same thread as connect)."""
        if self.conn is not None:
            # Use context manager exit to ensure proper thread cleanup
            self.conn.__exit__(None, None, None)
            self.conn = None


class TursoAsyncConnection:
    """
    Async wrapper for pyturso connections.

    Supports two modes determined by whether an executor is supplied:
    - **Native async** (executor=None): the holder contains a
      turso.aio / turso.aio.sync connection. All calls delegate
      directly — no threading overhead.
    - **Legacy sync** (executor provided): the holder contains a
      turso.connect() connection. Calls are dispatched via
      run_in_executor so the sync driver never blocks the event loop.
    """

    _declaro_dialect = "declaro_persistum.pool.turso"

    def __init__(
        self, holder: _TursoConnectionHolder, executor: ThreadPoolExecutor | None = None
    ) -> None:
        self._holder = holder
        self._executor = executor
        self._loop = asyncio.get_event_loop()
        self._closed = False

    async def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> TursoAsyncCursor:
        """Execute SQL and return an async cursor with pre-fetched results."""
        if self._executor is not None:
            rows, description, rowcount = await self._loop.run_in_executor(
                self._executor, self._holder.execute, sql, parameters
            )
            return TursoAsyncCursor(rows, description, rowcount)
        # Native async path
        cursor = await self._holder.conn.execute(sql, parameters)
        try:
            rows = await cursor.fetchall()
        except Exception:
            rows = []
        return TursoAsyncCursor(
            rows,
            getattr(cursor, "description", None),
            getattr(cursor, "rowcount", 0),
        )

    async def executemany(self, sql: str, parameters: list[tuple]) -> TursoAsyncCursor:
        """Execute SQL with multiple parameter sets."""
        if self._executor is not None:
            rowcount = await self._loop.run_in_executor(
                self._executor, self._holder.executemany, sql, parameters
            )
            return TursoAsyncCursor([], None, rowcount)
        cursor = await self._holder.conn.executemany(sql, parameters)
        return TursoAsyncCursor([], None, getattr(cursor, "rowcount", 0))

    async def commit(self) -> None:
        """Commit the current transaction."""
        if self._executor is not None:
            await self._loop.run_in_executor(self._executor, self._holder.commit)
            await self.sync()
        else:
            await self._holder.conn.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._executor is not None:
            await self._loop.run_in_executor(self._executor, self._holder.rollback)
        else:
            await self._holder.conn.rollback()

    async def sync(self) -> None:
        """Sync the database."""
        if self._executor is not None:
            await self._loop.run_in_executor(self._executor, self._holder.sync)
        else:
            await self._holder.pull()

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        self._closed = True
        if self._executor is not None:
            await self._loop.run_in_executor(self._executor, self._holder.close)
        elif self._holder.conn is not None:
            await self._holder.conn.close()
            self._holder.conn = None


class TursoAsyncCursor:
    """
    Async cursor wrapper with pre-fetched results.

    Since pyturso cursors can't cross threads, we pre-fetch all results
    when execute() is called and store them in this wrapper.
    """

    def __init__(self, rows: list[Any], description: Any, rowcount: int) -> None:
        self._rows = rows
        self._description = description
        self._rowcount = rowcount
        self._position = 0

    async def fetchone(self) -> Any:
        """Fetch one row."""
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    async def fetchall(self) -> list[Any]:
        """Fetch all remaining rows."""
        rows = self._rows[self._position :]
        self._position = len(self._rows)
        return rows

    async def fetchmany(self, size: int = 1) -> list[Any]:
        """Fetch many rows."""
        rows = self._rows[self._position : self._position + size]
        self._position += len(rows)
        return rows

    @property
    def description(self) -> Any:
        """Column descriptions."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected."""
        return self._rowcount
