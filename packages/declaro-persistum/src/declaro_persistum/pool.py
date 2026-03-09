"""
Unified connection pool for declaro_persistum.

Provides a consistent async context manager API for:
- PostgreSQL (asyncpg with native pooling)
- SQLite (aiosqlite with semaphore-based limiting)
- Turso (pyturso with semaphore-based limiting) - embedded SQLite-compatible DB
- LibSQL (libsql_experimental) - Turso cloud connections

Example:
    pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")
    async with pool.acquire() as conn:
        results = await users.select().execute(conn)
    await pool.close()
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from declaro_persistum.exceptions import (
    PoolClosedError,
    PoolConnectionError,
    PoolExhaustedError,
)

if TYPE_CHECKING:
    import aiosqlite
    import asyncpg


class _TursoConnectionHolder:
    """
    Holds a pyturso connection on its creation thread.

    Since pyturso connections cannot cross thread boundaries, we keep
    the connection reference in a container that stays on the executor
    thread. All operations go through this holder.
    """

    def __init__(self, database_path: str) -> None:
        import turso

        self._turso = turso
        self.database_path = database_path
        self.conn: Any = None

    def connect(self) -> None:
        """Open connection (must be called on executor thread)."""
        self.conn = self._turso.connect(self.database_path)

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
    Async wrapper for pyturso synchronous connections.

    Provides an async interface similar to aiosqlite by running
    sync operations in a dedicated thread pool. All pyturso objects
    (Connection, Cursor) must be created, used, and destroyed on the
    same thread, so we use a holder class that stays on the executor.
    """

    def __init__(self, holder: _TursoConnectionHolder, executor: ThreadPoolExecutor) -> None:
        self._holder = holder
        self._executor = executor
        self._loop = asyncio.get_event_loop()
        self._closed = False

    async def execute(self, sql: str, parameters: tuple[Any, ...] = ()) -> TursoAsyncCursor:
        """Execute SQL and return an async cursor with pre-fetched results."""
        rows, description, rowcount = await self._loop.run_in_executor(
            self._executor, self._holder.execute, sql, parameters
        )
        return TursoAsyncCursor(rows, description, rowcount)

    async def executemany(self, sql: str, parameters: list[tuple]) -> TursoAsyncCursor:
        """Execute SQL with multiple parameter sets."""
        rowcount = await self._loop.run_in_executor(
            self._executor, self._holder.executemany, sql, parameters
        )
        return TursoAsyncCursor([], None, rowcount)

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._loop.run_in_executor(self._executor, self._holder.commit)

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self._loop.run_in_executor(self._executor, self._holder.rollback)

    async def sync(self) -> None:
        """Sync the database."""
        await self._loop.run_in_executor(self._executor, self._holder.sync)

    async def close(self) -> None:
        """Close the connection on its thread."""
        if self._closed:
            return
        self._closed = True
        await self._loop.run_in_executor(self._executor, self._holder.close)


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


class BasePool:
    """
    Protocol-style base for connection pools.

    Subclasses implement acquire(), close(), and closed.
    No ABC/abstractmethod — structural subtyping via duck typing.
    Connection pools are inherently stateful (Honest Code exemption
    for file handles, network connections, database cursors).
    """

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        """Acquire a connection from the pool."""
        raise NotImplementedError

    async def close(self) -> None:
        """Close the pool and all connections."""
        raise NotImplementedError

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        raise NotImplementedError


class PostgreSQLPool(BasePool):
    """
    PostgreSQL connection pool using asyncpg.

    Wraps asyncpg's native pool implementation which provides:
    - Connection reuse
    - Automatic reconnection
    - Prepared statement caching
    """

    def __init__(
        self,
        connection_string: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._connection_string = connection_string
        self._min_size = min_size
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._pool: asyncpg.Pool | None = None
        self._closed = False

    async def _ensure_pool(self) -> asyncpg.Pool:
        """Lazily create the pool on first acquire."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        if self._pool is None:
            try:
                import asyncpg

                self._pool = await asyncpg.create_pool(
                    self._connection_string,
                    min_size=self._min_size,
                    max_size=self._max_size,
                )
            except Exception as e:
                raise PoolConnectionError(f"Failed to create PostgreSQL pool: {e}") from e

        return self._pool

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        """Acquire a connection from the pool."""
        pool = await self._ensure_pool()
        try:
            async with asyncio.timeout(self._acquire_timeout):
                async with pool.acquire() as conn:
                    yield conn
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

    async def close(self) -> None:
        """Close the pool and all connections."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        self._closed = True

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Current number of connections in the pool."""
        if self._pool is None:
            return 0
        return int(self._pool.get_size())

    @property
    def available(self) -> int:
        """Number of idle connections available."""
        if self._pool is None:
            return 0
        return int(self._pool.get_idle_size())


class SQLitePool(BasePool):
    """
    SQLite connection pool using aiosqlite.

    SQLite connections are cheap to create, so this uses a semaphore
    to limit concurrent connections (important for WAL mode which
    supports up to ~5 concurrent writers).
    """

    def __init__(
        self,
        database_path: str,
        *,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._database_path = database_path
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._semaphore = asyncio.Semaphore(max_size)
        self._closed = False
        self._active_connections = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[aiosqlite.Connection]:
        """Acquire a connection (creates fresh connection each time)."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        conn = None
        try:
            import aiosqlite

            conn = await aiosqlite.connect(self._database_path)
            # Enable WAL mode for better concurrency
            await conn.execute("PRAGMA journal_mode=WAL")
        except Exception as e:
            self._semaphore.release()
            raise PoolConnectionError(f"Failed to connect to SQLite: {e}") from e

        self._active_connections += 1
        try:
            yield conn
        finally:
            self._active_connections -= 1
            await conn.close()
            self._semaphore.release()

    async def close(self) -> None:
        """Mark the pool as closed."""
        self._closed = True

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Maximum number of concurrent connections."""
        return self._max_size

    @property
    def available(self) -> int:
        """Number of available connection slots."""
        # Semaphore doesn't expose internal counter, so we track it
        return self._max_size - self._active_connections


def _named_to_positional(sql: str, parameters: dict | tuple) -> tuple[str, tuple]:
    """Convert :name parameters to ? positional parameters for libsql_experimental.

    libsql_experimental only accepts ? placeholders with tuple params.
    This converts named parameters (`:name` + dict) to positional (`?` + tuple).
    If parameters is already a tuple, returns as-is.
    """
    if isinstance(parameters, tuple):
        return sql, parameters

    if not isinstance(parameters, dict):
        return sql, parameters

    ordered_values: list = []

    def replacer(match: re.Match) -> str:
        name = match.group(1)
        ordered_values.append(parameters[name])
        return "?"

    positional_sql = re.sub(r":(\w+)", replacer, sql)
    return positional_sql, tuple(ordered_values)


class _LibSQLConnectionHolder:
    """
    Holds a libsql_experimental connection on its creation thread.

    libsql_experimental provides a synchronous DB-API 2.0 interface.
    """

    def __init__(self, url: str, auth_token: str | None) -> None:
        import libsql_experimental as libsql

        self._libsql = libsql
        self.url = url
        self.auth_token = auth_token
        self.conn: Any = None

    def connect(self) -> None:
        """Open connection (must be called on executor thread)."""
        if self.auth_token:
            self.conn = self._libsql.connect(self.url, auth_token=self.auth_token)
        else:
            self.conn = self._libsql.connect(self.url)

    def execute(self, sql: str, parameters: tuple) -> tuple[list, Any, int]:
        cursor = self.conn.cursor()
        cursor.execute(sql, parameters)
        try:
            rows = cursor.fetchall()
        except Exception:
            rows = []
        return (rows, cursor.description, cursor.rowcount)

    def executemany(self, sql: str, parameters: list) -> int:
        cursor = self.conn.cursor()
        cursor.executemany(sql, parameters)
        return cursor.rowcount

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def sync(self) -> None:
        """Sync with Turso cloud (libsql-specific)."""
        if hasattr(self.conn, "sync"):
            self.conn.sync()

    def close(self) -> None:
        """Close connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class _LibSQLEmbeddedConnectionHolder:
    """
    Holds a libsql_experimental embedded replica connection.

    Embedded replicas maintain a local SQLite file that syncs from
    Turso Cloud. Reads hit the local file (sub-millisecond).
    Writes go to cloud and sync back.
    """

    def __init__(self, local_path: str, sync_url: str, auth_token: str) -> None:
        import libsql_experimental as libsql

        self._libsql = libsql
        self.local_path = local_path
        self.sync_url = sync_url
        self.auth_token = auth_token
        self.conn: Any = None

    def connect(self) -> None:
        """Open embedded replica connection (must be called on executor thread)."""
        self.conn = self._libsql.connect(
            self.local_path,
            sync_url=self.sync_url,
            auth_token=self.auth_token,
        )

    def execute(self, sql: str, parameters: tuple) -> tuple[list, Any, int]:
        cursor = self.conn.cursor()
        cursor.execute(sql, parameters)
        try:
            rows = cursor.fetchall()
        except Exception:
            rows = []
        return (rows, cursor.description, cursor.rowcount)

    def executemany(self, sql: str, parameters: list) -> int:
        cursor = self.conn.cursor()
        cursor.executemany(sql, parameters)
        return cursor.rowcount

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def sync(self) -> None:
        """Sync local replica with Turso cloud."""
        self.conn.sync()

    def close(self) -> None:
        """Close connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class LibSQLAsyncConnection:
    """
    Async wrapper for libsql_experimental synchronous connections.

    Provides an async interface by running sync operations in a
    dedicated thread pool.
    """

    def __init__(self, holder: _LibSQLConnectionHolder, executor: ThreadPoolExecutor) -> None:
        self._holder = holder
        self._executor = executor
        self._loop = asyncio.get_event_loop()
        self._closed = False

    async def execute(self, sql: str, parameters: tuple | dict = ()) -> TursoAsyncCursor:
        """Execute SQL and return an async cursor with pre-fetched results."""
        sql, parameters = _named_to_positional(sql, parameters)
        rows, description, rowcount = await self._loop.run_in_executor(
            self._executor, self._holder.execute, sql, parameters
        )
        return TursoAsyncCursor(rows, description, rowcount)

    async def executemany(self, sql: str, parameters: list[tuple]) -> TursoAsyncCursor:
        """Execute SQL with multiple parameter sets."""
        rowcount = await self._loop.run_in_executor(
            self._executor, self._holder.executemany, sql, parameters
        )
        return TursoAsyncCursor([], None, rowcount)

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._loop.run_in_executor(self._executor, self._holder.commit)

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self._loop.run_in_executor(self._executor, self._holder.rollback)

    async def sync(self) -> None:
        """Sync with Turso cloud."""
        await self._loop.run_in_executor(self._executor, self._holder.sync)

    async def close(self) -> None:
        """Close the connection on its thread."""
        if self._closed:
            return
        self._closed = True
        await self._loop.run_in_executor(self._executor, self._holder.close)


class LibSQLPool(BasePool):
    """
    LibSQL connection pool using libsql_experimental.

    Used for Turso cloud connections. Reuses connections across acquire()
    calls to avoid paying DNS/TCP/TLS/Hrana setup on every query.
    """

    def __init__(
        self,
        url: str,
        *,
        auth_token: str | None = None,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._url = url
        self._auth_token = auth_token
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._semaphore = asyncio.Semaphore(max_size)
        self._closed = False
        self._active_connections = 0
        self._idle: list[tuple[_LibSQLConnectionHolder, ThreadPoolExecutor]] = []
        self._lock = asyncio.Lock()

    async def _get_or_create_connection(self) -> tuple[_LibSQLConnectionHolder, ThreadPoolExecutor]:
        """Get an idle connection or create a new one."""
        async with self._lock:
            if self._idle:
                return self._idle.pop()

        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="libsql")
        holder = _LibSQLConnectionHolder(self._url, self._auth_token)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(executor, holder.connect)
        return holder, executor

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[LibSQLAsyncConnection]:
        """Acquire a connection, reusing idle connections when available."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        holder = None
        executor = None
        async_conn = None

        try:
            holder, executor = await self._get_or_create_connection()
            async_conn = LibSQLAsyncConnection(holder, executor)
        except Exception as e:
            if executor:
                executor.shutdown(wait=False)
            self._semaphore.release()
            raise PoolConnectionError(f"Failed to connect to LibSQL: {e}") from e

        self._active_connections += 1
        try:
            yield async_conn
        finally:
            self._active_connections -= 1
            if async_conn:
                async_conn._closed = True  # prevent close() from destroying connection
            # Return to idle pool instead of destroying
            if holder and executor and not self._closed:
                async with self._lock:
                    if len(self._idle) < self._max_size:
                        self._idle.append((holder, executor))
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None, holder.close
                        )
                        executor.shutdown(wait=False)
            elif holder:
                await asyncio.get_event_loop().run_in_executor(None, holder.close)
                if executor:
                    executor.shutdown(wait=False)
            self._semaphore.release()

    async def close(self) -> None:
        """Close the pool and all idle connections."""
        self._closed = True
        async with self._lock:
            for holder, executor in self._idle:
                try:
                    holder.close()
                except Exception:
                    pass
                executor.shutdown(wait=False)
            self._idle.clear()

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Maximum number of concurrent connections."""
        return self._max_size

    @property
    def available(self) -> int:
        """Number of available connection slots."""
        return self._max_size - self._active_connections


class LibSQLEmbeddedPool(BasePool):
    """
    LibSQL embedded replica pool using libsql_experimental.

    Maintains a local SQLite replica that syncs from Turso Cloud.
    Reads hit the local file (sub-millisecond). Writes go to cloud.
    A single connection is reused across acquire() calls.
    Syncs automatically when stale (configurable interval).
    """

    def __init__(
        self,
        local_path: str,
        *,
        sync_url: str,
        auth_token: str,
        sync_interval_seconds: float = 5.0,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._local_path = local_path
        self._sync_url = sync_url
        self._auth_token = auth_token
        self._sync_interval = sync_interval_seconds
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._semaphore = asyncio.Semaphore(max_size)
        self._closed = False
        self._active_connections = 0
        self._holder: _LibSQLEmbeddedConnectionHolder | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._last_sync: float = 0.0
        self._lock = asyncio.Lock()

    async def _ensure_connection(self) -> tuple[_LibSQLEmbeddedConnectionHolder, ThreadPoolExecutor]:
        """Create or return existing connection, syncing if stale."""
        async with self._lock:
            if self._holder is None or self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix="libsql-embedded"
                )
                self._holder = _LibSQLEmbeddedConnectionHolder(
                    self._local_path, self._sync_url, self._auth_token
                )
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self._executor, self._holder.connect)
                await loop.run_in_executor(self._executor, self._holder.sync)
                self._last_sync = time.monotonic()
                logger.info(
                    f"Embedded replica connected and synced: {self._local_path}"
                )
            elif (
                self._sync_interval > 0
                and time.monotonic() - self._last_sync > self._sync_interval
            ):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(self._executor, self._holder.sync)
                self._last_sync = time.monotonic()

            return self._holder, self._executor

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[LibSQLAsyncConnection]:
        """Acquire a connection to the local replica."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        try:
            holder, executor = await self._ensure_connection()
            async_conn = LibSQLAsyncConnection(holder, executor)
        except Exception as e:
            self._semaphore.release()
            raise PoolConnectionError(
                f"Failed to connect to embedded replica: {e}"
            ) from e

        self._active_connections += 1
        try:
            yield async_conn
        finally:
            self._active_connections -= 1
            async_conn._closed = True  # prevent close() from destroying shared connection
            self._semaphore.release()

    async def sync(self) -> None:
        """Manually trigger a sync from cloud."""
        if self._holder is None or self._executor is None:
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self._holder.sync)
        self._last_sync = time.monotonic()

    async def close(self) -> None:
        """Close the connection and clean up."""
        self._closed = True
        async with self._lock:
            if self._holder:
                try:
                    self._holder.close()
                except Exception:
                    pass
                self._holder = None
            if self._executor:
                self._executor.shutdown(wait=False)
                self._executor = None

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def size(self) -> int:
        return self._max_size

    @property
    def available(self) -> int:
        return self._max_size - self._active_connections


class TursoPool(BasePool):
    """
    Turso connection pool using pyturso.

    Turso is an embedded SQLite-compatible database with additional
    features like vector search and CDC. Uses semaphore-based limiting.

    pyturso provides a synchronous DB-API 2.0 interface. We wrap it with
    TursoAsyncConnection to provide an async interface. Each connection
    uses a dedicated single-thread executor since pyturso connections
    cannot be shared across threads.
    """

    def __init__(
        self,
        database_path: str,
        *,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._database_path = database_path
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._semaphore = asyncio.Semaphore(max_size)
        self._closed = False
        self._active_connections = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a connection (creates fresh connection each time)."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        # Each connection gets its own single-thread executor
        # This ensures all operations on the connection happen on the same thread
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="turso")
        holder = _TursoConnectionHolder(self._database_path)
        async_conn = None

        try:
            loop = asyncio.get_event_loop()
            # Create connection on the executor's thread
            await loop.run_in_executor(executor, holder.connect)
            async_conn = TursoAsyncConnection(holder, executor)
        except Exception as e:
            executor.shutdown(wait=False)
            self._semaphore.release()
            raise PoolConnectionError(f"Failed to connect to Turso: {e}") from e

        self._active_connections += 1
        try:
            yield async_conn
        finally:
            self._active_connections -= 1
            if async_conn:
                await async_conn.close()
            executor.shutdown(wait=True)
            self._semaphore.release()

    async def close(self) -> None:
        """Mark the pool as closed."""
        self._closed = True

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Maximum number of concurrent connections."""
        return self._max_size

    @property
    def available(self) -> int:
        """Number of available connection slots."""
        return self._max_size - self._active_connections


class ConnectionPool:
    """
    Unified connection pool factory.

    Usage:
        # PostgreSQL
        pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")

        # SQLite
        pool = await ConnectionPool.sqlite("./app.db")

        # Turso (embedded, using pyturso)
        pool = await ConnectionPool.turso("./app.db")

        # LibSQL (Turso cloud)
        pool = await ConnectionPool.libsql("libsql://db.turso.io", auth_token="...")

        # Use with queries
        async with pool.acquire() as conn:
            results = await users.select().execute(conn)

        await pool.close()
    """

    @staticmethod
    async def postgresql(
        connection_string: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> PostgreSQLPool:
        """
        Create a PostgreSQL connection pool.

        Args:
            connection_string: PostgreSQL connection URL
            min_size: Minimum pool size (default: 1)
            max_size: Maximum pool size (default: 10)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)

        Returns:
            PostgreSQLPool instance
        """
        pool = PostgreSQLPool(
            connection_string,
            min_size=min_size,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )
        # Pre-create the pool to fail fast on bad connection strings
        await pool._ensure_pool()
        return pool

    @staticmethod
    async def sqlite(
        database_path: str,
        *,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> SQLitePool:
        """
        Create a SQLite connection pool.

        Args:
            database_path: Path to SQLite database (or ":memory:" for in-memory)
            max_size: Maximum concurrent connections (default: 5 for WAL mode)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)

        Returns:
            SQLitePool instance
        """
        return SQLitePool(
            database_path,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )

    @staticmethod
    async def turso(
        database_path: str,
        *,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> TursoPool:
        """
        Create a Turso connection pool using pyturso.

        Turso is an embedded SQLite-compatible database with additional
        features like vector search and change data capture (CDC).

        Args:
            database_path: Path to database (or ":memory:" for in-memory)
            max_size: Maximum concurrent connections (default: 5)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)

        Returns:
            TursoPool instance

        Note:
            Requires `pyturso` package: `pip install pyturso`
        """
        return TursoPool(
            database_path,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )

    @staticmethod
    async def libsql(
        url: str,
        *,
        auth_token: str | None = None,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> LibSQLPool:
        """
        Create a LibSQL connection pool for Turso cloud.

        Args:
            url: LibSQL/Turso cloud URL (e.g., "libsql://db.turso.io")
            auth_token: Authentication token for Turso cloud
            max_size: Maximum concurrent connections (default: 10)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)

        Returns:
            LibSQLPool instance

        Note:
            Requires `libsql-experimental` package: `pip install libsql-experimental`
        """
        return LibSQLPool(
            url,
            auth_token=auth_token,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )

    @staticmethod
    async def libsql_embedded(
        local_path: str,
        *,
        sync_url: str,
        auth_token: str,
        sync_interval_seconds: float = 5.0,
        max_size: int = 10,
        acquire_timeout: float = 30.0,
    ) -> LibSQLEmbeddedPool:
        """
        Create a LibSQL embedded replica pool.

        Reads are local (sub-millisecond). Writes go to Turso Cloud.

        Args:
            local_path: Path for the local replica file
                        (e.g., "/tmp/multicardz/replicas/central.db")
            sync_url: Turso Cloud URL
                      (e.g., "libsql://mc-central-multicardz.turso.io")
            auth_token: Turso auth token
            sync_interval_seconds: How often to auto-sync from cloud (default: 5.0)
            max_size: Maximum concurrent connections (default: 10)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)

        Returns:
            LibSQLEmbeddedPool instance
        """
        return LibSQLEmbeddedPool(
            local_path,
            sync_url=sync_url,
            auth_token=auth_token,
            sync_interval_seconds=sync_interval_seconds,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )


class TursoCloudManager:
    """
    Multi-tenant database manager for Turso cloud.

    Turso cloud is designed for one database per tenant. This manager
    handles database provisioning, token management, and connection pooling
    for multi-tenant applications.

    Usage:
        manager = TursoCloudManager(
            org="mycompany",
            api_token=os.environ["TURSO_API_TOKEN"],
        )

        # Create database for new tenant
        db_info = await manager.create_database("tenant-123")

        # Get connection pool for tenant
        pool = await manager.get_pool("tenant-123")
        async with pool.acquire() as conn:
            ...

        # Delete tenant database
        await manager.delete_database("tenant-123")
    """

    def __init__(
        self,
        org: str,
        api_token: str,
        *,
        group: str = "default",
        region: str | None = None,
        pool_max_size: int = 10,
        pool_acquire_timeout: float = 30.0,
    ) -> None:
        """
        Initialize the Turso cloud manager.

        Args:
            org: Turso organization name
            api_token: Platform API token (from `turso auth api-tokens mint`)
            group: Database group (default: "default")
            region: Optional region hint for new databases
            pool_max_size: Max connections per tenant pool (default: 10)
            pool_acquire_timeout: Connection acquire timeout (default: 30s)
        """
        self._org = org
        self._api_token = api_token
        self._group = group
        self._region = region
        self._pool_max_size = pool_max_size
        self._pool_acquire_timeout = pool_acquire_timeout
        self._base_url = "https://api.turso.tech/v1"

        # Cache for tenant tokens and pools
        self._tokens: dict[str, str] = {}
        self._pools: dict[str, LibSQLPool] = {}

    def _build_db_url(self, db_name: str) -> str:
        """Build the libsql URL for a database."""
        # Turso URLs follow pattern: libsql://{db_name}-{org}.{region}.turso.io
        # The exact format is returned by the API, but we can construct it
        return f"libsql://{db_name}-{self._org}.turso.io"

    async def _api_request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
    ) -> dict:
        """Make a request to the Turso Platform API."""
        import json
        import urllib.error
        import urllib.request

        url = f"{self._base_url}/organizations/{self._org}{endpoint}"

        body = json.dumps(data).encode() if data is not None else None

        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {self._api_token}")
        if body:
            req.add_header("Content-Type", "application/json")

        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(None, lambda: urllib.request.urlopen(req))
            return json.loads(response.read())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            raise PoolConnectionError(f"Turso API error ({e.code}): {error_body}") from e

    async def create_database(
        self,
        db_name: str,
        *,
        size_limit: str | None = None,
    ) -> dict:
        """
        Create a new database for a tenant.

        Args:
            db_name: Database name (lowercase, numbers, dashes; max 64 chars)
            size_limit: Optional size limit (e.g., "256mb", "1gb")

        Returns:
            Dict with database info (DbId, Hostname, Name)
        """
        payload: dict[str, Any] = {
            "name": db_name,
            "group": self._group,
        }
        if size_limit:
            payload["size_limit"] = size_limit

        result = await self._api_request("POST", "/databases", payload)
        return result.get("database", result)

    async def delete_database(self, db_name: str) -> None:
        """
        Delete a tenant's database.

        Args:
            db_name: Database name to delete
        """
        # Remove from cache
        self._tokens.pop(db_name, None)
        pool = self._pools.pop(db_name, None)
        if pool:
            await pool.close()

        await self._api_request("DELETE", f"/databases/{db_name}")

    async def create_token(self, db_name: str) -> str:
        """
        Create an auth token for a database.

        Args:
            db_name: Database name

        Returns:
            JWT auth token
        """
        result = await self._api_request(
            "POST",
            f"/databases/{db_name}/auth/tokens",
            {},
        )
        token = result.get("jwt", "")
        self._tokens[db_name] = token
        return token

    async def get_token(self, db_name: str) -> str:
        """
        Get auth token for a database (creates if not cached).

        Args:
            db_name: Database name

        Returns:
            JWT auth token
        """
        if db_name not in self._tokens:
            await self.create_token(db_name)
        return self._tokens[db_name]

    async def get_pool(self, db_name: str) -> LibSQLPool:
        """
        Get a connection pool for a tenant's database.

        Creates the pool on first access and caches it.

        Args:
            db_name: Database name (tenant identifier)

        Returns:
            LibSQLPool for the tenant's database
        """
        if db_name not in self._pools:
            token = await self.get_token(db_name)
            url = self._build_db_url(db_name)
            self._pools[db_name] = LibSQLPool(
                url,
                auth_token=token,
                max_size=self._pool_max_size,
                acquire_timeout=self._pool_acquire_timeout,
            )
        return self._pools[db_name]

    async def list_databases(self) -> list[dict]:
        """
        List all databases in the organization.

        Returns:
            List of database info dicts
        """
        result = await self._api_request("GET", "/databases")
        return result.get("databases", [])

    async def database_exists(self, db_name: str) -> bool:
        """
        Check if a database exists.

        Args:
            db_name: Database name to check

        Returns:
            True if database exists
        """
        try:
            await self._api_request("GET", f"/databases/{db_name}")
            return True
        except PoolConnectionError:
            return False

    async def get_or_create_database(self, db_name: str) -> dict:
        """
        Get existing database or create if it doesn't exist.

        Args:
            db_name: Database name

        Returns:
            Database info dict
        """
        if await self.database_exists(db_name):
            result = await self._api_request("GET", f"/databases/{db_name}")
            return result.get("database", result)
        return await self.create_database(db_name)

    async def close(self) -> None:
        """Close all cached connection pools."""
        for pool in self._pools.values():
            await pool.close()
        self._pools.clear()
        self._tokens.clear()


# =============================================================================
# Mirror Pool Classes (for replication verification)
# =============================================================================


class MirrorCursor:
    """
    Cursor wrapper for mirror results.

    Either wraps a real cursor or serves pre-fetched rows.
    """

    def __init__(self, cursor: Any | None, prefetched_rows: list | None) -> None:
        self._cursor = cursor
        self._rows = prefetched_rows or []
        self._position = 0

    async def fetchone(self) -> Any:
        """Fetch one row."""
        if self._cursor:
            result = self._cursor.fetchone()
            if asyncio.iscoroutine(result):
                return await result
            return result
        if self._position < len(self._rows):
            row = self._rows[self._position]
            self._position += 1
            return row
        return None

    async def fetchall(self) -> list:
        """Fetch all remaining rows."""
        if self._cursor:
            result = self._cursor.fetchall()
            if asyncio.iscoroutine(result):
                return await result
            return result
        rows = self._rows[self._position :]
        self._position = len(self._rows)
        return rows

    async def fetchmany(self, size: int = 1) -> list:
        """Fetch many rows."""
        if self._cursor:
            result = self._cursor.fetchmany(size)
            if asyncio.iscoroutine(result):
                return await result
            return result
        rows = self._rows[self._position : self._position + size]
        self._position += len(rows)
        return rows

    @property
    def rowcount(self) -> int:
        """Number of rows affected."""
        if self._cursor and hasattr(self._cursor, "rowcount"):
            return self._cursor.rowcount
        return len(self._rows)


class MirrorConnection:
    """
    Connection wrapper that mirrors operations to two databases.

    Writes go to both databases in parallel.
    Reads fetch from both and compare results, logging any disagreements.
    """

    def __init__(
        self,
        primary_conn: Any,
        mirror_conn: Any,
        *,
        logger: logging.Logger,
        fail_open: bool = True,
        compare_on_read: bool = True,
    ) -> None:
        self._primary = primary_conn
        self._mirror = mirror_conn
        self._logger = logger
        self._fail_open = fail_open
        self._compare_on_read = compare_on_read

    def _is_read_query(self, sql: str) -> bool:
        """Check if query is a read (SELECT) query."""
        normalized = sql.strip().upper()
        return normalized.startswith("SELECT") or normalized.startswith("WITH")

    async def execute(self, sql: str, parameters: tuple = ()) -> MirrorCursor:
        """Execute SQL on both connections."""
        timestamp = datetime.now(UTC).isoformat()

        if self._is_read_query(sql) and self._compare_on_read:
            return await self._execute_with_comparison(sql, parameters, timestamp)
        else:
            return await self._execute_parallel(sql, parameters, timestamp)

    async def _execute_parallel(self, sql: str, parameters: tuple, timestamp: str) -> MirrorCursor:
        """Execute on both databases in parallel (for writes)."""
        results = await asyncio.gather(
            self._safe_execute(self._primary, sql, parameters),
            self._safe_execute(self._mirror, sql, parameters),
            return_exceptions=True,
        )

        primary_result, mirror_result = results

        if isinstance(mirror_result, Exception):
            self._log_mirror_error("execute", sql, parameters, timestamp, mirror_result)
            if not self._fail_open:
                raise mirror_result

        if isinstance(primary_result, Exception):
            raise primary_result

        return MirrorCursor(primary_result, None)

    async def _execute_with_comparison(
        self, sql: str, parameters: tuple, timestamp: str
    ) -> MirrorCursor:
        """Execute on both and compare results (for reads)."""
        results = await asyncio.gather(
            self._safe_execute(self._primary, sql, parameters),
            self._safe_execute(self._mirror, sql, parameters),
            return_exceptions=True,
        )

        primary_result, mirror_result = results

        if isinstance(primary_result, Exception):
            raise primary_result

        if isinstance(mirror_result, Exception):
            self._log_mirror_error("execute", sql, parameters, timestamp, mirror_result)
            if not self._fail_open:
                raise mirror_result
            return MirrorCursor(primary_result, None)

        # Both succeeded - fetch and compare results
        primary_rows = await self._fetch_all(primary_result)
        mirror_rows = await self._fetch_all(mirror_result)

        if primary_rows != mirror_rows:
            self._log_data_disagreement(sql, parameters, timestamp, primary_rows, mirror_rows)

        return MirrorCursor(None, primary_rows)

    async def _safe_execute(self, conn: Any, sql: str, parameters: tuple) -> Any:
        """Execute with error handling for different connection types."""
        return await conn.execute(sql, parameters)

    async def _fetch_all(self, cursor: Any) -> list:
        """Fetch all rows from cursor."""
        if hasattr(cursor, "fetchall"):
            result = cursor.fetchall()
            if asyncio.iscoroutine(result):
                return await result
            return result
        return []

    def _log_mirror_error(
        self, op: str, sql: str, params: tuple, timestamp: str, error: Exception
    ) -> None:
        """Log mirror operation failure."""
        self._logger.warning(
            "Mirror %s failed | time=%s | sql=%r | params=%r | error=%s",
            op,
            timestamp,
            sql,
            params,
            error,
        )

    def _log_data_disagreement(
        self,
        sql: str,
        params: tuple,
        timestamp: str,
        primary_rows: list,
        mirror_rows: list,
    ) -> None:
        """Log data disagreement between primary and mirror."""
        self._logger.error(
            "DATA DISAGREEMENT DETECTED\n"
            "  timestamp: %s\n"
            "  sql: %r\n"
            "  parameters: %r\n"
            "  primary_row_count: %d\n"
            "  mirror_row_count: %d\n"
            "  primary_data: %r\n"
            "  mirror_data: %r\n"
            "  diff: %s",
            timestamp,
            sql,
            params,
            len(primary_rows),
            len(mirror_rows),
            primary_rows,
            mirror_rows,
            self._compute_diff(primary_rows, mirror_rows),
        )

    def _compute_diff(self, primary: list, mirror: list) -> str:
        """Compute human-readable diff between results."""
        primary_set = set(map(tuple, primary)) if primary else set()
        mirror_set = set(map(tuple, mirror)) if mirror else set()

        only_primary = primary_set - mirror_set
        only_mirror = mirror_set - primary_set

        parts = []
        if only_primary:
            parts.append(f"only_in_primary={list(only_primary)}")
        if only_mirror:
            parts.append(f"only_in_mirror={list(only_mirror)}")
        return "; ".join(parts) if parts else "row order differs"

    async def executemany(self, sql: str, parameters: list[tuple]) -> MirrorCursor:
        """Execute with multiple parameter sets on both databases."""
        timestamp = datetime.now(UTC).isoformat()

        results = await asyncio.gather(
            self._primary.executemany(sql, parameters),
            self._mirror.executemany(sql, parameters),
            return_exceptions=True,
        )

        if isinstance(results[1], Exception):
            self._log_mirror_error("executemany", sql, parameters, timestamp, results[1])
            if not self._fail_open:
                raise results[1]

        if isinstance(results[0], Exception):
            raise results[0]

        return MirrorCursor(results[0], None)

    async def commit(self) -> None:
        """Commit on both connections in parallel."""
        await asyncio.gather(
            self._primary.commit(),
            self._mirror.commit(),
            return_exceptions=True,
        )

    async def rollback(self) -> None:
        """Rollback on both connections in parallel."""
        await asyncio.gather(
            self._primary.rollback(),
            self._mirror.rollback(),
            return_exceptions=True,
        )

    async def close(self) -> None:
        """Close both connections."""
        await asyncio.gather(
            self._primary.close(),
            self._mirror.close(),
            return_exceptions=True,
        )


class MirrorPool(BasePool):
    """
    Database mirroring pool for replication verification.

    Wraps two pools (primary and mirror) to:
    - Write to both databases in parallel
    - Read from both and compare results
    - Return primary data but log disagreements

    Usage:
        primary = await ConnectionPool.postgresql("postgresql://primary/db")
        mirror = await ConnectionPool.sqlite("./mirror.db")
        pool = MirrorPool(primary, mirror)

        async with pool.acquire() as conn:
            # Writes go to both
            await conn.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Alice"))
            await conn.commit()

            # Reads compare results, return primary, log disagreements
            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()

        await pool.close()
    """

    def __init__(
        self,
        primary: BasePool,
        mirror: BasePool,
        *,
        logger: logging.Logger | None = None,
        fail_open: bool = True,
        compare_on_read: bool = True,
    ) -> None:
        """
        Initialize the mirror pool.

        Args:
            primary: Primary database pool (source of truth)
            mirror: Mirror database pool (for comparison)
            logger: Logger for disagreement messages (default: declaro_persistum.mirror)
            fail_open: If True, continue with primary if mirror fails (default: True)
            compare_on_read: If True, compare SELECT results (default: True)
        """
        self._primary = primary
        self._mirror = mirror
        self._logger = logger or logging.getLogger("declaro_persistum.mirror")
        self._fail_open = fail_open
        self._compare_on_read = compare_on_read
        self._closed = False

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[MirrorConnection | Any]:
        """Acquire connections from both pools (or primary only if mirror detached)."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        if self._mirror is None:
            # Mirror detached — pass-through to primary
            async with self._primary.acquire() as primary_conn:
                yield primary_conn
        else:
            async with (
                self._primary.acquire() as primary_conn,
                self._mirror.acquire() as mirror_conn,
            ):
                yield MirrorConnection(
                    primary_conn,
                    mirror_conn,
                    logger=self._logger,
                    fail_open=self._fail_open,
                    compare_on_read=self._compare_on_read,
                )

    async def close(self) -> None:
        """Close both pools (or primary only if mirror detached)."""
        self._closed = True
        if self._mirror is not None:
            await asyncio.gather(
                self._primary.close(),
                self._mirror.close(),
                return_exceptions=True,
            )
        else:
            await self._primary.close()

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Size of primary pool."""
        return getattr(self._primary, "size", 0)

    @property
    def available(self) -> int:
        """Available connections in primary pool."""
        return getattr(self._primary, "available", 0)

    def promote_mirror(self) -> None:
        """
        Swap primary and mirror pools.

        After this call, the former mirror becomes the primary (source of truth)
        and the former primary becomes the mirror (shadow). Useful for live
        cutover: run dual-write verification, then promote when confident.
        """
        self._primary, self._mirror = self._mirror, self._primary

    def detach_mirror(self) -> BasePool:
        """
        Detach and return the mirror pool.

        After this call, the MirrorPool operates as a pass-through to the
        primary pool only. The returned pool can be closed independently.

        Returns:
            The detached mirror pool.

        Raises:
            PoolError: If no mirror is attached.
        """
        if self._mirror is None:
            from declaro_persistum.exceptions import PoolError
            raise PoolError("No mirror pool attached")
        mirror = self._mirror
        self._mirror = None
        return mirror
