"""
Unified connection pool for declaro_persistum.

Provides a consistent async context manager API for:
- PostgreSQL (asyncpg with native pooling)
- SQLite (aiosqlite with semaphore-based limiting)
- Turso (pyturso with semaphore-based limiting + optional cloud sync)

Example:
    pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")
    async with pool.acquire() as conn:
        results = await users.select().execute(conn)
    await pool.close()
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

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


class BasePool:
    """
    Protocol-style base for connection pools.

    Subclasses implement acquire(), close(), and closed.
    No ABC/abstractmethod — structural subtyping via duck typing.
    Connection pools are inherently stateful (Honest Code exemption
    for file handles, network connections, database cursors).

    Instrumentation fields (set by configure_instrumentation()):
        _tier: str — label for every latency record from this pool
        _latency_logger: logging.Logger | None — None means disabled (zero overhead)
        _write_queue: Any | None — reserved for Phase 3 write queue
    """

    _tier: str = ""
    _latency_logger: Any = None  # logging.Logger when instrumentation enabled
    _write_queue: Any = None     # WriteQueue instance when enabled (Phase 3)

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

    def configure_instrumentation(
        self,
        *,
        tier_label: str = "",
        sink: str | None = None,
        path: str | None = None,
        callable_sink: Any = None,
    ) -> None:
        """
        Enable latency instrumentation on this pool.

        Args:
            tier_label: Tag every record with this label (e.g. "central", "project")
            sink: "jsonl" to write JSONL to path, or None for callable_sink only
            path: File path for JSONL sink (required when sink="jsonl")
            callable_sink: Callable(record: dict) -> None for custom sinks
        """
        from declaro_persistum.instrumentation import (
            get_latency_logger,
            setup_callable_sink,
            setup_jsonl_sink,
        )

        self._tier = tier_label
        logger = get_latency_logger()

        if sink == "jsonl" and path:
            setup_jsonl_sink(logger, path)
        if callable_sink is not None:
            setup_callable_sink(logger, callable_sink)

        self._latency_logger = logger

    def configure_write_queue(
        self,
        *,
        persistence_path: str | None = None,
        threshold_ms: float = 50.0,
        max_concurrent_drains: int = 3,
    ) -> None:
        """
        Attach a write queue to this pool and start the supervisor.

        Args:
            persistence_path: JSONL file to persist queue across restarts
            threshold_ms: Write latency threshold before queuing (default: 50ms)
            max_concurrent_drains: Max concurrent drain tasks (default: 3)
        """
        from declaro_persistum.write_queue import WriteQueue

        queue = WriteQueue(
            self,
            persistence_path=persistence_path,
            threshold_ms=threshold_ms,
            max_concurrent_drains=max_concurrent_drains,
        )
        queue.load_from_disk()
        queue.start_supervisor()
        self._write_queue = queue

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        """No-op transaction passthrough until real transaction support ships.

        Yields the pool itself.  Writes already go through acquire_write()
        (auto-commit per statement for Turso) so a passthrough is
        semantically correct for the current single-writer architecture.
        Subclasses can override for real BEGIN/COMMIT semantics.
        """
        yield self


class PostgreSQLPool(BasePool):
    """
    PostgreSQL connection pool using asyncpg.

    Wraps asyncpg's native pool implementation which provides:
    - Connection reuse
    - Automatic reconnection
    - Prepared statement caching
    """

    @property
    def dialect(self) -> str:
        return "postgresql"

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
        if self._write_queue is not None:
            await self._write_queue.stop_supervisor()
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

    _write_queue: Any = None

    @property
    def dialect(self) -> str:
        return "sqlite"

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
        if self._write_queue is not None:
            await self._write_queue.stop_supervisor()
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


class TursoPool(BasePool):
    """
    Turso connection pool using pyturso with optional cloud sync.

    Two modes:
    - **Local-only** (no remote_url): turso.aio.connect() — embedded
      SQLite-compatible DB with MVCC for concurrent writers.
    - **Cloud sync** (remote_url provided): turso.aio.sync.connect() —
      local writes commit sub-ms, background push loop sends changes
      to Turso Cloud every ``push_interval_s`` seconds.

    All connections are natively async (turso.aio) — no ThreadPoolExecutor.
    MVCC with ``BEGIN CONCURRENT`` enables concurrent writers.
    """

    def __init__(
        self,
        database_path: str,
        *,
        remote_url: str | None = None,
        auth_token: str | None = None,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
        push_interval_s: float = 1.0,
    ) -> None:
        self._database_path = database_path
        self._remote_url = remote_url
        self._auth_token = auth_token
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._push_interval_s = push_interval_s
        self._semaphore = asyncio.Semaphore(max_size)
        self._closed = False
        self._active_connections = 0
        self._push_task: asyncio.Task[None] | None = None
        self._write_holder: _TursoConnectionHolder | None = None

    async def _initialize(self) -> None:
        """Bootstrap from cloud (if remote_url) and configure journal mode."""
        self._write_holder = _TursoConnectionHolder(
            self._database_path, self._remote_url, self._auth_token
        )
        await self._write_holder.connect_async()
        if self._remote_url:
            await self._write_holder.pull()
        # MVCC and cache_size pragmas are only for local-only connections.
        # Cloud sync (CDC replication) has its own journaling — MVCC is
        # incompatible and the commit() triggers a CDC sync that fails.
        if not self._remote_url:
            try:
                cur = await self._write_holder.conn.execute("PRAGMA journal_mode = 'mvcc'")
                rows = await cur.fetchall()
                mode = rows[0][0] if rows else "unknown"
                if mode == "mvcc":
                    self._mvcc = True
                else:
                    self._mvcc = False
                    logger.info("MVCC not available (got %s), using WAL — writes are still sub-ms", mode)
            except Exception:
                self._mvcc = False
            try:
                await self._write_holder.conn.execute("PRAGMA cache_size = -256")
            except Exception:
                logger.debug("PRAGMA cache_size not supported — using default")
            await self._write_holder.conn.commit()
        else:
            self._mvcc = False
        if self._remote_url:
            self._push_task = asyncio.create_task(self._push_loop())

    async def _push_loop(self) -> None:
        """Periodically push local commits to Turso Cloud."""
        while not self._closed:
            await asyncio.sleep(self._push_interval_s)
            try:
                if self._write_holder:
                    await self._write_holder.push()
            except Exception:
                logger.exception("push to cloud failed")

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a natively async connection."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        holder = _TursoConnectionHolder(self._database_path, self._remote_url, self._auth_token)
        async_conn = None

        try:
            await holder.connect_async()
            async_conn = TursoAsyncConnection(holder)  # no executor — native async
        except Exception as e:
            self._semaphore.release()
            raise PoolConnectionError(f"Failed to connect to Turso: {e}") from e

        self._active_connections += 1
        try:
            yield async_conn
        finally:
            self._active_connections -= 1
            if async_conn:
                await async_conn.close()
            self._semaphore.release()

    @asynccontextmanager
    async def acquire_write(self, *, concurrent: bool = True) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a write connection.

        Args:
            concurrent: Use BEGIN CONCURRENT when MVCC is available (default True).
                        Pass False for DDL operations which require exclusive transactions.
        """
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        holder = _TursoConnectionHolder(self._database_path, self._remote_url, self._auth_token)
        await holder.connect_async()
        async_conn = TursoAsyncConnection(holder)
        if concurrent and getattr(self, "_mvcc", False):
            await async_conn.execute("BEGIN CONCURRENT")
        try:
            yield async_conn
            await async_conn.commit()
        except Exception:
            await async_conn.rollback()
            raise
        finally:
            await async_conn.close()

    async def close(self) -> None:
        """Final push, cancel push loop, close write holder."""
        if self._write_queue is not None:
            await self._write_queue.stop_supervisor()
        self._closed = True
        # Final push before shutdown
        if self._write_holder:
            try:
                await self._write_holder.push()
            except Exception:
                logger.exception("final push failed on close")
            if self._write_holder.conn is not None:
                await self._write_holder.conn.close()
                self._write_holder.conn = None
        if self._push_task and not self._push_task.done():
            self._push_task.cancel()
            try:
                await self._push_task
            except asyncio.CancelledError:
                pass

    @property
    def dialect(self) -> str:
        return "turso"

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

        # Turso (local)
        pool = await ConnectionPool.turso("./app.db")

        # Turso (with cloud sync)
        pool = await ConnectionPool.turso("./app.db", remote_url="https://db.turso.io")

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
        instrumentation: bool = False,
        tier_label: str = "",
        latency_sink: str | None = None,
        latency_path: str | None = None,
        write_queue_path: str | None = None,
        write_queue_threshold_ms: float = 50.0,
        write_queue_concurrency: int = 3,
    ) -> "PostgreSQLPool":
        """
        Create a PostgreSQL connection pool.

        Args:
            connection_string: PostgreSQL connection URL
            min_size: Minimum pool size (default: 1)
            max_size: Maximum pool size (default: 10)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)
            instrumentation: Enable latency recording (default: False)
            tier_label: Label for every latency record from this pool
            latency_sink: "jsonl" to write to latency_path, or None
            latency_path: File path for JSONL sink
            write_queue_path: JSONL path to persist write queue across restarts
            write_queue_threshold_ms: Latency threshold before queuing (default: 50ms)
            write_queue_concurrency: Max concurrent drain tasks (default: 3)

        Returns:
            PostgreSQLPool instance
        """
        pool = PostgreSQLPool(
            connection_string,
            min_size=min_size,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )
        await pool._ensure_pool()
        if instrumentation:
            pool.configure_instrumentation(
                tier_label=tier_label, sink=latency_sink, path=latency_path
            )
        if write_queue_path is not None:
            pool.configure_write_queue(
                persistence_path=write_queue_path,
                threshold_ms=write_queue_threshold_ms,
                max_concurrent_drains=write_queue_concurrency,
            )
        return pool

    @staticmethod
    async def sqlite(
        database_path: str,
        *,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
        instrumentation: bool = False,
        tier_label: str = "",
        latency_sink: str | None = None,
        latency_path: str | None = None,
        write_queue_path: str | None = None,
        write_queue_threshold_ms: float = 50.0,
        write_queue_concurrency: int = 3,
    ) -> "SQLitePool":
        """
        Create a SQLite connection pool.

        Args:
            database_path: Path to SQLite database (or ":memory:" for in-memory)
            max_size: Maximum concurrent connections (default: 5 for WAL mode)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)
            instrumentation: Enable latency recording (default: False)
            tier_label: Label for every latency record from this pool
            latency_sink: "jsonl" to write to latency_path, or None
            latency_path: File path for JSONL sink
            write_queue_path: JSONL path to persist write queue across restarts
            write_queue_threshold_ms: Latency threshold before queuing (default: 50ms)
            write_queue_concurrency: Max concurrent drain tasks (default: 3)

        Returns:
            SQLitePool instance
        """
        pool = SQLitePool(
            database_path,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
        )
        if instrumentation:
            pool.configure_instrumentation(
                tier_label=tier_label, sink=latency_sink, path=latency_path
            )
        if write_queue_path is not None:
            pool.configure_write_queue(
                persistence_path=write_queue_path,
                threshold_ms=write_queue_threshold_ms,
                max_concurrent_drains=write_queue_concurrency,
            )
        return pool

    @staticmethod
    async def turso(
        database_path: str,
        *,
        remote_url: str | None = None,
        auth_token: str | None = None,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
        push_interval_s: float = 1.0,
        instrumentation: bool = False,
        tier_label: str = "",
        latency_sink: str | None = None,
        latency_path: str | None = None,
        write_queue_path: str | None = None,
        write_queue_threshold_ms: float = 50.0,
        write_queue_concurrency: int = 3,
    ) -> "TursoPool":
        """
        Create a Turso connection pool using pyturso.

        When remote_url is provided, the pool bootstraps from Turso Cloud
        via pull() at startup and pushes local commits in the background
        every push_interval_s seconds.

        Args:
            database_path: Path to database (or ":memory:" for in-memory)
            remote_url: Turso Cloud URL for sync (e.g. "https://db.turso.io")
            auth_token: Turso Cloud auth token for authenticated sync
            max_size: Maximum concurrent connections (default: 5)
            acquire_timeout: Timeout for acquiring connection in seconds (default: 30)
            push_interval_s: Seconds between background push cycles (default: 1.0)
            instrumentation: Enable latency recording (default: False)
            tier_label: Label for every latency record from this pool
            latency_sink: "jsonl" to write to latency_path, or None
            latency_path: File path for JSONL sink
            write_queue_path: JSONL path to persist write queue across restarts
            write_queue_threshold_ms: Latency threshold before queuing (default: 50ms)
            write_queue_concurrency: Max concurrent drain tasks (default: 3)

        Returns:
            TursoPool instance, initialised and ready (pulled from cloud if remote_url)
        """
        pool = TursoPool(
            database_path,
            remote_url=remote_url,
            auth_token=auth_token,
            max_size=max_size,
            acquire_timeout=acquire_timeout,
            push_interval_s=push_interval_s,
        )
        await pool._initialize()
        if instrumentation:
            pool.configure_instrumentation(
                tier_label=tier_label, sink=latency_sink, path=latency_path
            )
        if write_queue_path is not None:
            pool.configure_write_queue(
                persistence_path=write_queue_path,
                threshold_ms=write_queue_threshold_ms,
                max_concurrent_drains=write_queue_concurrency,
            )
        return pool


# =============================================================================
# Synchronous Pool Classes (for testing)
# =============================================================================


class SyncSQLitePool:
    """
    Synchronous SQLite connection pool for testing.

    Provides a simple synchronous interface without async/await overhead.
    """

    def __init__(self, database_path: str, *, max_size: int = 5) -> None:
        self._database_path = database_path
        self._max_size = max_size
        self._closed = False

    def acquire(self) -> SyncSQLiteConnection:
        """Acquire a synchronous connection."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")
        import sqlite3

        conn = sqlite3.connect(self._database_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return SyncSQLiteConnection(conn)

    def close(self) -> None:
        """Mark pool as closed."""
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class SyncSQLiteConnection:
    """Synchronous SQLite connection wrapper."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, parameters: tuple = ()) -> Any:
        return self._conn.execute(sql, parameters)

    def executemany(self, sql: str, parameters: list) -> Any:
        return self._conn.executemany(sql, parameters)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SyncSQLiteConnection:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class SyncTursoPool:
    """
    Synchronous Turso (pyturso) connection pool for testing.

    Provides a simple synchronous interface without async/await overhead.
    """

    def __init__(self, database_path: str, *, max_size: int = 5) -> None:
        self._database_path = database_path
        self._max_size = max_size
        self._closed = False

    def acquire(self) -> SyncTursoConnection:
        """Acquire a synchronous connection."""
        if self._closed:
            raise PoolClosedError("Pool has been closed")
        import turso

        conn = turso.connect(self._database_path)
        return SyncTursoConnection(conn)

    def close(self) -> None:
        """Mark pool as closed."""
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class SyncTursoConnection:
    """Synchronous Turso connection wrapper."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, parameters: tuple = ()) -> Any:
        cursor = self._conn.cursor()
        cursor.execute(sql, parameters)
        return cursor

    def executemany(self, sql: str, parameters: list) -> Any:
        cursor = self._conn.cursor()
        cursor.executemany(sql, parameters)
        return cursor

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def sync(self) -> None:
        if hasattr(self._conn, "sync"):
            self._conn.sync()

    def close(self) -> None:
        self._conn.__exit__(None, None, None)

    def __enter__(self) -> SyncTursoConnection:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class SyncConnectionPool:
    """
    Synchronous connection pool factory for testing.

    Usage:
        # SQLite
        pool = SyncConnectionPool.sqlite("./test.db")
        with pool.acquire() as conn:
            conn.execute("SELECT 1")
        pool.close()

        # Turso (pyturso)
        pool = SyncConnectionPool.turso("./test.db")
    """

    @staticmethod
    def sqlite(database_path: str, *, max_size: int = 5) -> SyncSQLitePool:
        """Create a synchronous SQLite pool."""
        return SyncSQLitePool(database_path, max_size=max_size)

    @staticmethod
    def turso(database_path: str, *, max_size: int = 5) -> SyncTursoPool:
        """Create a synchronous Turso pool."""
        return SyncTursoPool(database_path, max_size=max_size)

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
        self._pools: dict[str, TursoPool] = {}

    def _build_remote_url(self, db_name: str) -> str:
        """Build the Turso Cloud remote URL for a database."""
        return f"https://{db_name}-{self._org}.turso.io"

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

    async def get_pool(self, db_name: str) -> TursoPool:
        """
        Get a connection pool for a tenant's database.

        Creates the pool on first access and caches it.

        Args:
            db_name: Database name (tenant identifier)

        Returns:
            TursoPool for the tenant's database
        """
        if db_name not in self._pools:
            token = await self.get_token(db_name)
            remote_url = self._build_remote_url(db_name)
            # TODO: local_path derivation for multi-tenant (db_dir/db_name.db)
            local_path = f"./db/{db_name}.db"
            self._pools[db_name] = await ConnectionPool.turso(
                local_path,
                remote_url=remote_url,
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
