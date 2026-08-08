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
import inspect
import logging
import os
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
        max_drain_attempts: int | None = None,
    ) -> None:
        """
        Attach a write queue to this pool and start the supervisor.

        Args:
            persistence_path: JSONL file to persist queue across restarts
            threshold_ms: Write latency threshold before queuing (default: 50ms)
            max_concurrent_drains: Max concurrent drain tasks (default: 3)
            max_drain_attempts: Quarantine an entry to the dead-letter set after
                this many failed drain attempts. None = retry forever (default).
        """
        from declaro_persistum.write_queue import WriteQueue

        queue = WriteQueue(
            self,
            persistence_path=persistence_path,
            threshold_ms=threshold_ms,
            max_concurrent_drains=max_concurrent_drains,
            max_drain_attempts=max_drain_attempts,
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
        push_retry_base_s: float = 0.1,
        background_pull: bool = True,
        mvcc: bool = True,
    ) -> None:
        self._database_path = database_path
        self._remote_url = remote_url
        self._push_retry_base_s = push_retry_base_s
        self._auth_token = auth_token
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._push_interval_s = push_interval_s
        self._background_pull = background_pull
        # Whether to ask the engine for MVCC. On by default: concurrent writes
        # are a Turso feature and the pool should not decline them. Set False
        # only to force WAL deliberately.
        self._mvcc_requested = mvcc
        # Read connections. Reads do not sync, so each gets its own plain
        # local connection and they run in parallel. See acquire().
        self._read_holders: list[_TursoConnectionHolder] = []
        self._free_readers: asyncio.LifoQueue[_TursoConnectionHolder] | None = None
        # The push runs on its own sync connection, so a cloud round trip
        # never blocks a write. See _push_once.
        self._push_holder: _TursoConnectionHolder | None = None
        self._writes_since_push = 0
        self._pushes_without_revision_change = 0
        # Initial-sync state. The event is created in _initialize, on the
        # running loop, rather than here — a pool may be constructed outside
        # the loop that later runs it.
        self._initial_sync_event: asyncio.Event | None = None
        self._initial_sync_task: asyncio.Task[None] | None = None
        self._initial_sync_error: Exception | None = None
        self._semaphore = asyncio.Semaphore(max_size)
        self._conn_lock = asyncio.Lock()
        self._closed = False
        self._active_connections = 0
        self._push_task: asyncio.Task[None] | None = None
        self._write_holder: _TursoConnectionHolder | None = None
        # Push-durability state (W2): a persistently failing push must be
        # observable by the application, not merely logged.
        self._consecutive_push_failures = 0
        self._last_push_error: Exception | None = None
        self._push_failure_callback: Any = None
        self._push_failure_threshold = 0
        self._push_failure_notified = False

    async def _initialize(self) -> None:
        """Bootstrap from cloud (if remote_url) and configure journal mode."""
        self._write_holder = _TursoConnectionHolder(
            self._database_path, self._remote_url, self._auth_token
        )
        # Whether a usable local replica already exists is observable, so
        # observe it rather than assuming. Checked BEFORE connect_async,
        # which bootstraps the file into existence when it is missing and
        # would make the check trivially true afterwards.
        had_local_data = self._local_replica_has_data()

        await self._write_holder.connect_async()
        if self._remote_url:
            self._initial_sync_event = asyncio.Event()
            if self._background_pull and had_local_data:
                # There is a populated replica on disk, so reads can be served
                # the instant connect_async returns. The initial sync is then
                # a freshness step, not a correctness precondition, and
                # blocking the open on it would push network latency onto
                # every caller for no benefit.
                #
                # Callers needing a primary-consistent view await
                # initial_pull_complete(). apply_migrations_async does exactly
                # that before introspecting, so a backgrounded sync can never
                # feed the differ a stale schema.
                self._initial_sync_task = asyncio.create_task(self._initial_sync())
            else:
                # No local data to serve (first creation, or a wiped ephemeral
                # disk). Returning here without syncing would hand out a pool
                # that reads an empty database and reports success — silently
                # wrong. Blocking is the only correct option in this case, and
                # it is a once-per-replica cost, not a per-open one.
                await self._initial_sync()
        # MVCC is requested on every pool, cloud-backed or not. Turso supports
        # concurrent writes through BEGIN CONCURRENT over MVCC, and
        # acquire_write only issues BEGIN CONCURRENT when self._mvcc is true.
        # This block previously ran only when there was no remote_url, so
        # every cloud pool serialized its writes while the engine below it
        # supported concurrent ones — the configuration that needs the
        # throughput least likely to get it.
        #
        # The engine has the last word. If it does not return 'mvcc' the pool
        # records that and continues on WAL; nothing here fails the open.
        if self._mvcc_requested:
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
        else:
            self._mvcc = False

        if not self._remote_url:
            try:
                await self._write_holder.conn.execute("PRAGMA cache_size = -256")
            except Exception:
                logger.debug("PRAGMA cache_size not supported — using default")
            await self._write_holder.conn.commit()
        else:
            # Cloud-sync replicas must enforce the same FK constraints as the
            # primary. The FK *definition* bootstraps down with the schema, but
            # pyturso leaves enforcement OFF per connection by default — so a
            # write that violates a primary FK commits locally, fails to push
            # ("FOREIGN KEY constraint failed"), and is silently lost on the
            # next re-sync. Enabling enforcement makes that write fail fast at
            # commit instead. Must run outside any transaction (fresh conn here).
            await self._enable_replica_fk_enforcement()
        if self._remote_url:
            await self._open_push_connection()
            self._push_task = asyncio.create_task(self._push_loop())

    async def _enable_replica_fk_enforcement(self) -> None:
        """Turn on FK enforcement for the replica write-holder connection."""
        if not self._write_holder or self._write_holder.conn is None:
            return
        try:
            await self._write_holder.conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            logger.warning(
                "PRAGMA foreign_keys = ON not accepted on replica connection — "
                "FK-violating writes may commit locally and fail to push"
            )

    async def refresh_connections(self) -> None:
        """Close and reopen _write_holder to pick up schema changes.

        Call after DDL migrations — the write holder's cached connection
        state doesn't see tables created via acquire_remote() or by
        other connections, even after pull().
        """
        if self._write_holder:
            if self._write_holder.conn is not None:
                await self._write_holder.conn.close()
                self._write_holder.conn = None
            await self._write_holder.connect_async()
            if self._remote_url:
                # W3: push un-pushed local frames before pull() (see _initialize).
                await self._push_once()
                try:
                    await self._write_holder.pull()
                except Exception:
                    pass
                await self._enable_replica_fk_enforcement()
            logger.info("Write holder connection refreshed after migration")

    def pause_push(self) -> None:
        """Pause the background push loop (e.g. during migrations)."""
        self._push_paused = True

    def resume_push(self) -> None:
        """Resume the background push loop."""
        self._push_paused = False

    def set_push_failure_callback(self, callback: Any, *, threshold: int = 1) -> None:
        """Register a callback invoked when the push loop crosses ``threshold``
        consecutive failures.

        The callback is called as ``callback(error, consecutive_failures)`` once
        per failure episode (re-armed after the next successful push). Use this
        to surface non-durable writes — a committed write whose push keeps
        failing is otherwise only visible as a WARNING log line.
        """
        self._push_failure_callback = callback
        self._push_failure_threshold = threshold

    @property
    def last_push_error(self) -> Exception | None:
        """The most recent push failure, or None if the last push succeeded."""
        return self._last_push_error

    @property
    def push_healthy(self) -> bool:
        """True when the last push attempt succeeded (or none has failed)."""
        return self._last_push_error is None

    def _record_push_failure(self, error: Exception) -> None:
        self._consecutive_push_failures += 1
        self._last_push_error = error
        logger.warning("Push to cloud failed: %s", error)
        if (
            self._push_failure_callback is not None
            and self._push_failure_threshold
            and self._consecutive_push_failures >= self._push_failure_threshold
            and not self._push_failure_notified
        ):
            self._push_failure_notified = True
            try:
                self._push_failure_callback(error, self._consecutive_push_failures)
            except Exception:
                logger.exception("push failure callback raised")

    def _record_push_success(self) -> None:
        if self._consecutive_push_failures > 0:
            logger.info(
                "Push to cloud recovered after %d failures",
                self._consecutive_push_failures,
            )
        self._consecutive_push_failures = 0
        self._last_push_error = None
        self._push_failure_notified = False

    def _local_replica_has_data(self) -> bool:
        """True when a non-empty local replica file already exists.

        Decides whether the initial sync can be backgrounded: with data on
        disk the pool can serve reads immediately, without it the pool would
        otherwise hand out an empty database.

        An unreadable or missing path answers False — the conservative
        direction, since a False answer only costs a blocking sync while a
        wrong True serves empty results.
        """
        try:
            return os.path.getsize(self._database_path) > 0
        except OSError:
            return False

    async def _initial_sync(self) -> None:
        """Deliver un-pushed local writes, then pull cloud state.

        The push must precede the pull: a prior process may have committed
        locally and died before pushing, and pull() would overwrite those
        frames with cloud state (W3). That ordering holds whether this runs
        inline or as a background task.

        Never raises. When backgrounded there is no caller to catch it, and a
        failed refresh must not kill the pool — the replica stays readable at
        its current revision and the push loop keeps retrying. The error is
        recorded for initial_pull_complete() to re-raise at a call site that
        did ask to wait.
        """
        try:
            await self._push_once()
            if self._write_holder:
                await self._write_holder.pull()
        except Exception as e:
            self._initial_sync_error = e
            logger.warning(
                "Initial sync failed for %s; serving the local replica at its "
                "current revision and retrying via the push loop: %s",
                self._database_path,
                e,
            )
        finally:
            if self._initial_sync_event:
                self._initial_sync_event.set()

    async def initial_pull_complete(self) -> None:
        """Wait until the pool's initial cloud sync has finished.

        Await this before any operation that must not observe a stale
        replica — schema introspection above all, where a stale read makes
        the differ compute against a schema that is not the primary's and
        emit operations that correct code then faithfully applies.

        Returns immediately for local-only pools, and for pools whose sync
        already ran inline. Re-raises the initial sync's failure, so a caller
        that asked for a consistent view is told it did not get one rather
        than proceeding on stale data.
        """
        if self._initial_sync_event is not None:
            await self._initial_sync_event.wait()
        if self._initial_sync_error is not None:
            raise self._initial_sync_error

    async def _open_push_connection(self) -> None:
        """Open the dedicated push connection, once, during initialization.

        The push runs on its own sync connection so it never takes
        _conn_lock, and so a write never waits for a cloud round trip. A push
        on a separate connection to the same local replica does deliver the
        frames committed on the write connection: verified under free-threaded
        CPython with the GIL off — 1353 writes on one connection, 40 pushes on
        another, and a fresh third connection pulled all 1353 rows back from
        cloud.

        Opened here rather than lazily in _push_once, so that a remote which
        cannot be reached costs one failed connect at startup instead of one
        on every push cycle forever. On failure the pool keeps pushing on the
        write connection: slower, because it blocks writes for each round
        trip, but still delivering.
        """
        if not self._remote_url:
            return
        try:
            holder = _TursoConnectionHolder(
                self._database_path, self._remote_url, self._auth_token
            )
            await holder.connect_async()
            self._push_holder = holder
        except Exception as e:
            self._push_holder = None
            logger.warning(
                "Could not open a dedicated push connection (%s); pushing on the "
                "write connection instead, which blocks writes for the duration "
                "of each round trip",
                e,
            )

    async def _push_once(self) -> bool:
        """Push pending frames to cloud, recording the outcome.

        Does not hold _conn_lock. The push has its own connection, so writes
        and reads proceed while a round trip is in flight.
        """
        holder = self._push_holder or self._write_holder
        if holder is None:
            return True

        # Snapshot before the push so non-delivery is detectable afterwards.
        pending_before = self._writes_since_push
        revision_before = await self._sync_revision(holder)

        try:
            if holder is self._write_holder:
                # Fallback path only: the write connection still needs the
                # lock, because reads and writes share it.
                async with self._conn_lock:
                    await holder.push()
            else:
                await holder.push()
        except Exception as e:
            self._record_push_failure(e)
            return False

        self._writes_since_push = max(0, self._writes_since_push - pending_before)
        await self._check_push_delivered(holder, pending_before, revision_before)
        self._record_push_success()
        return True

    async def _sync_revision(self, holder: "_TursoConnectionHolder") -> Any:
        """Read the replica's sync revision, or None if unavailable.

        stats() is a coroutine function on the async connection and a plain
        method on the sync one, so the result is awaited only when it is
        awaitable. Calling it without awaiting returns a coroutine, and
        reading .revision off a coroutine yields None — which silently
        disabled the tripwire below, because it treats None as "cannot
        tell" and returns early. Python reported the un-awaited coroutine,
        but only under sustained load, since nothing asserted the tripwire
        could observe a moving revision.
        """
        conn = getattr(holder, "conn", None)
        stats = getattr(conn, "stats", None)
        if stats is None:
            return None
        try:
            result = stats()
            if inspect.isawaitable(result):
                result = await result
            return getattr(result, "revision", None)
        except Exception:
            return None

    async def _check_push_delivered(
        self, holder: "_TursoConnectionHolder", pending_before: int, revision_before: Any
    ) -> None:
        """Warn if a push reported success but appears to have delivered nothing.

        This is the tripwire for the failure mode that decoupling the push
        could introduce: a push that succeeds on its own connection while
        delivering none of the write connection's frames. That would be
        silent data loss, which is worse than the write stall the decoupling
        removes.

        It logs rather than raises. The exact semantics of the engine's
        revision counter are not pinned down here, so a mismatch is evidence
        worth surfacing, not proof worth failing a healthy push over. If this
        ever fires in the field it should be investigated before it is
        silenced.
        """
        if pending_before <= 0 or revision_before is None:
            return
        revision_after = await self._sync_revision(holder)
        if revision_after is None or revision_after != revision_before:
            return
        self._pushes_without_revision_change += 1
        logger.warning(
            "Push reported success with %d write(s) pending but the sync "
            "revision did not change (%s); %d consecutive occurrence(s). "
            "Verify writes are reaching the cloud primary.",
            pending_before,
            revision_before,
            self._pushes_without_revision_change,
        )

    async def _push_loop(self) -> None:
        """Guaranteed eventual consistency loop.

        Retries indefinitely with exponential backoff (capped at 30s).
        Acquires _conn_lock for push, then releases — reads and writes
        can proceed between push attempts without waiting for cloud I/O.
        Failure/recovery state is tracked on the pool (see _record_push_*).
        """
        max_backoff = 30.0

        while not self._closed:
            if getattr(self, "_push_paused", False):
                await asyncio.sleep(self._push_interval_s)
                continue

            success = await self._push_once()

            if success:
                await asyncio.sleep(self._push_interval_s)
            else:
                delay = min(
                    self._push_retry_base_s * (2 ** self._consecutive_push_failures),
                    max_backoff,
                )
                logger.warning(
                    "Push to cloud: %d consecutive failures, retrying in %.1fs",
                    self._consecutive_push_failures, delay,
                )
                await asyncio.sleep(delay)

    async def _get_reader(self) -> "_TursoConnectionHolder":
        """Take a read connection from the free list, opening one if needed.

        Read connections are plain local connections to the same replica
        file. They never push and never pull, so they hold no sync state and
        cannot diverge from the write connection's view of the cloud. That is
        what lets them run outside _conn_lock.

        At most max_size are ever opened, because the semaphore admits at
        most max_size callers before any of them reach this point.
        """
        assert self._free_readers is not None
        if not self._free_readers.empty():
            return self._free_readers.get_nowait()

        holder = _TursoConnectionHolder(self._database_path)
        await holder.connect_async()
        self._read_holders.append(holder)
        return holder

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a read connection.

        Each caller gets its own connection, so reads run in parallel up to
        max_size. Previously every read was served from the single write
        connection under _conn_lock, and the lock was held for as long as the
        caller held the connection — so max_size bounded how many callers
        could queue, not how many could proceed. Five readers doing 100ms of
        work each took 500ms rather than 100ms.

        Reads see the write connection's committed data because both open the
        same local replica file; a commit is visible to every connection on
        that file. Reads never reach the network.
        """
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        try:
            async with asyncio.timeout(self._acquire_timeout):
                await self._semaphore.acquire()
        except TimeoutError as err:
            raise PoolExhaustedError(
                f"Timed out waiting for connection after {self._acquire_timeout}s"
            ) from err

        if self._write_holder is None or self._write_holder.conn is None:
            self._semaphore.release()
            raise PoolConnectionError("Write holder not initialized")

        if self._free_readers is None:
            self._free_readers = asyncio.LifoQueue()

        holder = await self._get_reader()
        self._active_connections += 1
        try:
            yield TursoAsyncConnection(holder)
        finally:
            self._active_connections -= 1
            self._free_readers.put_nowait(holder)
            self._semaphore.release()

    @asynccontextmanager
    async def acquire_write(self, *, concurrent: bool = True) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire the write connection (shared with _write_holder).

        All writes go through _write_holder so the push loop and
        explicit push after commit push the ACTUAL changes to cloud.
        Creating separate connections per write caused push failures
        because each connection tracked its own sync state independently.

        Args:
            concurrent: Use BEGIN CONCURRENT when MVCC is available (default True).
                        Pass False for DDL operations which require exclusive transactions.
        """
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        if self._write_holder is None or self._write_holder.conn is None:
            raise PoolConnectionError("Write holder not initialized")

        async with self._conn_lock:
            async_conn = TursoAsyncConnection(self._write_holder)
            if concurrent and getattr(self, "_mvcc", False):
                await async_conn.execute("BEGIN CONCURRENT")
            try:
                yield async_conn
                await async_conn.commit()
                # Counted so the push can tell "nothing to deliver" from
                # "delivered nothing" — see _check_push_delivered.
                self._writes_since_push += 1
            except Exception:
                await async_conn.rollback()
                raise
        # No push here — the push loop handles cloud delivery.
        # Any push attempt (even fire-and-forget) acquires _conn_lock
        # and blocks reads during the cloud round-trip.

    async def flush(self) -> None:
        """Block until all pending local writes have been pushed to cloud.

        Retries indefinitely with exponential backoff.  Does NOT close
        the pool — the connection remains usable after flush returns.
        """
        if self._write_holder and self._remote_url:
            attempt = 0
            while not await self._push_once():
                attempt += 1
                delay = min(self._push_retry_base_s * (2 ** attempt), 30.0)
                logger.warning("Flush attempt %d failed, retrying in %.1fs", attempt, delay)
                await asyncio.sleep(delay)

    async def close(self) -> None:
        """Flush all pending writes to cloud, then close.

        Retries push indefinitely until cloud confirms receipt.
        After this method returns, all local writes are guaranteed
        to be on cloud.  It is safe to delete local DB files after
        close() completes.

        Call this on SIGTERM / application shutdown before exiting.
        """
        if self._write_queue is not None:
            await self._write_queue.stop_supervisor()
        self._closed = True
        # Final push — retry indefinitely.  No persistent disk means
        # data not pushed is lost permanently.
        if self._write_holder and self._remote_url:
            attempt = 0
            while not await self._push_once():
                attempt += 1
                delay = min(self._push_retry_base_s * (2 ** attempt), 30.0)
                logger.warning("Final push attempt %d failed, retrying in %.1fs", attempt, delay)
                await asyncio.sleep(delay)
        # The push connection closes after the final push above has used it.
        if self._push_holder is not None and self._push_holder.conn is not None:
            try:
                await self._push_holder.conn.close()
            except Exception:
                logger.debug("Push connection already closed")
            self._push_holder.conn = None
        self._push_holder = None

        # Read connections hold no unpushed state, so they close after the
        # final push rather than before it.
        for reader in self._read_holders:
            if reader.conn is not None:
                try:
                    await reader.conn.close()
                except Exception:
                    logger.debug("Read connection already closed")
                reader.conn = None
        self._read_holders.clear()

        if self._write_holder:
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
        background_pull: bool = True,
        mvcc: bool = True,
        instrumentation: bool = False,
        tier_label: str = "",
        latency_sink: str | None = None,
        latency_path: str | None = None,
        write_queue_path: str | None = None,
        write_queue_threshold_ms: float = 50.0,
        write_queue_concurrency: int = 3,
        write_queue_max_attempts: int | None = None,
    ) -> "TursoPool":
        """
        Create a Turso connection pool using pyturso.

        When remote_url is provided, the pool syncs with Turso Cloud at
        startup and pushes local commits in the background every
        push_interval_s seconds.

        background_pull (default True) keeps network latency off the open
        path. When a populated local replica already exists, the pool becomes
        usable as soon as the connection is open and the initial sync runs as
        a background task; opening does not wait on the cloud. When no local
        replica exists there is nothing to serve, so the sync is awaited
        inline — a once-per-replica cost, not a per-open one.

        mvcc (default True) asks the engine for MVCC journal mode, which is
        what lets acquire_write issue BEGIN CONCURRENT and run writes
        concurrently. It is requested on cloud pools as well as local ones.
        The engine decides: if it does not grant MVCC the pool falls back to
        WAL and says so in the log. Pass mvcc=False to force WAL.

        Reads take their own local connection, so up to max_size of them run
        at once and none of them waits on a cloud push.

        Reads issued before the background sync finishes see the replica at
        its last-synced revision. Callers that must not observe a stale
        replica await initial_pull_complete(); apply_migrations_async does so
        before introspecting, so schema diffs are never computed against
        stale state. Pass background_pull=False to restore fully inline
        syncing.

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
            background_pull=background_pull,
            mvcc=mvcc,
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
                max_drain_attempts=write_queue_max_attempts,
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
        use_tursodb: bool = False,
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
            use_tursodb: Beta — create databases on the new tursodb (Rust)
                engine by default. Overridable per-call. Keep False until the
                compatibility gate passes. (default: False)
        """
        self._org = org
        self._api_token = api_token
        self._group = group
        self._region = region
        self._pool_max_size = pool_max_size
        self._pool_acquire_timeout = pool_acquire_timeout
        self._use_tursodb = use_tursodb
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
        use_tursodb: bool | None = None,
        seed: dict[str, Any] | None = None,
    ) -> dict:
        """
        Create a new database for a tenant.

        Args:
            db_name: Database name (lowercase, numbers, dashes; max 64 chars)
            size_limit: Optional size limit (e.g., "256mb", "1gb")
            use_tursodb: Beta — create on the new tursodb (Rust) engine.
                None (default) inherits the manager's use_tursodb setting;
                an explicit True/False overrides it for this call.
            seed: Optional seed spec, passed through to the Turso API
                unmodified so the new database is created as a copy of an
                existing one rather than empty. Provisioning a tenant from a
                pre-built template becomes a single copy instead of
                create-empty then migrate then insert.

                Passed through as an opaque dict rather than being modelled
                as named parameters, so seed variants the API grows are
                usable without a release here. Two forms are documented today:

                    {"type": "database", "name": "<source-db>"}
                    {"type": "database_upload"}

                With type "database", an optional "timestamp" (ISO 8601)
                selects a recovery point rather than the current state —
                within the last 24 hours, or 30 days on the scaler plan:

                    {"type": "database", "name": "tpl", "timestamp": "..."}

                Not validated here; the API is the authority on which
                combinations are legal and reports violations itself.

        Returns:
            Dict with database info (DbId, Hostname, Name)
        """
        payload: dict[str, Any] = {
            "name": db_name,
            "group": self._group,
        }
        if size_limit:
            payload["size_limit"] = size_limit
        if seed is not None:
            payload["seed"] = seed

        resolved_use_tursodb = (
            self._use_tursodb if use_tursodb is None else use_tursodb
        )
        if resolved_use_tursodb:
            payload["use_tursodb"] = True

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
