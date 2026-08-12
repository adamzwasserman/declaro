"""Dual-write mirroring, for verifying a replication cutover.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). This is not connection pooling: it wraps TWO
pools and runs every operation against both, so a migration can be
verified against live traffic before the old database is retired.

Writes go to both in parallel. Reads fetch from both, compare, return the
PRIMARY's answer, and log any disagreement. The mirror never changes what
a caller sees — it only reports.

`fail_open` decides what a mirror failure means: True keeps serving from
the primary and logs, False lets the error out. During a cutover the
first is almost always what you want, because the mirror is the database
you do not trust yet.

Used by cutover.py. Re-exported from declaro_persistum for consumers.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from declaro_persistum.exceptions import PoolClosedError
from declaro_persistum.pool_base import BasePool

logger = logging.getLogger(__name__)


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
