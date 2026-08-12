"""The blocking API surface: pools without an event loop.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx).

This surface still exists and is still exported. A note in the project
memory once claimed the sync code had been removed and the library was
async-only; that was false, and repeating it would tell a consumer a
public class does not exist. What WAS removed is SyncLibSQLPool,
SyncLibSQLConnection, apply_migrations_sync and TursoApplier.apply_sync.

The async path is the one that is maintained and exercised. These are
here for callers with no event loop.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
