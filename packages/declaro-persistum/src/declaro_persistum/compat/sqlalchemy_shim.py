"""SQLAlchemy compatibility shim for declaro-persistum.

Provides SQLAlchemy-like interfaces that delegate to declaro-persistum's
ConnectionPool. This allows code written for SQLAlchemy to work with
declaro-persistum with minimal changes.

When deployed back into buckler/idd, these imports should be replaced
with the actual SQLAlchemy implementations.
"""

from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

from declaro_persistum.pool import ConnectionPool


# Global pool reference - must be configured before use
_pool: Optional[ConnectionPool] = None


def configure_database(pool: ConnectionPool) -> None:
    """Configure the global database pool.

    Args:
        pool: ConnectionPool instance to use for database operations
    """
    global _pool
    _pool = pool


class _DeclarativeBaseMeta(type):
    """Metaclass that tracks model classes for schema generation."""

    _registry: dict[str, type] = {}

    def __new__(mcs, name: str, bases: tuple, namespace: dict) -> type:
        cls = super().__new__(mcs, name, bases, namespace)
        if name != "Base" and "__tablename__" in namespace:
            mcs._registry[namespace["__tablename__"]] = cls
        return cls


class Base(metaclass=_DeclarativeBaseMeta):
    """Declarative base class for ORM-style models.

    This is a compatibility shim. Models inheriting from this class
    should define __tablename__ and column attributes.

    When deployed back into buckler/idd, replace with:
        from backend.models.base import Base
    """

    __tablename__: str

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        """Convert model instance to dictionary."""
        return {
            key: getattr(self, key)
            for key in dir(self)
            if not key.startswith("_") and not callable(getattr(self, key))
        }


class SessionLocal:
    """SQLAlchemy Session-like interface backed by ConnectionPool.

    This is a compatibility shim. Use as a context manager for database operations.

    When deployed back into buckler/idd, replace with:
        from backend.data.db import SessionLocal
    """

    def __init__(self) -> None:
        if _pool is None:
            raise RuntimeError(
                "Database not configured. Call configure_database(pool) first."
            )
        self._pool = _pool
        self._conn: Any = None

    async def __aenter__(self) -> "SessionLocal":
        self._conn = await self._pool.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._conn:
            await self._pool.release(self._conn)
            self._conn = None

    @property
    def connection(self) -> Any:
        """Get the underlying database connection."""
        return self._conn

    async def execute(self, query: str, params: Optional[dict[str, Any]] = None) -> Any:
        """Execute a SQL query."""
        if self._conn is None:
            raise RuntimeError("Session not active. Use as context manager.")
        return await self._conn.execute(query, params or {})

    async def commit(self) -> None:
        """Commit the current transaction."""
        # ConnectionPool handles transactions automatically
        pass

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        # ConnectionPool handles transactions automatically
        pass


@asynccontextmanager
async def get_db() -> AsyncGenerator[SessionLocal, None]:
    """Dependency injection for FastAPI routes.

    Usage:
        @router.get("/items")
        async def get_items(db: SessionLocal = Depends(get_db)):
            ...

    When deployed back into buckler/idd, replace with:
        from backend.data.db import get_db
    """
    session = SessionLocal()
    try:
        async with session:
            yield session
    finally:
        pass
