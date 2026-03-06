"""SQLAlchemy compatibility layer for declaro-persistum.

This module provides SQLAlchemy-compatible interfaces for code migrating from
SQLAlchemy ORM to declaro-persistum. When deployed back into buckler/idd,
these will be replaced with the actual SQLAlchemy implementations.

Exports:
    - Base: Declarative base class for ORM models
    - SessionLocal: Session factory (returns async connection from pool)
    - get_db: Dependency injection for FastAPI routes
"""

from declaro_persistum.compat.sqlalchemy_shim import (
    Base,
    SessionLocal,
    get_db,
    configure_database,
)

__all__ = [
    "Base",
    "SessionLocal",
    "get_db",
    "configure_database",
]
