"""
Unit tests for the connection pool.

Tests SQLite pool with in-memory database (no external dependencies).
PostgreSQL and Turso tests are integration tests that require real databases.
"""

import asyncio
import pytest

from declaro_persistum.pool import (
    ConnectionPool,
    SQLitePool,
    PostgreSQLPool,
    TursoPool,
    TursoCloudManager,
)
from declaro_persistum.exceptions import (
    PoolClosedError,
    PoolExhaustedError,
    PoolConnectionError,
)

















