"""Integration tests for SQLite.

These tests use in-memory SQLite databases.
"""

import pytest


@pytest.fixture
async def sqlite_connection():
    """Create SQLite test connection."""
    import aiosqlite

    conn = await aiosqlite.connect(":memory:")

    yield conn

    await conn.close()






