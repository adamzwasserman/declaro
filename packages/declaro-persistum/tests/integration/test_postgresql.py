"""Integration tests for PostgreSQL.

These tests require a running PostgreSQL database.
Set TEST_POSTGRESQL_URL environment variable to run.
"""

import os

import pytest

# Skip all tests in this module if no PostgreSQL URL is set
pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRESQL_URL"),
    reason="TEST_POSTGRESQL_URL not set",
)


@pytest.fixture
async def pg_connection():
    """Create PostgreSQL test connection."""
    import asyncpg

    url = os.environ.get("TEST_POSTGRESQL_URL")
    conn = await asyncpg.connect(url)

    # Create a test schema
    await conn.execute("CREATE SCHEMA IF NOT EXISTS declaro_test")
    await conn.execute("SET search_path TO declaro_test")

    yield conn

    # Cleanup
    await conn.execute("DROP SCHEMA declaro_test CASCADE")
    await conn.close()






