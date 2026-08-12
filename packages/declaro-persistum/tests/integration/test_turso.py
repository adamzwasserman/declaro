"""Integration tests for Turso (pyturso).

These tests can run against:
1. A local temp database via TursoPool (default) - for CI/quick testing
2. A Turso cloud database - set TEST_TURSO_CLOUD=1 *and* TEST_TURSO_URL and
   TEST_TURSO_AUTH_TOKEN.

Cloud mode is destructive: the fixture drops every table matching 'test_%'
on the remote database, so it must be requested deliberately.
"""

import os
import tempfile
import pytest


# Determine test mode.
#
# Cloud mode is opt-in via TEST_TURSO_CLOUD, not merely by having credentials
# present. tests/conftest.py calls load_dotenv(), so TEST_TURSO_URL and
# TEST_TURSO_AUTH_TOKEN are populated from .env on every run — keying cloud
# mode off their presence alone silently pointed the whole suite at a real
# remote database, where this fixture DROPs every table matching 'test_%'.
# A destructive remote run must be asked for explicitly.
TURSO_URL = os.environ.get("TEST_TURSO_URL")
TURSO_TOKEN = os.environ.get("TEST_TURSO_AUTH_TOKEN")
USE_CLOUD = bool(os.environ.get("TEST_TURSO_CLOUD") and TURSO_URL and TURSO_TOKEN)


@pytest.fixture
async def turso_connection():
    """Create Turso areplica connection via TursoPool."""
    from declaro_persistum.pool import ConnectionPool

    if USE_CLOUD:
        # auth_token must be passed explicitly: ConnectionPool.turso threads
        # it into turso.aio.sync.connect(auth_token=...). Omitting it made
        # every cloud run fail with "401 unauthorized ... empty JWT token",
        # which read as missing credentials rather than a dropped argument.
        pool = await ConnectionPool.turso(
            "./db/test_integration.db",
            remote_url=TURSO_URL,
            auth_token=TURSO_TOKEN,
        )
        try:
            async with pool.acquire() as conn:
                # Clean up any existing test tables from prior runs
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'test_%'"
                )
                rows = await cursor.fetchall()
                for row in rows:
                    await conn.execute(f"DROP TABLE IF EXISTS {row[0]}")
                await conn.commit()

                yield conn

                # Cleanup
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'test_%'"
                )
                rows = await cursor.fetchall()
                for row in rows:
                    await conn.execute(f"DROP TABLE IF EXISTS {row[0]}")
                await conn.commit()
        finally:
            # The cloud branch previously leaked the pool; only the local
            # branch closed it.
            await pool.close()

    else:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        pool = await ConnectionPool.turso(db_path)
        async with pool.acquire() as conn:
            yield conn

        await pool.close()
        os.unlink(db_path)






