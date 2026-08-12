"""
Tests for atomic ``increment(...)`` and Prisma ``update_many``.

Pure SQL-emission tests assert the shape of the UPDATE statement. Integration
tests use a real SQLite pool — no mocks — to verify the operation is
genuinely atomic at the storage layer and that the returned row count
reflects rows actually modified.
"""

from typing import Any

import pytest

from declaro_persistum import increment, Increment, table_factory
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query.table import table
from declaro_persistum.query.update import UpdateQuery


SCHEMA: dict[str, Any] = {
    "tags": {
        "columns": {
            "tag_id": {"type": "text", "primary_key": True, "nullable": False},
            "card_count": {"type": "integer", "nullable": False},
            "last_touched": {"type": "text", "nullable": True},
        },
    },
}


# ---------------------------------------------------------------------------
# Pure SQL-emission tests
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Integration tests against real SQLite — atomic semantics + row count
# ---------------------------------------------------------------------------


@pytest.fixture
async def tags_pool(tmp_path):
    """SQLite pool with a tags table seeded with three rows."""
    db_path = str(tmp_path / "increment_test.db")
    pool = await ConnectionPool.sqlite(db_path)
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE tags ("
            "  tag_id TEXT PRIMARY KEY,"
            "  card_count INTEGER NOT NULL,"
            "  last_touched TEXT"
            ")"
        )
        await conn.execute(
            "INSERT INTO tags (tag_id, card_count) VALUES "
            "('a', 0), ('b', 5), ('c', 10)"
        )
        await conn.commit()
    yield pool
    await pool.close()








