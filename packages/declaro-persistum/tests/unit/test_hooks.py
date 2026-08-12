"""
Tests for query hooks — pre/post function injection.

Unit tests rely on pure-function assertions. Integration tests use real
in-memory SQLite to exercise the end-to-end hook flow.

Hook design:
    pre_hook:  (query_object) -> query_object      # runs before SQL is built
    post_hook: (rows, QueryMeta) -> rows           # runs after DB returns
"""

from typing import Any

import pytest

from declaro_persistum import PostHook, PreHook, QueryMeta, table_factory
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query.delete import DeleteQuery
from declaro_persistum.query.insert import InsertQuery
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.table import TableProxy, table
from declaro_persistum.query.update import UpdateQuery


# ---------------------------------------------------------------------------
# Minimal schema used across tests (no DB needed to construct/inspect queries)
# ---------------------------------------------------------------------------

SCHEMA = {
    "items": {
        "columns": {
            "id": {"type": "integer", "primary_key": True, "nullable": False},
            "name": {"type": "text", "nullable": False},
            "owner": {"type": "text", "nullable": True},
            "deleted_at": {"type": "text", "nullable": True},
        },
    },
}


# ---------------------------------------------------------------------------
# Pure tests — no DB, no pool required
# ---------------------------------------------------------------------------










# ---------------------------------------------------------------------------
# Execution tests — real SQLite pool, no mocks
# ---------------------------------------------------------------------------


@pytest.fixture
async def sqlite_pool(tmp_path):
    """Real file-backed SQLite pool with a seeded items table.

    File-backed (not ``:memory:``) so the schema is visible across the
    multiple short-lived connections the pool hands out per execute.
    """
    db_path = str(tmp_path / "hooks_test.db")
    pool = await ConnectionPool.sqlite(db_path)
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE items ("
            "  id INTEGER PRIMARY KEY,"
            "  name TEXT NOT NULL,"
            "  owner TEXT,"
            "  deleted_at TEXT"
            ")"
        )
        await conn.execute(
            "INSERT INTO items (id, name, owner) VALUES "
            "(1, 'a', 'alice'),"
            "(2, 'b', 'bob'),"
            "(3, 'c', 'alice')"
        )
        await conn.commit()
    yield pool
    await pool.close()






