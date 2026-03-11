"""
BDD step definitions for LibSQL embedded replica write visibility tests.

@precommit scenarios run against a local SQLite file (via aiosqlite) — no
Turso Cloud credentials required.  The contract under test (commit makes
writes visible; sync before commit does not) is identical in both SQLite
and libsql embedded replica mode.

@libsql-only scenarios (no @precommit) require TEST_TURSO_URL and
TEST_TURSO_AUTH_TOKEN environment variables and are skipped otherwise.
"""

import asyncio
import os
import tempfile
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/pool/libsql_embedded_replica_writes.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def replica_ctx(tmp_path):
    """Per-scenario mutable context dict."""
    return {
        "pool": None,
        "conn": None,       # the "current" connection (held open across steps)
        "new_conn": None,   # second connection opened mid-scenario
        "db_path": str(tmp_path / "replica.db"),
        "is_cloud": False,
        "turso_url": os.environ.get("TEST_TURSO_URL", ""),
        "turso_token": os.environ.get("TEST_TURSO_AUTH_TOKEN", ""),
    }


# =============================================================================
# Background steps
# =============================================================================

_ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS items (
    id    TEXT    PRIMARY KEY,
    name  TEXT,
    value INTEGER
)
"""

_ITEMS_SCHEMA = {
    "items": {
        "columns": {
            "id":    {"type": "text",    "primary_key": True},
            "name":  {"type": "text"},
            "value": {"type": "integer"},
        }
    }
}


@given("a LibSQL embedded replica pool connected to Turso Cloud")
def given_libsql_pool(replica_ctx, request):
    """
    Set up the pool.

    - @precommit  → local SQLite via aiosqlite (fast, no cloud needed)
    - @libsql only → LibSQLPool against Turso Cloud (skip if unconfigured)
    """
    markers = {m.name for m in request.node.iter_markers()}

    if "precommit" in markers:
        from declaro_persistum.pool import SQLitePool
        replica_ctx["pool"] = SQLitePool(replica_ctx["db_path"])
        replica_ctx["is_cloud"] = False
    else:
        # Cloud-only scenario
        turso_url = replica_ctx["turso_url"]
        turso_token = replica_ctx["turso_token"]
        if not turso_url:
            pytest.skip("TEST_TURSO_URL not set — skipping Turso Cloud scenario")

        from declaro_persistum.pool import LibSQLPool

        async def _make_pool():
            pool = LibSQLPool(
                sync_url=turso_url,
                local_path=replica_ctx["db_path"],
                auth_token=turso_token or None,
            )
            await pool._initial_sync()
            return pool

        replica_ctx["pool"] = asyncio.run(_make_pool())
        replica_ctx["is_cloud"] = True


@given(parsers.parse('the "{table}" table exists with columns:'))
def given_table_exists(replica_ctx, table):
    """Create the items table and leave a persistent connection in context."""

    async def _create():
        async with replica_ctx["pool"].acquire() as conn:
            await conn.execute(_ITEMS_DDL, ())
            await conn.commit()

    asyncio.run(_create())


@given("the table is empty")
def given_table_empty(replica_ctx):
    """Delete all rows from items."""

    async def _clear():
        async with replica_ctx["pool"].acquire() as conn:
            await conn.execute("DELETE FROM items", ())
            await conn.commit()

    asyncio.run(_clear())


@given("the table contains 3 rows")
def given_table_has_3_rows(replica_ctx):
    """Insert three seed rows."""

    async def _seed():
        async with replica_ctx["pool"].acquire() as conn:
            for i in range(1, 4):
                await conn.execute(
                    "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
                    (f"seed-{i:03d}", f"seed_{i}", i),
                )
            await conn.commit()

    asyncio.run(_seed())


# =============================================================================
# When steps — write operations
# =============================================================================


@when(parsers.parse('I insert a row with id "{row_id}" name "{name}" value {value:d}'))
def when_insert_row(replica_ctx, row_id, name, value):
    """Insert a row via the persistent background-loop connection."""
    _open_conn_and_insert(replica_ctx, row_id, name, value)


# We need commit() and sync() to operate on the same connection that was used
# for INSERT.  The challenge: pytest-bdd runs each step synchronously and we
# can't keep an asyncio context manager alive across steps using asyncio.run().
#
# Solution: use a background thread-per-scenario event loop that keeps one
# persistent connection alive.  Steps post coroutines to it via
# asyncio.run_coroutine_threadsafe().

import threading


def _open_conn_and_insert(ctx, row_id, name, value):
    """
    Start a per-scenario background event loop (if not already running),
    acquire one connection that stays open, and insert the row.
    """
    if ctx.get("_loop") is None:
        _start_background_loop(ctx)

    future = asyncio.run_coroutine_threadsafe(
        _do_insert(ctx, row_id, name, value), ctx["_loop"]
    )
    future.result(timeout=10)


async def _do_insert(ctx, row_id, name, value):
    if ctx.get("_conn") is None:
        ctx["_acm"] = ctx["pool"].acquire()
        ctx["_conn"] = await ctx["_acm"].__aenter__()

    await ctx["_conn"].execute(
        "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
        (row_id, name, value),
    )


def _start_background_loop(ctx):
    loop = asyncio.new_event_loop()
    ctx["_loop"] = loop

    def _run():
        loop.run_forever()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    ctx["_thread"] = t


def _close_background_conn(ctx):
    """Release the persistent connection and stop the background loop."""
    loop = ctx.get("_loop")
    if loop is None:
        return

    async def _close():
        acm = ctx.get("_acm")
        if acm is not None:
            await acm.__aexit__(None, None, None)
            ctx["_acm"] = None
            ctx["_conn"] = None

    future = asyncio.run_coroutine_threadsafe(_close(), loop)
    future.result(timeout=10)
    loop.call_soon_threadsafe(loop.stop)


@pytest.fixture(autouse=False)
def _cleanup_background_loop(replica_ctx):
    yield
    _close_background_conn(replica_ctx)


@when("I call commit() on the connection")
def when_commit(replica_ctx):
    """Commit on the persistent connection."""
    future = asyncio.run_coroutine_threadsafe(
        _do_commit(replica_ctx), replica_ctx["_loop"]
    )
    future.result(timeout=10)


async def _do_commit(ctx):
    await ctx["_conn"].commit()


@when("I call sync() on the connection")
def when_sync(replica_ctx):
    """Sync on the persistent connection (no-op for SQLite)."""
    future = asyncio.run_coroutine_threadsafe(
        _do_sync(replica_ctx), replica_ctx["_loop"]
    )
    future.result(timeout=10)


async def _do_sync(ctx):
    conn = ctx["_conn"]
    if hasattr(conn, "sync"):
        await conn.sync()


@when("I call sync() WITHOUT calling commit() first")
def when_sync_no_commit(replica_ctx):
    """Sync without committing — the insert should remain invisible."""
    future = asyncio.run_coroutine_threadsafe(
        _do_sync(replica_ctx), replica_ctx["_loop"]
    )
    future.result(timeout=10)


@when("I open a new connection to the same replica")
def when_new_connection(replica_ctx):
    """Acquire a second connection and store it for Then steps."""

    async def _open():
        acm = replica_ctx["pool"].acquire()
        conn = await acm.__aenter__()
        replica_ctx["_new_acm"] = acm
        replica_ctx["_new_conn"] = conn

    future = asyncio.run_coroutine_threadsafe(_open(), replica_ctx["_loop"])
    future.result(timeout=10)


@when("I open a remote-only connection to the Turso Cloud primary")
def when_remote_connection(replica_ctx):
    """Open a direct Turso Cloud connection (no local replica)."""
    turso_url = replica_ctx["turso_url"]
    turso_token = replica_ctx["turso_token"]
    if not turso_url:
        pytest.skip("TEST_TURSO_URL not set")

    from declaro_persistum.pool import LibSQLPool

    async def _open():
        pool = LibSQLPool(
            sync_url=turso_url,
            local_path=str(replica_ctx["db_path"]) + ".remote.db",
            auth_token=turso_token or None,
        )
        await pool._initial_sync()
        replica_ctx["_remote_pool"] = pool
        acm = pool.acquire()
        conn = await acm.__aenter__()
        replica_ctx["_remote_acm"] = acm
        replica_ctx["_remote_conn"] = conn

    future = asyncio.run_coroutine_threadsafe(_open(), replica_ctx["_loop"])
    future.result(timeout=10)


@when(
    parsers.parse(
        'I acquire a connection from the pool and insert a row with id "{row_id}"'
        ' name "{name}" value {value:d} then release it'
    )
)
def when_pool_insert_and_release(replica_ctx, row_id, name, value):
    """Insert a row through the pool and release the connection immediately."""

    async def _insert_and_release():
        async with replica_ctx["pool"].acquire() as conn:
            await conn.execute(
                "INSERT INTO items (id, name, value) VALUES (?, ?, ?)",
                (row_id, name, value),
            )
            await conn.commit()

    asyncio.run(_insert_and_release())


@when("I acquire a new connection from the pool")
def when_acquire_new_pool_conn(replica_ctx):
    """Acquire a fresh connection for a subsequent read."""
    if replica_ctx.get("_loop") is None:
        _start_background_loop(replica_ctx)

    async def _open():
        acm = replica_ctx["pool"].acquire()
        conn = await acm.__aenter__()
        replica_ctx["_new_acm"] = acm
        replica_ctx["_new_conn"] = conn

    future = asyncio.run_coroutine_threadsafe(_open(), replica_ctx["_loop"])
    future.result(timeout=10)


# =============================================================================
# Then steps — assertions
# =============================================================================


async def _count_on(conn) -> int:
    cursor = await conn.execute("SELECT COUNT(*) FROM items", ())
    row = await cursor.fetchone()
    return int(row[0])


@then(parsers.parse("SELECT COUNT(*) on the same connection returns {expected:d}"))
def then_count_same_conn(replica_ctx, expected):
    future = asyncio.run_coroutine_threadsafe(
        _count_on(replica_ctx["_conn"]), replica_ctx["_loop"]
    )
    actual = future.result(timeout=10)
    assert actual == expected, f"Expected {expected} rows, got {actual}"


@then(parsers.parse("SELECT COUNT(*) on the new connection returns {expected:d}"))
def then_count_new_conn(replica_ctx, expected):
    future = asyncio.run_coroutine_threadsafe(
        _count_on(replica_ctx["_new_conn"]), replica_ctx["_loop"]
    )
    actual = future.result(timeout=10)
    assert actual == expected, f"Expected {expected} rows, got {actual}"


@then(parsers.parse("SELECT COUNT(*) on a fresh connection returns {expected:d}"))
def then_count_fresh_conn(replica_ctx, expected):
    """Open a brand-new connection, count, then close it."""

    async def _fresh_count():
        async with replica_ctx["pool"].acquire() as conn:
            return await _count_on(conn)

    actual = asyncio.run(_fresh_count())
    assert actual == expected, f"Expected {expected} rows, got {actual}"


@then(parsers.parse("SELECT COUNT(*) on the remote connection returns {expected:d}"))
def then_count_remote_conn(replica_ctx, expected):
    future = asyncio.run_coroutine_threadsafe(
        _count_on(replica_ctx["_remote_conn"]), replica_ctx["_loop"]
    )
    actual = future.result(timeout=10)
    assert actual == expected, f"Expected {expected} rows, got {actual}"
