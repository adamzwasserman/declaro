"""`writing(db)` commits when the block ends, and rolls back when it raises.

IT DID NEITHER. It opened a connection, yielded it and closed it, so a block
that forgot `await conn.commit()` had its writes discarded and reported
nothing. `execute_fk_ordered` omitted the commit and every one of its writes
was rolled back while `cursor.rowcount` still said success.

BOTH PATHS, WHICH IS THE POINT OF THIS FILE. `writing` branches: a local
database returns early with no lock, a replicated one takes the serialise
lock. The first version of the fix went into the second branch only. Every
local write still vanished, the suite stayed green at 830 passing, and the
probe that caught it read 0 rows where it had read 1 BEFORE the fix — the
change made it worse and nothing said so.

So these tests run against both branches. `serialise` is what selects one, and
it is a field, so a test can set it.

POSTGRESQL IS A DELIBERATE NO-OP. asyncpg has no `commit()`; `postgres_write_one`
runs inside `conn.transaction()`, which commits on exit. Measured 2026-08-14:
a `writing` block with no explicit commit leaves the row in place there too, by
a different mechanism.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.database import new_write_lock, reading, writing
from declaro_persistum.sqlite_database import open_sqlite
from declaro_persistum.turso_database import migrating, open_turso
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.turso


async def _sqlite(tmp_path, name):
    db = await open_sqlite(
        str(tmp_path / name), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    async with writing(db) as conn:
        await conn.execute("CREATE TABLE t (v INT)")
    return db


async def _count(db) -> int:
    async with reading(db) as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM t")
        return (await cursor.fetchone())[0]


@pytest.mark.parametrize("serialised", [False, True])
@pytest.mark.asyncio
async def test_a_clean_block_commits_on_both_paths(
    tmp_path, serialised: bool
) -> None:
    """No explicit commit, and the write is still there."""
    db = await _sqlite(tmp_path, f"c{serialised}.db")
    if serialised:
        db["serialise"] = new_write_lock(asyncio.Lock())

    async with writing(db) as conn:
        await conn.execute("INSERT INTO t VALUES (1)")

    assert await _count(db) == 1, (
        "the block ended cleanly and its write was discarded"
    )


@pytest.mark.parametrize("serialised", [False, True])
@pytest.mark.asyncio
async def test_a_block_that_raises_rolls_back_on_both_paths(
    tmp_path, serialised: bool
) -> None:
    db = await _sqlite(tmp_path, f"r{serialised}.db")
    if serialised:
        db["serialise"] = new_write_lock(asyncio.Lock())

    with pytest.raises(RuntimeError):
        async with writing(db) as conn:
            await conn.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("the caller changed its mind")

    assert await _count(db) == 0, "a failed block left its write behind"


@pytest.mark.parametrize("serialised", [False, True])
@pytest.mark.asyncio
async def test_an_explicit_commit_still_works(tmp_path, serialised: bool) -> None:
    """A second commit is harmless, so existing code is not broken by this."""
    db = await _sqlite(tmp_path, f"e{serialised}.db")
    if serialised:
        db["serialise"] = new_write_lock(asyncio.Lock())

    async with writing(db) as conn:
        await conn.execute("INSERT INTO t VALUES (1)")
        await conn.commit()

    assert await _count(db) == 1


@pytest.mark.asyncio
async def test_the_serialise_lock_is_released_after_a_failed_block(
    tmp_path,
) -> None:
    """A rollback must not leave the lock held, or the next writer hangs."""
    db = await _sqlite(tmp_path, "lock.db")
    db["serialise"] = new_write_lock(asyncio.Lock())

    with pytest.raises(RuntimeError):
        async with writing(db) as conn:
            await conn.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")

    async with asyncio.timeout(5):
        async with writing(db) as conn:
            await conn.execute("INSERT INTO t VALUES (2)")
    assert await _count(db) == 1


@pytest.mark.asyncio
async def test_turso_commits_too(tmp_path) -> None:
    """The other DB-API engine, on the path it actually takes."""
    db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
    async with migrating(db) as conn:
        await conn.execute("CREATE TABLE t (v INT)")
        await conn.commit()

    async with writing(db) as conn:
        await conn.execute("INSERT INTO t VALUES (1)")

    assert await _count(db) == 1


def test_both_paths_finish_a_block_the_same_way() -> None:
    """The structural guard: two paths that must agree share one function.

    IT USED TO COUNT `"_write_connection(db)"` TWICE IN `writing`'S SOURCE,
    and on 2026-08-22 the two branches became `_WRITE_PATH`, a table read by
    `writing`. The string left that function and the test went red while the
    property it guards still held, which is the same failure it exists to
    catch pointed the other way: it was reading one function's text rather
    than asking where the paths go.

    It asks the table now. Every entry either is `_write_connection` or
    reaches it, so a future edit that gives one path its own commit fails
    here whichever shape the choice is written in.
    """
    import inspect

    from declaro_persistum import database

    shared = database._write_connection
    stray = [
        f"{key}: {entry.__name__}"
        for key, entry in database._WRITE_PATH.items()
        if entry is not shared
        and "_write_connection(db)" not in inspect.getsource(entry)
    ]

    assert not stray, (
        f"these write paths no longer go through the shared transaction "
        f"scope: {stray}. That is exactly how the commit came to apply to "
        f"replicated databases only."
    )
    assert len(database._WRITE_PATH) == 2, (
        "the write paths are no longer two, so this test is measuring "
        "something other than the pair it was written for"
    )


@pytest.mark.asyncio
async def test_every_engine_scopes_a_block_the_same_way() -> None:
    """One surface over every database, which is priority 2, asserted.

    A block that raises rolled back on SQLite and Turso and COMMITTED on
    PostgreSQL. Same call, opposite outcome, measured 2026-08-14:

        sqlite     rows after a block that raised: 0
        turso      rows after a block that raised: 0
        postgresql rows after a block that raised: 1

    The cause was shape. Commit and rollback were two tables, which let the
    PostgreSQL half be right about the crew path and wrong about this one:
    `writing` hands the raw asyncpg connection to the caller, who executes
    outside any transaction, so asyncpg autocommits and there is nothing left
    to roll back. Two halves of one concept are now one entry.

    Asserted on the table rather than by driving a live PostgreSQL, so it runs
    everywhere. The behaviour itself was measured on all three.
    """
    from declaro_persistum.writers import TRANSACTIONS

    assert set(TRANSACTIONS) == {"sqlite", "turso", "postgresql"}
    assert all(TRANSACTIONS.values()), "a dialect has no transaction scope"
    # PostgreSQL must NOT be a no-op. That was the defect.
    from declaro_persistum.writers import postgres_transaction

    class _Conn:
        def transaction(self):
            return "asyncpg-transaction"

    assert postgres_transaction(_Conn()) == "asyncpg-transaction", (
        "the PostgreSQL entry does not open a transaction, so a failed block "
        "leaves its writes committed"
    )


def test_a_write_with_no_sql_is_refused_at_the_door() -> None:
    """The proof for a silence Umbra named, written by hand.

    Umbra reported: "Parameter 'sql' permits the empty string region, and no
    literal test call demonstrates it." Following it to a real database showed
    the region is reachable and wasteful rather than harmless:

        empty-SQL write receipt:
          {'ok': False, 'error': 'no SQL statements to execute'}

    That answer came from the ENGINE, after a ticket, a drainer, a connection
    and a round trip. `deposit` now refuses it, which is Rule 13: the boundary
    states the contract so the interior can trust that `sql` means a
    statement.
    """
    from declaro_persistum.write_queue import deposit, new_room

    room = new_room()
    for empty in ("", "   ", "\n"):
        with pytest.raises(ValueError, match="needs SQL"):
            deposit(room, {"sql": empty, "params": ()})
    assert room["writes"] == [], "a refused write was queued anyway"
