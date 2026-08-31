"""A tmp table that could not be recovered must stop the migration.

WRITTEN BEFORE THE CODE, against a measured data loss.

A reconstruction copies rows into a tmp table, drops the original, then renames
the tmp into its place. `_recover_orphaned_tmp_tables` runs before every
migration to finish that rename if the process died between the two steps.

MEASURED 2026-08-17. Put a view in the way so the rename cannot land:

    Failed to clean up tmp table _declaro_tmp_orders_b2dd4c8f:
      there is already another table or index with this name: orders
    returned: 0
    remaining: _declaro_tmp_orders_b2dd4c8f (table), orders (view)

The failure is logged at error and the loop carries on. The function returns 0.
Its single caller, `apply_migrations_async`, writes

    await _recover_orphaned_tmp_tables(db)

with no assignment, so the count is computed and discarded. The migration then
proceeds, sees `orders` as absent, and creates it empty. The rows are still in
the tmp table and nothing will ever query it.

So the swallow is not a caution, it is the loss. Rule 8: the function raises and
the boundary decides. `apply_migrations_async` is the boundary, and its correct
decision here is to stop, because a migration run against a half-recovered
database is what produced the tmp table in the first place.

THE SECOND SWALLOW IS DIFFERENT AND IS NARROWED RATHER THAN REMOVED. Clearing
the stale schema hash after a recovery legitimately finds no meta table on a
fresh database. That case is expected and is now prevented rather than caught:
the table is ensured first, so anything raised afterwards is real.
"""

from __future__ import annotations

import pytest

from declaro_persistum import close, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.migrations import (
    META_TABLE,
    _recover_orphaned_tmp_tables,
    temp_table_name,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


async def _database(tmp_path):
    return await open_sqlite(
        str(tmp_path / "recover.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )


async def _tables(db) -> dict[str, str]:
    async with writing(db) as conn:
        cursor = await conn.execute("SELECT name, type FROM sqlite_master")
        return {row[0]: row[1] for row in await cursor.fetchall()}


async def test_a_recoverable_table_is_renamed_back(tmp_path) -> None:
    """The ordinary path, so the failure paths below mean something."""
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.execute(f'INSERT INTO "{tmp}" VALUES (7)')
            await conn.commit()

        assert await _recover_orphaned_tmp_tables(db) == 1

        async with writing(db) as conn:
            cursor = await conn.execute("SELECT id FROM orders")
            assert await cursor.fetchall() == [(7,)]
    finally:
        await close(db)


async def test_a_leftover_tmp_beside_a_live_original_is_dropped(tmp_path) -> None:
    """The original survived, so the tmp is junk and recovering nothing is right."""
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute("CREATE TABLE orders (id INT)")
            await conn.execute("INSERT INTO orders VALUES (1)")
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.commit()

        assert await _recover_orphaned_tmp_tables(db) == 0

        tables = await _tables(db)
        assert tmp not in tables, "the leftover tmp table was not cleaned up"
        assert tables["orders"] == "table"
    finally:
        await close(db)


async def test_a_recovery_that_cannot_land_raises(tmp_path) -> None:
    """The measured data loss, in one assertion.

    A view occupying the name blocks the rename. Before this, the failure was
    logged, the count came back 0, the caller discarded it, and the migration
    created `orders` empty beside a tmp table holding the rows.
    """
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.execute(f'INSERT INTO "{tmp}" VALUES (1)')
            await conn.execute("CREATE TABLE src (id INT)")
            await conn.execute("CREATE VIEW orders AS SELECT * FROM src")
            await conn.commit()

        with pytest.raises(MigrationError) as caught:
            await _recover_orphaned_tmp_tables(db)

        assert tmp in str(caught.value), (
            f"the error does not name the table that could not be recovered: {caught.value}"
        )
    finally:
        await close(db)


async def test_the_rows_are_still_there_after_a_failed_recovery(tmp_path) -> None:
    """Raising must not destroy what it could not move.

    The tmp table holds the only copy of the rows. A recovery that gives up
    must leave them where an operator can find them.
    """
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.execute(f'INSERT INTO "{tmp}" VALUES (1), (2), (3)')
            await conn.execute("CREATE TABLE src (id INT)")
            await conn.execute("CREATE VIEW orders AS SELECT * FROM src")
            await conn.commit()

        with pytest.raises(MigrationError):
            await _recover_orphaned_tmp_tables(db)

        async with writing(db) as conn:
            cursor = await conn.execute(f'SELECT id FROM "{tmp}" ORDER BY id')
            assert await cursor.fetchall() == [(1,), (2,), (3,)], (
                "the rows the recovery could not move are gone"
            )
    finally:
        await close(db)


async def test_a_recovery_clears_the_stale_schema_hash(tmp_path) -> None:
    """Otherwise the next start believes the schema is current when it is not.

    The stamp says "this schema was applied". After a recovery it was applied
    to a database that then lost a table, so it is no longer true.
    """
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute(
                f'CREATE TABLE "{META_TABLE}" '
                f"(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            await conn.execute(
                f'INSERT INTO "{META_TABLE}" VALUES (?, ?, ?)',
                ("schema_hash:models.py:abc", "abc", "2026-08-17"),
            )
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.commit()

        assert await _recover_orphaned_tmp_tables(db) == 1

        async with writing(db) as conn:
            cursor = await conn.execute(f'SELECT COUNT(*) FROM "{META_TABLE}"')
            assert (await cursor.fetchone())[0] == 0, (
                "the stale stamp survived the recovery, so the next start skips "
                "the migration that would rebuild what was lost"
            )
    finally:
        await close(db)


async def test_a_fresh_database_with_no_meta_table_still_recovers(tmp_path) -> None:
    """The case the second swallow was written for, and it must not need one.

    A database that has never been migrated has no `_declaro_meta`. Clearing a
    stamp from a table that does not exist used to raise and be caught. The
    table is ensured first now, so the expected case is prevented rather than
    absorbed, and a genuine failure to clear the stamp is free to escape.
    """
    db = await _database(tmp_path)
    tmp = temp_table_name("orders")
    try:
        async with writing(db) as conn:
            await conn.execute(f'CREATE TABLE "{tmp}" (id INT)')
            await conn.commit()

        assert (await _tables(db)).get(META_TABLE) is None

        assert await _recover_orphaned_tmp_tables(db) == 1

        tables = await _tables(db)
        assert tables.get("orders") == "table", "the recovery did not happen"
    finally:
        await close(db)


async def test_nothing_to_recover_is_not_a_failure(tmp_path) -> None:
    """The common case: every start of every healthy database."""
    db = await _database(tmp_path)
    try:
        async with writing(db) as conn:
            await conn.execute("CREATE TABLE orders (id INT)")
            await conn.commit()

        assert await _recover_orphaned_tmp_tables(db) == 0
    finally:
        await close(db)
