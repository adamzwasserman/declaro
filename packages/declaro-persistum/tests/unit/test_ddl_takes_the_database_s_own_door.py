"""A schema change must not leave a live reader looking at the old schema.

`migrations._for_ddl` returned `writing(db)` for every backend. On a LOCAL
Turso database that is an MVCC connection, and a table created on one is
invisible to any other connection that has already READ. The migration
reported success; the reader went on seeing the old schema; nothing raised.

THE TRIGGER IS A PRIOR READ, WHICH IS NOT WHAT THE OLD DOCSTRING SAID.
`migrating` blamed "both connections opened before any write". Four variants,
the other connection open before the DDL in every one, measured 2026-08-14 on
pyturso 0.7.2:

    A writes, then DDL                  fine
    the other connection READ first     "Parse error: no such table: t"
    ALTER instead of CREATE             fine
    the other connection WROTE first    fine

One row fails, and it fails every run.

`migrating` IS NOT A DROP-IN REPLACEMENT, which is why this took a field on
the Database rather than a one-line swap:

    other connection open, has not read   migrating works
    other connection open, has read       "database is locked"

Raising is the right answer. A loud, true failure beats a silent one, and
`crew.py` already requires that migration finish before a crew starts.

THE SCOPE IS ONE DATABASE, NOT THE PROCESS. The lock is on the file, so
`migrating(db)` quiesces that `Database` and nothing else. One database per
tenant is the shape this package is built for, and migrating tenant 123 must
not stop tenants 456 and 789 from serving. It does not: they are different
files with different locks. Nor does the quiet period apply to a replicated
database at all, whose door is `writing` and takes no exclusive file lock.

AND IT CANNOT BE APPLIED EVERYWHERE. On a replicated database `writing` is the
only correct door: the held connection is the one bound to the primary, and
`migrating` would open a fresh non-sync connection whose DDL never leaves the
machine. So the door is a property of the database, settled at open.
"""

from __future__ import annotations

import pytest

from declaro_persistum.database import writing
from declaro_persistum.migrations import _for_ddl
from declaro_persistum.sqlite_database import open_sqlite
from declaro_persistum.turso_database import migrating, open_turso

pytestmark = pytest.mark.turso


async def _seeded(db):
    async with _for_ddl(db) as conn:
        await conn.execute("CREATE TABLE seed (v INT)")
        await conn.commit()
    return db


@pytest.mark.asyncio
async def test_a_reader_is_not_blinded_by_a_migration(tmp_path) -> None:
    """The regression, in the exact shape that failed.

    Under `writing`, the final SELECT raised "Parse error: no such table: t".
    """
    db = await _seeded(
        await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately")
    )

    reader = await db["connect"](db)
    cursor = await reader.execute("SELECT COUNT(*) FROM seed")
    await cursor.fetchone()  # THE TRIGGER. Without this read, nothing fails.

    try:
        async with _for_ddl(db) as conn:
            await conn.execute("CREATE TABLE t (v INT)")
            await conn.commit()
    except Exception as e:
        # The local Turso door is WAL and wants the file to itself. Refusing
        # while a reader holds it is the loud failure this exists to produce.
        assert "locked" in str(e).lower(), f"unexpected refusal: {e}"
        return
    finally:
        await db["close_connection"](reader)

    cursor = await reader.execute("SELECT COUNT(*) FROM t")
    assert (await cursor.fetchone())[0] == 0, "the reader cannot see the new table"


@pytest.mark.asyncio
async def test_migrating_one_tenant_does_not_stop_another_from_serving(
    tmp_path,
) -> None:
    """The quiet period is one FILE wide, and multi-tenancy rests on that.

    One database per tenant is the shape Turso Cloud is built for. If
    `migrating` quiesced the process rather than the database, migrating one
    tenant would stop every other tenant, and rolling migrations would be
    impossible. Asserted rather than assumed, because "different files, so
    different locks" is exactly the kind of reasoning that turns out to be
    wrong about an engine.
    """
    a = await open_turso(str(tmp_path / "tenant-a.db"), shutdown="exit_immediately")
    b = await _seeded(
        await open_turso(str(tmp_path / "tenant-b.db"), shutdown="exit_immediately")
    )

    reader = await b["connect"](b)
    cursor = await reader.execute("SELECT COUNT(*) FROM seed")
    await cursor.fetchone()  # tenant B is live and has read

    async with _for_ddl(a) as conn:  # tenant A migrates anyway
        await conn.execute("CREATE TABLE t (v INT)")
        await conn.commit()

    cursor = await reader.execute("SELECT COUNT(*) FROM seed")
    assert (await cursor.fetchone())[0] == 0, "tenant B stopped serving"
    await b["close_connection"](reader)


@pytest.mark.asyncio
async def test_a_local_turso_database_sends_ddl_through_the_wal_door(
    tmp_path,
) -> None:
    """The wiring, asserted on the value rather than on behaviour."""
    db = await open_turso(str(tmp_path / "local.db"), shutdown="exit_immediately")
    assert db["for_ddl"] is migrating, (
        "a local Turso database writes on MVCC, where DDL is invisible to a "
        "connection that has read; its DDL door must be the WAL one"
    )


@pytest.mark.asyncio
async def test_sqlite_uses_the_same_door_for_ddl_and_writes(tmp_path) -> None:
    """Not every engine needs a second door, and inventing one would be noise."""
    db = await open_sqlite(
        str(tmp_path / "s.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    assert db["for_ddl"] is writing


@pytest.mark.asyncio
async def test_ddl_still_runs_through_the_door_the_database_names(tmp_path) -> None:
    """`_for_ddl` asks the value. It must not have kept a favourite."""
    db = await open_turso(str(tmp_path / "asks.db"), shutdown="exit_immediately")
    asked: list[str] = []

    def record(_db):
        asked.append("asked")
        return migrating(_db)

    db["for_ddl"] = record
    async with _for_ddl(db) as conn:
        await conn.execute("CREATE TABLE t (v INT)")
        await conn.commit()

    assert asked == ["asked"], "_for_ddl chose a door instead of asking for one"


def test_the_door_is_required_at_construction() -> None:
    """Rule 14. There is no sensible guess for which door an engine needs."""
    import inspect

    from declaro_persistum.database import new_database

    param = inspect.signature(new_database).parameters["for_ddl"]
    assert param.default is inspect.Parameter.empty
