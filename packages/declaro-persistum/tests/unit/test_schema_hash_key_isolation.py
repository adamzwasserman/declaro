"""Two services sharing one database must not fight over the skip-if-clean stamp.

The stamp lives in `_declaro_meta` in the database being migrated, under a key
built from the schema file's name AND its hash. Two services that both migrate
the same database therefore write to the same table.

They disagree whenever their computed hash differs, and the hash covers both
the schema file's contents and the library version. So:

  - two services with their own models.py, different content, same filename
  - or two services on different declaro-persistum versions

each read the other's stamp, see a mismatch, re-introspect, and re-stamp.
Neither is wrong and neither can win. The result is a permanent never-clean
state with operations re-proposed on every boot, which is also the symptom of a
genuine schema drift, so it is easy to misdiagnose.

Reported by a consumer whose stage service pointed its central pool at the
production central database.

A REAL SQLITE DATABASE, NOT A DICT PRETENDING TO BE ONE. These tests used
`_FakeMetaConn`, a class that read the SQL as a string, matched `insert into`
and `select value from` by prefix, and kept a Python dict. It could not fail
for any reason the real table would, because it never ran the statement.

What it specifically could not see is the whole mechanism under test. The real
`_store_hash` is an UPSERT:

    INSERT INTO "_declaro_meta" (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value, ...

and the double implemented that as `rows[key] = value`, which is upsert
semantics by luck of a dict rather than by the clause. A broken `ON CONFLICT`,
a wrong column in `excluded`, a primary key that is not one: every one of those
passes against a dict and fails against SQLite.

`conn.commit()` was also `raise NotImplementedError("CODE NOT WRITTEN")` after
the stub sweep on 2026-08-16, which is what made these five fail and is why
they are here now.
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.migrations import (
    META_TABLE,
    _ensure_meta_table,
    _get_stored_hash,
    _store_hash,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


@pytest.fixture
async def meta(tmp_path):
    """One real database with a real `_declaro_meta`, per test.

    Built by `_ensure_meta_table`, the same function migrations call, so the
    table under test is the table the library makes rather than one this file
    invented to match.
    """
    async with aiosqlite.connect(tmp_path / "shared.db") as conn:
        await _ensure_meta_table(conn)
        yield conn


async def _rows(conn) -> list[tuple]:
    cursor = await conn.execute(f'SELECT key, value FROM "{META_TABLE}" ORDER BY key')
    return list(await cursor.fetchall())


async def test_different_schema_content_does_not_collide(meta) -> None:
    """Two services, each with its own models.py, must not evict each other.

    Same filename, different content, so different hashes.
    """
    await _store_hash(meta, "models.py", "hash-from-service-a")
    await _store_hash(meta, "models.py", "hash-from-service-b")

    a = await _get_stored_hash(meta, "models.py", "hash-from-service-a")
    b = await _get_stored_hash(meta, "models.py", "hash-from-service-b")

    assert a == "hash-from-service-a", (
        "service A's stamp was evicted by service B. A will re-introspect on "
        "every boot, and so will B, forever, and it looks like schema drift."
    )
    assert b == "hash-from-service-b"

    assert len(await _rows(meta)) == 2, (
        f"the two services share one row: {await _rows(meta)}. The key must "
        f"carry the hash, or the second write overwrites the first."
    )


async def test_version_skew_does_not_collide(meta) -> None:
    """Same models file, two library versions, must not evict each other.

    The version is mixed into the hash deliberately, so an upgraded service and
    a not-yet-upgraded one compute different hashes for identical models. Both
    must be able to record that they are clean.
    """
    await _store_hash(meta, "models.py", "hash-under-0.1.8")
    await _store_hash(meta, "models.py", "hash-under-0.1.14")

    assert await _get_stored_hash(meta, "models.py", "hash-under-0.1.8") == (
        "hash-under-0.1.8"
    )
    assert await _get_stored_hash(meta, "models.py", "hash-under-0.1.14") == (
        "hash-under-0.1.14"
    )


async def test_unknown_hash_reads_as_absent(meta) -> None:
    """A hash never stored must read as absent, so the schema is dirty.

    `None` and "a stamp that does not match" have to be the same answer here.
    A stored-but-different hash reading as present would report clean.
    """
    await _store_hash(meta, "models.py", "known")

    assert await _get_stored_hash(meta, "models.py", "never-stored") is None
    assert await _get_stored_hash(meta, "never-seen.py", "known") is None


async def test_same_service_restamping_is_idempotent(meta) -> None:
    """Storing the same hash twice must not create a second row.

    THIS IS THE ONE THE DICT COULD NOT TEST. Against a dict, a second
    assignment to the same key is idempotent whatever the SQL says. Against
    SQLite the `ON CONFLICT (key) DO UPDATE` clause is what makes it so, and a
    missing one raises `UNIQUE constraint failed` on the second call.
    """
    await _store_hash(meta, "models.py", "same")
    await _store_hash(meta, "models.py", "same")

    rows = await _rows(meta)
    assert len(rows) == 1, f"restamping created a second row: {rows}"


async def test_restamping_moves_the_timestamp_forward(meta) -> None:
    """The upsert must UPDATE, not merely leave the old row alone.

    `DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`
    is the clause. `DO NOTHING` would also keep the row count at one and pass
    the test above, while silently never refreshing the stamp.
    """
    await _store_hash(meta, "models.py", "same")
    cursor = await meta.execute(f'SELECT updated_at FROM "{META_TABLE}"')
    first = (await cursor.fetchone())[0]

    await _store_hash(meta, "models.py", "same")
    cursor = await meta.execute(f'SELECT updated_at FROM "{META_TABLE}"')
    second = (await cursor.fetchone())[0]

    assert second > first, (
        f"the stamp did not move: {first} to {second}. `DO NOTHING` would also "
        f"keep the row count at one and pass the test above, so a strict "
        f"comparison is the only thing that tells the two clauses apart. "
        f"`datetime.now(UTC).isoformat()` carries microseconds, measured, so "
        f"two consecutive calls do differ."
    )


async def test_different_schema_files_stay_separate(meta) -> None:
    """Distinct schema filenames must remain distinct, as before."""
    await _store_hash(meta, "users.py", "h1")
    await _store_hash(meta, "orders.py", "h2")

    assert await _get_stored_hash(meta, "users.py", "h1") == "h1"
    assert await _get_stored_hash(meta, "orders.py", "h2") == "h2"
    assert len(await _rows(meta)) == 2


async def test_the_meta_table_is_the_one_migrations_build(meta) -> None:
    """The floor. Every assertion above is about a table this did not invent.

    If `_ensure_meta_table` stops making `key` a primary key, the upsert has no
    conflict target and `_store_hash` raises rather than upserting. Asserting
    the shape here says which of the two broke.
    """
    cursor = await meta.execute(f'PRAGMA table_info("{META_TABLE}")')
    columns = {row[1]: row for row in await cursor.fetchall()}

    assert set(columns) == {"key", "value", "updated_at"}, sorted(columns)
    assert columns["key"][5] == 1, (
        f"`key` is not the primary key: {columns['key']}. The upsert's "
        f"ON CONFLICT (key) needs it, and without it every restamp inserts."
    )
    assert columns["value"][3] == 1, "`value` is nullable; a stamp is not optional"
