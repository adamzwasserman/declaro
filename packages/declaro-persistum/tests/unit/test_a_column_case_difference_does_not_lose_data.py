"""A rebuild must copy a column the engine considers the same column.

SQLite folds identifier case: `Note` and `note` are one column for every query
you can write. Python's `in` does not fold anything, and the rebuild decides
what to copy with

    common_columns = [col for col in old_columns if col in new_columns]

MEASURED 2026-08-17:

    stored columns:            ['id', 'Note']
    declared:                  {'id': ..., 'note': ...}
    rows after reconstruction: [(1, None)]

The row survives and the value does not. `'keep me'` is gone.

NOTHING WARNS, AND THAT IS THE WORST PART. The rebuild logs "No common columns
... No data will be copied" only when the intersection is EMPTY. Here `id`
matched, so the intersection is non-empty and the copy proceeds, quietly
missing one column. The loud case is the one you would notice anyway, because
the table comes out empty. The quiet case is the one you would not.

HOW A SCHEMA ENDS UP MIXED-CASE. A table created by hand, by an earlier tool,
or by a migration written before a naming convention. The models file then
spells it lower case, everything reads correctly for the life of the database,
and the data disappears the first time a rebuild runs.

This is the same family as the Turso table-name folding that dropped and
recreated a mixed-case table on every deploy.
"""

from __future__ import annotations

import aiosqlite
import pytest

from declaro_persistum.abstractions.table_reconstruction import reconstruct_table
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]


async def _table(path, ddl: str, row: tuple):
    conn = await aiosqlite.connect(path)
    await conn.execute(ddl)
    marks = ", ".join("?" for _ in row)
    await conn.execute(f"INSERT INTO t VALUES ({marks})", row)
    await conn.commit()
    return conn


async def test_a_column_whose_case_matches_exactly_is_copied(tmp_path) -> None:
    """The baseline, so the case below is about case and nothing else."""
    conn = await _table(
        tmp_path / "same.db",
        'CREATE TABLE t (id INTEGER PRIMARY KEY, "note" TEXT)',
        (1, "keep me"),
    )
    try:
        await reconstruct_table(conn, "t", {
            "id": {"type": "INTEGER", "primary_key": True},
            "note": {"type": "TEXT", "nullable": True},
        }, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
        await conn.commit()

        cursor = await conn.execute("SELECT id, note FROM t")
        assert await cursor.fetchall() == [(1, "keep me")]
    finally:
        await conn.close()


@pytest.mark.parametrize(
    ("stored", "declared"),
    [("Note", "note"), ("NOTE", "note"), ("note", "Note"), ("nOtE", "NoTe")],
)
async def test_a_column_that_differs_only_in_case_is_still_copied(
    tmp_path, stored: str, declared: str
) -> None:
    """The measured loss, in every spelling.

    One case pair would leave three untested, and the rule is about folding
    rather than about any particular pair.
    """
    conn = await _table(
        tmp_path / f"{stored}.db",
        f'CREATE TABLE t (id INTEGER PRIMARY KEY, "{stored}" TEXT)',
        (1, "keep me"),
    )
    try:
        await reconstruct_table(conn, "t", {
            "id": {"type": "INTEGER", "primary_key": True},
            declared: {"type": "TEXT", "nullable": True},
        }, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
        await conn.commit()

        cursor = await conn.execute(f'SELECT id, "{declared}" FROM t')
        assert await cursor.fetchall() == [(1, "keep me")], (
            f"a column stored as {stored!r} and declared as {declared!r} lost "
            f"its data. SQLite treats them as one column everywhere else."
        )
    finally:
        await conn.close()


async def test_a_column_the_new_schema_drops_is_still_dropped(tmp_path) -> None:
    """Folding case must not resurrect a column somebody meant to remove."""
    conn = await _table(
        tmp_path / "drop.db",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT, gone TEXT)",
        (1, "keep me", "remove me"),
    )
    try:
        await reconstruct_table(conn, "t", {
            "id": {"type": "INTEGER", "primary_key": True},
            "note": {"type": "TEXT", "nullable": True},
        }, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
        await conn.commit()

        cursor = await conn.execute("PRAGMA table_info(t)")
        assert [r[1] for r in await cursor.fetchall()] == ["id", "note"]

        cursor = await conn.execute("SELECT note FROM t")
        assert await cursor.fetchall() == [("keep me",)]
    finally:
        await conn.close()


async def test_a_new_column_is_added_empty(tmp_path) -> None:
    """A column with nothing to copy from must not break the copy."""
    conn = await _table(
        tmp_path / "add.db",
        "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)",
        (1, "keep me"),
    )
    try:
        await reconstruct_table(conn, "t", {
            "id": {"type": "INTEGER", "primary_key": True},
            "note": {"type": "TEXT", "nullable": True},
            "added": {"type": "TEXT", "nullable": True},
        }, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
        await conn.commit()

        cursor = await conn.execute("SELECT id, note, added FROM t")
        assert await cursor.fetchall() == [(1, "keep me", None)]
    finally:
        await conn.close()


async def test_a_rebuild_with_no_shared_columns_still_copies_nothing(
    tmp_path,
) -> None:
    """The case that already warned must keep warning rather than folding into a match."""
    conn = await _table(
        tmp_path / "none.db",
        "CREATE TABLE t (old_id INTEGER PRIMARY KEY, old_note TEXT)",
        (1, "keep me"),
    )
    try:
        await reconstruct_table(conn, "t", {
            "new_id": {"type": "INTEGER", "primary_key": True},
            "new_note": {"type": "TEXT", "nullable": True},
        }, preserve_data=NOTHING, manage_foreign_keys=NOTHING)
        await conn.commit()

        cursor = await conn.execute("SELECT COUNT(*) FROM t")
        assert (await cursor.fetchone())[0] == 0
    finally:
        await conn.close()
