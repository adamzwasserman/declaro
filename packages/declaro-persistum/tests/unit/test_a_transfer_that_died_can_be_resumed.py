"""Resume is what makes a transfer restartable, and none of it had run.

MEASURED 2026-08-18. Of the 23 statements in `transfer.py` that the whole
suite never executed, these are the ones that matter:

    skip a table a previous run completed        7 statements
    restart a table left in_progress             4 statements
    report progress through the callback         5 statements
    read a table's status through asyncpg        5 statements

`resume` defaults to True in both `bulk_transfer` and `begin_cutover`, so the
untested branch is the one every caller gets. A cutover that dies halfway is
the case resume exists for, and nothing had ever half-died.

A TRANSFER THAT FAILED ONCE COULD NEVER SUCCEED ON RESUME. Measured 2026-08-18
with three rows, batch size 1, and a row already sitting in the target that
collides with the source's third:

    first run    success=False  UNIQUE constraint failed: widgets.id
    status left  ('failed', 0)  though two rows had landed
    operator removes the colliding row and resumes
    resumed run  success=False  UNIQUE constraint failed: widgets.id

The cause was removed and the retry failed identically, because the rows the
first attempt landed were still there and only `in_progress` cleared them.
`failed` is set after batches have already been written, so it means the same
thing `in_progress` means -- partly copied -- and it was treated as though it
meant nothing had happened.

WHAT `in_progress` MEANS AND WHY IT DELETES. A table left in_progress was
partly copied, so resuming has to remove what landed before copying again or
the rows arrive twice. This asserts the rows, not the status, because a
status that says completed while the table holds six copies of three rows is
the failure the status was supposed to prevent.

The dead run is simulated by writing the progress row a dead run leaves.
Nothing is patched: the row is real, the table is real, and the second
transfer reads it exactly as it would read one left by a process that died.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite, writing
from declaro_persistum.transfer import TRANSFER_STATE_TABLE, bulk_transfer

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

MODELS = '''
from pydantic import BaseModel
from declaro_persistum import table


@table("widgets")
class Widget(BaseModel):
    id: int
    name: str
'''

ROWS = [(1, "first"), (2, "second"), (3, "third")]


@pytest.fixture
def schema_path() -> Any:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(MODELS)
    yield str(path)


async def _database(name: str) -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), name),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _seeded_pair() -> tuple[Any, Any]:
    source, target = await _database("s.db"), await _database("t.db")
    for database in (source, target):
        async with writing(database) as connection:
            await connection.execute(
                'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)'
            )
            await connection.commit()
    async with writing(source) as connection:
        for row in ROWS:
            await connection.execute(
                'INSERT INTO "widgets" (id, name) VALUES (?, ?)', row
            )
        await connection.commit()
    return source, target


async def _transfer(
    source: Any, target: Any, schema_path: str, *, resume: bool, on_progress: Any
) -> Any:
    return await bulk_transfer(
        source,
        target,
        "sqlite",
        "sqlite",
        schema_path,
        batch_size=2,
        tables=["widgets"],
        expand_enums=False,
        on_progress=on_progress,
        resume=resume,
    )


async def _rows(database: Any) -> list[tuple]:
    async with writing(database) as connection:
        cursor = await connection.execute(
            'SELECT id, name FROM "widgets" ORDER BY id'
        )
        return [tuple(r) for r in await cursor.fetchall()]


async def _mark(target: Any, status: str, rows_transferred: int) -> None:
    """Write the progress row a dead run leaves behind."""
    async with writing(target) as connection:
        await connection.execute(
            f'UPDATE "{TRANSFER_STATE_TABLE}" SET status = ?, '
            f"rows_transferred = ? WHERE table_name = ?",
            (status, rows_transferred, "widgets"),
        )
        await connection.commit()


async def test_a_completed_table_is_skipped_on_the_second_run(
    schema_path: str,
) -> None:
    """The branch every caller gets, because resume defaults to True."""
    source, target = await _seeded_pair()
    try:
        await _transfer(source, target, schema_path, resume=False, on_progress=None)

        again = await _transfer(
            source, target, schema_path, resume=True, on_progress=None
        )

        assert again["tables_skipped"] == 1, again
        assert again["tables_transferred"] == 0, again
    finally:
        await close(source)
        await close(target)


async def test_skipping_does_not_copy_the_rows_twice(schema_path: str) -> None:
    """The floor. Counting a skip is not the same as having skipped."""
    source, target = await _seeded_pair()
    try:
        await _transfer(source, target, schema_path, resume=False, on_progress=None)
        await _transfer(source, target, schema_path, resume=True, on_progress=None)

        assert await _rows(target) == ROWS, await _rows(target)
    finally:
        await close(source)
        await close(target)


async def test_a_skipped_table_still_counts_its_rows(schema_path: str) -> None:
    """A resumed run reports the whole transfer, not only its own share.

    A caller comparing `total_rows` against the source would otherwise see a
    successful resume as having moved nothing.
    """
    source, target = await _seeded_pair()
    try:
        first = await _transfer(
            source, target, schema_path, resume=False, on_progress=None
        )
        again = await _transfer(
            source, target, schema_path, resume=True, on_progress=None
        )

        assert again["total_rows"] == first["total_rows"] == len(ROWS), (first, again)
    finally:
        await close(source)
        await close(target)


async def test_a_table_left_in_progress_is_restarted_not_appended(
    schema_path: str,
) -> None:
    """The case resume exists for, and the one that duplicates rows if wrong.

    A partly copied table has to be emptied before it is copied again.
    """
    source, target = await _seeded_pair()
    try:
        await _transfer(source, target, schema_path, resume=False, on_progress=None)
        await _mark(target, "in_progress", 2)

        again = await _transfer(
            source, target, schema_path, resume=True, on_progress=None
        )

        assert await _rows(target) == ROWS, (
            f"the table was appended to rather than restarted: "
            f"{await _rows(target)}"
        )
        assert again["tables_transferred"] == 1, again
    finally:
        await close(source)
        await close(target)


async def _failed_once(schema_path: str) -> tuple[Any, Any]:
    """A real half-transfer: a row in the target collides with the source's.

    Batch size 1, so the first two rows land and the third raises. That is
    how a transfer fails in the field -- partway, with rows already written.
    """
    source, target = await _seeded_pair()
    async with writing(target) as connection:
        await connection.execute(
            'INSERT INTO "widgets" (id, name) VALUES (?, ?)', (3, "squatter")
        )
        await connection.commit()

    result = await bulk_transfer(
        source,
        target,
        "sqlite",
        "sqlite",
        schema_path,
        batch_size=1,
        tables=["widgets"],
        expand_enums=False,
        on_progress=None,
        resume=True,
    )
    assert not result["success"], "the setup was meant to fail and did not"

    async with writing(target) as connection:
        await connection.execute('DELETE FROM "widgets" WHERE name = ?', ("squatter",))
        await connection.commit()
    return source, target


async def test_a_transfer_that_failed_once_can_still_succeed(
    schema_path: str,
) -> None:
    """The measured defect. The cause was removed and the retry failed anyway."""
    source, target = await _failed_once(schema_path)
    try:
        again = await _transfer(
            source, target, schema_path, resume=True, on_progress=None
        )

        assert again["success"], (
            f"a transfer that failed once can never succeed: {again['errors']}"
        )
    finally:
        await close(source)
        await close(target)


async def test_the_retry_leaves_each_row_once(schema_path: str) -> None:
    """The floor. Succeeding by appending duplicates is not succeeding."""
    source, target = await _failed_once(schema_path)
    try:
        await _transfer(source, target, schema_path, resume=True, on_progress=None)

        assert await _rows(target) == ROWS, await _rows(target)
    finally:
        await close(source)
        await close(target)


async def test_a_completed_table_is_skipped_with_its_rows_intact(
    schema_path: str,
) -> None:
    """Both halves of a skip, asserted together.

    THIS IS NOT A CONTROL AGAINST OVER-CLEARING, though it was written as one.
    Widening the delete to every recorded status still passes, because the
    completed branch returns before reaching it. That mutation is equivalent,
    not survivable: nothing can reach the delete with a completed status.
    """
    source, target = await _seeded_pair()
    try:
        await _transfer(source, target, schema_path, resume=False, on_progress=None)
        again = await _transfer(
            source, target, schema_path, resume=True, on_progress=None
        )

        assert again["tables_skipped"] == 1 and await _rows(target) == ROWS
    finally:
        await close(source)
        await close(target)


async def test_progress_is_reported_while_the_transfer_runs(
    schema_path: str,
) -> None:
    """The callback nothing had ever passed.

    Three rows in batches of two means the caller hears about the table more
    than once, which is the whole point of a progress callback.
    """
    seen: list[dict] = []
    source, target = await _seeded_pair()
    try:
        await _transfer(
            source, target, schema_path, resume=False, on_progress=seen.append
        )

        assert seen, "the callback was never called"
        assert {p["table"] for p in seen} == {"widgets"}, seen
        assert seen[-1]["rows_transferred"] == len(ROWS), seen[-1]
    finally:
        await close(source)
        await close(target)


async def test_progress_is_reported_for_a_skipped_table(
    schema_path: str,
) -> None:
    """A resumed run must still tell the caller what it did with each table.

    Silence on a skip reads as a table that was never considered.
    """
    source, target = await _seeded_pair()
    try:
        await _transfer(source, target, schema_path, resume=False, on_progress=None)

        seen: list[dict] = []
        await _transfer(
            source, target, schema_path, resume=True, on_progress=seen.append
        )

        assert [p["table"] for p in seen] == ["widgets"], seen
        assert seen[-1]["rows_transferred"] == len(ROWS), seen[-1]
    finally:
        await close(source)
        await close(target)
