"""The live cutover moves a production database, and nothing had run it.

MEASURED 2026-08-18 with `coverage json` over the whole suite: `begin_cutover`
in `cutover.py` had zero of its 24 body lines executed. It is the entry point
to the four-phase workflow that moves a running application from one database
to another, and no test had ever called it.

WHAT THIS FILE DOES. It runs a real cutover between two real SQLite files,
with rows in the source, and asserts the parts that matter: the data arrives,
the mirror comes back pointing the right way, and phases 3 and 4 hand back
the database the caller now has to close. No mocks, because there is nothing
here that needs one.

SEVEN IMPLICIT DEFAULTS SIT ON THAT SIGNATURE, and two of them are undone
from below. `mirror()` requires `fail_open` and `compare_on_read` and gives
neither a default; `begin_cutover` defaults both to True. Someone made those
two decisions explicit at the layer that acts on them and the layer above
hands them back. `fail_open=True` means a replica failure keeps serving from
the primary, which is the direction you get by not knowing there was a
choice, and it is the same shape as `apply(dry_run=...)`, which used to
default to running the migration.

This file does not change that signature. It pins what the function does now,
so that changing it is a decision somebody makes rather than a thing that
happens.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite
from declaro_persistum.cutover import begin_cutover
from declaro_persistum.mirror import detach, promote

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

MODELS = """
from typing import Literal
from pydantic import BaseModel
from declaro_persistum import table


@table("widgets")
class Widget(BaseModel):
    id: int
    name: str
    status: Literal["draft", "live"]
"""

ROWS = [(1, "first", "draft"), (2, "second", "live"), (3, "third", "live")]


@pytest.fixture
def schema_path() -> Any:
    directory = tempfile.mkdtemp()
    path = Path(directory) / "models.py"
    path.write_text(MODELS)
    yield str(path)


async def _database(name: str) -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), name),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _seed(database: Any) -> None:
    """A source with rows in it. An empty transfer proves nothing."""
    from declaro_persistum import writing

    async with writing(database) as connection:
        await connection.execute(
            'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT, status TEXT)'
        )
        for row in ROWS:
            await connection.execute(
                'INSERT INTO "widgets" (id, name, status) VALUES (?, ?, ?)', row
            )


async def _count(database: Any, table: str) -> int:
    from declaro_persistum import writing

    async with writing(database) as connection:
        cursor = await connection.execute(f'SELECT count(*) FROM "{table}"')
        return (await cursor.fetchone())[0]


async def _cutover(schema_path: str) -> Any:
    source = await _database("source.db")
    target = await _database("target.db")
    await _seed(source)
    mirrored, result = await begin_cutover(
        source,
        target,
        schema_path,
        batch_size=2,
        tables=None,
        expand_enums=True,
        on_progress=None,
        resume=True,
        fail_open=True,
        compare_on_read=True,
    )
    return source, target, mirrored, result


async def test_phase_one_moves_the_rows(schema_path: str) -> None:
    """The point of the exercise. Everything else is arrangement."""
    source, target, _mirrored, result = await _cutover(schema_path)
    try:
        assert await _count(target, "widgets") == len(ROWS), result
    finally:
        await close(source)
        await close(target)


async def test_the_result_counts_what_it_moved(schema_path: str) -> None:
    """A report that disagrees with the database is worse than none."""
    source, target, _mirrored, result = await _cutover(schema_path)
    try:
        assert result["total_rows"] == await _count(target, "widgets"), result
    finally:
        await close(source)
        await close(target)


async def test_the_mirror_comes_back_pointing_at_the_source(
    schema_path: str,
) -> None:
    """Phase 2. The source is still primary until somebody promotes it."""
    source, target, mirrored, _result = await _cutover(schema_path)
    try:
        assert mirrored["primary"] is source
        assert mirrored["replica"] is target
    finally:
        await close(source)
        await close(target)


async def test_promote_then_detach_hands_back_the_target(
    schema_path: str,
) -> None:
    """Phases 3 and 4, which are the caller's to run and were never run here."""
    source, target, mirrored, _result = await _cutover(schema_path)
    try:
        survivor = detach(promote(mirrored))

        assert survivor is target, "the cutover ended on the database it started on"
    finally:
        await close(source)
        await close(target)


async def test_promote_leaves_the_caller_s_mirror_alone(
    schema_path: str,
) -> None:
    """`promote` returns a new mirror rather than mutating the one held.

    A caller still holding the old value keeps talking to the database it
    thinks it is talking to. That is what the function's own docstring
    promises and nothing had checked it.
    """
    source, target, mirrored, _result = await _cutover(schema_path)
    try:
        promote(mirrored)

        assert mirrored["primary"] is source
    finally:
        await close(source)
        await close(target)
