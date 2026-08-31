"""The PostgreSQL half of resume had never run.

MEASURED 2026-08-20 with `coverage json` over the whole suite. Dark in
`bulk_loader.py`:

    postgres_read_progress      reads a table's status through asyncpg
    postgres_delete_rows        empties a table before a retry
    postgres_disable_fk_checks  `SET session_replication_role = 'replica'`
    postgres_enable_fk_checks   and back

The transfer tests move rows both ways between SQLite and PostgreSQL, and the
progress table lives on the TARGET. So the PostgreSQL progress functions ran
only when PostgreSQL was the target, and every resume test used a SQLite one.

WHY THESE ARE NOT THE DB-API VERSIONS UNDER ANOTHER NAME. asyncpg takes `$1`
positionally and returns a row that is already a mapping; the DB-API drivers
take `?` as a tuple and return a cursor whose row is a plain tuple. And
`SET session_replication_role` is PostgreSQL's own way to suspend foreign-key
enforcement: SQLite uses a PRAGMA and Turso another. A copy of one used for
the other would fail on the first call.

THE DELETE IS THE ONE THAT MATTERS. It is what makes a failed transfer
retryable: a table left `failed` or `in_progress` is emptied before being
copied again, and without it the retry collides with its own first attempt.
That was fixed on SQLite on 2026-08-18 and the PostgreSQL path to it was
never taken.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_postgresql, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.transfer import bulk_transfer
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.precommit, pytest.mark.postgresql, pytest.mark.integration]

PROBE_URL = probe_url()

MODELS = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    name: str
"""

ROWS = [(1, "first"), (2, "second"), (3, "third")]


@pytest.fixture
def schema_path() -> Any:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(MODELS)
    yield str(path)


async def _pair() -> tuple[Any, Any]:
    """A SQLite source with rows, and an empty PostgreSQL target."""
    import asyncpg

    try:
        probe = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)
    for table in ("widgets", "_dp_transfer_state"):
        await probe.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    await probe.close()

    source = await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "s.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )
    target = await open_postgresql(PROBE_URL, shutdown="exit_immediately", busy_timeout_s=5.0)

    async with writing(source) as connection:
        await connection.execute('CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)')
        for row in ROWS:
            await connection.execute('INSERT INTO "widgets" (id, name) VALUES (?, ?)', row)
        await connection.commit()

    async with writing(target) as connection:
        await connection.execute('CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)')
    return source, target


async def _transfer(source: Any, target: Any, models: str, *, resume: bool) -> Any:
    return await bulk_transfer(
        source,
        target,
        "sqlite",
        "postgresql",
        models,
        batch_size=2,
        tables=["widgets"],
        expand_enums=False,
        on_progress=None,
        resume=resume,
    )


async def _cleanup(target: Any) -> None:
    async with writing(target) as connection:
        for table in ("widgets", "_dp_transfer_state"):
            await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


async def _count(target: Any) -> int:
    async with writing(target) as connection:
        return int(await connection.fetchval('SELECT count(*) FROM "widgets"'))


async def test_a_completed_table_is_skipped(schema_path: str) -> None:
    """`postgres_read_progress` reading its own status back."""
    source, target = await _pair()
    try:
        await _transfer(source, target, schema_path, resume=False)

        again = await _transfer(source, target, schema_path, resume=True)

        assert again["tables_skipped"] == 1, again
        assert again["tables_transferred"] == 0, again
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_skipping_does_not_copy_the_rows_twice(schema_path: str) -> None:
    """The floor. Counting a skip is not the same as having skipped."""
    source, target = await _pair()
    try:
        await _transfer(source, target, schema_path, resume=False)
        await _transfer(source, target, schema_path, resume=True)

        assert await _count(target) == len(ROWS)
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_a_failed_table_is_emptied_before_the_retry(
    schema_path: str,
) -> None:
    """`postgres_delete_rows`, which is what makes a retry possible at all.

    A row already in the target collides with the source's third, so two
    batches land and the third raises. The retry has to empty the table first
    or it collides with its own first attempt.
    """
    source, target = await _pair()
    try:
        async with writing(target) as connection:
            await connection.execute('INSERT INTO "widgets" (id, name) VALUES (3, $1)', "squatter")

        first = await _transfer(source, target, schema_path, resume=True)
        assert not first["success"], "the setup was meant to fail and did not"

        async with writing(target) as connection:
            await connection.execute('DELETE FROM "widgets" WHERE name = $1', "squatter")

        again = await _transfer(source, target, schema_path, resume=True)

        assert again["success"], again["errors"]
        assert await _count(target) == len(ROWS)
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_the_rows_are_each_there_once(schema_path: str) -> None:
    """Succeeding by appending duplicates is not succeeding."""
    source, target = await _pair()
    try:
        async with writing(target) as connection:
            await connection.execute('INSERT INTO "widgets" (id, name) VALUES (3, $1)', "squatter")
        await _transfer(source, target, schema_path, resume=True)
        async with writing(target) as connection:
            await connection.execute('DELETE FROM "widgets" WHERE name = $1', "squatter")
        await _transfer(source, target, schema_path, resume=True)

        async with writing(target) as connection:
            rows = await connection.fetch('SELECT id, name FROM "widgets" ORDER BY id')

        assert [tuple(r) for r in rows] == ROWS, rows
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)
