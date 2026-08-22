"""A cross-engine transfer through PostgreSQL had never been executed.

MEASURED 2026-08-18 with `coverage json` over the whole suite. Every function
in the PostgreSQL half of the bulk loader was dark:

    postgres_read_rows          postgres_delete_rows
    postgres_load_rows          postgres_disable_fk_checks
    postgres_count_rows         postgres_enable_fk_checks
    _portable

`_portable` is the one its own docstring calls the single place a cross-engine
transfer has to translate a value: asyncpg hands back `uuid.UUID` objects and
the DB-API targets reject them. It had never run.

This is the product feature. `begin_cutover` moves a running application from
one database to another and `bulk_transfer` is its first phase, so the path
that carries the data was the path with no test on it.

WHAT IS EXERCISED HERE THAT A SINGLE-ENGINE TRANSFER CANNOT BE: the two
dialects use different machinery, not different spellings. PostgreSQL loads
with COPY and reads `ORDER BY ctid`; the DB-API side uses INSERT. Both
directions run below, because reading is not writing and each engine does
one of them in each direction.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_postgresql, open_sqlite, writing
from declaro_persistum.transfer import bulk_transfer
from tests.conftest import probe_url

pytestmark = [pytest.mark.precommit, pytest.mark.postgresql, pytest.mark.integration]

PROBE_URL = probe_url()

MODELS = '''
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table


@table("widgets")
class Widget(BaseModel):
    id: UUID
    name: str
    count: int
'''

ROWS = [
    (uuid.uuid4(), "first", 1),
    (uuid.uuid4(), "second", 2),
    (uuid.uuid4(), "third", 3),
]


@pytest.fixture
def schema_path() -> Any:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(MODELS)
    yield str(path)


async def _sqlite() -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "s.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _postgresql() -> Any:
    import asyncpg

    try:
        probe = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        pytest.skip(f"no server at {PROBE_URL}: {unreachable}")
    await probe.execute('DROP TABLE IF EXISTS "widgets" CASCADE')
    await probe.close()
    return await open_postgresql(
        PROBE_URL, shutdown="exit_immediately", busy_timeout_s=5.0
    )


async def _make_widgets(database: Any, dialect: str, *, with_rows: bool) -> None:
    uuid_type = "UUID" if dialect == "postgresql" else "TEXT"
    async with writing(database) as connection:
        await connection.execute(
            f'CREATE TABLE "widgets" (id {uuid_type} PRIMARY KEY, '
            f"name TEXT, count INTEGER)"
        )
        if not with_rows:
            return
        for row in ROWS:
            if dialect == "postgresql":
                await connection.execute(
                    'INSERT INTO "widgets" (id, name, count) VALUES ($1, $2, $3)',
                    *row,
                )
            else:
                await connection.execute(
                    'INSERT INTO "widgets" (id, name, count) VALUES (?, ?, ?)',
                    (str(row[0]), row[1], row[2]),
                )
        if dialect != "postgresql":
            await connection.commit()


async def _rows_of(database: Any, dialect: str) -> list[tuple]:
    """The dialect is passed, not sniffed.

    asyncpg's `execute` returns a status string and its `fetch` returns rows;
    the DB-API drivers return a cursor from `execute`. Asking the connection
    which it is -- `hasattr(conn, "fetch")` -- is what `transfer.py` does at
    three sites, and it is a dispatch on type wearing a duck-typing coat.
    """
    sql = 'SELECT name, count FROM "widgets" ORDER BY name'
    async with writing(database) as connection:
        if dialect == "postgresql":
            return [tuple(r) for r in await connection.fetch(sql)]
        cursor = await connection.execute(sql)
        return [tuple(r) for r in await cursor.fetchall()]


async def _cleanup(database: Any) -> None:
    async with writing(database) as connection:
        await connection.execute('DROP TABLE IF EXISTS "widgets"')


async def test_sqlite_to_postgresql_moves_the_rows(schema_path: str) -> None:
    """The cutover's own direction: local file to remote server."""
    source, target = await _sqlite(), await _postgresql()
    try:
        await _make_widgets(source, "sqlite", with_rows=True)
        await _make_widgets(target, "postgresql", with_rows=False)

        result = await bulk_transfer(
            source,
            target,
            "sqlite",
            "postgresql",
            schema_path,
            batch_size=2,
            tables=["widgets"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        assert result["total_rows"] == len(ROWS), result
        assert await _rows_of(target, "postgresql") == [
            ("first", 1),
            ("second", 2),
            ("third", 3),
        ]
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_postgresql_to_sqlite_moves_the_rows(schema_path: str) -> None:
    """The other direction, which is the only one that reads through asyncpg.

    This is what exercises `_portable`. PostgreSQL hands back `uuid.UUID`
    objects and SQLite refuses them, so a transfer that did not translate
    would fail here and nowhere else.
    """
    source, target = await _postgresql(), await _sqlite()
    try:
        await _make_widgets(source, "postgresql", with_rows=True)
        await _make_widgets(target, "sqlite", with_rows=False)

        result = await bulk_transfer(
            source,
            target,
            "postgresql",
            "sqlite",
            schema_path,
            batch_size=2,
            tables=["widgets"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        assert result["total_rows"] == len(ROWS), result
        assert await _rows_of(target, "sqlite") == [
            ("first", 1),
            ("second", 2),
            ("third", 3),
        ]
    finally:
        await _cleanup(source)
        await close(source)
        await close(target)


async def test_a_uuid_arrives_as_a_string_sqlite_can_hold(
    schema_path: str,
) -> None:
    """`_portable` on its own, asserted on the value rather than the count.

    A row count can be right while every UUID in it is wrong.
    """
    source, target = await _postgresql(), await _sqlite()
    try:
        await _make_widgets(source, "postgresql", with_rows=True)
        await _make_widgets(target, "sqlite", with_rows=False)

        await bulk_transfer(
            source,
            target,
            "postgresql",
            "sqlite",
            schema_path,
            batch_size=10,
            tables=["widgets"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        async with writing(target) as connection:
            cursor = await connection.execute(
                'SELECT id FROM "widgets" ORDER BY name'
            )
            landed = [r[0] for r in await cursor.fetchall()]

        assert landed == [str(row[0]) for row in ROWS], landed
    finally:
        await _cleanup(source)
        await close(source)
        await close(target)
