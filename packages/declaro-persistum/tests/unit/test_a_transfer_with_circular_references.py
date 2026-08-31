"""Suspending foreign keys for a circular transfer had never been done.

MEASURED 2026-08-20. The last two functions in `bulk_loader.py` that do real
work and had never run:

    postgres_disable_fk_checks  SET session_replication_role = 'replica'
    postgres_enable_fk_checks   and back to 'origin'

They fire on one condition: the tables being transferred reference each other,
so no load order satisfies every key and the constraints have to be suspended
for the duration.

THE OTHER ENGINES USE SOMETHING ELSE ENTIRELY. SQLite and Turso have
`PRAGMA foreign_keys`, and `SET session_replication_role` is PostgreSQL's own
mechanism and needs superuser. A copy of one used for the other fails on the
first call, which is why they are separate entries in the loader rather than
one function.

WHAT MAKES THIS THE CASE THEY EXIST FOR. `transfer.py` toposorts the tables so
parents load before children. A cycle has no such order, so it detects one,
suspends enforcement, loads everything, and restores. Without the suspension
the first row of each table in the cycle is refused.

RE-ENABLING IS THE HALF THAT MATTERS, AND NOTHING HERE CAN CHECK IT.
`session_replication_role` is a SESSION setting, and `bulk_transfer` runs
inside a connection scope that closes when it returns, so the suspension dies
with the connection whether or not it is restored. Turning
`postgres_enable_fk_checks` into a no-op passes every test in this file.

That is worth stating rather than implying a coverage this does not have.
The restore matters when a connection outlives the transfer -- a pooled one,
or a caller that hands in its own -- and no test here does that. What these
tests do check is that a later write on a fresh connection is refused, which
is the state a caller sees, and that the suspension itself happens: removing
`postgres_disable_fk_checks` fails them, and so does substituting SQLite's
PRAGMA, which PostgreSQL does not have.
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

# Two tables that reference each other. No load order satisfies both.
MODELS = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("husbands")
class Husband(BaseModel):
    id: int = field(primary_key=True)
    wife_id: int | None = field(references="wives.id", default=None)


@table("wives")
class Wife(BaseModel):
    id: int = field(primary_key=True)
    husband_id: int | None = field(references="husbands.id", default=None)
"""

TABLES = ("husbands", "wives", "_dp_transfer_state")


@pytest.fixture
def schema_path() -> Any:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(MODELS)
    yield str(path)


async def _pair() -> tuple[Any, Any]:
    import asyncpg

    try:
        probe = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)
    for table in TABLES:
        await probe.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    await probe.close()

    source = await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "c.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )
    target = await open_postgresql(PROBE_URL, shutdown="exit_immediately", busy_timeout_s=5.0)

    # THE SOURCE CARRIES THE SAME KEYS, and it has to. `bulk_transfer`
    # decides whether to suspend enforcement by toposorting the SOURCE
    # schema, while the constraints that would be violated are on the target.
    # The first version of this file gave the keys to the target alone, found
    # no cycle in the source, suspended nothing, and reported the target
    # refusing every row -- which is the package behaving correctly on a
    # setup that could not happen: `bulk_transfer` migrates the target from
    # the same models, so the two shapes match.
    async with writing(source) as connection:
        await connection.execute(
            'CREATE TABLE "husbands" (id INTEGER PRIMARY KEY, '
            'wife_id INTEGER REFERENCES "wives"(id))'
        )
        await connection.execute(
            'CREATE TABLE "wives" (id INTEGER PRIMARY KEY, '
            'husband_id INTEGER REFERENCES "husbands"(id))'
        )
        await connection.execute('INSERT INTO "husbands" VALUES (1, 1)')
        await connection.execute('INSERT INTO "wives" VALUES (1, 1)')
        await connection.commit()

    async with writing(target) as connection:
        await connection.execute(
            'CREATE TABLE "husbands" (id INTEGER PRIMARY KEY, wife_id INTEGER)'
        )
        await connection.execute(
            'CREATE TABLE "wives" (id INTEGER PRIMARY KEY, husband_id INTEGER, '
            'FOREIGN KEY (husband_id) REFERENCES "husbands"(id))'
        )
        await connection.execute(
            'ALTER TABLE "husbands" ADD CONSTRAINT "fk_w" '
            'FOREIGN KEY (wife_id) REFERENCES "wives"(id)'
        )
    return source, target


async def _cleanup(target: Any) -> None:
    async with writing(target) as connection:
        for table in TABLES:
            await connection.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')


async def test_the_rows_land_despite_the_cycle(schema_path: str) -> None:
    """Neither table can be loaded first with enforcement on."""
    source, target = await _pair()
    try:
        result = await bulk_transfer(
            source,
            target,
            schema_path,
            batch_size=10,
            tables=["husbands", "wives"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        assert result["success"], result["errors"]
        async with writing(target) as connection:
            assert await connection.fetchval('SELECT count(*) FROM "husbands"') == 1
            assert await connection.fetchval('SELECT count(*) FROM "wives"') == 1
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_enforcement_is_restored_afterwards(schema_path: str) -> None:
    """The half that matters more than the transfer succeeding.

    A connection left with enforcement suspended accepts writes against
    constraints nobody is checking, which is worse than the transfer failing.
    """
    source, target = await _pair()
    try:
        await bulk_transfer(
            source,
            target,
            schema_path,
            batch_size=10,
            tables=["husbands", "wives"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        async with writing(target) as connection:
            role = await connection.fetchval("SHOW session_replication_role")
            assert role == "origin", role
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)


async def test_a_violating_row_is_refused_after_the_transfer(
    schema_path: str,
) -> None:
    """The floor under the test above.

    `session_replication_role` reading 'origin' is a setting. Whether the
    engine acts on it is what a row proves.
    """
    import asyncpg

    source, target = await _pair()
    try:
        await bulk_transfer(
            source,
            target,
            schema_path,
            batch_size=10,
            tables=["husbands", "wives"],
            expand_enums=False,
            on_progress=None,
            resume=False,
        )

        async with writing(target) as connection:
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await connection.execute('INSERT INTO "wives" (id, husband_id) VALUES (99, 404)')
    finally:
        await _cleanup(target)
        await close(source)
        await close(target)
