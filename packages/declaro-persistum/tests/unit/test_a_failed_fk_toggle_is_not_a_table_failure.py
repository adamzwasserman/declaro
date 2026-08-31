"""`bulk_transfer` must not proceed as if it prepared the target when it did not.

Two tables that reference each other cannot be loaded in any order with foreign
key checks on, so `bulk_transfer` detects the cycle and disables them for the
run. Both halves of that were wrapped in `except Exception` and a warning.

DISABLING IS A PRECONDITION, NOT A COURTESY. If it fails, every table in the
cycle then fails on its first row, and each of those is recorded against the
table. One failed pragma becomes N table failures, and the report names N
tables that are all fine. That is the same misattribution `A_BUG_IN_THIS_PACKAGE`
was added for, arriving by a different route: there the cause was our bug, here
it is our own unmet precondition.

RE-ENABLING IS TEARDOWN AND IS DIFFERENT. It runs in a `finally`, so raising
there would replace whatever failure is already in flight with a second one.
What it must not do is stay silent: the connection is left enforcing nothing,
and a caller that reuses it writes against unchecked constraints. Measured
behaviour is pinned rather than changed.
"""

from __future__ import annotations

import pathlib

import pytest

import declaro_persistum.transfer as transfer_module
from declaro_persistum import bulk_transfer, close, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

# `authors` and `books` reference each other, so no load order satisfies both.
CIRCULAR = """from pydantic import BaseModel
from declaro_persistum import table, field

@table("authors")
class Author(BaseModel):
    id: str = field(primary_key=True)
    favourite_book: str = field(references="books.id", nullable=True)

@table("books")
class Book(BaseModel):
    id: str = field(primary_key=True)
    author_id: str = field(references="authors.id", nullable=True)
"""


async def _pair(directory: pathlib.Path):
    models = directory / "models.py"
    models.write_text(CIRCULAR)
    source = await open_sqlite(
        str(directory / "s.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    target = await open_sqlite(
        str(directory / "t.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    # THE FOREIGN KEYS MUST BE IN THE DDL, not only in the models file.
    # `bulk_transfer` toposorts the schema it INTROSPECTS from the source
    # database, so tables created without REFERENCES have no cycle and the
    # whole disable/enable path is never reached. My first version of this
    # built plain TEXT columns and every assertion below passed for the wrong
    # reason: `_toposort_tables` reported `circular=[]` and `disable_fk_checks`
    # was called zero times.
    ddl = (
        'CREATE TABLE "books" (id TEXT PRIMARY KEY, author_id TEXT REFERENCES "authors"(id))',
        'CREATE TABLE "authors" (id TEXT PRIMARY KEY, favourite_book TEXT REFERENCES "books"(id))',
    )
    for db in (source, target):
        async with writing(db) as conn:
            for statement in ddl:
                await conn.execute(statement)
            await conn.commit()
    async with writing(source) as conn:
        await conn.execute('INSERT INTO "authors" VALUES (?, ?)', ("a1", "b1"))
        await conn.execute('INSERT INTO "books" VALUES (?, ?)', ("b1", "a1"))
        await conn.commit()
    return source, target, models


async def test_a_circular_schema_transfers(tmp_path) -> None:
    """The baseline, and the reason the toggle exists at all."""
    source, target, models = await _pair(tmp_path)
    try:
        result = await bulk_transfer(
            source,
            target,
            "sqlite",
            "sqlite",
            str(models),
            tables=NOTHING,
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True, result
        assert result["tables_transferred"] == 2, result
    finally:
        await close(source)
        await close(target)


async def test_a_failed_re_enable_is_reported_loudly_but_does_not_mask(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Teardown, so it must not replace an in-flight failure with its own.

    The connection is left enforcing nothing either way, which is worth an
    ERROR rather than a warning: a caller reusing it writes against unchecked
    constraints and never knows.
    """
    source, target, models = await _pair(tmp_path)

    async def cannot_enable(_conn):
        raise RuntimeError("the target refused to re-enable foreign key checks")

    loaders = dict(transfer_module.BULK_LOADERS)
    loaders["sqlite"] = {**loaders["sqlite"], "enable_fk_checks": cannot_enable}
    monkeypatch.setattr(transfer_module, "BULK_LOADERS", loaders)

    try:
        result = await bulk_transfer(
            source,
            target,
            "sqlite",
            "sqlite",
            str(models),
            tables=NOTHING,
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True, (
            "a teardown failure changed the outcome of a transfer that worked"
        )
        # AND IT REACHES THE CALLER. "Reported loudly" used to mean an ERROR
        # in a log, which the caller holding this result cannot read. The
        # rows transferred, so `success` stays true about the rows, and the
        # target left enforcing nothing is an error the transfer survived --
        # which is the field `errors` already exists for.
        assert "_restore_fk_checks" in result["errors"], (
            "the target was left with foreign key checks off and the result "
            f"does not say so: {result['errors']}"
        )
        assert "NOT checked against its constraints" in (result["errors"]["_restore_fk_checks"])
        assert (
            "the target refused to re-enable foreign key checks"
            in (result["errors"]["_restore_fk_checks"])
        ), "the result names the failure but not what caused it"
    finally:
        await close(source)
        await close(target)
