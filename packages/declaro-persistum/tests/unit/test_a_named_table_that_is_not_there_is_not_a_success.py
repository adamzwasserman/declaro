"""Naming a table that does not exist must not come back as a clean transfer.

`bulk_transfer(..., tables=[...])` filters the caller's list against the source
schema:

    transfer_tables = [t for t in tables if t in source_schema]

A name that is not there is dropped. Not skipped, not counted, not reported.

MEASURED 2026-08-17, asking for `["users", "orders_typo"]` against a source
that has only `users`:

    success=True  transferred=1  skipped=0  failed=0  errors={}

One table moved, one did not, and nothing in the result mentions the second.
A caller migrating a named list reads success and moves on, and that table's
rows are still in the old database.

THE CALLER MADE A FACTUAL CLAIM AND IT WAS WRONG. `tables=[...]` says "these
exist and I want them". Silently intersecting it with reality turns a typo, a
stale deploy script or a half-finished rename into a green result.

`tables=None` still means everything, and that is not a claim about any
particular table.

NOT THE SAME AS A SKIP. `tables_skipped` counts a table already transferred by
an earlier run, which is resumption working correctly. This is a table that was
never going to be looked at.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum import bulk_transfer, close, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.exceptions import TransferError
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

MODELS = (
    "from pydantic import BaseModel\n"
    "from declaro_persistum import table, field\n"
    '@table("users")\n'
    "class U(BaseModel):\n"
    "    id: str = field(primary_key=True)\n\n"
    '@table("orders")\n'
    "class O(BaseModel):\n"
    "    id: str = field(primary_key=True)\n"
)


async def _pair(directory: pathlib.Path):
    models = directory / "models.py"
    models.write_text(MODELS)
    source = await open_sqlite(
        str(directory / "s.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    target = await open_sqlite(
        str(directory / "t.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    for db in (source, target):
        async with writing(db) as conn:
            await conn.execute('CREATE TABLE "users" (id TEXT PRIMARY KEY)')
            await conn.execute('CREATE TABLE "orders" (id TEXT PRIMARY KEY)')
            await conn.commit()
    async with writing(source) as conn:
        await conn.execute('INSERT INTO "users" VALUES (?)', ("u1",))
        await conn.execute('INSERT INTO "orders" VALUES (?)', ("o1",))
        await conn.commit()
    return source, target, models


async def test_naming_tables_that_exist_transfers_them(tmp_path) -> None:
    """The baseline. Every refusal below must not cost this."""
    source, target, models = await _pair(tmp_path)
    try:
        result = await bulk_transfer(
            source,
            target,
            str(models),
            tables=["users", "orders"],
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True, result
        assert result["tables_transferred"] == 2
    finally:
        await close(source)
        await close(target)


async def test_naming_a_subset_still_works(tmp_path) -> None:
    """A caller may transfer part of a database. That is the feature."""
    source, target, models = await _pair(tmp_path)
    try:
        result = await bulk_transfer(
            source,
            target,
            str(models),
            tables=["users"],
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True
        assert result["tables_transferred"] == 1

        async with writing(target) as conn:
            cursor = await conn.execute('SELECT COUNT(*) FROM "orders"')
            assert (await cursor.fetchone())[0] == 0, "a table nobody asked for moved"
    finally:
        await close(source)
        await close(target)


async def test_naming_a_table_that_is_not_there_raises(tmp_path) -> None:
    """The measured silence, in one assertion."""
    source, target, models = await _pair(tmp_path)
    try:
        with pytest.raises(TransferError) as caught:
            await bulk_transfer(
                source,
                target,
                str(models),
                tables=["users", "orders_typo"],
                on_progress=NOTHING,
                batch_size=NOTHING,
                expand_enums=NOTHING,
                resume=NOTHING,
            )

        assert "orders_typo" in str(caught.value), (
            f"the error does not name the table that is missing: {caught.value}"
        )
    finally:
        await close(source)
        await close(target)


async def test_the_error_names_every_missing_table_not_just_the_first(
    tmp_path,
) -> None:
    """A stale deploy script names several. Fixing them one run at a time is cruel."""
    source, target, models = await _pair(tmp_path)
    try:
        with pytest.raises(TransferError) as caught:
            await bulk_transfer(
                source,
                target,
                str(models),
                tables=["users", "gone_one", "gone_two"],
                on_progress=NOTHING,
                batch_size=NOTHING,
                expand_enums=NOTHING,
                resume=NOTHING,
            )

        message = str(caught.value)
        assert "gone_one" in message and "gone_two" in message, message
    finally:
        await close(source)
        await close(target)


async def test_nothing_is_transferred_when_a_name_is_wrong(tmp_path) -> None:
    """Refusing must happen before any data moves.

    A partial transfer that then raises is worse than either outcome: the
    target holds some of the rows and the caller has an exception.
    """
    source, target, models = await _pair(tmp_path)
    try:
        with pytest.raises(TransferError):
            await bulk_transfer(
                source,
                target,
                str(models),
                tables=["users", "orders_typo"],
                on_progress=NOTHING,
                batch_size=NOTHING,
                expand_enums=NOTHING,
                resume=NOTHING,
            )

        async with writing(target) as conn:
            cursor = await conn.execute('SELECT COUNT(*) FROM "users"')
            assert (await cursor.fetchone())[0] == 0, "rows moved before the bad name was noticed"
    finally:
        await close(source)
        await close(target)


async def test_asking_for_everything_is_not_a_claim_about_any_table(
    tmp_path,
) -> None:
    """`tables=None` means whatever is there, so nothing can be missing."""
    source, target, models = await _pair(tmp_path)
    try:
        result = await bulk_transfer(
            source,
            target,
            str(models),
            tables=None,
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True
        assert result["tables_transferred"] == 2
    finally:
        await close(source)
        await close(target)
