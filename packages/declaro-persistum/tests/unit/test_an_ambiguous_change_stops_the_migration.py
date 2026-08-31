"""A rename that might be a drop must not be guessed at, and never was tested.

MEASURED 2026-08-19 with `coverage json` over the whole suite. Two branches in
`apply_migrations_async` had never run, and both decide whether a migration
happens at all:

    the ambiguity check       returns success=False and applies nothing
    the replica partition     skips reconstruction and returns success=True

WHY AN AMBIGUITY IS NOT A CHOICE THE PACKAGE MAY MAKE. Renaming `name` to
`title` and dropping `name` while adding `title` produce the same two schemas.
One preserves the rows in that column and the other destroys them. The differ
reports both readings and declines to pick, so the migration stops:

    Column 'name' removed, 'title' added with same type. Is this a rename
    (preserves data) or drop+add (loses data)?

A migration that guessed would be right half the time and would empty a
column the other half, silently, on a deploy nobody was watching.

WHAT IS ASSERTED IS THE DATA, NOT ONLY THE FLAG. `success: False` with the
column already dropped would satisfy any check of the return value, so each
test reads the table afterwards.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

BEFORE = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    name: str
"""

# `name` gone, `title` added, same type. A rename or a drop and an add, and
# the two cannot be told apart from the schemas alone.
AMBIGUOUS = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    title: str
"""

# The same change, said out loud. `renamed_from` is the author answering the
# question the differ asked.
DECLARED_RENAME = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    title: str = field(renamed_from="name")
"""

# NULLABLE ON PURPOSE. The first version added `note: str`, and SQLite
# refused it: "Cannot add a NOT NULL column with default value NULL". That is
# the engine protecting the row already in the table, and it is right. A new
# NOT NULL column needs a default or an empty table, and the validator has a
# rule saying so which this path does not consult.
UNAMBIGUOUS = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    name: str
    note: str | None = None
"""


def _models(body: str) -> str:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(body)
    return str(path)


async def _database() -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "m.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _seeded() -> Any:
    """A database at BEFORE, with a row whose `name` is worth keeping."""
    database = await _database()
    await apply_migrations_async(
        database, "sqlite", _models(BEFORE), expand_enums=NOTHING, force=NOTHING
    )
    async with writing(database) as connection:
        await connection.execute('INSERT INTO "users" (id, name) VALUES (?, ?)', (1, "adam"))
        await connection.commit()
    return database


async def _columns(database: Any) -> list[str]:
    async with writing(database) as connection:
        cursor = await connection.execute('PRAGMA table_info("users")')
        return [row[1] for row in await cursor.fetchall()]


async def test_an_ambiguous_change_is_refused() -> None:
    database = await _seeded()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(AMBIGUOUS), expand_enums=NOTHING, force=NOTHING
        )

        assert result["success"] is False, result
        assert result["operations_applied"] == 0, result
    finally:
        await close(database)


async def test_the_refusal_says_what_is_ambiguous() -> None:
    """An error naming neither the column nor the choice cannot be acted on."""
    database = await _seeded()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(AMBIGUOUS), expand_enums=NOTHING, force=NOTHING
        )

        assert "name" in result["error"] and "title" in result["error"], result
    finally:
        await close(database)


async def test_nothing_is_applied_when_the_change_is_refused() -> None:
    """The floor, and the whole point.

    A refusal that had already dropped the column would report failure and
    have destroyed the data anyway.
    """
    database = await _seeded()
    try:
        await apply_migrations_async(
            database, "sqlite", _models(AMBIGUOUS), expand_enums=NOTHING, force=NOTHING
        )

        assert await _columns(database) == ["id", "name"], await _columns(database)
    finally:
        await close(database)


async def test_the_row_is_still_there_afterwards() -> None:
    database = await _seeded()
    try:
        await apply_migrations_async(
            database, "sqlite", _models(AMBIGUOUS), expand_enums=NOTHING, force=NOTHING
        )

        async with writing(database) as connection:
            cursor = await connection.execute('SELECT name FROM "users"')
            assert [tuple(r) for r in await cursor.fetchall()] == [("adam",)]
    finally:
        await close(database)


async def test_the_same_change_declared_as_a_rename_is_applied() -> None:
    """The control on the refusal.

    A migration that refused every change would pass the four tests above.
    `renamed_from` is the author answering the question, and the answer has to
    be taken.
    """
    database = await _seeded()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(DECLARED_RENAME), expand_enums=NOTHING, force=NOTHING
        )

        assert result["success"] is True, result
        assert await _columns(database) == ["id", "title"], await _columns(database)
    finally:
        await close(database)


async def test_a_declared_rename_keeps_the_rows() -> None:
    """Which is the whole difference between a rename and a drop plus an add."""
    database = await _seeded()
    try:
        await apply_migrations_async(
            database, "sqlite", _models(DECLARED_RENAME), expand_enums=NOTHING, force=NOTHING
        )

        async with writing(database) as connection:
            cursor = await connection.execute('SELECT title FROM "users"')
            assert [tuple(r) for r in await cursor.fetchall()] == [("adam",)]
    finally:
        await close(database)


async def test_an_added_column_is_not_ambiguous() -> None:
    """The other control. Adding a column removes none, so nothing is in doubt."""
    database = await _seeded()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(UNAMBIGUOUS), expand_enums=NOTHING, force=NOTHING
        )

        assert result["success"] is True, result
        assert await _columns(database) == ["id", "name", "note"]
    finally:
        await close(database)
