"""Two early returns decide a migration before it starts, and neither ran.

MEASURED 2026-08-19 with `coverage json` over the whole suite. Unrun in
`apply_migrations_async`:

    the schema file does not exist   -> success False, nothing applied
    the file declares no tables      -> success True, and the hash is stored

THE SECOND IS THE INTERESTING ONE. An empty models file is treated as a
successful migration and its hash is written to `_declaro_meta`, so the next
boot compares hashes, sees a match, and never looks at the file again. That
is correct when the file really is empty, and it is the reason a file that
fails to LOAD must never reach here reporting zero tables: the stamp would
make the mistake permanent.

It cannot any more. `load_schema_from_models` was taught on 2026-08-17 to
raise rather than skip a file it cannot import, and the TOML loader was taught
this morning to refuse a column with no type and a table with no columns. This
file records what the empty case does, so that the difference between "nothing
declared" and "nothing loaded" stays visible.

WHY A MISSING FILE IS NOT SUCCESS. It is the one an operator most likely
mistyped, and reporting success would stamp the hash of a path that holds
nothing.
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

NO_TABLES = '''
"""A models file with no @table decorated models in it."""

from pydantic import BaseModel


class NotATable(BaseModel):
    id: int
'''

ONE_TABLE = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
"""


def _models(body: str) -> str:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(body)
    return str(path)


async def _database() -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "n.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _tables(database: Any) -> list[str]:
    async with writing(database) as connection:
        cursor = await connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE '_declaro%' AND name NOT LIKE 'sqlite_%'"
        )
        return sorted(row[0] for row in await cursor.fetchall())


async def test_a_missing_schema_file_is_not_a_successful_migration() -> None:
    """The path an operator most likely mistyped."""
    database = await _database()
    try:
        result = await apply_migrations_async(
            database,
            "sqlite",
            str(Path(tempfile.mkdtemp()) / "not_here.py"),
            expand_enums=NOTHING,
            force=NOTHING,
        )

        assert result["success"] is False, result
        assert result["operations_applied"] == 0, result
    finally:
        await close(database)


async def test_a_missing_schema_file_creates_nothing() -> None:
    database = await _database()
    try:
        await apply_migrations_async(
            database,
            "sqlite",
            str(Path(tempfile.mkdtemp()) / "not_here.py"),
            expand_enums=NOTHING,
            force=NOTHING,
        )

        assert await _tables(database) == []
    finally:
        await close(database)


async def test_every_return_carries_the_same_keys() -> None:
    """A caller reading `result["error"]` must not meet a KeyError.

    Nine returns leave this function. `skipped_operations` is on the replica
    path alone, and everything else is on all of them.
    """
    database = await _database()
    try:
        missing = await apply_migrations_async(
            database,
            "sqlite",
            str(Path(tempfile.mkdtemp()) / "not_here.py"),
            expand_enums=NOTHING,
            force=NOTHING,
        )
        empty = await apply_migrations_async(
            database, "sqlite", _models(NO_TABLES), expand_enums=NOTHING, force=NOTHING
        )
        real = await apply_migrations_async(
            database, "sqlite", _models(ONE_TABLE), expand_enums=NOTHING, force=NOTHING
        )

        common = {
            "success",
            "operations_applied",
            "tables_in_schema",
            "tables_in_database",
            "skipped",
            "error",
        }
        for name, result in (("missing", missing), ("empty", empty), ("real", real)):
            assert common <= set(result), f"{name} lacks {sorted(common - set(result))}"
    finally:
        await close(database)


async def test_a_file_with_no_tables_is_a_successful_no_op() -> None:
    """Nothing declared is a real answer, and applying nothing is the right one."""
    database = await _database()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(NO_TABLES), expand_enums=NOTHING, force=NOTHING
        )

        assert result["success"] is True, result
        assert result["operations_applied"] == 0, result
        assert await _tables(database) == []
    finally:
        await close(database)


async def test_a_file_with_no_tables_is_stamped_and_skipped_next_time() -> None:
    """The stamp is what makes "nothing loaded" dangerous if it ever got here.

    A second call reads the hash, sees a match, and does not look at the file.
    """
    database = await _database()
    models = _models(NO_TABLES)
    try:
        await apply_migrations_async(
            database, "sqlite", models, expand_enums=NOTHING, force=NOTHING
        )

        again = await apply_migrations_async(
            database, "sqlite", models, expand_enums=NOTHING, force=NOTHING
        )

        assert again["skipped"] is True, again
    finally:
        await close(database)


async def test_a_real_schema_is_not_confused_with_an_empty_one() -> None:
    """The control. A function returning early always would pass the rest."""
    database = await _database()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(ONE_TABLE), expand_enums=NOTHING, force=NOTHING
        )

        assert result["success"] is True, result
        assert result["operations_applied"] > 0, result
        assert await _tables(database) == ["users"]
    finally:
        await close(database)
