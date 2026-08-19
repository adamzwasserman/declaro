"""The validator's rules never ran on the path that migrates.

MEASURED 2026-08-19: `apply_migrations_async` does not call `validate_schema`
anywhere. Every rule the validator holds fires only under `declaro validate`,
which is a command an author has to think to run.

So the checks exist and do not protect the operation they were written for.
The unrenderable-name check added earlier today is the clearest case: it
refuses a table named `quo"ted` because the appliers cannot write it, and a
migration went straight past it into the engine, which answered with a syntax
error naming a fragment of the name.

WHAT THIS COSTS IS THE MESSAGE, NOT USUALLY THE OUTCOME. Most invalid schemas
fail at the engine too. They fail with the engine's words: "near \\"ted\\":
syntax error" rather than "Table 'quo\\"ted' cannot be migrated: it contains
'\\"', and the appliers do not escape a double quote inside a quoted name.
Rename it." The second names the table, the character, the reason and the
action. The first names a fragment.

Some do not fail at all. A foreign key pointing at a table nobody declared is
an error the validator catches by comparing the schema against itself, and
SQLite accepts the DDL without complaint because it does not check a
reference at creation time.

BLAST RADIUS, MEASURED BEFORE WIRING IT IN. Every schema this package ships
validates clean, and of the fourteen models files the tests build, thirteen
validate clean and none has an error. Nothing that works today starts
failing.

WARNINGS ARE NOT ERRORS. The validator separates them and only the errors
stop a migration. That distinction is load-bearing rather than tidy: an
unknown type is a warning because it may be a custom type or an enum, and a
circular foreign key is a warning because deferrable constraints exist. A
gate that stopped on warnings would refuse schemas that work.

The first version of this file could not tell the difference. Widening the
condition to `errors or warnings` passed every test in it, because none of
its schemas produced a warning. Two now do.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite, writing
from declaro_persistum.migrations import apply_migrations_async

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

CLEAN = '''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    name: str
'''

# A name the appliers cannot write. The engine answers with a syntax error
# naming a fragment; the validator names the table and the character.
UNRENDERABLE = '''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("quo\\"ted")
class Quoted(BaseModel):
    id: int = field(primary_key=True)
'''

# A reference to a table nobody declared. SQLite accepts this DDL: it does
# not check a reference at creation time, so nothing fails and the column
# points at nothing.
DANGLING_REFERENCE = '''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("posts")
class Post(BaseModel):
    id: int = field(primary_key=True)
    author_id: int = field(references="authors.id")
'''


def _models(body: str) -> str:
    path = Path(tempfile.mkdtemp()) / "models.py"
    path.write_text(body)
    return str(path)


async def _database() -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "v.db"),
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


async def test_a_clean_schema_still_migrates() -> None:
    """The control, and the one that matters most.

    A gate that refused everything would satisfy every other test here, and
    every schema this package ships validates clean.
    """
    database = await _database()
    try:
        result = await apply_migrations_async(database, "sqlite", _models(CLEAN))

        assert result["success"] is True, result
        assert await _tables(database) == ["users"]
    finally:
        await close(database)


async def test_a_dangling_reference_is_refused() -> None:
    """The case the engine does not catch at all.

    SQLite creates the table and the column points at nothing.
    """
    database = await _database()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(DANGLING_REFERENCE)
        )

        assert result["success"] is False, result
    finally:
        await close(database)


async def test_nothing_is_created_when_the_schema_is_refused() -> None:
    """A refusal that had already created the table is not a refusal."""
    database = await _database()
    try:
        await apply_migrations_async(
            database, "sqlite", _models(DANGLING_REFERENCE)
        )

        assert await _tables(database) == [], await _tables(database)
    finally:
        await close(database)


async def test_the_refusal_carries_the_validator_s_words() -> None:
    """Not the engine's. The whole point is the better message."""
    database = await _database()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(DANGLING_REFERENCE)
        )

        assert "authors" in result["error"], result["error"]
    finally:
        await close(database)


async def test_an_unrenderable_name_is_refused_before_the_engine_sees_it() -> None:
    """The check added this morning, now on the path it was written for."""
    database = await _database()
    try:
        result = await apply_migrations_async(
            database, "sqlite", _models(UNRENDERABLE)
        )

        assert result["success"] is False, result
        assert "Rename it" in result["error"], result["error"]
    finally:
        await close(database)


# Schemas the validator warns about and does not refuse. An unknown type may
# be a custom type or an enum; a circular reference may want deferrable
# constraints. Both migrate.
WARNS_UNKNOWN_TYPE = '''
from typing import Any
from pydantic import BaseModel
from declaro_persistum import table, field


@table("shapes")
class Shape(BaseModel):
    id: int = field(primary_key=True)
    area: Any = field(db_type="geography")
'''


async def test_a_schema_the_validator_only_warns_about_still_migrates() -> None:
    """The control on errors-versus-warnings, which nothing else here measures.

    Widening the gate to stop on warnings passes every other test in this
    file. It fails this one, which is the point: an unknown type is a warning
    precisely so a custom type or an enum keeps working.
    """
    from declaro_persistum.pydantic_loader import load_schema_from_models
    from declaro_persistum.validator import validate_schema

    models = _models(WARNS_UNKNOWN_TYPE)
    schema = load_schema_from_models(str(Path(models).parent))
    report = validate_schema(schema)
    assert report["warnings"] and not report["errors"], report

    database = await _database()
    try:
        result = await apply_migrations_async(database, "sqlite", models)

        assert result["success"] is True, result
        assert await _tables(database) == ["shapes"], await _tables(database)
    finally:
        await close(database)
