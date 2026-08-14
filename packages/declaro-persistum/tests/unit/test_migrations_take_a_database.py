"""The migration entry point must work with the only object this package builds.

`apply_migrations_async` is exported from `__init__.py` and is the package's
headline function. It spoke to an object that had been deleted, and nothing
caught it because the first parameter was annotated `Any`. 221 tests passed
over a feature that could not run.

The last assertion in this file names the old parameter deliberately: it is the
guard that fails if it comes back.

REPLACING THE TWO CALLS THAT HAVE NO DIRECT EQUIVALENT:

  pause_push          nothing. A migration is a writer, and opportunistic
                      replication already defers to writers.
  initial_pull_complete
                      `refresh(db)` before the diff. Warm opens no longer
                      replicate, so a migration would otherwise diff a stale
                      copy and emit DDL for a schema that has moved.
"""

from __future__ import annotations

import inspect
import pathlib

import pytest

from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.turso_database import open_turso

pytestmark = pytest.mark.precommit


def _schema(tmp_path: pathlib.Path) -> str:
    """A Python module of Pydantic models, which is what the entry point loads.

    `load_models_from_module` recognises a model by `__tablename__`.
    """
    f = tmp_path / "models.py"
    f.write_text(
        "from pydantic import BaseModel\n"
        "\n"
        "class Users(BaseModel):\n"
        "    __tablename__ = 'users'\n"
        "    id: int\n"
        "    email: str\n"
    )
    return str(f)


@pytest.mark.asyncio
async def test_a_migration_runs_against_a_database(tmp_path):
    """The whole point. It creates the table the schema asks for."""
    db = await open_turso(str(tmp_path / "app.db"), shutdown="exit_immediately")

    await apply_migrations_async(db, "turso", _schema(tmp_path))

    from declaro_persistum.database import reading

    async with reading(db) as conn:
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        )
        assert await cur.fetchone(), (
            "the migration reported no error and created no table"
        )


@pytest.mark.asyncio
async def test_running_it_twice_changes_nothing_the_second_time(tmp_path):
    """A migration is a diff, so a second run against the same schema is empty."""
    db = await open_turso(str(tmp_path / "app.db"), shutdown="exit_immediately")
    schema = _schema(tmp_path)

    await apply_migrations_async(db, "turso", schema)
    second = await apply_migrations_async(db, "turso", schema)

    applied = second.get("operations_applied", second.get("applied", 0))
    assert applied == 0, (
        f"the second run applied {applied} operations against an unchanged "
        f"schema, so it is not diffing — it is reapplying"
    )


def test_the_entry_point_names_the_type_it_takes():
    """`Any` is what let a deleted API survive on the package's main function."""
    params = inspect.signature(apply_migrations_async).parameters
    first = next(iter(params.values()))
    assert first.name != "pool", (
        "the first parameter is still called 'pool', which is the object that "
        "no longer exists"
    )
    assert first.annotation is not inspect.Parameter.empty, (
        "the first parameter is unannotated"
    )
    assert "Any" not in str(first.annotation), (
        f"the first parameter is annotated {first.annotation}; an untyped "
        f"entry point is what hid this break from every checker"
    )
