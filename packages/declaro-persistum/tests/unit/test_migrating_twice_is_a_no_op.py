"""The second migration must do nothing. That is what declarative means.

You declare a state; the engine reaches it. Reaching a state you are already
in is zero work, so a re-run that proposes operations is the engine saying the
database differs from the declaration when it does not.

THREE SEPARATE DEFECTS BROKE IT, ALL ON SQLITE AND TURSO, ALL MEASURED
2026-08-14 with an unchanged models file and `force=True` to skip the hash:

    a uuid column                 second run BLOCKED by an ambiguity
    a materialized view           re-run: 3 operations
    the matview's backing table   re-run: 2 operations

Each has its own cause and its own fix:

    `uuid` is spelled TEXT, so introspection said "text" and the differ
    compared raw strings. `same_type(left, right, dialect)` compares them as
    the engine spells them.

    A materialized view is not in `sqlite_master` as a view; the emulation is
    a table plus a metadata row. The inspector hard-coded `materialized:
    False`, so the declaration and the database disagreed forever.

    That emulation table then looked like an undeclared user table, so the
    differ proposed dropping it — which would have discarded the view's
    contents on every deploy.

The third is the one that mattered most and was the quietest: dropping a
materialized view empties the cache the view exists to be, and the migration
reports success.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.sqlite_database import open_sqlite
from declaro_persistum.turso_database import open_turso

pytestmark = pytest.mark.turso

MODELS = '''
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel
from declaro_persistum import table, field
from declaro_persistum.types import View

@table("users")
class User(BaseModel):
    id: UUID = field(primary_key=True)
    at: datetime
    flag: bool
    status: str

plain: View = {
    "name": "active_users",
    "query": "SELECT id FROM users WHERE status = 'active'",
    "materialized": False,
}
stats: View = {
    "name": "user_stats",
    "query": "SELECT status, COUNT(*) c FROM users GROUP BY status",
    "materialized": True,
    "refresh": "manual",
}
'''


async def _open(kind: str, tmp_path: pathlib.Path):
    if kind == "sqlite":
        return await open_sqlite(
            str(tmp_path / f"{kind}.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
    return await open_turso(str(tmp_path / f"{kind}.db"), shutdown="exit_immediately")


@pytest.mark.parametrize("kind", ["sqlite", "turso"])
@pytest.mark.asyncio
async def test_the_second_migration_does_nothing(kind: str, tmp_path) -> None:
    """Every declared shape at once: uuid, datetime, bool, a view, a matview."""
    models = tmp_path / "models.py"
    models.write_text(MODELS)
    db = await _open(kind, tmp_path)

    first = await apply_migrations_async(
        db, dialect=kind, schema_path=str(models)
    )
    assert first["success"], first["error"]
    assert first["operations_applied"] > 0, "nothing was created to begin with"

    # `force` skips the schema-hash cache, which would otherwise return before
    # diffing and hide every defect this file exists for.
    again = await apply_migrations_async(
        db, dialect=kind, schema_path=str(models), force=True
    )
    assert again["success"], again["error"]
    assert again["operations_applied"] == 0, (
        f"nothing changed and the engine proposed "
        f"{again['operations_applied']} operation(s)"
    )


@pytest.mark.parametrize("kind", ["sqlite", "turso"])
@pytest.mark.asyncio
async def test_a_materialized_view_is_not_reported_as_a_table(
    kind: str, tmp_path
) -> None:
    """The quiet one, named on its own so a regression says which.

    The backing table introspected as an ordinary table, was undeclared, and
    would have been dropped with its contents on every migration.
    """
    from declaro_persistum import reading
    from declaro_persistum.inspector import introspect_with_views

    models = tmp_path / "models.py"
    models.write_text(MODELS)
    db = await _open(kind, tmp_path)
    await apply_migrations_async(db, dialect=kind, schema_path=str(models))

    async with reading(db) as conn:
        schema, views = await introspect_with_views(conn, kind)

    assert "user_stats" not in schema, (
        "the materialized view's backing table is reported as a user table, "
        "so the differ will propose dropping it"
    )
    assert views["user_stats"]["materialized"] is True, (
        "the view is reported as not materialized, so the declaration and the "
        "database disagree on every run"
    )
    assert "users" in schema, "the real table went missing with the guard"
