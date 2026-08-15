"""A view declared in a models file must end up in the database.

IT DID NOT, AND EVERY PIECE OF THE CHAIN EXISTED. The inspectors return views
when asked. The differ has `diff_views`, which emits `create_view` and
`drop_view`. The appliers execute both. Only the wiring was absent: the loader
read no views, `apply_migrations_async` never called `diff_views`, and
`usage.md` said views worked end to end. They did not.

TWO SEPARATE OMISSIONS, AND THE SECOND ONE PASSED ITS OWN TEST. Appending the
view operations to `diff_result["operations"]` is not enough; their INDICES
have to go into `execution_order` too, because that list is what the applier
iterates. The first version appended the operations alone, and the migration
reported success having created nothing:

    applied: 1 ops | success: True
    views in the database: []

That is the exact shape of a silent failure, and only running the migration
against a real database and then asking the database showed it.

THREE OTHER DIFFS WENT INSTEAD OF BEING WIRED. `diff_enums`, `diff_triggers`
and `diff_procedures` emitted eight operation kinds no applier could execute,
for callers that did not exist. `diff_views` was the one worth keeping because
its operations were already in the applier's table.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum.differ import diff_views
from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.pydantic_loader import LoaderError, load_declarations
from declaro_persistum.sqlite_database import open_sqlite

pytestmark = pytest.mark.precommit

MODELS = '''
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field
from declaro_persistum.types import View

@table("users")
class User(BaseModel):
    id: UUID = field(primary_key=True)
    status: str

active_users: View = {
    "name": "active_users",
    "query": "SELECT id FROM users WHERE status = 'active'",
    "materialized": False,
}
'''


def _models(tmp_path, body=MODELS, name="models.py"):
    path = tmp_path / name
    path.write_text(body)
    return path


@pytest.mark.asyncio
async def test_a_declared_view_exists_after_a_migration(tmp_path) -> None:
    """The regression, asked of the database rather than of the return value.

    `operations_applied` said 1 and success said True while no view existed.
    A result dict is not evidence that a thing happened.
    """
    path = _models(tmp_path)
    db = await open_sqlite(
        str(tmp_path / "a.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    result = await apply_migrations_async(db, dialect="sqlite", schema_path=str(path))
    assert result["success"]

    from declaro_persistum import reading

    async with reading(db) as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )
        names = [row[0] for row in await cursor.fetchall()]
        assert names == ["active_users"], (
            f"the migration reported {result['operations_applied']} operations "
            f"applied and the database has views {names}"
        )
        # And it is a working view, not just a row in sqlite_master.
        await conn.execute("SELECT * FROM active_users")


@pytest.mark.asyncio
async def test_the_view_operations_are_in_the_execution_order(tmp_path) -> None:
    """The second omission, which the first test would also catch.

    Kept separate because it names the mechanism: an operation the applier
    never reaches is indistinguishable from an operation nobody generated.
    """
    path = _models(tmp_path)
    db = await open_sqlite(
        str(tmp_path / "b.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    result = await apply_migrations_async(db, dialect="sqlite", schema_path=str(path))
    assert result["operations_applied"] == 2, (
        "one table and one view were declared; "
        f"{result['operations_applied']} operation(s) ran"
    )


def test_the_loader_reads_views_and_tables_in_one_import(tmp_path) -> None:
    schema, views = load_declarations(_models(tmp_path))
    assert list(schema) == ["users"]
    assert list(views) == ["active_users"]
    assert views["active_users"]["query"].startswith("SELECT id FROM users")


def test_a_misspelled_view_key_is_refused_at_load(tmp_path) -> None:
    """The same defect `index_from_meta` fixed, on the other declaration.

    A plain dict silently drops an unknown key, so `materialised` would leave
    a view that is quietly not materialized.
    """
    body = MODELS.replace('"materialized": False', '"materialised": False')
    with pytest.raises(LoaderError, match="materialised"):
        load_declarations(_models(tmp_path, body, "typo.py"))


def test_a_dict_that_is_not_a_view_is_left_alone(tmp_path) -> None:
    """Views are picked up by shape, so the shape must be narrow enough."""
    body = MODELS + '\nSETTINGS = {"retries": 3, "timeout": 5}\n'
    _schema, views = load_declarations(_models(tmp_path, body, "extra.py"))
    assert list(views) == ["active_users"]


def test_an_unchanged_view_produces_no_operation() -> None:
    """A migration that rewrites what nobody changed cannot be told from one
    that had a reason to."""
    same = {"v": {"name": "v", "query": "SELECT 1"}}
    assert diff_views(same, same) == []


def test_a_changed_query_replaces_the_view() -> None:
    before = {"v": {"name": "v", "query": "SELECT 1"}}
    after = {"v": {"name": "v", "query": "SELECT 2"}}
    ops = diff_views(before, after)
    assert [op["op"] for op in ops] == ["drop_view", "create_view"], (
        "a view is replaced whole; there is no ALTER VIEW to reconcile it with"
    )


def test_the_three_unapplyable_diffs_are_gone() -> None:
    """They emitted eight operation kinds the applier's table does not hold.

    Asserted rather than trusted, because they survived two earlier passes of
    deleting code no entry point reached: BDD steps kept them importable.
    """
    from declaro_persistum import differ
    from declaro_persistum.applier.shared import generate_operation_sql  # noqa: F401

    for gone in ("diff_enums", "diff_triggers", "diff_procedures"):
        assert not hasattr(differ, gone), f"{gone} is back without an applier"


def test_every_operation_the_differ_emits_can_be_applied() -> None:
    """The property underneath the deletion, stated as a rule.

    A differ that emits an operation no applier handles is a migration that
    reports success and does nothing.
    """
    import inspect
    import re

    from declaro_persistum.applier import shared
    from declaro_persistum.differ import core, extended

    emitted = set()
    for module in (core, extended):
        emitted |= set(re.findall(r'"op": "([a-z_]+)"', inspect.getsource(module)))

    handled = set(shared._SQL_GENERATORS)
    assert emitted <= handled, (
        f"the differ emits {sorted(emitted - handled)}, which no applier can "
        f"execute"
    )


@pytest.mark.asyncio
async def test_migrating_twice_changes_nothing_the_second_time(tmp_path) -> None:
    """Idempotence, which is the one property a declarative engine must have.

    It did not hold. `uuid` is spelled TEXT on SQLite, so introspection
    returned "text", the differ compared raw strings, and every run after the
    first reported an ambiguity about a column nobody had touched:

        first : True, 1 operation
        second: False, "Column 'id' type change from text to uuid may cause
                data loss. Confirm this change?"

    An ambiguity stops the migration, so the second edit anyone made to their
    models file was blocked. `force=True` skips the hash cache, which is what
    makes this test see the diff rather than the skip.
    """
    path = _models(tmp_path, MODELS.replace("status: str", "status: str\n    at: datetime"))
    path.write_text("from datetime import datetime\n" + path.read_text())
    db = await open_sqlite(
        str(tmp_path / "twice.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    first = await apply_migrations_async(db, dialect="sqlite", schema_path=str(path))
    assert first["success"] and first["operations_applied"] > 0

    again = await apply_migrations_async(
        db, dialect="sqlite", schema_path=str(path), force=True
    )
    assert again["success"], again["error"]
    assert again["operations_applied"] == 0, (
        f"nothing changed and the engine proposed "
        f"{again['operations_applied']} operation(s)"
    )


@pytest.mark.asyncio
async def test_a_changed_view_query_reaches_the_database(tmp_path) -> None:
    """`create_view_sql` emits CREATE VIEW IF NOT EXISTS, which is a no-op
    against a view that exists. Editing a query changed nothing and said it
    had."""
    path = _models(tmp_path, name="changing.py")
    db = await open_sqlite(
        str(tmp_path / "chg.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    await apply_migrations_async(db, dialect="sqlite", schema_path=str(path))
    path.write_text(path.read_text().replace("'active'", "'archived'"))
    result = await apply_migrations_async(db, dialect="sqlite", schema_path=str(path))
    assert result["success"], result["error"]

    from declaro_persistum import reading

    async with reading(db) as conn:
        cursor = await conn.execute(
            "SELECT sql FROM sqlite_master WHERE name='active_users'"
        )
        sql = (await cursor.fetchone())[0]
    assert "archived" in sql, f"the database still serves the old view: {sql}"


def test_the_type_comparison_needs_a_dialect() -> None:
    """Rule 14, and the reason the defect existed at all."""
    import inspect

    from declaro_persistum.differ import diff

    assert inspect.signature(diff).parameters["dialect"].default is (
        inspect.Parameter.empty
    )


def test_a_real_type_change_still_trips_on_postgresql() -> None:
    """The fix must not silence the check it was narrowing."""
    from declaro_persistum.applier.shared import same_type

    assert same_type("uuid", "text", "sqlite")
    assert not same_type("uuid", "text", "postgresql")
