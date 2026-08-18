"""A constraint change the differ does not notice is a silent no-op.

MEASURED 2026-08-17, at the table level, on SQLite:

    unique removed    []
    unique added      []
    check removed     []
    on_delete changed []
    type changed      ['alter_column']

`apply` then reports `success: True` and the DDL is unchanged. A user adds
`unique=True` to a column, deploys, is told the migration succeeded, and the
constraint is not there. The same for a CHECK, and for changing a foreign
key's ON DELETE from CASCADE to RESTRICT -- which decides whether deleting a
row destroys its children.

`_compute_column_alterations` compared type, nullable and default.
`_diff_foreign_keys` compared whether a reference exists, not what it does on
delete. Everything else a column can declare was invisible.

HOW IT WAS FOUND, because the route matters more than the defect: dropping a
UNIQUE column fails on SQLite -- the engine refuses -- so I looked for the
workaround, which is to drop the constraint first and the column second. Step
one reported success and changed nothing, so there was no workaround and the
column could not be dropped at all.

    step 1 operations: []
    step 1 applied
    step 2 FAILED: cannot drop UNIQUE column: "spare"

WHAT THIS DOES NOT FIX: dropping a UNIQUE column in ONE migration. SQLite
still refuses that, the failure is loud, names the column, and rolls back, and
the two-step route now works because step one does something. Making it one
step means routing `drop_column` through table reconstruction, which is a
larger change and a separate decision.
"""

from __future__ import annotations

import os
import tempfile

import aiosqlite
import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ import diff
from declaro_persistum.inspector import introspect

pytestmark = pytest.mark.precommit

DIALECTS = ["postgresql", "sqlite", "turso"]


def _table(column: dict) -> dict:
    return {
        "t": {
            "columns": {"id": {"type": "text", "primary_key": True}, "c": column}
        }
    }


def _ops(before: dict, after: dict, dialect: str) -> list[str]:
    return [
        o["op"] for o in diff(_table(before), _table(after), dialect=dialect)["operations"]
    ]


# ---------------------------------------------------------------------------
# unique
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_adding_a_unique_constraint_emits_an_operation(dialect: str) -> None:
    assert _ops({"type": "text"}, {"type": "text", "unique": True}, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_removing_a_unique_constraint_emits_an_operation(dialect: str) -> None:
    assert _ops({"type": "text", "unique": True}, {"type": "text"}, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_unchanged_unique_emits_nothing(dialect: str) -> None:
    """The floor. Emitting always would make every migration rebuild."""
    same = {"type": "text", "unique": True}
    assert _ops(same, dict(same), dialect) == []


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_check_constraint_change_emits_an_operation(dialect: str) -> None:
    assert _ops({"type": "integer", "check": "c > 0"},
                {"type": "integer", "check": "c > 10"}, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_removing_a_check_emits_an_operation(dialect: str) -> None:
    assert _ops({"type": "integer", "check": "c > 0"}, {"type": "integer"}, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_unchanged_check_emits_nothing(dialect: str) -> None:
    same = {"type": "integer", "check": "c > 0"}
    assert _ops(same, dict(same), dialect) == []


# ---------------------------------------------------------------------------
# the cascade, which decides whether deleting a row destroys its children
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_changing_on_delete_emits_an_operation(dialect: str) -> None:
    """CASCADE and RESTRICT are opposite answers to "delete the parent"."""
    before = {"type": "text", "references": "u.id", "on_delete": "cascade"}
    after = {"type": "text", "references": "u.id", "on_delete": "restrict"}

    assert _ops(before, after, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_changing_on_update_emits_an_operation(dialect: str) -> None:
    before = {"type": "text", "references": "u.id", "on_update": "cascade"}
    after = {"type": "text", "references": "u.id", "on_update": "set null"}

    assert _ops(before, after, dialect) != []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_unchanged_cascade_emits_nothing(dialect: str) -> None:
    same = {"type": "text", "references": "u.id", "on_delete": "cascade"}
    assert _ops(same, dict(same), dialect) == []


# ---------------------------------------------------------------------------
# Executed, because an operation is not the same as a changed database
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_removed_unique_is_gone_from_the_database() -> None:
    """The step that reported success and changed nothing."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        with_unique = _table({"type": "text", "unique": True})
        without = _table({"type": "text"})

        plan = diff({}, with_unique, dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff({"t": actual["t"]}, without, dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.commit()

        # The constraint is what matters, not the reported schema.
        await conn.execute("INSERT INTO t VALUES ('2', 'x')")
        await conn.commit()

        cursor = await conn.execute("SELECT COUNT(*) FROM t")
        assert (await cursor.fetchone())[0] == 2
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_an_added_unique_constrains_the_database() -> None:
    """The other direction, and the floor under the test above."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, _table({"type": "text"}), dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff({"t": actual["t"]}, _table({"type": "text", "unique": True}),
                    dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.commit()

        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute("INSERT INTO t VALUES ('2', 'x')")
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_two_step_route_to_dropping_a_unique_column_works() -> None:
    """SQLite refuses to drop a UNIQUE column, so the constraint goes first.

    That route was closed while step one did nothing.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, _table({"type": "text", "unique": True}), dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff({"t": actual["t"]}, _table({"type": "text"}), dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        gone = {"t": {"columns": {"id": {"type": "text", "primary_key": True}}}}
        plan = diff({"t": actual["t"]}, gone, dialect="sqlite")
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite",
                    dry_run=False)
        await conn.commit()

        cursor = await conn.execute("PRAGMA table_info(t)")
        assert [row[1] for row in await cursor.fetchall()] == ["id"]
        cursor = await conn.execute("SELECT id FROM t")
        assert await cursor.fetchall() == [("1",)], "the rows did not survive"
    finally:
        await conn.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# The fold, at the inputs where it is observable
# ---------------------------------------------------------------------------
#
# TWO MUTATION CONTROLS PASSED AND BOTH WERE REAL GAPS.
#
# Folding only the CURRENT side changed nothing, because every test above gets
# its current side from introspection, which has already folded. The call
# matters for a schema written by hand -- a snapshot file, or a diff between
# two declared schemas -- where the unique may be on the index and the other
# side puts it on the column.
#
# And nothing exercised a MULTI-COLUMN unique index. Removing the length check
# folds `unique` onto whichever column happens to be first, which would claim
# a single column is unique when only the combination is.


def test_a_unique_index_on_the_current_side_is_folded_too() -> None:
    """Both sides fold, so a hand-written schema compares equal to a declared
    one that spells the same constraint the other way."""
    by_index = {
        "t": {
            "columns": {"id": {"type": "text", "primary_key": True},
                        "c": {"type": "text"}},
            "indexes": {"i_c": {"columns": ["c"], "unique": True}},
        }
    }
    by_column = {
        "t": {
            "columns": {"id": {"type": "text", "primary_key": True},
                        "c": {"type": "text", "unique": True}},
            "indexes": {"i_c": {"columns": ["c"], "unique": True}},
        }
    }

    assert diff(by_index, by_column, dialect="sqlite")["operations"] == []
    assert diff(by_column, by_index, dialect="sqlite")["operations"] == []


def test_a_multi_column_unique_index_marks_no_single_column_unique() -> None:
    """`UNIQUE (a, b)` says the PAIR is unique, not either column.

    Folding it onto one would claim a constraint the database does not have,
    and the differ would then propose removing a uniqueness nobody declared.
    """
    pair = {
        "t": {
            "columns": {"a": {"type": "text"}, "b": {"type": "text"}},
            "indexes": {"i_ab": {"columns": ["a", "b"], "unique": True}},
        }
    }
    neither = {
        "t": {
            "columns": {"a": {"type": "text"}, "b": {"type": "text"}},
            "indexes": {"i_ab": {"columns": ["a", "b"], "unique": True}},
        }
    }

    assert diff(pair, neither, dialect="sqlite")["operations"] == []

    from declaro_persistum.differ.core import _with_unique_folded

    folded = _with_unique_folded(pair["t"])
    assert "unique" not in folded["a"], folded["a"]
    assert "unique" not in folded["b"], folded["b"]


def test_a_primary_key_is_not_marked_unique_by_its_own_index() -> None:
    """A PRIMARY KEY is already unique, and saying so twice would make the
    differ propose adding a constraint the column has by definition."""
    from declaro_persistum.differ.core import _with_unique_folded

    table = {
        "columns": {"id": {"type": "text", "primary_key": True}},
        "indexes": {"i_id": {"columns": ["id"], "unique": True}},
    }

    assert "unique" not in _with_unique_folded(table)["id"]
