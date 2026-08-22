"""What decides whether SQLite rebuilds a table, which no test had called.

SQLite cannot express every change as an ALTER, so some operations go through
table reconstruction: create the new shape, copy the rows across, drop the old,
rename the new. It is the most destructive machinery in the package, and the
four functions that steer it -- `requires_reconstruction`,
`single_change_property`, `apply_reconstruction_changes` and `dry_run_preview`
-- were called by nothing in the suite.

THE OPERATIONS COME FROM THE DIFFER. A hand-written operation tests my idea of
the shape, and this exact function has already been given one: the differ
emits `{"from": X, "to": Y}` for a change, reconstruction once assigned that
DICT straight into a column definition, and `_map_type` then called `.lower()`
on it. `single_change_property` unwrapping `{"to": ...}` is what that fixed,
so it is pinned here.
"""

from __future__ import annotations

import pytest

from declaro_persistum.applier.shared import (
    _SQL_GENERATORS,
    apply_reconstruction_changes,
    dry_run_preview,
    requires_reconstruction,
    single_change_property,
)
from declaro_persistum.differ import diff
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit


def _alter(**changes) -> dict:
    """An alter_column operation, built the way the differ builds one."""
    return {
        "op": "alter_column",
        "table": "t",
        "details": {"column": "c", "changes": changes},
    }


# ---------------------------------------------------------------------------
# Which operations rebuild
# ---------------------------------------------------------------------------

REBUILDS = {"alter_column", "add_foreign_key", "drop_foreign_key"}


@pytest.mark.parametrize("op", sorted(_SQL_GENERATORS))
def test_only_the_operations_sqlite_cannot_alter_rebuild(op: str) -> None:
    """A rebuild copies the whole table, so the list must be exactly right.

    Too wide and every migration copies data it did not need to. Too narrow
    and a change SQLite cannot express fails.
    """
    operation = {"op": op, "table": "t", "details": {}}

    assert requires_reconstruction(operation) is (op in REBUILDS), op


def test_creating_a_table_does_not_rebuild_it() -> None:
    """The common case, taken from the differ rather than written."""
    plan = diff({}, {"t": {"columns": {"c": {"type": "text"}}}}, dialect="sqlite", decisions=NOTHING)

    assert not any(requires_reconstruction(o) for o in plan["operations"])


def test_changing_a_column_type_rebuilds() -> None:
    current = {"t": {"columns": {"c": {"type": "text"}}}}
    target = {"t": {"columns": {"c": {"type": "integer"}}}}
    plan = diff(current, target, dialect="sqlite", decisions=NOTHING)

    assert [requires_reconstruction(o) for o in plan["operations"]] == [True]


# ---------------------------------------------------------------------------
# The single-property fast path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prop", ["nullable", "type", "default"])
def test_a_lone_change_is_recognised_and_unwrapped(prop: str) -> None:
    """The `{"from": X, "to": Y}` wrapper is what the differ emits.

    Assigning that dict into a column definition is a defect this package has
    already had: `_map_type` then called `.lower()` on a dict.
    """
    assert single_change_property(_alter(**{prop: {"from": "a", "to": "b"}})) == (
        prop,
        "b",
    )


def test_a_bare_value_is_taken_as_it_is() -> None:
    """Not every caller wraps, so an unwrapped value must survive."""
    assert single_change_property(_alter(nullable=False)) == ("nullable", False)


def test_two_changes_take_the_general_path() -> None:
    """A specialised ALTER handles one property. Two is a rebuild."""
    assert single_change_property(_alter(type={"to": "integer"}, nullable={"to": True})) is None


def test_another_operation_is_not_a_column_change() -> None:
    assert single_change_property({"op": "drop_table", "table": "t", "details": {}}) is None


@pytest.mark.parametrize("prop", ["unique", "check"])
def test_a_property_with_no_specialised_alter_takes_the_general_path(
    prop: str,
) -> None:
    """`unique` and `check` joined the differ's comparison today.

    Neither has a specialised ALTER, and SQLite cannot change either in place,
    so both must fall through to a full rebuild. Returning a key here without
    a handler beside it would silently do nothing.
    """
    assert single_change_property(_alter(**{prop: {"from": None, "to": True}})) is None


# ---------------------------------------------------------------------------
# Applying the change to the column definitions
# ---------------------------------------------------------------------------


def test_a_type_change_lands_unwrapped_in_the_column() -> None:
    columns = {"c": {"type": "text"}}

    result = apply_reconstruction_changes(columns, _alter(type={"from": "text", "to": "integer"}))

    assert result["c"]["type"] == "integer", "the wrapper reached the column"


def test_a_foreign_key_is_added_with_its_cascade() -> None:
    columns = {"c": {"type": "text"}}
    operation = {
        "op": "add_foreign_key",
        "table": "t",
        "details": {"column": "c", "references": "u.id", "on_delete": "cascade"},
    }

    result = apply_reconstruction_changes(columns, operation)

    assert result["c"]["references"] == "u.id"
    assert result["c"]["on_delete"] == "cascade"


def test_dropping_a_foreign_key_takes_its_cascade_with_it() -> None:
    """A leftover `on_delete` on a column with no reference is a lie."""
    columns = {"c": {"type": "text", "references": "u.id", "on_delete": "cascade"}}
    operation = {
        "op": "drop_foreign_key",
        "table": "t",
        "details": {"column": "c"},
    }

    result = apply_reconstruction_changes(columns, operation)

    assert "references" not in result["c"]
    assert "on_delete" not in result["c"]


def test_a_change_to_a_column_that_is_not_there_raises() -> None:
    """Silently creating it would rebuild the table with a column nobody
    declared."""
    operation = {
        "op": "alter_column",
        "table": "t",
        "details": {"column": "gone", "changes": {"type": {"to": "integer"}}},
    }

    with pytest.raises(ValueError, match="not found"):
        apply_reconstruction_changes({"c": {"type": "text"}}, operation)


def test_an_operation_reconstruction_cannot_apply_raises() -> None:
    """The vocabulary here is smaller than the differ's, on purpose."""
    with pytest.raises(ValueError, match="reconstruction"):
        apply_reconstruction_changes(
            {"c": {"type": "text"}},
            {"op": "drop_table", "table": "t", "details": {}},
        )


# ---------------------------------------------------------------------------
# The preview, which must run nothing
# ---------------------------------------------------------------------------


def test_a_preview_renders_every_operation() -> None:
    plan = diff(
        {},
        {"t": {"columns": {"c": {"type": "text"}}, "indexes": {"i": {"columns": ["c"]}}}},
        dialect="sqlite", decisions=NOTHING)

    preview = dry_run_preview(plan["operations"], plan["execution_order"])

    assert preview["success"] is True
    assert preview["operations_applied"] == len(plan["operations"])
    assert any("CREATE TABLE" in s.upper() for s in preview["executed_sql"])


def test_a_preview_says_where_a_rebuild_would_happen() -> None:
    """A reconstruction has no single statement to show, and saying nothing
    would make a preview look like a no-op."""
    current = {"t": {"columns": {"c": {"type": "text"}}}}
    target = {"t": {"columns": {"c": {"type": "integer"}}}}
    plan = diff(current, target, dialect="sqlite", decisions=NOTHING)

    preview = dry_run_preview(plan["operations"], plan["execution_order"])

    assert any("reconstruction" in s.lower() for s in preview["executed_sql"]), (
        preview["executed_sql"]
    )


def test_a_preview_of_nothing_is_a_success_with_nothing_in_it() -> None:
    preview = dry_run_preview([], [])

    assert preview == {
        "success": True,
        "executed_sql": [],
        "operations_applied": 0,
        "error": None,
        "error_operation": None,
    }
