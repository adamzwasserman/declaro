"""Each ordering rule the toposort declares must actually hold.

MEASURED 2026-08-18 with `coverage json` over the whole suite. Unrun in
`differ/toposort.py`:

    a DROP COLUMN waiting on the DROP FOREIGN KEY that frees it
    a DROP COLUMN waiting on the DROP INDEX that covers it
    a CREATE TABLE waiting on the table it references
    an ADD FOREIGN KEY waiting on the ADD COLUMN it points at
    an ADD INDEX waiting on the ADD COLUMN it indexes
    an ALTER COLUMN waiting on the ADD INDEX over that column
    a DROP VIEW waiting on the view that depends on it
    the cycle report

`build_dependency_graph` documents thirteen rules in its docstring and eight
of them had never been exercised. An order that is wrong does not produce a
subtly wrong database: it produces a migration that fails, because the engine
refuses to create a child before its parent or drop a column an index still
covers.

THE OPERATIONS COME FROM THE DIFFER WHERE THE DIFFER CAN PRODUCE THEM, and
are written out only for the shapes a single schema pair cannot reach, such as
a view that another view depends on. Where they are written out, the shape is
copied from what `diff()` returns rather than from the docstring.

WHAT IS ASSERTED IS THE RELATION, NOT THE SEQUENCE. Pinning the exact output
list would fail whenever an unrelated tie is broken differently, and would say
nothing about whether the dependency was honoured. Each test says one
operation comes before another and nothing about the rest.
"""

from __future__ import annotations

from typing import Any

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.differ.toposort import (
    build_dependency_graph,
    topological_sort,
)
from declaro_persistum.exceptions import CycleError
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit]


def _order(operations: list[Any]) -> list[int]:
    """The graph and the sort are two calls, not one.

    `topological_sort(operations, dependencies)` takes the graph rather than
    building it, so a caller that forgets `build_dependency_graph` gets a
    TypeError instead of an unordered migration.
    """
    return topological_sort(operations, build_dependency_graph(operations))


def _ordered(operations: list[Any]) -> list[Any]:
    return [operations[i] for i in _order(operations)]


def _position(ordered: list[Any], op: str, table: str) -> int:
    for i, operation in enumerate(ordered):
        if operation["op"] == op and operation["table"] == table:
            return i
    raise AssertionError(f"{op} on {table} is not in the order: {ordered}")


def _plan(current: dict[str, Any], target: dict[str, Any]) -> list[Any]:
    return diff(current, target, dialect="sqlite", decisions=NOTHING)["operations"]


PARENT = {"columns": {"id": {"type": "integer", "primary_key": True}}}


def test_a_referenced_table_is_created_before_the_table_that_points_at_it() -> None:
    """The engine refuses a foreign key to a table that is not there yet."""
    target = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }

    ordered = _ordered(_plan({}, target))

    assert _position(ordered, "create_table", "parents") < _position(
        ordered, "create_table", "children"
    )


def test_a_foreign_key_is_added_after_the_column_it_points_at() -> None:
    """`add_foreign_key` on a column that does not exist yet cannot work."""
    current = {"parents": PARENT, "children": PARENT}
    target = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }

    ordered = _ordered(_plan(current, target))

    assert _position(ordered, "add_column", "children") < _position(
        ordered, "add_foreign_key", "children"
    )


def test_an_index_is_created_after_the_column_it_covers() -> None:
    current = {"widgets": PARENT}
    target = {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "slug": {"type": "text"},
            },
            "indexes": {"ix_slug": {"columns": ["slug"]}},
        }
    }

    ordered = _ordered(_plan(current, target))

    assert _position(ordered, "add_column", "widgets") < _position(
        ordered, "add_index", "widgets"
    )


def test_an_index_is_dropped_before_the_column_it_covers() -> None:
    """SQLite refuses to drop a column an index still covers."""
    current = {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "slug": {"type": "text"},
            },
            "indexes": {"ix_slug": {"columns": ["slug"]}},
        }
    }

    ordered = _ordered(_plan(current, {"widgets": PARENT}))

    assert _position(ordered, "drop_index", "widgets") < _position(
        ordered, "drop_column", "widgets"
    )


def test_a_foreign_key_is_dropped_before_the_column_carrying_it() -> None:
    current = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }
    target = {"parents": PARENT, "children": PARENT}

    ordered = _ordered(_plan(current, target))

    assert _position(ordered, "drop_foreign_key", "children") < _position(
        ordered, "drop_column", "children"
    )


def test_a_referencing_table_is_dropped_before_the_table_it_points_at() -> None:
    """The defect this file found, and it found it by being flaky.

    This assertion passed on its own and failed in the full suite. Both runs
    had random ordering off, so the order was not random: `drop_table` carried
    an empty details dict, no edge existed between the two drops, and Kahn's
    algorithm emitted whichever came first. Measured against real engines:

        sqlite      parent first  ok        child first  ok
        postgresql  parent first  FAILS     child first  ok

    So removing two related tables from the models broke the migration on
    PostgreSQL, or did not, depending on nothing.
    """
    current = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }

    operations = _plan(current, {})

    graph = build_dependency_graph(operations)
    parent = next(
        i for i, o in enumerate(operations) if o["table"] == "parents"
    )
    child = next(
        i for i, o in enumerate(operations) if o["table"] == "children"
    )

    # THE EDGE, NOT THE ORDER. Asserting the emitted order cannot see this
    # defect: with no edge the sort returns whichever drop came first, and
    # that happened to be right when this file ran alone. Both mutations that
    # remove the fix survived an order assertion and neither survives this.
    assert child in graph[parent], (
        f"dropping parents does not wait for children: {graph}"
    )


# ---------------------------------------------------------------------------
# Shapes one schema pair cannot produce
# ---------------------------------------------------------------------------

# A VIEW BUILT ON A VIEW. `diff_views` emits create_view and drop_view with
# the view record as `details`, keyed on the table `_views`; `depends_on` is
# one of the optional keys a view may carry. Copied from what `diff_views`
# returns, not from the ordering docstring.
VIEW_OPERATIONS: list[Any] = [
    {
        "op": "create_view",
        "table": "_views",
        "details": {"name": "base", "query": "SELECT 1"},
    },
    {
        "op": "create_view",
        "table": "_views",
        "details": {
            "name": "derived",
            "query": "SELECT * FROM base",
            "depends_on": ["base"],
        },
    },
]


def test_a_view_is_created_after_the_view_it_is_built_on() -> None:
    order = _order(VIEW_OPERATIONS)
    names = [VIEW_OPERATIONS[i]["details"]["name"] for i in order]

    assert names.index("base") < names.index("derived"), names


def test_a_dependent_view_is_dropped_before_the_one_it_is_built_on() -> None:
    """Dropping the base first leaves the dependent pointing at nothing."""
    drops = [
        {"op": "drop_view", "table": "_views", "details": dict(op["details"])}
        for op in VIEW_OPERATIONS
    ]

    order = _order(drops)
    names = [drops[i]["details"]["name"] for i in order]

    assert names.index("derived") < names.index("base"), names


def test_a_cycle_is_reported_rather_than_ordered() -> None:
    """Two tables that reference each other have no order that works.

    Returning some order anyway would hand the engine a migration that fails
    halfway, which is worse than refusing it.
    """
    cyclic: list[Any] = [
        {
            "op": "create_view",
            "table": "_views",
            "details": {"name": "a", "query": "SELECT 1", "depends_on": ["b"]},
        },
        {
            "op": "create_view",
            "table": "_views",
            "details": {"name": "b", "query": "SELECT 1", "depends_on": ["a"]},
        },
    ]

    with pytest.raises(CycleError):
        _order(cyclic)


def test_every_operation_appears_exactly_once_in_the_order() -> None:
    """The floor under every test above.

    An order that silently dropped an operation would satisfy any
    before-and-after assertion about the ones it kept.
    """
    target = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
                "slug": {"type": "text"},
            },
            "indexes": {"ix_slug": {"columns": ["slug"]}},
        },
    }
    operations = _plan({}, target)

    order = _order(operations)

    assert sorted(order) == list(range(len(operations))), (order, len(operations))


def test_the_graph_names_no_operation_that_is_not_there() -> None:
    """A dependency on an index that does not exist would order nothing."""
    target = {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }
    operations = _plan({}, target)

    graph = build_dependency_graph(operations)

    for node, deps in graph.items():
        assert 0 <= node < len(operations), node
        for dep in deps:
            assert 0 <= dep < len(operations), (node, dep)
