"""What must happen before what, when one migration carries several operations.

MEASURED 2026-08-20 by branch coverage. Thirteen partial branches in
`differ/toposort.py`, and they are the ordering rules themselves: an FK to a
table created in the same migration, an FK to a COLUMN added in the same
migration, a self-reference that must not depend on itself, an index that must
be dropped before the column it covers, and the same index before a type change
on that column. Every rule was correct when I ran it. None was written down.

WHY THIS FILE EXISTS AT ALL. Ordering is the one thing a migration tool cannot
get wrong quietly. A wrong dependency does not produce a wrong answer, it
produces a migration that runs half way and stops, with the database in a state
that is neither the old schema nor the new one. The engine reports the failure
from the middle of the work, so the operator learns the order was wrong only
after it has already been applied.

The graph is asserted directly rather than the sorted order, because a sort can
put B before A by accident whenever nothing forces the other way round. Asking
`build_dependency_graph` what it believes tells the two apart: `{0: [1]}` is
the rule, `{}` with a lucky order is its absence.
"""

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort
from declaro_persistum.exceptions import CycleError
from declaro_persistum.types import NOTHING


def _ordered(operations: list[dict]) -> list[str]:
    graph = build_dependency_graph(operations)
    return [
        f"{operations[i]['op']}/{operations[i]['table']}"
        for i in topological_sort(operations, graph)
    ]


def test_a_table_is_created_before_the_table_whose_key_points_at_it() -> None:
    """`post.author_id` references `author.id`, so `author` must exist first.

    The two CREATE TABLE statements are handed over in the wrong order on
    purpose. Nothing in the operation list says which comes first; only the
    `references` string does.
    """
    operations = [
        {
            "op": "create_table",
            "table": "post",
            "details": {
                "columns": {"author_id": {"type": "integer", "references": "author.id"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
        {
            "op": "create_table",
            "table": "author",
            "details": {
                "columns": {"id": {"type": "integer"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
    ]

    assert build_dependency_graph(operations) == {0: [1], 1: []}
    assert _ordered(operations) == ["create_table/author", "create_table/post"]


def test_a_table_that_references_itself_does_not_wait_for_itself() -> None:
    """A tree's `parent_id` points back at its own table.

    The rule that orders one CREATE TABLE after another has to exclude the
    table it is ordering, or a self-reference becomes a dependency on itself
    and the sort reports a cycle in a schema that has none.
    """
    operations = [
        {
            "op": "create_table",
            "table": "node",
            "details": {
                "columns": {"parent_id": {"type": "integer", "references": "node.id"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
    ]

    assert build_dependency_graph(operations) == {0: []}
    assert _ordered(operations) == ["create_table/node"]


def test_a_foreign_key_waits_for_the_table_it_points_at() -> None:
    """ADD FOREIGN KEY on an existing table, pointing at a new one."""
    operations = [
        {
            "op": "add_foreign_key",
            "table": "post",
            "details": {"column": "author_id", "references": "author.id"},
        },
        {
            "op": "create_table",
            "table": "author",
            "details": {
                "columns": {"id": {"type": "integer"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
    ]

    assert build_dependency_graph(operations) == {0: [1], 1: []}
    assert _ordered(operations) == ["create_table/author", "add_foreign_key/post"]


def test_a_foreign_key_waits_for_the_column_it_points_at() -> None:
    """The referenced TABLE already exists; the referenced COLUMN does not.

    `author` is not being created here, so the create_table rule finds nothing.
    What the key waits for is the ADD COLUMN that puts `author.code` there. A
    tool that only tracked tables would emit the constraint against a column
    that arrives one statement later.
    """
    operations = [
        {
            "op": "add_foreign_key",
            "table": "post",
            "details": {"column": "author_id", "references": "author.code"},
        },
        {
            "op": "add_column",
            "table": "author",
            "details": {"column": "code", "definition": {"type": "text"}},
        },
    ]

    assert build_dependency_graph(operations) == {0: [1], 1: []}
    assert _ordered(operations) == ["add_column/author", "add_foreign_key/post"]


def test_an_index_is_dropped_before_the_column_it_covers() -> None:
    """Dropping `users.email` while `ix_users_email` still names it."""
    operations = [
        {"op": "drop_column", "table": "users", "details": {"column": "email"}},
        {
            "op": "drop_index",
            "table": "users",
            "details": {"index": "ix_users_email", "definition": {"columns": ["email"]}},
        },
    ]

    assert build_dependency_graph(operations) == {0: [1], 1: []}
    assert _ordered(operations) == ["drop_index/users", "drop_column/users"]


def test_the_differ_hands_the_sort_what_the_edge_needs() -> None:
    """The test above passed for years while the edge never fired.

    It builds its own operations, and the shape it built was not the shape the
    differ emits. The differ wrote `drop_index` as `{"index": name}` with no
    definition, the sort read `details.get("definition", {}).get("columns", [])`
    off it, got an empty list, matched nothing, and added no edge. Measured on
    2026-08-24: `build_dependency_graph` returned `{0: [], 1: []}` for a real
    plan, where it now returns `{0: [1], 1: []}`.

    SQLite refuses to drop a column an index covers and PostgreSQL drops the
    index for you, so which engine ran the migration decided whether the
    missing constraint was visible.

    This one starts from two schemas, so it can only pass if the differ hands
    the sort what the sort reads.
    """
    current = {
        "users": {
            "columns": {"id": {"type": "integer"}, "email": {"type": "text"}},
            "indexes": {"ix_users_email": {"columns": ["email"]}},
        }
    }
    target = {"users": {"columns": {"id": {"type": "integer"}}}}

    plan = diff(current, target, dialect="sqlite", decisions=NOTHING)
    operations = plan["operations"]

    dropped_index = next(
        i for i, op in enumerate(operations) if op["op"] == "drop_index"
    )
    dropped_column = next(
        i for i, op in enumerate(operations) if op["op"] == "drop_column"
    )

    dependencies = build_dependency_graph(operations)
    assert dropped_index in dependencies[dropped_column], (
        f"dropping the column does not wait for the index that covers it: "
        f"{dependencies}"
    )

    order = plan["execution_order"]
    assert order.index(dropped_index) < order.index(dropped_column), (
        f"the index is dropped after the column it covers: "
        f"{[operations[i]['op'] for i in order]}"
    )


def test_an_index_is_dropped_before_its_column_changes_type() -> None:
    """A type change is a rewrite, and an index over the old type blocks it."""
    operations = [
        {
            "op": "alter_column",
            "table": "users",
            "details": {
                "column": "email",
                "changes": {"type": {"from": "text", "to": "varchar(50)"}},
            },
        },
        {
            "op": "drop_index",
            "table": "users",
            "details": {"index": "ix_users_email", "definition": {"columns": ["email"]}},
        },
    ]

    assert build_dependency_graph(operations) == {0: [1], 1: []}
    assert _ordered(operations) == ["drop_index/users", "alter_column/users"]


def test_a_column_that_only_changes_its_default_does_not_wait_for_the_index() -> None:
    """The negative half of the rule above, and the reason it reads `type`.

    Setting a default rewrites no data and invalidates no index, so ordering it
    after the DROP INDEX would be a dependency the schema never asked for. A
    rule that fired on any alter_column would pass the previous test and fail
    this one.
    """
    operations = [
        {
            "op": "alter_column",
            "table": "users",
            "details": {"column": "email", "changes": {"default": {"from": None, "to": "x"}}},
        },
        {
            "op": "drop_index",
            "table": "users",
            "details": {"index": "ix_users_email", "definition": {"columns": ["email"]}},
        },
    ]

    assert build_dependency_graph(operations) == {0: [], 1: []}


def test_two_tables_that_reference_each_other_are_refused_by_name() -> None:
    """A real cycle, and the message has to say which tables are in it.

    A sort that cannot proceed is not an internal error; the operator has
    written a schema no single-pass migration can build. The message earns its
    place by naming both tables, so the reader knows where to break the loop
    without reading the schema back.
    """
    operations = [
        {
            "op": "create_table",
            "table": "a",
            "details": {
                "columns": {"b_id": {"type": "integer", "references": "b.id"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
        {
            "op": "create_table",
            "table": "b",
            "details": {
                "columns": {"a_id": {"type": "integer", "references": "a.id"}},
                "constraints": {},
                "indexes": {},
                "primary_key": [],
            },
        },
    ]

    with pytest.raises(CycleError) as raised:
        _ordered(operations)

    message = str(raised.value)
    assert "a" in message and "b" in message
    assert "create_table" in message
