"""Dropping a column is a sequence, and the thing that drops it writes it.

WHAT THIS REPLACES. The differ used to emit one `drop_column` and leave the
sort to discover, by scanning every other operation, that an index over that
column had to go first. Three separate mechanisms then argued about the
result: a table of per-kind priorities, a dependency edge the sort inferred,
and nothing else. The edge was broken for as long as it existed, because it
read a key off `drop_index` that `drop_index` never carried, and the priority
table was quietly carrying the order instead. Nothing could say which.

`dropping_a_column` writes the whole sequence. The order is constructed rather
than discovered, so these tests ask the differ what it emits and never consult
the sort.
"""

from __future__ import annotations

import pytest

from declaro_persistum.differ.core import diff, dropping_a_column
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit]

INDEXED = {
    "users": {
        "columns": {"id": {"type": "integer"}, "email": {"type": "text"}},
        "indexes": {"ix_email": {"columns": ["email"]}},
    }
}
WITHOUT_EMAIL = {"users": {"columns": {"id": {"type": "integer"}}}}


def test_the_sequence_puts_the_index_before_the_column() -> None:
    """Written in order by the function that knows what dropping entails."""
    operations = dropping_a_column("users", "email", INDEXED["users"])

    assert [op["op"] for op in operations] == ["drop_index", "drop_column"]


def test_the_sequence_puts_a_key_before_the_column() -> None:
    """A key on the column goes first, for the same reason an index does."""
    table = {
        "columns": {
            "id": {"type": "integer"},
            "uid": {"type": "integer", "references": "users.id"},
        }
    }
    operations = dropping_a_column("posts", "uid", table)

    assert [op["op"] for op in operations] == ["drop_foreign_key", "drop_column"]


def test_a_column_with_neither_is_one_statement() -> None:
    """Nothing invented for a column that carries no index and no key."""
    operations = dropping_a_column("users", "id", WITHOUT_EMAIL["users"])

    assert [op["op"] for op in operations] == ["drop_column"]


def test_an_index_over_another_column_is_left_alone() -> None:
    """Only the indexes naming this column, not every index on the table."""
    table = {
        "columns": {"id": {"type": "integer"}, "email": {"type": "text"}},
        "indexes": {"ix_id": {"columns": ["id"]}},
    }
    operations = dropping_a_column("users", "email", table)

    assert [op["op"] for op in operations] == ["drop_column"]


def test_the_differ_writes_the_sequence_it_needs() -> None:
    """The plan, end to end, with the index also gone from the declaration."""
    plan = diff(INDEXED, WITHOUT_EMAIL, dialect="sqlite", decisions=NOTHING)

    assert [op["op"] for op in plan["operations"]] == ["drop_index", "drop_column"]


def test_the_index_is_dropped_once_not_twice() -> None:
    """Two drops of one index is a migration that dies on the second.

    The column drop writes the index drop it requires, and the index diff sees
    the same index missing from the target. Without a word between them both
    write a `drop_index` and the engine refuses the second as missing.
    """
    plan = diff(INDEXED, WITHOUT_EMAIL, dialect="sqlite", decisions=NOTHING)

    drops = [op for op in plan["operations"] if op["op"] == "drop_index"]
    assert len(drops) == 1, drops


def test_the_declaration_keeping_the_index_changes_nothing() -> None:
    """An index the author still declares over a column they removed.

    The database has an index pointing at a column that is going, and that is
    true whatever the declaration says. The sequence is read off the current
    table for exactly this reason.

    On the supported path the validator refuses this schema before the diff
    runs, because an index naming a column the table does not declare is a
    contradiction. This asks what the differ does when handed one anyway.
    """
    still_declared = {
        "users": {
            "columns": {"id": {"type": "integer"}},
            "indexes": {"ix_email": {"columns": ["email"]}},
        }
    }
    plan = diff(INDEXED, still_declared, dialect="sqlite", decisions=NOTHING)

    assert [op["op"] for op in plan["operations"]] == ["drop_index", "drop_column"]
