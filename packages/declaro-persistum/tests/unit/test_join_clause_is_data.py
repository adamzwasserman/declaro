"""A JOIN clause is data. It has no behaviour to justify a class.

First slice of the strangler on `query/table.py`. `JoinClause` was a class
whose entire body was `__init__` assigning three fields — Rule 2 (data is
just data) and Rule 11 (config on self) in four lines. It was chosen as the
first slice because it is the smallest: one L1.18 offender, one method, and
five references outside its own module.

The shape was already admitted elsewhere in the codebase. `query/builder.py`
renders joins from PLAIN DICTS — `join["table"]`, `join.get("type")` — so two
representations of one concept existed side by side, and only one of them
needed a class.

`join_type: str = "inner"` was also an implicit default (Rule 14): a caller
who omitted it could not be distinguished from one who chose "inner". The
TypedDict has no defaults, and `SelectQuery.join` passes the value
explicitly, so the omission is now impossible rather than silent.

The vocabulary of join types is bounded and small, so every member is
rendered here rather than a sample of them.
"""

from typing import get_type_hints

import pytest

from declaro_persistum.query.table import JoinClause, table

JOIN_TYPES = ["inner", "left", "right", "full"]

SCHEMA = {
    "users": {"columns": {"id": {"type": "int"}, "name": {"type": "text"}}},
    "orders": {"columns": {"id": {"type": "int"}, "user_id": {"type": "int"}}},
}


def test_join_clause_is_a_typed_dict():
    """Not a class with a constructor — a declared shape."""
    assert issubclass(JoinClause, dict), (
        "JoinClause is still a class; a join clause carries no behaviour"
    )
    assert set(get_type_hints(JoinClause)) == {"table", "on", "type"}


def test_a_join_clause_is_an_ordinary_dict_at_runtime():
    users = table("users", schema=SCHEMA)
    orders = table("orders", schema=SCHEMA)

    q = users.select(users.id).join(orders, on=users.id == orders.user_id)
    clause = q._joins[0]

    assert type(clause) is dict, f"a join clause is a {type(clause).__name__}"
    assert clause["table"] == "orders"
    assert clause["type"] == "inner"


@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_every_join_type_renders(join_type):
    """The vocabulary is bounded, so every member runs — not one sample."""
    users = table("users", schema=SCHEMA)
    orders = table("orders", schema=SCHEMA)

    sql, _params = (
        users.select(users.id)
        .join(orders, on=users.id == orders.user_id, type=join_type)
        .to_sql("postgresql")
    )

    assert f"{join_type.upper()} JOIN orders ON" in sql, sql


def test_the_join_type_is_never_defaulted_into_the_data():
    """Rule 14: absence must be impossible, not silently filled in.

    A TypedDict has no default values, so a clause missing `type` is a
    construction error rather than a clause that quietly means "inner".
    """
    with pytest.raises(KeyError):
        JoinClause(table="orders", on=None)["type"]


def test_two_joins_both_survive_to_the_sql():
    users = table("users", schema=SCHEMA)
    orders = table("orders", schema=SCHEMA)

    sql, _ = (
        users.select(users.id)
        .join(orders, on=users.id == orders.user_id, type="left")
        .join(orders, on=users.id == orders.id, type="inner")
        .to_sql("postgresql")
    )

    assert sql.count("JOIN orders ON") == 2, sql
