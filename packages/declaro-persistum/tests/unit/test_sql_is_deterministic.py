"""The same query renders the same SQL. Every time, in any order.

`Condition._global_param_counter` and `CaseExpression._global_case_counter`
were process-global integers incremented in `__init__`, and parameter names
came from them. Building the same query twice produced:

    first :  SELECT users.id FROM users WHERE users.age > :_p_1
    second:  SELECT users.id FROM users WHERE users.age > :_p_2

A database plan cache keyed on SQL text therefore never hit, the counters grew
without bound, and no test asserted a whole statement because no whole
statement was stable enough to assert.

BOTH COUNTERS ARE GONE WITH THE CLASSES THAT HELD THEM. A condition is a dict
now, and parameter names come from the node's POSITION in the clause. That is
not a fix layered on the old design — it is a property the new one cannot fail
to have, because there is no per-object identity left to draw a name from.

This file was rewritten from the fluent API (`u.select(u.id).where(...)`) to
the data API. The property under test is unchanged; only the door is.
"""

from __future__ import annotations

import pytest

from declaro_persistum import delete, select, update

BUILDERS = {
    "simple": lambda: select("id", from_table="users", where={"age": {"gt": 18}}),
    "compound": lambda: select(
        "id", from_table="users", where={"age": {"gt": 18}, "status": "active"}
    ),
    "in_list": lambda: select("id", from_table="users", where={"id": {"in": [1, 2, 3]}}),
    "between": lambda: select(
        "id", from_table="users", where={"age": {"between": [18, 65]}}
    ),
    "or_group": lambda: select(
        "id",
        from_table="users",
        where={"or": [{"status": "active"}, {"status": "pending"}]},
    ),
    "nested": lambda: select(
        "id",
        from_table="users",
        where={
            "or": [
                {"and": [{"status": "active"}, {"age": {"gt": 18}}]},
                {"and": [{"status": "pending"}, {"age": {"lt": 65}}]},
            ]
        },
    ),
    "update": lambda: update(
        "users", {"status": "x"}, where={"id": {"eq": 1}}
    ),
    "delete": lambda: delete(from_table="users", where={"id": {"eq": 1}}),
}


@pytest.mark.parametrize("name", sorted(BUILDERS))
def test_the_same_query_renders_identically_twice(name):
    """The reported defect, one case per construct that names a parameter."""
    build = BUILDERS[name]
    first, second = build(), build()
    assert first == second, f"{name}:\n  {first}\n  {second}"


@pytest.mark.parametrize("name", sorted(BUILDERS))
def test_rendering_is_stable_under_unrelated_construction(name):
    """Building other queries in between must not shift the names.

    A global counter fails this even when the two renderings are adjacent,
    because anything constructed between them advances it.
    """
    build = BUILDERS[name]
    first = build()
    for other in BUILDERS.values():
        other()
    assert first == build(), f"{name}: unrelated construction changed the SQL"


def test_the_counters_cannot_come_back():
    """The mechanism, not the symptom.

    A symptom test passes the moment names stop colliding by luck. This fails
    if anything in the query layer starts carrying process-global state.
    """
    import declaro_persistum.query.conditions as conditions
    import declaro_persistum.query.table as table_mod

    for module in (conditions, table_mod):
        counters = [
            n
            for n, v in vars(module).items()
            if n.startswith("_global") or (n.endswith("counter") and isinstance(v, int))
        ]
        assert counters == [], f"{module.__name__} carries {counters}"


class TestNamesStayUniqueWithinOneStatement:
    """Uniqueness within a statement is the only property the names need."""

    def test_two_conditions_on_one_column_do_not_collide(self):
        q = select(
            "id",
            from_table="users",
            where={"and": [{"age": {"gt": 18}}, {"age": {"lt": 65}}]},
        )
        assert len(q["params"]) == 2, q
        assert sorted(q["params"].values()) == [18, 65]

    def test_where_and_having_do_not_collide(self):
        """Two independent roots in one statement — the collision risk."""
        q = select(
            "status",
            from_table="users",
            where={"age": {"gt": 18}},
            group_by=["status"],
            having={"age": {"lt": 65}},
        )
        assert len(q["params"]) == 2, q
        assert sorted(q["params"].values()) == [18, 65]

    def test_a_caller_parameter_survives_a_dict_where(self):
        """A string WHERE brings the caller's params; a dict brings its own.

        They merge, and the caller's win — a dict-built name is derived from
        the path and cannot collide with one a caller chose, so an overlap
        means the caller meant it.
        """
        q = select(
            "id", from_table="users", where="tenant = :t", params={"t": "acme"}
        )
        assert q["params"] == {"t": "acme"}
