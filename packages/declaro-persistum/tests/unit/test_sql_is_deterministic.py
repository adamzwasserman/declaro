"""The same query must render the same SQL. Every time, in any order.

`Condition._global_param_counter` and `CaseExpression._global_case_counter`
were process-global counters incremented in `__init__`, and parameter names
were drawn from them. Building the same query twice produced:

    first :  SELECT users.id FROM users WHERE users.age > :_p_1
    second:  SELECT users.id FROM users WHERE users.age > :_p_2

Three consequences. A database plan cache keyed on SQL text never hits,
because every query is textually novel. The counters grow without bound for
the life of the process. And SQL text cannot be compared across runs, which
is why no test asserted a full statement before this one.

Parameter names now come from the node's POSITION in the query being
rendered — a path supplied by the renderer — so they are unique within a
statement, which is the only place they must be unique, and identical
between two renderings of the same query.

The two counters were separate, so fixing either alone left the defect
reproducing through the other. Both arms are asserted here.
"""

import pytest

from declaro_persistum.query.table import case_, table

SCHEMA = {
    "users": {"columns": {"id": {"type": "int"}, "age": {"type": "int"},
                          "name": {"type": "text"}, "status": {"type": "text"}}},
    "roles": {"columns": {"user_id": {"type": "int"}, "name": {"type": "text"}}},
}


def _users():
    return table("users", schema=SCHEMA)


def _simple():
    u = _users()
    return u.select(u.id).where(u.age > 18)


def _with_case():
    u = _users()
    return u.select(u.id, case_((u.age > 18, "adult"), else_="minor").as_("band"))


def _compound():
    u = _users()
    return u.select(u.id).where((u.age > 18) & (u.status == "active"))


def _in_list():
    u = _users()
    return u.select(u.id).where(u.id.in_([1, 2, 3]))


def _between():
    u = _users()
    return u.select(u.id).where(u.age.between(18, 65))


BUILDERS = [
    ("simple", _simple),
    ("case", _with_case),
    ("compound", _compound),
    ("in_list", _in_list),
    ("between", _between),
]


@pytest.mark.parametrize("name,build", BUILDERS)
def test_the_same_query_renders_identically_twice(name, build):
    """The reported defect, one case per construct that names a parameter."""
    first = build().to_sql("postgresql")
    second = build().to_sql("postgresql")

    assert first == second, (
        f"{name}: the same query rendered two different statements\n"
        f"  first : {first}\n  second: {second}"
    )


@pytest.mark.parametrize("name,build", BUILDERS)
def test_rendering_is_stable_under_unrelated_construction(name, build):
    """Building other queries in between must not shift the names.

    A global counter fails this even when the two renderings are adjacent,
    because anything constructed between them advances it.
    """
    first = build().to_sql("postgresql")
    for _ in range(5):
        _compound()
        _with_case()
    second = build().to_sql("postgresql")

    assert first == second, f"{name}: unrelated construction changed the SQL"


def test_the_counters_are_gone():
    """The mechanism, not just the symptom.

    Asserted separately because a symptom test passes the moment names stop
    colliding by luck; this fails if either counter is reintroduced.
    """
    from declaro_persistum.query.table import CaseExpression, Condition

    assert not hasattr(Condition, "_global_param_counter"), (
        "Condition still carries a process-global parameter counter"
    )
    assert not hasattr(CaseExpression, "_global_case_counter"), (
        "CaseExpression still carries a process-global counter"
    )


