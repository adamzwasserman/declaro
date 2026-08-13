"""A WHERE clause is data. Nothing evaluates it into an AST, because it is one.

    where = {"age": {"gt": 18}, "status": "active"}

This replaces four classes and eight dunder methods:

    TableProxy.__getattr__          so `users.age` resolved to something
    ColumnProxy.__eq__ __ne__ __lt__ __le__ __gt__ __ge__
    Condition.__and__ __or__

All of which existed to borrow Python's expression grammar so the interpreter
would build the AST for us. A dict is already the AST, so none of it is
needed.

Two defects go with them. `ColumnProxy.__eq__` returned a `Condition` instead
of a bool, which is a lie about a Python operator — `col == col` was not True,
and `col in [a, b]` silently did the wrong thing because `in` uses `__eq__`.
And overriding `__eq__` without `__hash__` made `ColumnProxy` UNHASHABLE, so a
column could not go in a set or be a dict key.

The operator vocabulary is bounded and small, so every member is rendered here
rather than a representative sample. Parameter names come from the node's PATH
in the clause, never a counter, so the same condition renders byte-identical
SQL every time (declaro-8a3).
"""

from __future__ import annotations

import pytest

from declaro_persistum.query.conditions import OPERATORS, render

# Every operator, its condition, and the SQL fragment it must produce.
CASES = [
    ("eq",         {"status": "active"},                 "status = :"),
    ("eq_explicit",{"status": {"eq": "active"}},         "status = :"),
    ("ne",         {"status": {"ne": "active"}},         "status != :"),
    ("gt",         {"age": {"gt": 18}},                  "age > :"),
    ("gte",        {"age": {"gte": 18}},                 "age >= :"),
    ("lt",         {"age": {"lt": 65}},                  "age < :"),
    ("lte",        {"age": {"lte": 65}},                 "age <= :"),
    ("like",       {"name": {"like": "a%"}},             "name LIKE :"),
    ("startswith", {"name": {"startswith": "a"}},        "name LIKE :"),
    ("endswith",   {"name": {"endswith": "z"}},          "name LIKE :"),
    ("contains",   {"name": {"contains": "mid"}},        "name LIKE :"),
]


@pytest.mark.parametrize("name,condition,fragment", CASES, ids=[c[0] for c in CASES])
def test_each_operator_renders(name, condition, fragment):
    sql, params = render(condition, "postgresql", "w")
    assert fragment in sql, f"{name}: {sql}"
    assert len(params) == 1, params


def test_every_operator_in_the_table_has_a_case():
    """The table is the vocabulary; this asserts the tests cover all of it.

    Without this, adding an operator and forgetting its test leaves a member
    of a bounded set unexercised — the exact gap a bounded set makes
    avoidable.
    """
    covered = {"eq", "ne", "gt", "gte", "lt", "lte", "like", "startswith",
               "endswith", "contains", "in", "not_in", "between",
               "is_null", "is_not_null"}
    assert set(OPERATORS) == covered, (
        f"untested operators: {sorted(set(OPERATORS) - covered)}; "
        f"tested but absent: {sorted(covered - set(OPERATORS))}"
    )


class TestOperatorsThatAreNotOneParameter:
    """The members whose shape differs from `col OP :param`."""

    def test_in_expands_to_one_placeholder_per_value(self):
        sql, params = render({"id": {"in": [1, 2, 3]}}, "postgresql", "w")
        assert "id IN (" in sql
        assert len(params) == 3, params

    def test_not_in_expands_the_same_way(self):
        sql, params = render({"id": {"not_in": [1, 2]}}, "postgresql", "w")
        assert "id NOT IN (" in sql
        assert len(params) == 2, params

    def test_between_binds_two(self):
        sql, params = render({"age": {"between": [18, 65]}}, "postgresql", "w")
        assert "age BETWEEN" in sql
        assert sorted(params.values()) == [18, 65]

    def test_is_null_binds_nothing(self):
        sql, params = render({"deleted_at": {"is_null": True}}, "postgresql", "w")
        assert sql.strip() == "deleted_at IS NULL"
        assert params == {}

    def test_is_not_null_binds_nothing(self):
        sql, params = render({"deleted_at": {"is_not_null": True}}, "postgresql", "w")
        assert sql.strip() == "deleted_at IS NOT NULL"
        assert params == {}


class TestComposition:
    """`and` and `or` as data, replacing `&` and `|`."""

    def test_several_keys_are_anded(self):
        """The common case needs no combinator at all."""
        sql, params = render({"age": {"gt": 18}, "status": "active"}, "postgresql", "w")
        assert " AND " in sql
        assert len(params) == 2

    def test_explicit_or(self):
        sql, params = render(
            {"or": [{"status": "active"}, {"status": "pending"}]}, "postgresql", "w"
        )
        assert " OR " in sql
        assert len(params) == 2

    def test_nested_and_inside_or(self):
        """`(a & b) | (c & d)` — the shape that needed two dunders."""
        sql, params = render(
            {
                "or": [
                    {"and": [{"status": "active"}, {"age": {"gt": 18}}]},
                    {"and": [{"status": "pending"}, {"age": {"lt": 65}}]},
                ]
            },
            "postgresql",
            "w",
        )
        assert " OR " in sql and " AND " in sql
        assert len(params) == 4, params

    def test_an_empty_condition_renders_nothing(self):
        """The boundary. No WHERE clause is not an error."""
        assert render({}, "postgresql", "w") == ("", {})


class TestItIsDeterministic:
    """Same condition in, same SQL out — the declaro-8a3 property."""

    def test_the_same_condition_renders_identically_twice(self):
        c = {"age": {"gt": 18}, "status": "active"}
        assert render(c, "postgresql", "w") == render(c, "postgresql", "w")

    def test_two_conditions_on_one_column_do_not_collide(self):
        sql, params = render(
            {"and": [{"age": {"gt": 18}}, {"age": {"lt": 65}}]}, "postgresql", "w"
        )
        assert len(params) == 2, (sql, params)


def test_an_unknown_operator_fails_loudly():
    """A bounded vocabulary must not silently accept a member outside it."""
    with pytest.raises(ValueError, match="frobnicate"):
        render({"age": {"frobnicate": 1}}, "postgresql", "w")
