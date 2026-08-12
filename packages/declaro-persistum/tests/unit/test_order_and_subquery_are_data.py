"""ORDER BY terms and subquery expressions are data.

Strangler slices two, three and four on `query/table.py`, following
`JoinClause`. All three classes are PRODUCED by the fluent API — `col.asc()`,
`case_(...).desc()`, `subquery(q)` — and never constructed by name, so the
consumer API does not change when they become TypedDicts.

Each carries a `kind` tag, which is the point of the slice rather than
decoration. The rendering code dispatched on SHAPE:

    if hasattr(o, "to_sql_fragment"):     # select.py — CaseOrderBy
        ...
    else:                                 # OrderBy
        ...

    if isinstance(self.value, SubqueryExpr):   # table.py — Condition

`hasattr` dispatch silently reclassifies anything that grows or loses a
method, and `isinstance` cannot be used on a TypedDict at all. Both become a
dict lookup on `kind` (Rule 1), which fails loudly on an unknown tag instead
of falling through to the other branch.

The ORDER BY term vocabulary is bounded — a plain column term or a CASE term
— so both members are rendered here rather than one.
"""

from typing import get_type_hints

import pytest

from declaro_persistum.query.table import (
    CaseOrderBy,
    OrderBy,
    SubqueryExpr,
    case_,
    subquery,
    table,
)

SCHEMA = {
    "users": {"columns": {"id": {"type": "int"}, "name": {"type": "text"},
                          "age": {"type": "int"}}},
    "roles": {"columns": {"user_id": {"type": "int"}, "name": {"type": "text"}}},
}

SHAPES = [
    (OrderBy, {"kind", "column", "direction"}),
    (CaseOrderBy, {"kind", "expr", "direction"}),
    (SubqueryExpr, {"kind", "query"}),
]


@pytest.mark.parametrize("cls,fields", SHAPES)
def test_it_is_a_typed_dict_with_a_kind_tag(cls, fields):
    assert issubclass(cls, dict), f"{cls.__name__} is still a class"
    assert set(get_type_hints(cls)) == fields


def _users():
    return table("users", schema=SCHEMA)


class TestOrderTermsRenderByTag:
    """Both members of the bounded ORDER BY vocabulary, not a sample."""

    @pytest.mark.parametrize("direction", ["ASC", "DESC"])
    def test_a_column_term_renders(self, direction):
        users = _users()
        term = users.age.asc() if direction == "ASC" else users.age.desc()

        assert type(term) is dict, f"a term is a {type(term).__name__}"
        assert term["kind"] == "order_by"

        sql, _ = users.select(users.id).order_by(term).to_sql("postgresql")
        assert f"ORDER BY users.age {direction}" in sql, sql

    @pytest.mark.parametrize("direction", ["ASC", "DESC"])
    def test_a_case_term_renders(self, direction):
        users = _users()
        expr = case_((users.age > 18, 0), else_=1)
        term = expr.asc() if direction == "ASC" else expr.desc()

        assert type(term) is dict, f"a term is a {type(term).__name__}"
        assert term["kind"] == "case_order_by"

        sql, params = users.select(users.id).order_by(term).to_sql("postgresql")
        assert "ORDER BY CASE WHEN" in sql, sql
        assert sql.rstrip().endswith(direction), sql
        assert params, "the CASE term dropped its parameters"

    def test_both_kinds_in_one_query(self):
        """The dispatch must handle a mixed list, which is what broke before."""
        users = _users()
        expr = case_((users.age > 18, 0), else_=1)

        sql, _ = (
            users.select(users.id)
            .order_by(expr.desc(), users.name.asc())
            .to_sql("postgresql")
        )

        assert "CASE WHEN" in sql and "users.name ASC" in sql, sql

    def test_an_unknown_order_kind_raises_rather_than_falling_through(self):
        """A dict lookup fails loudly; the old hasattr chain picked a branch."""
        users = _users()
        with pytest.raises(KeyError):
            users.select(users.id).order_by({"kind": "nonsense"}).to_sql("postgresql")


class TestSubqueryRendersByTag:
    def test_subquery_is_data(self):
        roles = table("roles", schema=SCHEMA)
        expr = subquery(roles.select(roles.user_id))

        assert type(expr) is dict
        assert expr["kind"] == "subquery"

    @pytest.mark.parametrize("op,frag", [("in_", "IN"), ("not_in_", "NOT IN")])
    def test_a_subquery_renders_on_both_operators(self, op, frag):
        users, roles = _users(), table("roles", schema=SCHEMA)
        expr = subquery(roles.select(roles.user_id).where(roles.name == "admin"))

        cond = getattr(users.id, op)(expr)
        sql, params = users.select(users.id).where(cond).to_sql("postgresql")

        assert f"users.id {frag} (SELECT roles.user_id FROM roles" in sql, sql
        assert params, "the subquery dropped its parameters"

    def test_a_plain_list_still_renders_as_placeholders(self):
        """The tag check must not swallow the ordinary IN case."""
        users = _users()
        sql, params = (
            users.select(users.id).where(users.id.in_([1, 2, 3])).to_sql("postgresql")
        )
        assert "users.id IN (" in sql and len(params) == 3, (sql, params)
