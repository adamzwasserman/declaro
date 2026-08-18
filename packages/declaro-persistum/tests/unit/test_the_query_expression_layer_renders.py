"""The aggregate, CASE and ORDER BY renderers, none of which any test ran.

MEASURED 2026-08-17. Of 22 public callables across `query/table.py` and
`query/expressions.py`, 21 were never called by any test in the suite. The
thirteen that are functions rather than types:

    avg_  case_  count_  is_subquery  max_  min_  now_  render_case
    render_function  render_order_term  render_subquery  subquery  sum_

`count_` is imported and called by all three shipped example apps, so it is on
the path a reader walks first.

A FILE NAMED `test_query_expressions.py` SAT BESIDE THEM, holding 30 tests and
listed as hollow. Restored, all 30 failed on `table(name, schema, pool=None)`:
the connection pool is abolished, and the tests describe a `TableProxy` API
with `.alias()`, `._full_name` and `._table_name` that no longer exists.
`table(name, schema)` returns `{"columns": {...}}` now.

So that file is not restored. Its subject splits three ways and this file
covers the first:

    CASE, aggregates, ORDER BY   alive and untested   -> tested here
    table aliasing, self-joins   REMOVED in the rewrite -> not reimplemented
    subqueries                   EXPORTED AND BROKEN    -> pinned below

EVERY EXPECTED VALUE HERE WAS TAKEN BY RUNNING THE FUNCTION, not written from
the docstrings. The docstrings are what described the removed API.
"""

from __future__ import annotations

import importlib
import sys

import pytest

importlib.import_module("declaro_persistum.query.table")
importlib.import_module("declaro_persistum.query.expressions")
TABLE = sys.modules["declaro_persistum.query.table"]
EXPR = sys.modules["declaro_persistum.query.expressions"]

pytestmark = pytest.mark.precommit

DIALECTS = ["postgresql", "sqlite", "turso"]


# ---------------------------------------------------------------------------
# Aggregates: data in, data out
# ---------------------------------------------------------------------------


def test_an_aggregate_is_data_not_a_string() -> None:
    """The whole point of the rewrite: a function call returns a value."""
    assert TABLE.count_("*", alias="n") == {
        "kind": "function",
        "name": "COUNT",
        "args": ["*"],
        "alias": "n",
    }


@pytest.mark.parametrize(
    "builder,name",
    [
        (lambda: TABLE.sum_("amount", alias="t"), "SUM"),
        (lambda: TABLE.avg_("amount", alias="t"), "AVG"),
        (lambda: TABLE.min_("amount", alias="t"), "MIN"),
        (lambda: TABLE.max_("amount", alias="t"), "MAX"),
    ],
    ids=["sum", "avg", "min", "max"],
)
def test_every_aggregate_names_its_own_sql_function(builder, name: str) -> None:
    """A copy-paste slip here gives you SUM where you asked for MAX."""
    fn = builder()

    assert fn["name"] == name, fn
    assert fn["args"] == ["amount"], fn


def test_now_takes_no_column() -> None:
    assert TABLE.now_(alias=None) == {
        "kind": "function",
        "name": "NOW",
        "args": [],
        "alias": None,
    }


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_aggregate_renders_with_its_alias(dialect: str) -> None:
    sql, params = TABLE.render_function(TABLE.count_("*", alias="n"), dialect, "p")

    assert sql == "COUNT(*) AS n"
    assert params == {}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_aggregate_without_an_alias_renders_bare(dialect: str) -> None:
    sql, _ = TABLE.render_function(TABLE.count_("*", alias=None), dialect, "p")

    assert sql == "COUNT(*)", sql


# ---------------------------------------------------------------------------
# CASE
# ---------------------------------------------------------------------------


def _band():
    return TABLE.case_(
        ({"status": "open"}, "hot"),
        ({"status": "closed"}, "cold"),
        else_="other",
        alias="band",
    )


def test_a_case_expression_is_data() -> None:
    # TUPLES, NOT LISTS. My first version of this expected lists, because I
    # read the shape off a `json.dumps` of it and JSON has no tuple. Deriving
    # a test input from a rendering of the value tests the rendering.
    assert _band() == {
        "kind": "case",
        "whens": [({"status": "open"}, "hot"), ({"status": "closed"}, "cold")],
        "else_": "other",
        "alias": "band",
    }


def test_a_case_renders_every_branch_in_order() -> None:
    sql, _ = TABLE.render_case(_band(), "p", with_alias=True)

    assert sql.startswith("CASE WHEN "), sql
    assert sql.count("WHEN") == 2, sql
    assert sql.index("_w0_") < sql.index("_w1_"), (
        f"the branches came out in the wrong order, which changes which one "
        f"wins: {sql}"
    )
    assert sql.endswith("END AS band"), sql


def test_a_case_binds_its_values_rather_than_inlining_them() -> None:
    """Inlined values are an injection, and `'` would end the literal."""
    sql, params = TABLE.render_case(_band(), "p", with_alias=True)

    assert "open" not in sql and "hot" not in sql, sql
    assert set(params.values()) == {"open", "hot", "closed", "cold", "other"}


def test_a_case_used_in_order_by_drops_its_alias() -> None:
    """`... END AS band ASC` is not valid SQL.

    `with_alias` is required rather than defaulted precisely because the two
    callers want opposite things.
    """
    sql, _ = TABLE.render_case(_band(), "p", with_alias=False)

    assert sql.endswith("END"), sql
    assert "AS band" not in sql, sql


def test_two_cases_in_one_query_do_not_collide() -> None:
    """The `path` argument exists to keep parameter names apart."""
    _, first = TABLE.render_case(_band(), "a", with_alias=True)
    _, second = TABLE.render_case(_band(), "b", with_alias=True)

    assert not set(first) & set(second), (
        f"two CASE expressions produced the same parameter names, so one "
        f"overwrites the other: {sorted(set(first) & set(second))}"
    )


def test_a_case_inside_an_aggregate_renders_bare() -> None:
    """COUNT(CASE ... END) — the alias would be a syntax error inside it."""
    sql, params = TABLE.render_function(
        TABLE.count_(_band(), alias="n"), "sqlite", "p"
    )

    assert sql.startswith("COUNT(CASE WHEN "), sql
    assert sql.endswith("END) AS n"), sql
    assert params, "the CASE parameters were dropped on the way out"


# ---------------------------------------------------------------------------
# ORDER BY, which dispatches on a tag
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_plain_order_term_renders(dialect: str) -> None:
    sql, params = EXPR.render_order_term(
        {"kind": "order_by", "column": "id", "direction": "DESC"}, dialect, "p"
    )

    assert "id" in sql and "DESC" in sql, sql
    assert params == {}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_case_order_term_renders_without_its_alias(dialect: str) -> None:
    sql, params = EXPR.render_order_term(
        {"kind": "case_order_by", "expr": _band(), "direction": "ASC"},
        dialect,
        "p",
    )

    assert "AS band" not in sql, sql
    assert sql.endswith("ASC"), sql
    assert params


def test_an_unknown_order_term_kind_fails_loudly() -> None:
    """The tag replaced a `hasattr` check that reclassified silently."""
    with pytest.raises(KeyError):
        EXPR.render_order_term(
            {"kind": "something_else", "column": "id", "direction": "ASC"},
            "sqlite",
            "p",
        )


def test_the_renderer_table_covers_both_declared_kinds() -> None:
    """A `Literal` with a member the table lacks fails only at runtime."""
    assert set(EXPR.ORDER_TERM_RENDERERS) == {"order_by", "case_order_by"}


# ---------------------------------------------------------------------------
# Subqueries: exported, documented, and broken
# ---------------------------------------------------------------------------


def test_the_subquery_tag_check_works() -> None:
    """`is_subquery` is pure and is the one part of this that is fine."""
    assert EXPR.is_subquery(TABLE.subquery("SELECT id FROM x")) is True
    assert EXPR.is_subquery({"kind": "case"}) is False
    assert EXPR.is_subquery("SELECT 1") is False
    assert EXPR.is_subquery(None) is False


def test_rendering_a_subquery_raises_today() -> None:
    """PINNED AS BROKEN, because it is, and the suite must say so.

        render_subquery(expr, dialect) -> expr["query"].to_sql(dialect)

    Nothing in this package has a `to_sql` method. The only declaration of one
    is the `RendersCondition` Protocol, whose body raises `CODE NOT WRITTEN`
    and which declares `to_sql(self, dialect, path)` -- a different arity from
    the call above, so even an implementation would not fit.

    `SubqueryExpr`'s own docstring shows

        subquery(roles.select(roles.user_id).where(roles.name == "admin"))

    which is the removed `TableProxy` API. And `conditions.py`, which renders
    every WHERE clause now, has no subquery handling at all, so
    `WHERE id IN (SELECT ...)` cannot be expressed by any route.

    So the feature is exported in `__all__`, documented, and raises
    AttributeError on every call. This test holds that fact still. It is a
    RECORD, not an endorsement: when the feature is restored or removed, this
    is what has to change, on purpose.
    """
    with pytest.raises(AttributeError):
        EXPR.render_subquery(TABLE.subquery("SELECT id FROM x"), "sqlite")


def test_an_aggregate_over_a_subquery_raises_for_the_same_reason() -> None:
    """The one route by which a caller reaches `render_subquery` at all."""
    with pytest.raises(AttributeError):
        TABLE.render_function(
            TABLE.count_(TABLE.subquery("SELECT id FROM x"), alias="n"),
            "sqlite",
            "p",
        )


def test_where_clauses_have_no_subquery_support_to_reach() -> None:
    """The half that makes the above a feature gap rather than a bug.

    If `conditions.py` grows subquery handling, this test fails and the two
    above become the real work rather than a record.
    """
    from declaro_persistum.query import conditions

    source = __import__("inspect").getsource(conditions)

    assert "subquery" not in source, (
        "conditions.py now mentions subqueries, so WHERE IN (SELECT ...) may "
        "be reachable and the two pinned failures above are no longer the "
        "whole story"
    )
