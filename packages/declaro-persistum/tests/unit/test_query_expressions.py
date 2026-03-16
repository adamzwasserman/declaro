"""
Unit tests for advanced query builder expressions.

Tests SQL generation for:
- CaseExpression (case_() factory)
- CaseOrderBy
- SubqueryExpr (subquery() factory)
- SQLFunction.to_sql_fragment() with CaseExpression args
- TableProxy.alias()
- Condition.to_sql() with SubqueryExpr
"""

import pytest

from declaro_persistum.query.table import (
    CaseExpression,
    CaseOrderBy,
    ColumnProxy,
    Condition,
    OrderBy,
    SQLFunction,
    SubqueryExpr,
    TableProxy,
    case_,
    count_,
    subquery,
    sum_,
    table,
)
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.types import Schema


# ---------------------------------------------------------------------------
# Minimal schema
# ---------------------------------------------------------------------------

_SCHEMA: Schema = {
    "tickets": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "severity": {"type": "text"},
            "status": {"type": "text"},
            "amount": {"type": "integer"},
            "user_id": {"type": "integer"},
        }
    },
    "users": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "email": {"type": "text"},
        }
    },
    "roles": {
        "columns": {
            "user_id": {"type": "integer"},
            "name": {"type": "text"},
        }
    },
    "comments": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "parent_id": {"type": "integer"},
            "body": {"type": "text"},
        }
    },
}


# ---------------------------------------------------------------------------
# Feature #6: TableProxy.alias()
# ---------------------------------------------------------------------------


class TestTableAlias:
    """Tests for TableProxy.alias() — required for self-joins."""

    def test_alias_returns_new_proxy(self):
        """.alias() returns a fresh TableProxy, not the same object."""
        t = table("comments", _SCHEMA, pool=None)
        aliased = t.alias("replies")
        assert aliased is not t

    def test_alias_columns_use_alias_as_table_prefix(self):
        """Column proxies on the aliased table use alias as table prefix."""
        t = table("comments", _SCHEMA, pool=None)
        replies = t.alias("replies")
        assert replies.id._full_name == "replies.id"
        assert replies.parent_id._full_name == "replies.parent_id"

    def test_original_columns_unchanged(self):
        """Original table columns still use original name as prefix."""
        t = table("comments", _SCHEMA, pool=None)
        _ = t.alias("replies")
        assert t.id._full_name == "comments.id"

    def test_table_name_property_includes_alias(self):
        """_table_name emits 'table AS alias' for JOIN/FROM clauses."""
        t = table("comments", _SCHEMA, pool=None)
        replies = t.alias("replies")
        assert replies._table_name == "comments AS replies"

    def test_unaliased_table_name_is_just_name(self):
        """_table_name without alias is just the table name."""
        t = table("comments", _SCHEMA, pool=None)
        assert t._table_name == "comments"

    def test_alias_join_generates_correct_sql(self):
        """Self-join using alias generates correct SQL."""
        comments = table("comments", _SCHEMA, pool=None)
        replies = comments.alias("replies")

        query = (
            comments.select(comments.id, count_(replies.id).as_("reply_count"))
            .join(replies, on=(replies.parent_id == comments.id), type="left")
            .group_by(comments.id)
        )
        sql, params = query.to_sql("postgresql")

        assert "LEFT JOIN comments AS replies ON" in sql
        assert "replies.parent_id = comments.id" in sql
        assert "GROUP BY comments.id" in sql
        assert params == {}


# ---------------------------------------------------------------------------
# Feature #1: CaseExpression
# ---------------------------------------------------------------------------


class TestCaseExpression:
    """Tests for case_() expression."""

    def test_case_basic_sql(self):
        """Basic CASE WHEN ... THEN ... END generates correct SQL."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_(
            (tickets.severity == "critical", 0),
            (tickets.severity == "high", 1),
            else_=2,
        )
        sql, params = expr.to_sql_fragment("postgresql")

        assert sql.startswith("CASE")
        assert "WHEN tickets.severity = " in sql
        assert "THEN :_case_" in sql
        assert "ELSE :_case_" in sql
        assert "END" in sql
        assert 0 in params.values()
        assert 1 in params.values()
        assert 2 in params.values()

    def test_case_with_alias(self):
        """case_(...).as_() includes AS alias in SQL."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_(
            (tickets.status == "open", 1),
            else_=0,
        ).as_("is_open")
        sql, params = expr.to_sql_fragment("postgresql")
        assert sql.endswith("AS is_open")
        assert "END AS is_open" in sql

    def test_case_bare_sql_no_alias(self):
        """_bare_sql_fragment() never includes alias."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_(
            (tickets.status == "open", 1),
            else_=0,
        ).as_("is_open")
        bare_sql, _ = expr._bare_sql_fragment("postgresql")
        assert "AS is_open" not in bare_sql
        assert bare_sql.startswith("CASE")
        assert bare_sql.endswith("END")

    def test_case_as_returns_new_expression(self):
        """as_() returns a new CaseExpression, not the same object."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.status == "open", 1), else_=0)
        aliased = expr.as_("label")
        assert aliased is not expr
        assert aliased._alias == "label"
        assert expr._alias is None

    def test_case_in_select_generates_correct_sql(self):
        """CaseExpression in SELECT emits CASE ... END AS alias."""
        tickets = table("tickets", _SCHEMA, pool=None)
        priority = case_(
            (tickets.severity == "critical", 0),
            else_=1,
        ).as_("priority")

        query = tickets.select(tickets.id, priority)
        sql, params = query.to_sql("postgresql")

        assert "CASE" in sql
        assert "AS priority" in sql
        assert "FROM tickets" in sql
        assert 0 in params.values()
        assert 1 in params.values()

    def test_case_full_name_includes_alias(self):
        """_full_name includes alias when set."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.status == "open", 1), else_=0).as_("is_open")
        assert "AS is_open" in expr._full_name

    def test_case_full_name_no_alias(self):
        """_full_name without alias is just CASE ... END."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.status == "open", 1), else_=0)
        assert expr._full_name.startswith("CASE")
        assert "AS " not in expr._full_name

    def test_case_else_none_omitted(self):
        """No ELSE clause when else_=None (default)."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.status == "open", 1))
        sql, _ = expr.to_sql_fragment("postgresql")
        assert "ELSE" not in sql

    def test_case_column_then_value(self):
        """THEN value that is a ColumnProxy emits column name, not param."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_(
            (tickets.status == "open", tickets.amount),
            else_=0,
        )
        sql, params = expr.to_sql_fragment("postgresql")
        assert "THEN tickets.amount" in sql
        # amount itself should not be in params
        assert "tickets.amount" not in str(params)


# ---------------------------------------------------------------------------
# Feature #1: CaseOrderBy
# ---------------------------------------------------------------------------


class TestCaseOrderBy:
    """Tests for CaseExpression.asc() / .desc()."""

    def test_case_asc(self):
        """case_(...).asc() generates CASE ... END ASC."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.severity == "critical", 0), else_=1)
        order = expr.asc()
        assert isinstance(order, CaseOrderBy)
        sql, params = order.to_sql_fragment("postgresql")
        assert sql.endswith("ASC")
        assert "CASE" in sql

    def test_case_desc(self):
        """case_(...).desc() generates CASE ... END DESC."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.severity == "critical", 0), else_=1)
        order = expr.desc()
        sql, params = order.to_sql_fragment("postgresql")
        assert sql.endswith("DESC")

    def test_case_order_bare_no_alias(self):
        """CaseOrderBy emits bare expression even when CASE has alias."""
        tickets = table("tickets", _SCHEMA, pool=None)
        expr = case_((tickets.severity == "critical", 0), else_=1).as_("priority")
        order = expr.asc()
        sql, _ = order.to_sql_fragment("postgresql")
        assert "AS priority" not in sql
        assert sql.endswith("ASC")

    def test_case_order_in_select_query(self):
        """CaseOrderBy works in SelectQuery.order_by()."""
        tickets = table("tickets", _SCHEMA, pool=None)
        priority = case_((tickets.severity == "critical", 0), else_=1).as_("priority")

        query = tickets.select(tickets.id, priority).order_by(priority.asc())
        sql, params = query.to_sql("postgresql")

        assert "ORDER BY" in sql
        assert "CASE" in sql
        assert "ASC" in sql
        # params from SELECT and ORDER BY are both collected
        assert 0 in params.values()
        assert 1 in params.values()


# ---------------------------------------------------------------------------
# Feature #4: sum_(case_(...))
# ---------------------------------------------------------------------------


class TestSumCase:
    """Tests for sum_(case_(...)) composition."""

    def test_sum_of_case_expression(self):
        """sum_(case_(...)).as_() generates SUM(CASE ... END) AS alias."""
        tickets = table("tickets", _SCHEMA, pool=None)
        total = sum_(
            case_(
                (tickets.status == "paid", tickets.amount),
                else_=0,
            )
        ).as_("paid_total")

        sql, params = total.to_sql_fragment("postgresql")
        assert sql.startswith("SUM(CASE")
        assert "END) AS paid_total" in sql
        # alias from CASE is not included (bare form used inside aggregate)
        assert "SUM(CASE" in sql
        assert 0 in params.values()

    def test_sum_case_in_select(self):
        """sum_(case_(...)) in SELECT emits params correctly."""
        tickets = table("tickets", _SCHEMA, pool=None)
        paid_total = sum_(
            case_(
                (tickets.status == "paid", tickets.amount),
                else_=0,
            )
        ).as_("paid_total")

        query = tickets.select(tickets.id, paid_total)
        sql, params = query.to_sql("postgresql")

        assert "SUM(CASE" in sql
        assert "AS paid_total" in sql
        assert 0 in params.values()

    def test_sum_of_aliased_case_no_double_alias(self):
        """sum_(case_(...).as_('x')) — the inner alias is stripped."""
        tickets = table("tickets", _SCHEMA, pool=None)
        inner = case_((tickets.status == "paid", tickets.amount), else_=0).as_("inner_alias")
        total = sum_(inner).as_("paid_total")

        sql, _ = total.to_sql_fragment("postgresql")
        # 'inner_alias' must NOT appear inside SUM(...)
        assert "inner_alias" not in sql
        assert "SUM(CASE" in sql
        assert "AS paid_total" in sql


# ---------------------------------------------------------------------------
# Feature #5: subquery() in .in_()
# ---------------------------------------------------------------------------


class TestSubquery:
    """Tests for subquery() in IN / NOT IN."""

    def test_subquery_in_generates_correct_sql(self):
        """users.id.in_(subquery(...)) generates IN (SELECT ...) SQL."""
        users = table("users", _SCHEMA, pool=None)
        roles = table("roles", _SCHEMA, pool=None)

        admin_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "admin")
        )

        query = users.select(users.id, users.email).where(users.id.in_(admin_ids))
        sql, params = query.to_sql("postgresql")

        assert "IN (SELECT" in sql
        assert "FROM roles" in sql
        # The "admin" param should be present
        assert "admin" in params.values()

    def test_subquery_not_in(self):
        """users.id.not_in_(subquery(...)) generates NOT IN (SELECT ...) SQL."""
        users = table("users", _SCHEMA, pool=None)
        roles = table("roles", _SCHEMA, pool=None)

        banned_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "banned")
        )

        query = users.select(users.id).where(users.id.not_in_(banned_ids))
        sql, params = query.to_sql("postgresql")

        assert "NOT IN (SELECT" in sql
        assert "FROM roles" in sql
        assert "banned" in params.values()

    def test_subquery_expr_type(self):
        """subquery() returns a SubqueryExpr instance."""
        users = table("users", _SCHEMA, pool=None)
        result = subquery(users.select(users.id))
        assert isinstance(result, SubqueryExpr)

    def test_subquery_to_sql_fragment(self):
        """SubqueryExpr.to_sql_fragment() returns inner query SQL and params."""
        users = table("users", _SCHEMA, pool=None)
        inner = users.select(users.id).where(users.email == "alice@example.com")
        sq = SubqueryExpr(inner)
        sql, params = sq.to_sql_fragment("postgresql")

        assert "SELECT" in sql
        assert "FROM users" in sql
        assert "alice@example.com" in params.values()


# ---------------------------------------------------------------------------
# Feature #3: Compound OR in JOIN WHERE (verify already works)
# ---------------------------------------------------------------------------


class TestCompoundOrJoin:
    """Compound OR conditions in WHERE work correctly."""

    def test_compound_or_in_where(self):
        """(a == x) | (b == y) generates (col = :p) OR (col = :p) SQL."""
        tickets = table("tickets", _SCHEMA, pool=None)

        condition = (tickets.status == "open") | (tickets.status == "pending")
        query = tickets.select(tickets.id).where(condition)
        sql, params = query.to_sql("postgresql")

        assert "OR" in sql
        assert "open" in params.values()
        assert "pending" in params.values()

    def test_column_is_not_null_in_where(self):
        """is_not_null() generates IS NOT NULL."""
        tickets = table("tickets", _SCHEMA, pool=None)
        query = tickets.select(tickets.id).where(tickets.severity.is_not_null())
        sql, _ = query.to_sql("postgresql")
        assert "IS NOT NULL" in sql

    def test_compound_in_join_on(self):
        """Compound OR condition can be used in JOIN ON clause."""
        tickets = table("tickets", _SCHEMA, pool=None)
        users = table("users", _SCHEMA, pool=None)

        condition = (tickets.user_id == users.id) & tickets.status.is_not_null()
        query = (
            tickets.select(tickets.id, users.email)
            .join(users, on=condition, type="left")
        )
        sql, _ = query.to_sql("postgresql")

        assert "LEFT JOIN users ON" in sql
        assert "IS NOT NULL" in sql


# ---------------------------------------------------------------------------
# Feature #2: Self-JOIN with GROUP BY + aggregate
# ---------------------------------------------------------------------------


class TestSelfJoin:
    """Self-join using TableProxy.alias() works end-to-end."""

    def test_self_join_sql(self):
        """Self-join generates correct SQL with alias."""
        comments = table("comments", _SCHEMA, pool=None)
        replies = comments.alias("replies")

        query = (
            comments.select(
                comments.id,
                count_(replies.id).as_("reply_count"),
            )
            .join(replies, on=(replies.parent_id == comments.id), type="left")
            .group_by(comments.id)
        )
        sql, params = query.to_sql("postgresql")

        assert "SELECT comments.id" in sql
        assert "COUNT(replies.id) AS reply_count" in sql
        assert "LEFT JOIN comments AS replies ON" in sql
        assert "replies.parent_id = comments.id" in sql
        assert "GROUP BY comments.id" in sql
        assert params == {}
