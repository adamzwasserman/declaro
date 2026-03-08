"""
Unit tests for the schema-validated query builder.

Tests the new dot-notation API: table(), TableProxy, ColumnProxy,
SelectQuery, InsertQuery, UpdateQuery, DeleteQuery.
"""

import pytest
from typing import Any

from declaro_persistum.types import Schema
from declaro_persistum.query import (
    table,
    TableProxy,
    ColumnProxy,
    Condition,
    ConditionGroup,
    OrderBy,
    count_,
    sum_,
    now_,
)
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.insert import InsertQuery
from declaro_persistum.query.update import UpdateQuery
from declaro_persistum.query.delete import DeleteQuery


# Test fixtures


@pytest.fixture
def users_schema() -> Schema:
    """Schema with users table."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "name": {"type": "text"},
                "status": {"type": "text", "default": "'active'"},
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            }
        }
    }


@pytest.fixture
def orders_schema() -> Schema:
    """Schema with users and orders tables."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True},
                "email": {"type": "text", "nullable": False},
            }
        },
        "orders": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True},
                "user_id": {"type": "uuid", "references": "users.id"},
                "total": {"type": "numeric(10,2)"},
                "status": {"type": "text", "default": "'pending'"},
            }
        },
    }


# TableProxy tests


class TestTableProxy:
    """Tests for TableProxy class."""

    def test_create_table_proxy(self, users_schema: Schema):
        """TableProxy can be created from schema."""
        users = table("users", schema=users_schema)
        assert isinstance(users, TableProxy)
        assert users._table_name == "users"

    def test_table_not_in_schema(self, users_schema: Schema):
        """table() raises ValueError for unknown table."""
        with pytest.raises(ValueError, match="Table 'unknown' not found"):
            table("unknown", schema=users_schema)

    def test_column_access_valid(self, users_schema: Schema):
        """TableProxy allows access to valid columns."""
        users = table("users", schema=users_schema)
        email_col = users.email
        assert isinstance(email_col, ColumnProxy)
        assert email_col._col_name == "email"
        assert email_col._full_name == "users.email"

    def test_column_access_invalid(self, users_schema: Schema):
        """TableProxy raises AttributeError for invalid columns."""
        users = table("users", schema=users_schema)
        with pytest.raises(AttributeError, match="has no column 'invalid'"):
            _ = users.invalid



# ColumnProxy tests


class TestColumnProxy:
    """Tests for ColumnProxy comparison operators."""

    def test_equality(self, users_schema: Schema):
        """Column == value creates Condition."""
        users = table("users", schema=users_schema)
        cond = users.status == "active"
        assert isinstance(cond, Condition)
        assert cond.column == "users.status"
        assert cond.operator == "="
        assert cond.value == "active"

    def test_inequality(self, users_schema: Schema):
        """Column != value creates Condition."""
        users = table("users", schema=users_schema)
        cond = users.status != "deleted"
        assert cond.operator == "!="
        assert cond.value == "deleted"

    def test_less_than(self, users_schema: Schema):
        """Column < value creates Condition."""
        users = table("users", schema=users_schema)
        cond = users.id < 10
        assert cond.operator == "<"
        assert cond.value == 10

    def test_greater_than(self, users_schema: Schema):
        """Column > value creates Condition."""
        users = table("users", schema=users_schema)
        cond = users.id > 5
        assert cond.operator == ">"
        assert cond.value == 5

    def test_like(self, users_schema: Schema):
        """Column.like() creates LIKE condition."""
        users = table("users", schema=users_schema)
        cond = users.email.like("%@example.com")
        assert cond.operator == "LIKE"
        assert cond.value == "%@example.com"

    def test_ilike(self, users_schema: Schema):
        """Column.ilike() creates ILIKE condition."""
        users = table("users", schema=users_schema)
        cond = users.email.ilike("%alice%")
        assert cond.operator == "ILIKE"
        assert cond.value == "%alice%"

    def test_in_(self, users_schema: Schema):
        """Column.in_() creates IN condition."""
        users = table("users", schema=users_schema)
        cond = users.status.in_(["active", "pending"])
        assert cond.operator == "IN"
        assert cond.value == ["active", "pending"]

    def test_is_null(self, users_schema: Schema):
        """Column.is_null() creates IS NULL condition."""
        users = table("users", schema=users_schema)
        cond = users.name.is_null()
        assert cond.operator == "IS"
        assert cond.value is None

    def test_is_not_null(self, users_schema: Schema):
        """Column.is_not_null() creates IS NOT NULL condition."""
        users = table("users", schema=users_schema)
        cond = users.name.is_not_null()
        assert cond.operator == "IS NOT"
        assert cond.value is None

    def test_between(self, users_schema: Schema):
        """Column.between() creates BETWEEN condition."""
        users = table("users", schema=users_schema)
        cond = users.id.between(1, 100)
        assert cond.operator == "BETWEEN"
        assert cond.value == (1, 100)

    def test_desc(self, users_schema: Schema):
        """Column.desc() creates DESC OrderBy."""
        users = table("users", schema=users_schema)
        order = users.created_at.desc()
        assert isinstance(order, OrderBy)
        assert order.direction == "DESC"

    def test_asc(self, users_schema: Schema):
        """Column.asc() creates ASC OrderBy."""
        users = table("users", schema=users_schema)
        order = users.email.asc()
        assert order.direction == "ASC"


# Condition tests


class TestCondition:
    """Tests for Condition SQL generation."""

    def test_simple_condition_to_sql(self, users_schema: Schema):
        """Simple condition generates correct SQL."""
        users = table("users", schema=users_schema)
        cond = users.status == "active"
        sql, params = cond.to_sql("postgresql")
        assert "users.status =" in sql
        assert len(params) == 1

    def test_null_condition_to_sql(self, users_schema: Schema):
        """IS NULL condition generates correct SQL."""
        users = table("users", schema=users_schema)
        cond = users.name.is_null()
        sql, params = cond.to_sql("postgresql")
        assert sql == "users.name IS NULL"
        assert params == {}

    def test_in_condition_to_sql(self, users_schema: Schema):
        """IN condition generates correct SQL."""
        users = table("users", schema=users_schema)
        cond = users.status.in_(["a", "b"])
        sql, params = cond.to_sql("postgresql")
        assert "users.status IN" in sql
        assert len(params) == 2

    def test_between_condition_to_sql(self, users_schema: Schema):
        """BETWEEN condition generates correct SQL."""
        users = table("users", schema=users_schema)
        cond = users.id.between(1, 10)
        sql, params = cond.to_sql("postgresql")
        assert "BETWEEN" in sql
        assert len(params) == 2

    def test_ilike_postgresql(self, users_schema: Schema):
        """ILIKE works on PostgreSQL."""
        users = table("users", schema=users_schema)
        cond = users.email.ilike("%test%")
        sql, params = cond.to_sql("postgresql")
        assert "ILIKE" in sql

    def test_ilike_sqlite_fallback(self, users_schema: Schema):
        """ILIKE falls back to LOWER() LIKE on SQLite."""
        users = table("users", schema=users_schema)
        cond = users.email.ilike("%test%")
        sql, params = cond.to_sql("sqlite")
        assert "LOWER" in sql
        assert "LIKE" in sql

    def test_param_reference(self, users_schema: Schema):
        """Parameter reference (:name) is kept as-is."""
        users = table("users", schema=users_schema)
        cond = users.id == ":user_id"
        sql, params = cond.to_sql("postgresql")
        assert ":user_id" in sql
        assert params == {}


# ConditionGroup tests


class TestConditionGroup:
    """Tests for combining conditions with AND/OR."""

    def test_and_conditions(self, users_schema: Schema):
        """Conditions can be combined with &."""
        users = table("users", schema=users_schema)
        cond = (users.status == "active") & (users.email.is_not_null())
        assert isinstance(cond, ConditionGroup)
        assert cond.operator == "AND"

    def test_or_conditions(self, users_schema: Schema):
        """Conditions can be combined with |."""
        users = table("users", schema=users_schema)
        cond = (users.status == "active") | (users.status == "pending")
        assert isinstance(cond, ConditionGroup)
        assert cond.operator == "OR"

    def test_complex_conditions(self, users_schema: Schema):
        """Complex nested conditions work."""
        users = table("users", schema=users_schema)
        cond = (users.status == "active") & (
            (users.email.like("%@a.com")) | (users.email.like("%@b.com"))
        )
        sql, params = cond.to_sql("postgresql")
        assert "AND" in sql
        assert "OR" in sql


# SelectQuery tests


class TestSelectQuery:
    """Tests for SelectQuery builder."""

    def test_simple_select(self, users_schema: Schema):
        """Simple SELECT generates correct SQL."""
        users = table("users", schema=users_schema)
        query = users.select(users.id, users.email)
        sql, params = query.to_sql()
        assert "SELECT users.id, users.email FROM users" in sql

    def test_select_all(self, users_schema: Schema):
        """SELECT without columns selects all."""
        users = table("users", schema=users_schema)
        query = users.select()
        sql, params = query.to_sql()
        assert "SELECT * FROM users" in sql

    def test_select_with_where(self, users_schema: Schema):
        """SELECT with WHERE clause."""
        users = table("users", schema=users_schema)
        query = users.select(users.id).where(users.status == "active")
        sql, params = query.to_sql()
        assert "WHERE" in sql
        assert "users.status" in sql

    def test_select_with_order_by(self, users_schema: Schema):
        """SELECT with ORDER BY clause."""
        users = table("users", schema=users_schema)
        query = users.select(users.id).order_by(users.created_at.desc())
        sql, params = query.to_sql()
        assert "ORDER BY users.created_at DESC" in sql

    def test_select_with_limit(self, users_schema: Schema):
        """SELECT with LIMIT clause."""
        users = table("users", schema=users_schema)
        query = users.select(users.id).limit(10)
        sql, params = query.to_sql()
        assert "LIMIT 10" in sql

    def test_select_with_offset(self, users_schema: Schema):
        """SELECT with OFFSET clause."""
        users = table("users", schema=users_schema)
        query = users.select(users.id).limit(10).offset(20)
        sql, params = query.to_sql()
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_select_with_params(self, users_schema: Schema):
        """SELECT with named parameters."""
        users = table("users", schema=users_schema)
        query = (
            users
            .select(users.id, users.email)
            .where(users.id == ":user_id")
            .params(user_id="abc-123")
        )
        sql, params = query.to_sql()
        assert ":user_id" in sql
        assert params["user_id"] == "abc-123"

    def test_select_with_join(self, orders_schema: Schema):
        """SELECT with JOIN clause using column-to-column comparison."""
        users = table("users", schema=orders_schema)
        orders = table("orders", schema=orders_schema)
        query = (
            orders
            .select(orders.id, orders.total, users.email)
            .join(users, on=orders.user_id == users.id)
        )
        sql, params = query.to_sql()
        assert "INNER JOIN users ON orders.user_id = users.id" in sql
        assert params == {}  # column-to-column produces no params

    def test_select_with_left_join(self, orders_schema: Schema):
        """SELECT with LEFT JOIN clause using column-to-column comparison."""
        users = table("users", schema=orders_schema)
        orders = table("orders", schema=orders_schema)
        query = (
            orders
            .select(orders.id)
            .join(users, on=orders.user_id == users.id, type="left")
        )
        sql, params = query.to_sql()
        assert "LEFT JOIN users ON orders.user_id = users.id" in sql
        assert params == {}

    def test_column_to_column_comparison_operators(self, orders_schema: Schema):
        """All comparison operators work for column-to-column (non-equi joins)."""
        users = table("users", schema=orders_schema)
        orders = table("orders", schema=orders_schema)

        # != operator
        cond = orders.user_id != users.id
        sql, params = cond.to_sql("postgresql")
        assert sql == "orders.user_id != users.id"
        assert params == {}

        # < operator
        cond = orders.id < users.id
        sql, params = cond.to_sql("postgresql")
        assert sql == "orders.id < users.id"
        assert params == {}

        # > operator
        cond = orders.id > users.id
        sql, params = cond.to_sql("postgresql")
        assert sql == "orders.id > users.id"
        assert params == {}

    def test_select_with_count(self, users_schema: Schema):
        """SELECT with COUNT function."""
        users = table("users", schema=users_schema)
        query = users.select(count_("*"))
        sql, params = query.to_sql()
        assert "COUNT(*)" in sql

    def test_select_immutability(self, users_schema: Schema):
        """SelectQuery is immutable - methods return new instances."""
        users = table("users", schema=users_schema)
        q1 = users.select(users.id)
        q2 = q1.where(users.status == "active")
        q3 = q1.limit(10)

        # Original query is unchanged
        sql1, _ = q1.to_sql()
        assert "WHERE" not in sql1
        assert "LIMIT" not in sql1

        # New queries have modifications
        sql2, _ = q2.to_sql()
        assert "WHERE" in sql2

        sql3, _ = q3.to_sql()
        assert "LIMIT" in sql3


# InsertQuery tests


class TestInsertQuery:
    """Tests for InsertQuery builder."""

    def test_simple_insert(self, users_schema: Schema):
        """Simple INSERT generates correct SQL."""
        users = table("users", schema=users_schema)
        query = users.insert(email="test@example.com", name="Test")
        sql, params = query.to_sql()
        assert "INSERT INTO users" in sql
        assert "email" in sql
        assert "name" in sql
        assert "ins_email" in params
        assert "ins_name" in params

    def test_insert_with_param_reference(self, users_schema: Schema):
        """INSERT with parameter references."""
        users = table("users", schema=users_schema)
        query = users.insert(email=":email").params(email="test@example.com")
        sql, params = query.to_sql()
        assert ":email" in sql
        assert params["email"] == "test@example.com"

    def test_insert_with_function(self, users_schema: Schema):
        """INSERT with SQL function."""
        users = table("users", schema=users_schema)
        query = users.insert(email="test@example.com", created_at=now_())
        sql, params = query.to_sql("postgresql")
        assert "now()" in sql

    def test_insert_with_returning(self, users_schema: Schema):
        """INSERT with RETURNING clause."""
        users = table("users", schema=users_schema)
        query = users.insert(email="test@example.com").returning(users.id)
        sql, params = query.to_sql()
        assert "RETURNING id" in sql

    def test_insert_invalid_column(self, users_schema: Schema):
        """INSERT with invalid column raises AttributeError."""
        users = table("users", schema=users_schema)
        with pytest.raises(AttributeError, match="has no column 'invalid'"):
            users.insert(invalid="value")


# UpdateQuery tests


class TestUpdateQuery:
    """Tests for UpdateQuery builder."""

    def test_simple_update(self, users_schema: Schema):
        """Simple UPDATE generates correct SQL."""
        users = table("users", schema=users_schema)
        query = (
            users
            .update(name="New Name")
            .where(users.id == ":id")
            .params(id="abc-123")
        )
        sql, params = query.to_sql()
        assert "UPDATE users SET" in sql
        assert "name" in sql
        assert "WHERE" in sql

    def test_update_with_function(self, users_schema: Schema):
        """UPDATE with SQL function."""
        users = table("users", schema=users_schema)
        query = users.update(created_at=now_()).where(users.id == ":id")
        sql, params = query.to_sql("postgresql")
        assert "created_at = now()" in sql

    def test_update_with_returning(self, users_schema: Schema):
        """UPDATE with RETURNING clause."""
        users = table("users", schema=users_schema)
        query = (
            users
            .update(name="New")
            .where(users.id == ":id")
            .returning(users.id, users.name)
        )
        sql, params = query.to_sql()
        assert "RETURNING id, name" in sql

    def test_update_invalid_column(self, users_schema: Schema):
        """UPDATE with invalid column raises AttributeError."""
        users = table("users", schema=users_schema)
        with pytest.raises(AttributeError, match="has no column 'invalid'"):
            users.update(invalid="value")


# DeleteQuery tests


class TestDeleteQuery:
    """Tests for DeleteQuery builder."""

    def test_simple_delete(self, users_schema: Schema):
        """Simple DELETE generates correct SQL."""
        users = table("users", schema=users_schema)
        query = users.delete().where(users.id == ":id").params(id="abc-123")
        sql, params = query.to_sql()
        assert "DELETE FROM users" in sql
        assert "WHERE" in sql

    def test_delete_with_returning(self, users_schema: Schema):
        """DELETE with RETURNING clause."""
        users = table("users", schema=users_schema)
        query = users.delete().where(users.id == ":id").returning(users.id)
        sql, params = query.to_sql()
        assert "RETURNING id" in sql

    def test_delete_without_where(self, users_schema: Schema):
        """DELETE without WHERE is allowed (but dangerous)."""
        users = table("users", schema=users_schema)
        query = users.delete()
        sql, params = query.to_sql()
        assert "DELETE FROM users" in sql
        assert "WHERE" not in sql


# Function tests


class TestSQLFunctions:
    """Tests for SQL function wrappers."""

    def test_count_star(self, users_schema: Schema):
        """count_("*") generates COUNT(*)."""
        users = table("users", schema=users_schema)
        query = users.select(count_("*"))
        sql, _ = query.to_sql()
        assert "COUNT(*)" in sql

    def test_now_postgresql(self):
        """now_() generates now() for PostgreSQL."""
        from declaro_persistum.query.insert import _translate_function

        func = now_()
        sql = _translate_function(func, "postgresql")
        assert sql == "now()"

    def test_now_sqlite(self):
        """now_() generates datetime('now') for SQLite."""
        from declaro_persistum.query.insert import _translate_function

        func = now_()
        sql = _translate_function(func, "sqlite")
        assert sql == "datetime('now')"
