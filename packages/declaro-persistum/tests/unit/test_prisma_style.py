"""
Unit tests for Prisma-style query API.

Tests the PrismaQueryBuilder class and Prisma-style methods on TableProxy.
"""

import pytest
from typing import Any

from declaro_persistum.types import Schema
from declaro_persistum.query import table
from declaro_persistum.query.prisma_style import PrismaQueryBuilder


@pytest.fixture
def users_schema() -> Schema:
    """Schema with users table."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True},
                "email": {"type": "text", "nullable": False},
                "name": {"type": "text"},
                "status": {"type": "text", "default": "'active'"},
                "age": {"type": "integer"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            }
        }
    }


class TestPrismaWhereClause:
    """Tests for Prisma-style where clause parsing."""

    def test_simple_equality(self, users_schema: Schema):
        """Simple field equality."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({"status": "active"})
        assert len(conditions) == 1
        assert conditions[0].column == "users.status"
        assert conditions[0].value == "active"

    def test_multiple_fields(self, users_schema: Schema):
        """Multiple fields create multiple conditions."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "status": "active",
            "name": "Alice"
        })
        assert len(conditions) == 2

    def test_null_value(self, users_schema: Schema):
        """null value creates IS NULL condition."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({"name": None})
        assert len(conditions) == 1
        assert conditions[0].operator == "IS"
        assert conditions[0].value is None

    def test_equals_operator(self, users_schema: Schema):
        """Nested equals operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "status": {"equals": "active"}
        })
        assert len(conditions) == 1
        assert conditions[0].value == "active"

    def test_not_operator(self, users_schema: Schema):
        """Nested not operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "status": {"not": "deleted"}
        })
        assert len(conditions) == 1
        assert conditions[0].operator == "!="

    def test_in_operator(self, users_schema: Schema):
        """Nested in operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "status": {"in": ["active", "pending"]}
        })
        assert len(conditions) == 1
        assert conditions[0].operator == "IN"

    def test_lt_operator(self, users_schema: Schema):
        """Nested lt operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({"age": {"lt": 65}})
        assert len(conditions) == 1
        assert conditions[0].operator == "<"

    def test_gt_operator(self, users_schema: Schema):
        """Nested gt operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({"age": {"gt": 18}})
        assert len(conditions) == 1
        assert conditions[0].operator == ">"

    def test_gte_lte_operators(self, users_schema: Schema):
        """Nested gte and lte operators."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "age": {"gte": 18, "lte": 65}
        })
        assert len(conditions) == 2

    def test_contains_operator(self, users_schema: Schema):
        """Nested contains operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "email": {"contains": "example"}
        })
        assert len(conditions) == 1
        assert conditions[0].operator == "LIKE"
        assert "%example%" in conditions[0].value

    def test_startswith_operator(self, users_schema: Schema):
        """Nested startsWith operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "name": {"startsWith": "Al"}
        })
        assert len(conditions) == 1
        assert "Al%" in conditions[0].value

    def test_endswith_operator(self, users_schema: Schema):
        """Nested endsWith operator."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        conditions = builder._where_to_conditions({
            "email": {"endsWith": ".com"}
        })
        assert len(conditions) == 1
        assert "%.com" in conditions[0].value

    def test_invalid_column(self, users_schema: Schema):
        """Invalid column raises AttributeError."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        with pytest.raises(AttributeError, match="has no column"):
            builder._where_to_conditions({"invalid": "value"})


class TestPrismaOrderClause:
    """Tests for Prisma-style order clause."""

    def test_single_field_asc(self, users_schema: Schema):
        """Single field ascending."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        order_sql = builder._order_to_sql({"created_at": "asc"})
        assert "created_at ASC" in order_sql

    def test_single_field_desc(self, users_schema: Schema):
        """Single field descending."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        order_sql = builder._order_to_sql({"created_at": "desc"})
        assert "created_at DESC" in order_sql

    def test_multiple_fields(self, users_schema: Schema):
        """Multiple fields in order."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        order_sql = builder._order_to_sql([
            {"status": "asc"},
            {"created_at": "desc"}
        ])
        assert "status ASC" in order_sql
        assert "created_at DESC" in order_sql


class TestPrismaBuildSelectSQL:
    """Tests for building SELECT SQL."""

    def test_select_all(self, users_schema: Schema):
        """SELECT without where/order/limit."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(None, None, None, None, "postgresql")
        assert sql == "SELECT * FROM users"
        assert params == {}

    def test_select_with_where(self, users_schema: Schema):
        """SELECT with where clause."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(
            {"status": "active"}, None, None, None, "postgresql"
        )
        assert "WHERE" in sql
        assert len(params) == 1

    def test_select_with_order(self, users_schema: Schema):
        """SELECT with order clause."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(
            None, {"created_at": "desc"}, None, None, "postgresql"
        )
        assert "ORDER BY" in sql
        assert "created_at DESC" in sql

    def test_select_with_take(self, users_schema: Schema):
        """SELECT with take (LIMIT)."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(
            None, None, 10, None, "postgresql"
        )
        assert "LIMIT 10" in sql

    def test_select_with_skip(self, users_schema: Schema):
        """SELECT with skip (OFFSET)."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(
            None, None, None, 5, "postgresql"
        )
        assert "OFFSET 5" in sql

    def test_select_full(self, users_schema: Schema):
        """SELECT with all options."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        sql, params = builder._build_select_sql(
            {"status": "active"},
            {"created_at": "desc"},
            10,
            5,
            "postgresql"
        )
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 10" in sql
        assert "OFFSET 5" in sql


class TestTableProxyPrismaMethods:
    """Tests for Prisma-style methods on TableProxy."""

    def test_prisma_property(self, users_schema: Schema):
        """TableProxy.prisma returns PrismaQueryBuilder."""
        users = table("users", schema=users_schema)
        builder = users.prisma
        assert isinstance(builder, PrismaQueryBuilder)
