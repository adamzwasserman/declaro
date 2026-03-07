"""
Unit tests for Django-style query API.

Tests the QuerySet class and Django-style methods on TableProxy.
"""

import pytest
from typing import Any

from declaro_persistum.types import Schema
from declaro_persistum.query import table
from declaro_persistum.query.django_style import QuerySet, DoesNotExist, MultipleObjectsReturned


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


class TestQuerySet:
    """Tests for QuerySet class."""

    def test_filter_simple(self, users_schema: Schema):
        """filter() creates conditions."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active")
        sql, params = qs.to_sql()
        assert "WHERE" in sql
        assert "users.status" in sql
        assert len(params) == 1

    def test_filter_multiple(self, users_schema: Schema):
        """filter() with multiple kwargs creates AND conditions."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active", name="Alice")
        sql, params = qs.to_sql()
        assert "WHERE" in sql
        assert "AND" in sql
        assert len(params) == 2

    def test_filter_chained(self, users_schema: Schema):
        """Chained filter() calls create AND conditions."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active").filter(name="Alice")
        sql, params = qs.to_sql()
        assert "WHERE" in sql
        assert "AND" in sql

    def test_filter_lookup_gt(self, users_schema: Schema):
        """filter() supports __gt lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(age__gt=18)
        sql, params = qs.to_sql()
        assert ">" in sql

    def test_filter_lookup_lt(self, users_schema: Schema):
        """filter() supports __lt lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(age__lt=65)
        sql, params = qs.to_sql()
        assert "<" in sql

    def test_filter_lookup_gte(self, users_schema: Schema):
        """filter() supports __gte lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(age__gte=18)
        sql, params = qs.to_sql()
        assert ">=" in sql

    def test_filter_lookup_in(self, users_schema: Schema):
        """filter() supports __in lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(status__in=["active", "pending"])
        sql, params = qs.to_sql()
        assert "IN" in sql
        assert len(params) == 2

    def test_filter_lookup_contains(self, users_schema: Schema):
        """filter() supports __contains lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(email__contains="example")
        sql, params = qs.to_sql()
        assert "LIKE" in sql
        assert "%example%" in list(params.values())[0]

    def test_filter_lookup_startswith(self, users_schema: Schema):
        """filter() supports __startswith lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(name__startswith="Al")
        sql, params = qs.to_sql()
        assert "LIKE" in sql
        assert "Al%" in list(params.values())[0]

    def test_filter_lookup_isnull(self, users_schema: Schema):
        """filter() supports __isnull lookup."""
        users = table("users", schema=users_schema)
        qs = users.filter(name__isnull=True)
        sql, params = qs.to_sql()
        assert "IS NULL" in sql

    def test_filter_invalid_column(self, users_schema: Schema):
        """filter() raises AttributeError for invalid column."""
        users = table("users", schema=users_schema)
        with pytest.raises(AttributeError, match="has no column"):
            users.filter(invalid="value")

    def test_order_single(self, users_schema: Schema):
        """order() adds ORDER BY clause."""
        users = table("users", schema=users_schema)
        qs = users.order("name")
        sql, params = qs.to_sql()
        assert "ORDER BY users.name ASC" in sql

    def test_order_desc(self, users_schema: Schema):
        """order() with - prefix orders DESC."""
        users = table("users", schema=users_schema)
        qs = users.order("-created_at")
        sql, params = qs.to_sql()
        assert "ORDER BY users.created_at DESC" in sql

    def test_order_multiple(self, users_schema: Schema):
        """order() with multiple fields."""
        users = table("users", schema=users_schema)
        qs = users.order("status", "-created_at")
        sql, params = qs.to_sql()
        assert "ORDER BY" in sql
        assert "status ASC" in sql
        assert "created_at DESC" in sql

    def test_slice_limit(self, users_schema: Schema):
        """Slicing [:n] adds LIMIT."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active")[:10]
        sql, params = qs.to_sql()
        assert "LIMIT 10" in sql

    def test_slice_offset_limit(self, users_schema: Schema):
        """Slicing [n:m] adds OFFSET and LIMIT."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active")[5:15]
        sql, params = qs.to_sql()
        assert "OFFSET 5" in sql
        assert "LIMIT 10" in sql

    def test_filter_order_slice_chain(self, users_schema: Schema):
        """Full chain: filter().order()[:n]."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active").order("-created_at")[:10]
        sql, params = qs.to_sql()
        assert "WHERE" in sql
        assert "ORDER BY" in sql
        assert "LIMIT 10" in sql


class TestTableProxyDjangoMethods:
    """Tests for Django-style methods on TableProxy."""

    def test_objects_property(self, users_schema: Schema):
        """TableProxy.objects returns QuerySet."""
        users = table("users", schema=users_schema)
        qs = users.objects
        assert isinstance(qs, QuerySet)

    def test_filter_shortcut(self, users_schema: Schema):
        """TableProxy.filter() is shortcut for .objects.filter()."""
        users = table("users", schema=users_schema)
        qs = users.filter(status="active")
        assert isinstance(qs, QuerySet)
        sql, _ = qs.to_sql()
        assert "WHERE" in sql

    def test_order_shortcut(self, users_schema: Schema):
        """TableProxy.order() is shortcut for .objects.order()."""
        users = table("users", schema=users_schema)
        qs = users.order("-created_at")
        assert isinstance(qs, QuerySet)
        sql, _ = qs.to_sql()
        assert "ORDER BY" in sql

    def test_exclude_shortcut(self, users_schema: Schema):
        """TableProxy.exclude() is shortcut for .objects.exclude()."""
        users = table("users", schema=users_schema)
        qs = users.exclude(status="deleted")
        assert isinstance(qs, QuerySet)
        sql, _ = qs.to_sql()
        assert "WHERE" in sql


class TestQuerySetImmutability:
    """Tests for QuerySet immutability."""

    def test_filter_returns_new(self, users_schema: Schema):
        """filter() returns new QuerySet, doesn't modify original."""
        users = table("users", schema=users_schema)
        qs1 = users.filter(status="active")
        qs2 = qs1.filter(name="Alice")

        sql1, _ = qs1.to_sql()
        sql2, _ = qs2.to_sql()

        # qs1 should not have name filter
        assert "Alice" not in sql1 or "AND" not in sql1

    def test_order_returns_new(self, users_schema: Schema):
        """order() returns new QuerySet, doesn't modify original."""
        users = table("users", schema=users_schema)
        qs1 = users.filter(status="active")
        qs2 = qs1.order("-created_at")

        sql1, _ = qs1.to_sql()
        sql2, _ = qs2.to_sql()

        # qs1 should not have ORDER BY
        assert "ORDER BY" not in sql1
        assert "ORDER BY" in sql2
