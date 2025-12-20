"""
Unit tests for aggregate function wrappers.

Tests SQL generation for sum_, count_, avg_, min_, max_, string_agg_.
"""

import pytest
from typing import Any


class TestSumFunction:
    """Tests for sum_ aggregate function."""

    def test_sum_basic(self):
        """Generate SUM SQL."""
        from declaro_persistum.functions.aggregates import sum_

        func = sum_("orders.total")
        sql = func.to_sql("postgresql")
        assert "SUM(orders.total)" in sql

    def test_sum_with_alias(self):
        """Generate SUM with alias."""
        from declaro_persistum.functions.aggregates import sum_

        func = sum_("orders.total", alias="total_revenue")
        sql = func.to_sql("postgresql")
        assert "SUM(orders.total) AS total_revenue" in sql

    def test_sum_sqlite(self):
        """SUM works for SQLite."""
        from declaro_persistum.functions.aggregates import sum_

        func = sum_("orders.total")
        sql = func.to_sql("sqlite")
        assert "SUM(orders.total)" in sql


class TestCountFunction:
    """Tests for count_ aggregate function."""

    def test_count_star(self):
        """Generate COUNT(*) SQL."""
        from declaro_persistum.functions.aggregates import count_

        func = count_()
        sql = func.to_sql("postgresql")
        assert "COUNT(*)" in sql

    def test_count_column(self):
        """Generate COUNT(column) SQL."""
        from declaro_persistum.functions.aggregates import count_

        func = count_("users.email")
        sql = func.to_sql("postgresql")
        assert "COUNT(users.email)" in sql

    def test_count_with_alias(self):
        """Generate COUNT with alias."""
        from declaro_persistum.functions.aggregates import count_

        func = count_("*", alias="total_users")
        sql = func.to_sql("postgresql")
        assert "COUNT(*) AS total_users" in sql

    def test_count_distinct(self):
        """Generate COUNT(DISTINCT column) SQL."""
        from declaro_persistum.functions.aggregates import count_

        func = count_("users.country", distinct=True)
        sql = func.to_sql("postgresql")
        assert "COUNT(DISTINCT users.country)" in sql


class TestAvgFunction:
    """Tests for avg_ aggregate function."""

    def test_avg_basic(self):
        """Generate AVG SQL."""
        from declaro_persistum.functions.aggregates import avg_

        func = avg_("orders.total")
        sql = func.to_sql("postgresql")
        assert "AVG(orders.total)" in sql

    def test_avg_with_alias(self):
        """Generate AVG with alias."""
        from declaro_persistum.functions.aggregates import avg_

        func = avg_("orders.total", alias="avg_order_value")
        sql = func.to_sql("postgresql")
        assert "AVG(orders.total) AS avg_order_value" in sql


class TestMinMaxFunctions:
    """Tests for min_ and max_ aggregate functions."""

    def test_min_basic(self):
        """Generate MIN SQL."""
        from declaro_persistum.functions.aggregates import min_

        func = min_("orders.total")
        sql = func.to_sql("postgresql")
        assert "MIN(orders.total)" in sql

    def test_max_basic(self):
        """Generate MAX SQL."""
        from declaro_persistum.functions.aggregates import max_

        func = max_("orders.total")
        sql = func.to_sql("postgresql")
        assert "MAX(orders.total)" in sql

    def test_min_with_alias(self):
        """Generate MIN with alias."""
        from declaro_persistum.functions.aggregates import min_

        func = min_("orders.total", alias="min_order")
        sql = func.to_sql("postgresql")
        assert "MIN(orders.total) AS min_order" in sql

    def test_max_with_alias(self):
        """Generate MAX with alias."""
        from declaro_persistum.functions.aggregates import max_

        func = max_("orders.total", alias="max_order")
        sql = func.to_sql("postgresql")
        assert "MAX(orders.total) AS max_order" in sql


class TestStringAggFunction:
    """Tests for string_agg_ aggregate function."""

    def test_string_agg_basic(self):
        """Generate STRING_AGG SQL for PostgreSQL."""
        from declaro_persistum.functions.aggregates import string_agg_

        func = string_agg_("users.name", ", ")
        sql = func.to_sql("postgresql")
        assert "STRING_AGG(users.name, ', ')" in sql

    def test_string_agg_sqlite(self):
        """Generate GROUP_CONCAT SQL for SQLite."""
        from declaro_persistum.functions.aggregates import string_agg_

        func = string_agg_("users.name", ", ")
        sql = func.to_sql("sqlite")
        assert "GROUP_CONCAT(users.name, ', ')" in sql

    def test_string_agg_with_alias(self):
        """Generate STRING_AGG with alias."""
        from declaro_persistum.functions.aggregates import string_agg_

        func = string_agg_("users.name", ", ", alias="all_names")
        sql = func.to_sql("postgresql")
        assert "STRING_AGG(users.name, ', ') AS all_names" in sql

    def test_string_agg_with_order(self):
        """Generate STRING_AGG with ORDER BY."""
        from declaro_persistum.functions.aggregates import string_agg_

        func = string_agg_("users.name", ", ", order_by="users.name")
        sql = func.to_sql("postgresql")
        assert "ORDER BY" in sql


class TestArrayAggFunction:
    """Tests for array_agg_ aggregate function."""

    def test_array_agg_postgresql(self):
        """Generate ARRAY_AGG SQL for PostgreSQL."""
        from declaro_persistum.functions.aggregates import array_agg_

        func = array_agg_("orders.id")
        sql = func.to_sql("postgresql")
        assert "ARRAY_AGG(orders.id)" in sql

    def test_array_agg_sqlite(self):
        """Generate JSON_GROUP_ARRAY SQL for SQLite."""
        from declaro_persistum.functions.aggregates import array_agg_

        func = array_agg_("orders.id")
        sql = func.to_sql("sqlite")
        assert "JSON_GROUP_ARRAY(orders.id)" in sql

    def test_array_agg_with_alias(self):
        """Generate ARRAY_AGG with alias."""
        from declaro_persistum.functions.aggregates import array_agg_

        func = array_agg_("orders.id", alias="order_ids")
        sql = func.to_sql("postgresql")
        assert "ARRAY_AGG(orders.id) AS order_ids" in sql


class TestSQLFunctionInterface:
    """Tests for SQLFunction interface."""

    def test_function_is_expression(self):
        """SQLFunction can be used as expression."""
        from declaro_persistum.functions.aggregates import count_

        func = count_()
        # Should have to_sql method
        assert hasattr(func, "to_sql")
        assert callable(func.to_sql)

    def test_function_has_alias_property(self):
        """SQLFunction exposes alias."""
        from declaro_persistum.functions.aggregates import count_

        func = count_("*", alias="total")
        assert func.alias == "total"

    def test_function_without_alias(self):
        """SQLFunction alias is None when not specified."""
        from declaro_persistum.functions.aggregates import count_

        func = count_()
        assert func.alias is None


class TestAggregateInQueryBuilder:
    """Tests for using aggregates in query builder."""

    def test_aggregate_in_select(self):
        """Use aggregate function in SELECT."""
        from declaro_persistum.functions.aggregates import count_, sum_

        # These should be usable with the query builder
        # The actual integration is tested elsewhere, but functions should
        # produce valid SQL strings
        count_sql = count_().to_sql("postgresql")
        sum_sql = sum_("total").to_sql("postgresql")

        assert "COUNT" in count_sql
        assert "SUM" in sum_sql

    def test_multiple_aggregates(self):
        """Multiple aggregates in same query."""
        from declaro_persistum.functions.aggregates import count_, sum_, avg_

        count_sql = count_("*", alias="total_orders").to_sql("postgresql")
        sum_sql = sum_("total", alias="total_revenue").to_sql("postgresql")
        avg_sql = avg_("total", alias="avg_order").to_sql("postgresql")

        assert "total_orders" in count_sql
        assert "total_revenue" in sum_sql
        assert "avg_order" in avg_sql
