"""
Unit tests for scalar function wrappers.

Tests SQL generation for lower_, upper_, coalesce_, now_, gen_random_uuid_, etc.
"""

import pytest
from typing import Any


class TestLowerUpperFunctions:
    """Tests for lower_ and upper_ functions."""

    def test_lower_basic(self):
        """Generate LOWER SQL."""
        from declaro_persistum.functions.scalars import lower_

        func = lower_("users.email")
        sql = func.to_sql("postgresql")
        assert "LOWER(users.email)" in sql

    def test_upper_basic(self):
        """Generate UPPER SQL."""
        from declaro_persistum.functions.scalars import upper_

        func = upper_("users.name")
        sql = func.to_sql("postgresql")
        assert "UPPER(users.name)" in sql

    def test_lower_sqlite(self):
        """LOWER works for SQLite."""
        from declaro_persistum.functions.scalars import lower_

        func = lower_("users.email")
        sql = func.to_sql("sqlite")
        assert "LOWER(users.email)" in sql

    def test_lower_with_alias(self):
        """Generate LOWER with alias."""
        from declaro_persistum.functions.scalars import lower_

        func = lower_("users.email", alias="email_lower")
        sql = func.to_sql("postgresql")
        assert "LOWER(users.email) AS email_lower" in sql


class TestCoalesceFunction:
    """Tests for coalesce_ function."""

    def test_coalesce_two_values(self):
        """Generate COALESCE with two values."""
        from declaro_persistum.functions.scalars import coalesce_

        func = coalesce_("users.nickname", "users.name")
        sql = func.to_sql("postgresql")
        assert "COALESCE(users.nickname, users.name)" in sql

    def test_coalesce_multiple_values(self):
        """Generate COALESCE with multiple values."""
        from declaro_persistum.functions.scalars import coalesce_

        func = coalesce_("users.nickname", "users.name", "'Unknown'")
        sql = func.to_sql("postgresql")
        assert "COALESCE(users.nickname, users.name, 'Unknown')" in sql

    def test_coalesce_with_alias(self):
        """Generate COALESCE with alias."""
        from declaro_persistum.functions.scalars import coalesce_

        func = coalesce_("users.nickname", "users.name", alias="display_name")
        sql = func.to_sql("postgresql")
        assert "COALESCE(users.nickname, users.name) AS display_name" in sql


class TestLengthFunction:
    """Tests for length_ function."""

    def test_length_basic(self):
        """Generate LENGTH SQL."""
        from declaro_persistum.functions.scalars import length_

        func = length_("users.name")
        sql = func.to_sql("postgresql")
        assert "LENGTH(users.name)" in sql

    def test_length_sqlite(self):
        """LENGTH works for SQLite."""
        from declaro_persistum.functions.scalars import length_

        func = length_("users.name")
        sql = func.to_sql("sqlite")
        assert "LENGTH(users.name)" in sql


class TestTrimFunction:
    """Tests for trim_ function."""

    def test_trim_basic(self):
        """Generate TRIM SQL."""
        from declaro_persistum.functions.scalars import trim_

        func = trim_("users.name")
        sql = func.to_sql("postgresql")
        assert "TRIM(users.name)" in sql

    def test_ltrim_basic(self):
        """Generate LTRIM SQL."""
        from declaro_persistum.functions.scalars import ltrim_

        func = ltrim_("users.name")
        sql = func.to_sql("postgresql")
        assert "LTRIM(users.name)" in sql

    def test_rtrim_basic(self):
        """Generate RTRIM SQL."""
        from declaro_persistum.functions.scalars import rtrim_

        func = rtrim_("users.name")
        sql = func.to_sql("postgresql")
        assert "RTRIM(users.name)" in sql


class TestNowFunction:
    """Tests for now_ function."""

    def test_now_postgresql(self):
        """Generate NOW() for PostgreSQL."""
        from declaro_persistum.functions.scalars import now_

        func = now_()
        sql = func.to_sql("postgresql")
        assert "NOW()" in sql or "CURRENT_TIMESTAMP" in sql

    def test_now_sqlite(self):
        """Generate datetime('now') for SQLite."""
        from declaro_persistum.functions.scalars import now_

        func = now_()
        sql = func.to_sql("sqlite")
        assert "datetime('now')" in sql or "CURRENT_TIMESTAMP" in sql

    def test_now_with_alias(self):
        """Generate NOW() with alias."""
        from declaro_persistum.functions.scalars import now_

        func = now_(alias="created_at")
        sql = func.to_sql("postgresql")
        assert "AS created_at" in sql


class TestGenRandomUuidFunction:
    """Tests for gen_random_uuid_ function."""

    def test_uuid_postgresql(self):
        """Generate gen_random_uuid() for PostgreSQL."""
        from declaro_persistum.functions.scalars import gen_random_uuid_

        func = gen_random_uuid_()
        sql = func.to_sql("postgresql")
        assert "gen_random_uuid()" in sql

    def test_uuid_sqlite(self):
        """Generate UUID expression for SQLite."""
        from declaro_persistum.functions.scalars import gen_random_uuid_

        func = gen_random_uuid_()
        sql = func.to_sql("sqlite")
        # SQLite uses randomblob-based UUID generation
        assert "randomblob" in sql.lower() or "hex" in sql.lower()

    def test_uuid_with_alias(self):
        """Generate gen_random_uuid() with alias."""
        from declaro_persistum.functions.scalars import gen_random_uuid_

        func = gen_random_uuid_(alias="new_id")
        sql = func.to_sql("postgresql")
        assert "AS new_id" in sql


class TestExtractFunctions:
    """Tests for date/time extraction functions."""

    def test_extract_year(self):
        """Generate EXTRACT(YEAR) SQL."""
        from declaro_persistum.functions.scalars import extract_year_

        func = extract_year_("orders.created_at")
        sql = func.to_sql("postgresql")
        assert "EXTRACT(YEAR FROM orders.created_at)" in sql

    def test_extract_year_sqlite(self):
        """Generate strftime('%Y') for SQLite."""
        from declaro_persistum.functions.scalars import extract_year_

        func = extract_year_("orders.created_at")
        sql = func.to_sql("sqlite")
        assert "strftime('%Y'" in sql

    def test_extract_month(self):
        """Generate EXTRACT(MONTH) SQL."""
        from declaro_persistum.functions.scalars import extract_month_

        func = extract_month_("orders.created_at")
        sql = func.to_sql("postgresql")
        assert "EXTRACT(MONTH FROM orders.created_at)" in sql

    def test_extract_day(self):
        """Generate EXTRACT(DAY) SQL."""
        from declaro_persistum.functions.scalars import extract_day_

        func = extract_day_("orders.created_at")
        sql = func.to_sql("postgresql")
        assert "EXTRACT(DAY FROM orders.created_at)" in sql


class TestDateArithmeticFunctions:
    """Tests for date arithmetic functions."""

    def test_date_add_days(self):
        """Generate date addition SQL."""
        from declaro_persistum.functions.scalars import date_add_days_

        func = date_add_days_("orders.created_at", 7)
        sql = func.to_sql("postgresql")
        assert "orders.created_at" in sql
        assert "7" in sql or "INTERVAL" in sql

    def test_date_add_days_sqlite(self):
        """Generate date addition for SQLite."""
        from declaro_persistum.functions.scalars import date_add_days_

        func = date_add_days_("orders.created_at", 7)
        sql = func.to_sql("sqlite")
        assert "orders.created_at" in sql
        # SQLite uses date() or datetime() with modifiers
        assert "date" in sql.lower() or "+" in sql


class TestCastFunction:
    """Tests for cast_ function."""

    def test_cast_to_integer(self):
        """Generate CAST to INTEGER."""
        from declaro_persistum.functions.scalars import cast_

        func = cast_("users.age_str", "integer")
        sql = func.to_sql("postgresql")
        assert "CAST(users.age_str AS INTEGER)" in sql

    def test_cast_to_text(self):
        """Generate CAST to TEXT."""
        from declaro_persistum.functions.scalars import cast_

        func = cast_("orders.total", "text")
        sql = func.to_sql("postgresql")
        assert "CAST(orders.total AS TEXT)" in sql


class TestConcatFunction:
    """Tests for concat_ function."""

    def test_concat_two_values(self):
        """Generate CONCAT with two values."""
        from declaro_persistum.functions.scalars import concat_

        func = concat_("users.first_name", "' '", "users.last_name")
        sql = func.to_sql("postgresql")
        # PostgreSQL can use CONCAT or ||
        assert "CONCAT" in sql or "||" in sql

    def test_concat_sqlite(self):
        """Generate concatenation for SQLite."""
        from declaro_persistum.functions.scalars import concat_

        func = concat_("users.first_name", "' '", "users.last_name")
        sql = func.to_sql("sqlite")
        # SQLite uses || operator
        assert "||" in sql


class TestSubstringFunction:
    """Tests for substring_ function."""

    def test_substring_basic(self):
        """Generate SUBSTRING SQL."""
        from declaro_persistum.functions.scalars import substring_

        func = substring_("users.name", 1, 5)
        sql = func.to_sql("postgresql")
        assert "SUBSTRING" in sql or "SUBSTR" in sql
        assert "users.name" in sql

    def test_substring_sqlite(self):
        """Generate SUBSTR for SQLite."""
        from declaro_persistum.functions.scalars import substring_

        func = substring_("users.name", 1, 5)
        sql = func.to_sql("sqlite")
        assert "SUBSTR(users.name" in sql


class TestNullIfFunction:
    """Tests for nullif_ function."""

    def test_nullif_basic(self):
        """Generate NULLIF SQL."""
        from declaro_persistum.functions.scalars import nullif_

        func = nullif_("users.status", "'inactive'")
        sql = func.to_sql("postgresql")
        assert "NULLIF(users.status, 'inactive')" in sql


class TestAbsFunction:
    """Tests for abs_ function."""

    def test_abs_basic(self):
        """Generate ABS SQL."""
        from declaro_persistum.functions.scalars import abs_

        func = abs_("orders.discount")
        sql = func.to_sql("postgresql")
        assert "ABS(orders.discount)" in sql


class TestRoundFunction:
    """Tests for round_ function."""

    def test_round_basic(self):
        """Generate ROUND SQL."""
        from declaro_persistum.functions.scalars import round_

        func = round_("orders.total", 2)
        sql = func.to_sql("postgresql")
        assert "ROUND(orders.total, 2)" in sql

    def test_round_no_decimals(self):
        """Generate ROUND without decimals."""
        from declaro_persistum.functions.scalars import round_

        func = round_("orders.total")
        sql = func.to_sql("postgresql")
        assert "ROUND(orders.total)" in sql


class TestSQLFunctionInterface:
    """Tests for SQLFunction interface."""

    def test_function_is_expression(self):
        """SQLFunction can be used as expression."""
        from declaro_persistum.functions.scalars import lower_

        func = lower_("col")
        assert hasattr(func, "to_sql")
        assert callable(func.to_sql)

    def test_function_has_alias_property(self):
        """SQLFunction exposes alias."""
        from declaro_persistum.functions.scalars import lower_

        func = lower_("col", alias="lower_col")
        assert func.alias == "lower_col"
