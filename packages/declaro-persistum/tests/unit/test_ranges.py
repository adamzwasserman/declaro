"""
Unit tests for Range abstraction.

Tests portable range patterns using start/end columns with CHECK constraints.
"""

import pytest
from typing import Any


class TestRangeTypeParsing:
    """Tests for parsing range type declarations."""

    def test_parse_range_type_timestamptz(self):
        """Parse range<timestamptz> type."""
        from declaro_persistum.abstractions.ranges import parse_range_type

        element_type = parse_range_type("range<timestamptz>")
        assert element_type == "timestamptz"

    def test_parse_range_type_date(self):
        """Parse range<date> type."""
        from declaro_persistum.abstractions.ranges import parse_range_type

        element_type = parse_range_type("range<date>")
        assert element_type == "date"

    def test_parse_range_type_integer(self):
        """Parse range<integer> type."""
        from declaro_persistum.abstractions.ranges import parse_range_type

        element_type = parse_range_type("range<integer>")
        assert element_type == "integer"

    def test_parse_range_type_not_range(self):
        """Non-range type returns None."""
        from declaro_persistum.abstractions.ranges import parse_range_type

        result = parse_range_type("timestamptz")
        assert result is None


class TestRangeColumnGeneration:
    """Tests for generating range columns."""

    def test_generate_range_columns_basic(self):
        """Generate start/end columns for range."""
        from declaro_persistum.abstractions.ranges import generate_range_columns

        columns = generate_range_columns("valid_period", "timestamptz")

        assert "valid_period_start" in columns
        assert "valid_period_end" in columns
        assert columns["valid_period_start"]["type"] == "timestamptz"
        assert columns["valid_period_end"]["type"] == "timestamptz"

    def test_generate_range_columns_check_constraint(self):
        """Range columns have CHECK constraint for start <= end."""
        from declaro_persistum.abstractions.ranges import generate_range_columns

        columns = generate_range_columns("valid_period", "timestamptz")

        # Should have check that start <= end (when both are non-null)
        assert "check" in columns.get("valid_period_end", {}) or \
               any("check" in c for c in columns.values())

    def test_generate_range_columns_nullable(self):
        """Range columns are nullable for unbounded ranges."""
        from declaro_persistum.abstractions.ranges import generate_range_columns

        columns = generate_range_columns("valid_period", "date")

        # Both should be nullable (NULL = unbounded)
        assert columns["valid_period_start"].get("nullable", True) is not False
        assert columns["valid_period_end"].get("nullable", True) is not False


class TestRangeOperations:
    """Tests for range operation SQL generation."""

    def test_range_overlaps_sql(self):
        """Generate SQL for range overlap check."""
        from declaro_persistum.abstractions.ranges import range_overlaps_sql

        sql = range_overlaps_sql("valid_period")
        # Check for overlap logic: (a_start <= b_end) AND (a_end >= b_start)
        assert "valid_period_start" in sql
        assert "valid_period_end" in sql
        # Should handle NULLs as unbounded

    def test_range_contains_point_sql(self):
        """Generate SQL for point containment check."""
        from declaro_persistum.abstractions.ranges import range_contains_point_sql

        sql = range_contains_point_sql("valid_period")
        assert "valid_period_start" in sql
        assert "valid_period_end" in sql
        # Point is within range: start <= point AND end >= point

    def test_range_contains_range_sql(self):
        """Generate SQL for range containment check."""
        from declaro_persistum.abstractions.ranges import range_contains_range_sql

        sql = range_contains_range_sql("valid_period")
        # Outer contains inner: outer_start <= inner_start AND outer_end >= inner_end
        assert "valid_period_start" in sql
        assert "valid_period_end" in sql

    def test_range_adjacent_sql(self):
        """Generate SQL for adjacency check."""
        from declaro_persistum.abstractions.ranges import range_adjacent_sql

        sql = range_adjacent_sql("valid_period")
        # Adjacent: a_end = b_start OR a_start = b_end
        assert "valid_period_start" in sql
        assert "valid_period_end" in sql


class TestRangeNullSemantics:
    """Tests for NULL semantics in ranges."""

    def test_unbounded_start(self):
        """NULL start means unbounded lower bound."""
        from declaro_persistum.abstractions.ranges import range_contains_point_sql

        sql = range_contains_point_sql("period")
        # Should handle NULL start as -infinity
        assert "NULL" in sql or "COALESCE" in sql or "OR" in sql

    def test_unbounded_end(self):
        """NULL end means unbounded upper bound."""
        from declaro_persistum.abstractions.ranges import range_contains_point_sql

        sql = range_contains_point_sql("period")
        # Should handle NULL end as +infinity
        assert "NULL" in sql or "COALESCE" in sql or "OR" in sql


class TestRangeConversion:
    """Tests for converting to/from range representation."""

    def test_range_to_dict(self):
        """Convert range to dict."""
        from declaro_persistum.abstractions.ranges import range_to_dict
        from datetime import date

        result = range_to_dict(
            date(2024, 1, 1),
            date(2024, 12, 31),
            "valid_period"
        )
        assert result["valid_period_start"] == date(2024, 1, 1)
        assert result["valid_period_end"] == date(2024, 12, 31)

    def test_range_from_dict(self):
        """Convert dict to range tuple."""
        from declaro_persistum.abstractions.ranges import range_from_dict
        from datetime import date

        data = {
            "valid_period_start": date(2024, 1, 1),
            "valid_period_end": date(2024, 12, 31),
        }
        start, end = range_from_dict(data, "valid_period")
        assert start == date(2024, 1, 1)
        assert end == date(2024, 12, 31)

    def test_range_from_dict_unbounded(self):
        """Convert dict with NULL bounds."""
        from declaro_persistum.abstractions.ranges import range_from_dict
        from datetime import date

        data = {
            "valid_period_start": None,
            "valid_period_end": date(2024, 12, 31),
        }
        start, end = range_from_dict(data, "valid_period")
        assert start is None
        assert end == date(2024, 12, 31)


class TestRangeSQLite:
    """Tests for SQLite-specific range operations."""

    def test_range_overlaps_sqlite(self):
        """Generate SQLite-compatible overlap SQL."""
        from declaro_persistum.abstractions.ranges import range_overlaps_sql

        sql = range_overlaps_sql("period")
        # SQLite doesn't have special range operators
        assert "period_start" in sql
        assert "period_end" in sql
