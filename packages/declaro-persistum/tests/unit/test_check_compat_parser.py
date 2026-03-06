"""Unit tests for CHECK expression parser."""

import pytest

from declaro_persistum.abstractions.check_compat import (
    CheckParseError,
    parse_check_expression,
)


class TestParseComparison:
    """Test parsing comparison expressions."""

    def test_simple_comparison(self):
        ast = parse_check_expression("start <= end", "end")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "start"}
        assert ast["operator"] == "<="
        assert ast["right"] == {"_column": "end"}

    def test_value_comparison(self):
        ast = parse_check_expression("age >= 18", "age")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "age"}
        assert ast["operator"] == ">="
        assert ast["right"] == 18

    def test_equality_comparison(self):
        ast = parse_check_expression("status = 'active'", "status")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "status"}
        assert ast["operator"] == "="
        assert ast["right"] == "active"  # String literal, not column

    def test_not_equal_comparison(self):
        ast = parse_check_expression("status != 'deleted'", "status")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "status"}
        assert ast["operator"] == "!="
        assert ast["right"] == "deleted"  # String literal

    def test_less_than(self):
        ast = parse_check_expression("price < 1000", "price")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "price"}
        assert ast["operator"] == "<"
        assert ast["right"] == 1000

    def test_greater_than(self):
        ast = parse_check_expression("count > 0", "count")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "count"}
        assert ast["operator"] == ">"
        assert ast["right"] == 0

    def test_float_comparison(self):
        ast = parse_check_expression("rating >= 3.5", "rating")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "rating"}
        assert ast["operator"] == ">="
        assert ast["right"] == 3.5

    def test_negative_number(self):
        ast = parse_check_expression("balance >= -100", "balance")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "balance"}
        assert ast["operator"] == ">="
        assert ast["right"] == -100


class TestParseIn:
    """Test parsing IN clause expressions."""

    def test_string_values(self):
        ast = parse_check_expression("status IN ('a', 'b', 'c')", "status")
        assert ast["op"] == "in"
        assert ast["left"] == {"_column": "status"}
        assert ast["values"] == ["a", "b", "c"]

    def test_numeric_values(self):
        ast = parse_check_expression("priority IN (1, 2, 3)", "priority")
        assert ast["op"] == "in"
        assert ast["left"] == {"_column": "priority"}
        assert ast["values"] == [1, 2, 3]

    def test_single_value(self):
        ast = parse_check_expression("type IN ('premium')", "type")
        assert ast["op"] == "in"
        assert ast["left"] == {"_column": "type"}
        assert ast["values"] == ["premium"]

    def test_mixed_quotes(self):
        ast = parse_check_expression('status IN ("active", "pending")', "status")
        assert ast["op"] == "in"
        assert ast["left"] == {"_column": "status"}
        assert ast["values"] == ["active", "pending"]


class TestParseBetween:
    """Test parsing BETWEEN expressions."""

    def test_numeric_between(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        assert ast["op"] == "between"
        assert ast["left"] == {"_column": "price"}
        assert ast["low"] == 0
        assert ast["high"] == 1000

    def test_float_between(self):
        ast = parse_check_expression("rating BETWEEN 0.0 AND 5.0", "rating")
        assert ast["op"] == "between"
        assert ast["left"] == {"_column": "rating"}
        assert ast["low"] == 0.0
        assert ast["high"] == 5.0

    def test_negative_range(self):
        ast = parse_check_expression("temperature BETWEEN -10 AND 40", "temperature")
        assert ast["op"] == "between"
        assert ast["left"] == {"_column": "temperature"}
        assert ast["low"] == -10
        assert ast["high"] == 40


class TestParseNull:
    """Test parsing NULL check expressions."""

    def test_is_null(self):
        ast = parse_check_expression("deleted_at IS NULL", "deleted_at")
        assert ast["op"] == "is_null"
        assert ast["operand"] == {"_column": "deleted_at"}

    def test_is_not_null(self):
        ast = parse_check_expression("created_at IS NOT NULL", "created_at")
        assert ast["op"] == "is_not_null"
        assert ast["operand"] == {"_column": "created_at"}


class TestParseCompound:
    """Test parsing compound boolean expressions."""

    def test_and(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        assert ast["op"] == "and"
        assert ast["left"]["op"] == "compare"
        assert ast["right"]["op"] == "compare"

    def test_or(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        assert ast["op"] == "or"
        assert ast["left"]["op"] == "compare"
        assert ast["right"]["op"] == "compare"

    def test_not(self):
        ast = parse_check_expression("NOT (x = y)", "x")
        assert ast["op"] == "not"
        assert ast["operand"]["op"] == "compare"

    def test_complex_and_or(self):
        ast = parse_check_expression("a > 0 AND (b > 0 OR c > 0)", "a")
        assert ast["op"] == "and"
        assert ast["left"]["op"] == "compare"
        assert ast["right"]["op"] == "or"

    def test_multiple_and(self):
        ast = parse_check_expression("a > 0 AND b > 0 AND c > 0", "a")
        assert ast["op"] == "and"
        # Left is an AND of (a > 0) AND (b > 0)
        # Right is (c > 0)
        assert ast["left"]["op"] == "and"
        assert ast["right"]["op"] == "compare"


class TestParseParentheses:
    """Test parsing parenthesized expressions."""

    def test_parenthesized_comparison(self):
        ast = parse_check_expression("(start <= end)", "end")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "start"}
        assert ast["operator"] == "<="
        assert ast["right"] == {"_column": "end"}

    def test_nested_parentheses(self):
        ast = parse_check_expression("((a > 0))", "a")
        assert ast["op"] == "compare"
        assert ast["left"] == {"_column": "a"}
        assert ast["operator"] == ">"
        assert ast["right"] == 0


class TestParseErrors:
    """Test error handling in parser."""

    def test_empty_expression(self):
        with pytest.raises(CheckParseError) as exc_info:
            parse_check_expression("", "col")
        assert "Empty expression" in str(exc_info.value)

    def test_invalid_syntax(self):
        with pytest.raises(CheckParseError):
            parse_check_expression("start <= <= end", "end")

    def test_missing_closing_paren(self):
        with pytest.raises(CheckParseError) as exc_info:
            parse_check_expression("(a > 0", "a")
        assert "closing parenthesis" in str(exc_info.value)

    def test_missing_and_in_between(self):
        with pytest.raises(CheckParseError) as exc_info:
            parse_check_expression("price BETWEEN 0 100", "price")
        assert "AND" in str(exc_info.value)

    def test_missing_values_in_in(self):
        with pytest.raises(CheckParseError):
            parse_check_expression("status IN", "status")

    def test_unexpected_character(self):
        with pytest.raises(CheckParseError) as exc_info:
            parse_check_expression("a @ b", "a")
        assert "Unexpected character" in str(exc_info.value)


class TestCaseInsensitivity:
    """Test that keywords are case-insensitive."""

    def test_lowercase_and(self):
        ast = parse_check_expression("a > 0 and b > 0", "a")
        assert ast["op"] == "and"

    def test_mixed_case_or(self):
        ast = parse_check_expression("a > 0 Or b > 0", "a")
        assert ast["op"] == "or"

    def test_uppercase_between(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        assert ast["op"] == "between"

    def test_mixed_case_in(self):
        ast = parse_check_expression("status In ('a', 'b')", "status")
        assert ast["op"] == "in"

    def test_mixed_case_null(self):
        ast = parse_check_expression("deleted_at Is Null", "deleted_at")
        assert ast["op"] == "is_null"
