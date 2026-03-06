"""Unit tests for CHECK validator generator."""

import pytest

from declaro_persistum.abstractions.check_compat import (
    generate_validator,
    parse_check_expression,
)


class TestCompareValidator:
    """Test validators for comparison expressions."""

    def test_column_comparison_valid(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        is_valid, error = validator({"start": 5, "end": 10})
        assert is_valid
        assert error is None

    def test_column_comparison_invalid(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        is_valid, error = validator({"start": 10, "end": 5})
        assert not is_valid
        assert "CHECK constraint failed" in error

    def test_null_handling_left(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        # NULL should pass (SQL semantics)
        is_valid, error = validator({"start": None, "end": 10})
        assert is_valid

    def test_null_handling_right(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        is_valid, error = validator({"start": 5, "end": None})
        assert is_valid

    def test_value_comparison_valid(self):
        ast = parse_check_expression("age >= 18", "age")
        validator = generate_validator(ast, "users", "age", "age >= 18")

        is_valid, error = validator({"age": 25})
        assert is_valid

    def test_value_comparison_invalid(self):
        ast = parse_check_expression("age >= 18", "age")
        validator = generate_validator(ast, "users", "age", "age >= 18")

        is_valid, error = validator({"age": 15})
        assert not is_valid
        assert "CHECK constraint failed" in error

    def test_equality_valid(self):
        ast = parse_check_expression("status = 'active'", "status")
        validator = generate_validator(ast, "orders", "status", "status = 'active'")

        is_valid, error = validator({"status": "active"})
        assert is_valid

    def test_equality_invalid(self):
        ast = parse_check_expression("status = 'active'", "status")
        validator = generate_validator(ast, "orders", "status", "status = 'active'")

        is_valid, error = validator({"status": "pending"})
        assert not is_valid

    def test_not_equal_valid(self):
        ast = parse_check_expression("status != 'deleted'", "status")
        validator = generate_validator(
            ast, "orders", "status", "status != 'deleted'"
        )

        is_valid, error = validator({"status": "active"})
        assert is_valid

    def test_not_equal_invalid(self):
        ast = parse_check_expression("status != 'deleted'", "status")
        validator = generate_validator(
            ast, "orders", "status", "status != 'deleted'"
        )

        is_valid, error = validator({"status": "deleted"})
        assert not is_valid

    def test_less_than_valid(self):
        ast = parse_check_expression("price < 1000", "price")
        validator = generate_validator(ast, "products", "price", "price < 1000")

        is_valid, error = validator({"price": 500})
        assert is_valid

    def test_less_than_invalid(self):
        ast = parse_check_expression("price < 1000", "price")
        validator = generate_validator(ast, "products", "price", "price < 1000")

        is_valid, error = validator({"price": 1500})
        assert not is_valid

    def test_greater_than_valid(self):
        ast = parse_check_expression("count > 0", "count")
        validator = generate_validator(ast, "inventory", "count", "count > 0")

        is_valid, error = validator({"count": 5})
        assert is_valid

    def test_greater_than_invalid(self):
        ast = parse_check_expression("count > 0", "count")
        validator = generate_validator(ast, "inventory", "count", "count > 0")

        is_valid, error = validator({"count": 0})
        assert not is_valid

    def test_type_mismatch_skips_validation(self):
        ast = parse_check_expression("count > 0", "count")
        validator = generate_validator(ast, "inventory", "count", "count > 0")

        # Type mismatch should skip validation (return valid)
        is_valid, error = validator({"count": "text"})
        assert is_valid


class TestInValidator:
    """Test validators for IN expressions."""

    def test_valid_value(self):
        ast = parse_check_expression("status IN ('a', 'b')", "status")
        validator = generate_validator(
            ast, "orders", "status", "status IN ('a', 'b')"
        )

        is_valid, error = validator({"status": "a"})
        assert is_valid

    def test_invalid_value(self):
        ast = parse_check_expression("status IN ('a', 'b')", "status")
        validator = generate_validator(
            ast, "orders", "status", "status IN ('a', 'b')"
        )

        is_valid, error = validator({"status": "c"})
        assert not is_valid
        assert "CHECK constraint failed" in error

    def test_null_passes(self):
        ast = parse_check_expression("status IN ('a', 'b')", "status")
        validator = generate_validator(
            ast, "orders", "status", "status IN ('a', 'b')"
        )

        is_valid, error = validator({"status": None})
        assert is_valid

    def test_numeric_values(self):
        ast = parse_check_expression("priority IN (1, 2, 3)", "priority")
        validator = generate_validator(
            ast, "tasks", "priority", "priority IN (1, 2, 3)"
        )

        is_valid, error = validator({"priority": 2})
        assert is_valid

        is_valid, error = validator({"priority": 5})
        assert not is_valid


class TestBetweenValidator:
    """Test validators for BETWEEN expressions."""

    def test_valid_in_range(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": 500})
        assert is_valid

    def test_valid_at_lower_bound(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": 0})
        assert is_valid

    def test_valid_at_upper_bound(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": 1000})
        assert is_valid

    def test_invalid_below_range(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": -10})
        assert not is_valid

    def test_invalid_above_range(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": 1500})
        assert not is_valid

    def test_null_passes(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        validator = generate_validator(
            ast, "products", "price", "price BETWEEN 0 AND 1000"
        )

        is_valid, error = validator({"price": None})
        assert is_valid

    def test_float_values(self):
        ast = parse_check_expression("rating BETWEEN 0.0 AND 5.0", "rating")
        validator = generate_validator(
            ast, "reviews", "rating", "rating BETWEEN 0.0 AND 5.0"
        )

        is_valid, error = validator({"rating": 3.5})
        assert is_valid

        is_valid, error = validator({"rating": 6.0})
        assert not is_valid


class TestNullValidator:
    """Test validators for NULL check expressions."""

    def test_is_null_valid(self):
        ast = parse_check_expression("deleted_at IS NULL", "deleted_at")
        validator = generate_validator(
            ast, "users", "deleted_at", "deleted_at IS NULL"
        )

        is_valid, error = validator({"deleted_at": None})
        assert is_valid

    def test_is_null_invalid(self):
        ast = parse_check_expression("deleted_at IS NULL", "deleted_at")
        validator = generate_validator(
            ast, "users", "deleted_at", "deleted_at IS NULL"
        )

        is_valid, error = validator({"deleted_at": "2024-01-01"})
        assert not is_valid

    def test_is_not_null_valid(self):
        ast = parse_check_expression("created_at IS NOT NULL", "created_at")
        validator = generate_validator(
            ast, "users", "created_at", "created_at IS NOT NULL"
        )

        is_valid, error = validator({"created_at": "2024-01-01"})
        assert is_valid

    def test_is_not_null_invalid(self):
        ast = parse_check_expression("created_at IS NOT NULL", "created_at")
        validator = generate_validator(
            ast, "users", "created_at", "created_at IS NOT NULL"
        )

        is_valid, error = validator({"created_at": None})
        assert not is_valid


class TestCompoundValidator:
    """Test validators for compound boolean expressions."""

    def test_and_both_valid(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 AND b > 0")

        is_valid, error = validator({"a": 5, "b": 10})
        assert is_valid

    def test_and_left_invalid(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 AND b > 0")

        is_valid, error = validator({"a": -5, "b": 10})
        assert not is_valid

    def test_and_right_invalid(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 AND b > 0")

        is_valid, error = validator({"a": 5, "b": -10})
        assert not is_valid

    def test_and_both_invalid(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 AND b > 0")

        is_valid, error = validator({"a": -5, "b": -10})
        assert not is_valid

    def test_or_both_valid(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 OR b > 0")

        is_valid, error = validator({"a": 5, "b": 10})
        assert is_valid

    def test_or_left_valid(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 OR b > 0")

        is_valid, error = validator({"a": 5, "b": -10})
        assert is_valid

    def test_or_right_valid(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 OR b > 0")

        is_valid, error = validator({"a": -5, "b": 10})
        assert is_valid

    def test_or_both_invalid(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        validator = generate_validator(ast, "test", "a", "a > 0 OR b > 0")

        is_valid, error = validator({"a": -5, "b": -10})
        assert not is_valid

    def test_not_valid(self):
        ast = parse_check_expression("NOT (x = 0)", "x")
        validator = generate_validator(ast, "test", "x", "NOT (x = 0)")

        is_valid, error = validator({"x": 5})
        assert is_valid

    def test_not_invalid(self):
        ast = parse_check_expression("NOT (x = 0)", "x")
        validator = generate_validator(ast, "test", "x", "NOT (x = 0)")

        is_valid, error = validator({"x": 0})
        assert not is_valid

    def test_complex_expression(self):
        ast = parse_check_expression("a > 0 AND (b > 0 OR c > 0)", "a")
        validator = generate_validator(
            ast, "test", "a", "a > 0 AND (b > 0 OR c > 0)"
        )

        # a > 0, b > 0 -> valid
        is_valid, error = validator({"a": 5, "b": 10, "c": -10})
        assert is_valid

        # a > 0, c > 0 -> valid
        is_valid, error = validator({"a": 5, "b": -10, "c": 10})
        assert is_valid

        # a <= 0 -> invalid
        is_valid, error = validator({"a": -5, "b": 10, "c": 10})
        assert not is_valid

        # a > 0, b <= 0, c <= 0 -> invalid
        is_valid, error = validator({"a": 5, "b": -10, "c": -10})
        assert not is_valid
