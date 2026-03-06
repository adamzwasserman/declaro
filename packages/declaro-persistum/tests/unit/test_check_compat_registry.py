"""Unit tests for CHECK constraint registry."""

import pytest

from declaro_persistum.abstractions.check_compat import (
    clear_registry,
    get_affected_tables,
    get_table_validators,
    get_validation_stats,
    register_check_constraint,
    validate_row,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry before each test."""
    clear_registry()
    yield
    clear_registry()


class TestRegistry:
    """Test registry operations."""

    def test_register_and_validate(self):
        register_check_constraint("events", "end", "start <= end")

        is_valid, errors = validate_row("events", {"start": 5, "end": 10})
        assert is_valid
        assert not errors

    def test_validation_failure(self):
        register_check_constraint("events", "end", "start <= end")

        is_valid, errors = validate_row("events", {"start": 10, "end": 5})
        assert not is_valid
        assert len(errors) == 1
        assert "CHECK constraint failed" in errors[0]

    def test_no_constraints_passes(self):
        is_valid, errors = validate_row("unknown_table", {"a": 1})
        assert is_valid
        assert not errors

    def test_stats_tracking(self):
        register_check_constraint("t", "c", "c > 0")
        validate_row("t", {"c": 5})
        validate_row("t", {"c": -1})

        stats = get_validation_stats()
        assert stats["checks_registered"] == 1
        assert stats["validations_run"] == 2
        assert stats["validations_passed"] == 1
        assert stats["validations_failed"] == 1

    def test_affected_tables(self):
        register_check_constraint("users", "age", "age >= 18")
        register_check_constraint("orders", "total", "total > 0")

        affected = get_affected_tables()
        assert "users" in affected
        assert "orders" in affected

    def test_multiple_constraints_same_table(self):
        register_check_constraint("users", "age", "age >= 18")
        register_check_constraint("users", "score", "score >= 0")

        validators = get_table_validators("users")
        assert len(validators) == 2

    def test_clear_registry(self):
        register_check_constraint("t", "c", "c > 0")
        assert len(get_table_validators("t")) == 1

        clear_registry()
        assert len(get_table_validators("t")) == 0

        stats = get_validation_stats()
        assert stats["checks_registered"] == 0

    def test_invalid_expression_logs_warning(self, caplog):
        # This should log a warning but not raise
        register_check_constraint("t", "c", "INVALID SYNTAX @@")

        # Validation should pass (no validator registered)
        is_valid, errors = validate_row("t", {"c": 5})
        assert is_valid

        # Check that warning was logged
        assert any("Cannot parse CHECK" in record.message for record in caplog.records)


class TestMultipleConstraints:
    """Test tables with multiple CHECK constraints."""

    def test_all_pass(self):
        register_check_constraint("products", "price", "price > 0")
        register_check_constraint("products", "stock", "stock >= 0")

        is_valid, errors = validate_row(
            "products", {"price": 10, "stock": 5}, operation="INSERT"
        )
        assert is_valid
        assert not errors

    def test_one_fails(self):
        register_check_constraint("products", "price", "price > 0")
        register_check_constraint("products", "stock", "stock >= 0")

        is_valid, errors = validate_row(
            "products", {"price": -10, "stock": 5}, operation="INSERT"
        )
        assert not is_valid
        assert len(errors) == 1
        assert "price" in errors[0]

    def test_multiple_fail(self):
        register_check_constraint("products", "price", "price > 0")
        register_check_constraint("products", "stock", "stock >= 0")

        is_valid, errors = validate_row(
            "products", {"price": -10, "stock": -5}, operation="INSERT"
        )
        assert not is_valid
        assert len(errors) == 2

    def test_partial_row_validation(self):
        """Test validating only some columns (UPDATE scenario)."""
        register_check_constraint("products", "price", "price > 0")
        register_check_constraint("products", "stock", "stock >= 0")

        # Only updating price
        is_valid, errors = validate_row(
            "products", {"price": 10}, operation="UPDATE"
        )
        assert is_valid

        # Only updating stock with invalid value
        is_valid, errors = validate_row(
            "products", {"stock": -5}, operation="UPDATE"
        )
        assert not is_valid


class TestValidationStats:
    """Test validation statistics tracking."""

    def test_initial_stats(self):
        stats = get_validation_stats()
        assert stats["checks_registered"] == 0
        assert stats["validations_run"] == 0
        assert stats["validations_passed"] == 0
        assert stats["validations_failed"] == 0
        assert stats["total_constraints"] == 0
        assert stats["affected_tables"] == []

    def test_registration_increments(self):
        register_check_constraint("t1", "c1", "c1 > 0")
        stats = get_validation_stats()
        assert stats["checks_registered"] == 1
        assert stats["total_constraints"] == 1

        register_check_constraint("t2", "c2", "c2 > 0")
        stats = get_validation_stats()
        assert stats["checks_registered"] == 2
        assert stats["total_constraints"] == 2

    def test_validation_counters(self):
        register_check_constraint("t", "c", "c > 0")

        # Pass
        validate_row("t", {"c": 5})
        stats = get_validation_stats()
        assert stats["validations_run"] == 1
        assert stats["validations_passed"] == 1
        assert stats["validations_failed"] == 0

        # Fail
        validate_row("t", {"c": -5})
        stats = get_validation_stats()
        assert stats["validations_run"] == 2
        assert stats["validations_passed"] == 1
        assert stats["validations_failed"] == 1

        # Pass
        validate_row("t", {"c": 10})
        stats = get_validation_stats()
        assert stats["validations_run"] == 3
        assert stats["validations_passed"] == 2
        assert stats["validations_failed"] == 1

    def test_no_constraints_still_counts(self):
        # Validation on table without constraints
        validate_row("empty_table", {"a": 1})

        stats = get_validation_stats()
        assert stats["validations_run"] == 1
        assert stats["validations_passed"] == 1


class TestGetTableValidators:
    """Test getting validators for a table."""

    def test_empty_table(self):
        validators = get_table_validators("nonexistent")
        assert len(validators) == 0
        assert isinstance(validators, frozenset)

    def test_single_validator(self):
        register_check_constraint("t", "c", "c > 0")
        validators = get_table_validators("t")
        assert len(validators) == 1

        # Unpack the validator
        col_name, expr, validator_fn = list(validators)[0]
        assert col_name == "c"
        assert expr == "c > 0"
        assert callable(validator_fn)

    def test_multiple_validators(self):
        register_check_constraint("t", "c1", "c1 > 0")
        register_check_constraint("t", "c2", "c2 < 100")

        validators = get_table_validators("t")
        assert len(validators) == 2

        # Check column names
        col_names = {v[0] for v in validators}
        assert col_names == {"c1", "c2"}

    def test_validators_are_frozen(self):
        register_check_constraint("t", "c", "c > 0")
        validators = get_table_validators("t")

        # Should be immutable
        assert isinstance(validators, frozenset)


class TestOperationParameter:
    """Test the operation parameter in validate_row."""

    def test_insert_operation(self):
        register_check_constraint("t", "c", "c > 0")

        is_valid, errors = validate_row("t", {"c": -1}, operation="INSERT")
        assert not is_valid
        # Operation doesn't affect validation logic, just error messages

    def test_update_operation(self):
        register_check_constraint("t", "c", "c > 0")

        is_valid, errors = validate_row("t", {"c": -1}, operation="UPDATE")
        assert not is_valid

    def test_default_operation(self):
        register_check_constraint("t", "c", "c > 0")

        # Default is INSERT
        is_valid, errors = validate_row("t", {"c": -1})
        assert not is_valid
