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










