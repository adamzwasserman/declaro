"""Unit tests for Turso applier CHECK constraint handling.

Turso now supports CHECK constraints natively, so the applier emits
CHECK clauses directly in SQL (no longer requires Python-side emulation).
"""

import pytest

from declaro_persistum.abstractions.check_compat import (
    clear_registry,
    get_affected_tables,
    get_table_validators,
)
from declaro_persistum.applier.turso import TursoApplier


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry before each test."""
    clear_registry()
    yield
    clear_registry()


