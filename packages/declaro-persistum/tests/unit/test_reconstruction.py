"""
Unit tests for abstractions/reconstruction.py

Tests the table reconstruction abstraction for SQLite/Turso ALTER operations.
"""

import pytest
import aiosqlite

from declaro_persistum.abstractions.reconstruction import (
    execute_reconstruction_async,
    generate_create_table_sql,
    generate_data_copy_sql,
    get_reconstruction_columns,
)
from declaro_persistum.types import Column, Operation


# =============================================================================
# Pure Function Tests (No DB)
# =============================================================================




# =============================================================================
# Integration Tests (With DB)
# =============================================================================




# Note: Sync tests would be similar but use synchronous connection
# Turso Database/pyturso would be needed for actual sync testing
# For now, the async tests cover the core logic since both implementations
# share the same pure functions and follow the same flow
