"""
Edge case and corner case tests.

These tests verify correct handling of boundary conditions,
special characters, NULL values, and other edge cases.
"""

import pytest
import uuid
from datetime import datetime, timezone

from declaro_persistum.query.table import table

from tests.bdd.factories.data_factory import (
    EDGE_CASE_STRINGS,
    EDGE_CASE_INTEGERS,
    EDGE_CASE_DATES,
    TodoFactory,
)
from tests.bdd.factories.schema_factory import simple_todos_schema, simple_users_schema












