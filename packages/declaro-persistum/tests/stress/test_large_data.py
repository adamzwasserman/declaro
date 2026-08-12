"""
Large data and property-based tests for declaro_persistum.

Uses Hypothesis for property-based testing to verify correctness
with arbitrary inputs.
"""

import pytest
import uuid
from hypothesis import given, settings, strategies as st, assume

from declaro_persistum.query.table import table

from tests.bdd.factories.schema_factory import simple_todos_schema, simple_users_schema
from tests.bdd.factories.data_factory import (
    sql_safe_text,
    sql_integer,
    sql_boolean,
    sql_uuid,
)












