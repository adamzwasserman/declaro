"""
Load testing for declaro_persistum.

Tests query building performance with large numbers of queries
and verifies memory usage stays reasonable.
"""

import pytest
import time
import uuid

from declaro_persistum.query.table import table

from tests.bdd.factories.schema_factory import simple_todos_schema, complex_ecommerce_schema
from tests.bdd.factories.data_factory import TodoFactory








