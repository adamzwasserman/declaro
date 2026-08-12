"""
Concurrent operation tests for declaro_persistum.

Tests thread-safety and concurrent query building.
"""

import pytest
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from declaro_persistum.query.table import table

from tests.bdd.factories.schema_factory import simple_todos_schema






