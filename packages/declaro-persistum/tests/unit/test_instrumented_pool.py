"""
Tests for instrumented pool and write queue functionality.

Uses real SQLite in-memory databases — no mocks.
"""

import asyncio
import os
import tempfile
import pytest

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.instrumentation import classify_sql, is_write_op






