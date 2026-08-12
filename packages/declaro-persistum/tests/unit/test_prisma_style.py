"""
Unit tests for Prisma-style query API.

Tests the PrismaQueryBuilder class and Prisma-style methods on TableProxy.
"""

import pytest
from typing import Any

from declaro_persistum.types import Schema
from declaro_persistum.query import table
from declaro_persistum.query.prisma_style import PrismaQueryBuilder


@pytest.fixture
def users_schema() -> Schema:
    """Schema with users table."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True},
                "email": {"type": "text", "nullable": False},
                "name": {"type": "text"},
                "status": {"type": "text", "default": "'active'"},
                "age": {"type": "integer"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            }
        }
    }








