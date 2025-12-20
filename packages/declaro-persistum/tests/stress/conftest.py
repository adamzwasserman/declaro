"""
Stress test fixtures and configuration.
"""

import pytest
import os

from tests.bdd.factories.connection_factory import (
    ConnectionFactory,
    get_sqlite_connection,
    get_postgresql_connection,
)
from tests.bdd.factories.schema_factory import simple_todos_schema, simple_users_schema
from tests.bdd.factories.data_factory import TodoFactory, UserFactory


@pytest.fixture
def sqlite_factory() -> ConnectionFactory:
    """SQLite connection factory for stress tests."""
    return ConnectionFactory.sqlite()


@pytest.fixture
def postgresql_factory(require_postgresql) -> ConnectionFactory:
    """PostgreSQL connection factory for stress tests (requires real DB)."""
    return ConnectionFactory.postgresql()


@pytest.fixture
def todos_schema():
    """Simple todos schema."""
    return simple_todos_schema()


@pytest.fixture
def users_schema():
    """Simple users schema."""
    return simple_users_schema()


@pytest.fixture
def todo_factory():
    """Todo data factory."""
    return TodoFactory


@pytest.fixture
def user_factory():
    """User data factory."""
    return UserFactory


@pytest.fixture
def large_todo_batch():
    """Generate large batch of todos for stress testing."""
    return TodoFactory.create_batch(10000)


@pytest.fixture
def edge_case_todos():
    """Generate todos with edge case data."""
    return TodoFactory.create_with_edge_cases()
