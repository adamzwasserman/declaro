"""
BDD test fixtures and configuration.
"""

import pytest
from typing import Any

from declaro_persistum.types import Schema

from tests.bdd.factories.schema_factory import (
    simple_todos_schema,
    simple_users_schema,
    complex_ecommerce_schema,
    SchemaFactory,
)
from tests.bdd.factories.data_factory import TodoFactory, UserFactory
from tests.bdd.factories.connection_factory import (
    ConnectionFactory,
    get_sqlite_connection,
    get_postgresql_connection,
)


# =============================================================================
# Schema Fixtures
# =============================================================================


@pytest.fixture
def todos_schema() -> Schema:
    """Simple todos schema."""
    return simple_todos_schema()


@pytest.fixture
def users_schema() -> Schema:
    """Simple users schema."""
    return simple_users_schema()


@pytest.fixture
def ecommerce_schema() -> Schema:
    """Complex e-commerce schema."""
    return complex_ecommerce_schema()


@pytest.fixture
def schema_factory() -> type[SchemaFactory]:
    """Schema factory class for building custom schemas."""
    return SchemaFactory


# =============================================================================
# Data Factory Fixtures
# =============================================================================


@pytest.fixture
def todo_factory() -> type[TodoFactory]:
    """Todo data factory."""
    return TodoFactory


@pytest.fixture
def user_factory() -> type[UserFactory]:
    """User data factory."""
    return UserFactory


@pytest.fixture
def sample_todos():
    """Generate sample todos for testing."""
    return TodoFactory.create_batch(10)


@pytest.fixture
def sample_users():
    """Generate sample users for testing."""
    return UserFactory.create_batch(10)


# =============================================================================
# Connection Fixtures
# =============================================================================


@pytest.fixture
def sqlite_factory() -> ConnectionFactory:
    """SQLite connection factory."""
    return ConnectionFactory.sqlite()


@pytest.fixture
def postgresql_factory() -> ConnectionFactory:
    """PostgreSQL connection factory."""
    return ConnectionFactory.postgresql()


@pytest.fixture
async def sqlite_connection():
    """Get an SQLite connection."""
    async with get_sqlite_connection() as conn:
        yield conn


@pytest.fixture
async def postgresql_connection(require_postgresql):
    """Get a PostgreSQL connection (requires real database)."""
    async with get_postgresql_connection() as conn:
        yield conn


# =============================================================================
# BDD Context Fixture
# =============================================================================


class BDDContext:
    """Shared context for BDD scenarios."""

    def __init__(self):
        self.schema: Schema | None = None
        self.results: list[dict[str, Any]] = []
        self.error: Exception | None = None
        self.connection: Any = None
        self.connection_factory: Any = None
        self.dialect: str = "sqlite"

    def reset(self):
        """Reset context for new scenario."""
        self.schema = None
        self.results = []
        self.error = None


@pytest.fixture
def bdd_context() -> BDDContext:
    """Shared context for BDD scenarios."""
    return BDDContext()


# =============================================================================
# Scenario Hooks
# =============================================================================


@pytest.fixture(autouse=True)
def reset_schema_between_tests():
    """Reset state between tests."""
    yield
