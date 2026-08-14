"""
BDD test fixtures and configuration.
"""

from typing import Any, TypedDict

import pytest

from declaro_persistum.types import Schema

# =============================================================================
# Scenario context — data passed between Given, When and Then
# =============================================================================
#
# This was `BDDContext`, a class whose `__init__` assigned six fields and
# whose only method was `reset()`. Gherkin steps genuinely do need somewhere
# to put what the Given established so the Then can read it, but that is a
# BAG OF FACTS, not an object: nothing about it has behaviour.
#
# As a TypedDict the steps read `bdd_context["dialect"]` instead of
# `bdd_context["dialect"]`, which is longer by two characters and honest about
# what it is. `reset()` is gone — pytest builds a fresh one per scenario, so
# a method that re-blanks three of six fields was a second, partial way to do
# what the fixture already does completely.


class ScenarioContext(TypedDict, total=False):
    """What one scenario knows so far. Every key optional; steps fill them in."""

    schema: Schema | None
    results: list[dict[str, Any]]
    error: Exception | None
    connection: Any
    backend: Any
    dialect: str
    sql: str
    last_inserted_id: Any


def new_context() -> ScenarioContext:
    """A context with nothing established yet.

    `dialect` is the one field with a starting value, because a scenario that
    never names a backend is running on SQLite by the feature files' own
    convention.
    """
    return {
        "schema": None,
        "results": [],
        "error": None,
        "connection": None,
        "backend": None,
        "dialect": "sqlite",
        "sql": "",
        "last_inserted_id": None,
    }



from tests.bdd.factories.connection_factory import (
    get_postgresql_connection,
    get_sqlite_connection,
    postgresql_backend,
    sqlite_backend,
)
from tests.bdd.factories.data_factory import todos, users
from tests.bdd.factories.schema_factory import (
    complex_ecommerce_schema,
    simple_todos_schema,
    simple_users_schema,
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


# `schema_factory` returned the SchemaFactory CLASS so a step could call
# static methods off it. The schema builders are module-level functions and
# a test imports the one it wants; a fixture that hands back a namespace is
# indirection with nothing in it.


# =============================================================================
# Data Factory Fixtures
# =============================================================================


@pytest.fixture
def todo_factory():
    """The `todos(n)` builder, for steps that take a factory by name.

    pytest_bdd steps receive fixtures positionally, so a step written as
    `given_todos_with_rows(bdd_context, count, todo_factory)` needs a fixture
    of that name. It is now the FUNCTION, not a class to call methods on.
    """
    return todos


@pytest.fixture
def user_factory():
    return users


@pytest.fixture
def sample_todos() -> list[dict[str, Any]]:
    return todos(10)


@pytest.fixture
def sample_users() -> list[dict[str, Any]]:
    return users(10)


# =============================================================================
# Connection Fixtures
# =============================================================================


@pytest.fixture
def sqlite_factory():
    """The SQLite backend, as data — connect, setup and teardown callables."""
    return sqlite_backend()


@pytest.fixture
def postgresql_factory():
    return postgresql_backend()


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




@pytest.fixture
def bdd_context() -> ScenarioContext:
    """Shared context for BDD scenarios."""
    return new_context()


# =============================================================================
# Scenario Hooks
# =============================================================================


@pytest.fixture(autouse=True)
def reset_schema_between_tests():
    """Reset state between tests."""
    yield
