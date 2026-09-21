"""
BDD test fixtures and configuration.
"""

from typing import Any, TypedDict

import pytest

# =============================================================================
# Scenario context — data passed between Given, When and Then
# =============================================================================
#
# This was `BDDContext`, a class whose `__init__` assigned six fields and
# whose only method was `reset()`. Gherkin steps genuinely do need somewhere
# to put what the Given established so the Then can read it, but that is a
# BAG OF FACTS, not an object: nothing about it has behavior.
#
# As a TypedDict the steps read `bdd_context["engine"]` instead of
# `bdd_context["engine"]`, which is longer by two characters and honest about
# what it is. `reset()` is gone — pytest builds a fresh one per scenario, so
# a method that re-blanks three of six fields was a second, partial way to do
# what the fixture already does completely.


class ScenarioContext(TypedDict, total=False):
    """What one scenario knows so far. Every key optional; steps fill them in."""

    results: list[dict[str, Any]]
    error: Exception | None
    connection: Any
    backend: Any
    engine: str
    sql: str
    last_inserted_id: Any


def new_context() -> ScenarioContext:
    """A context with nothing established yet.

    `engine` is the one field with a starting value, because a scenario that
    never names a backend is running on SQLite by the feature files' own
    convention.
    """
    return {
        "results": [],
        "error": None,
        "connection": None,
        "backend": None,
        "engine": "sqlite",
        "sql": "",
        "last_inserted_id": None,
    }


# =============================================================================
# Schema Fixtures
# =============================================================================


# `schema_factory` returned the SchemaFactory CLASS so a step could call
# static methods off it. The schema builders are module-level functions and
# a test imports the one it wants; a fixture that hands back a namespace is
# indirection with nothing in it.


# =============================================================================
# Data Factory Fixtures
# =============================================================================


# =============================================================================
# Connection Fixtures
# =============================================================================


# =============================================================================
# BDD Context Fixture
# =============================================================================


@pytest.fixture
def bdd_context() -> ScenarioContext:
    """Shared context for BDD scenarios."""
    return new_context()


# =============================================================================
# Row builders, as fixtures
# =============================================================================
#
# pytest_bdd hands a step its fixtures by name, so a step written as
# `given_todos_with_rows(bdd_context, count, todo_factory)` needs a fixture
# called `todo_factory`. It is the batch FUNCTION, not a class to call methods
# on and not a row: the step asks for `count` rows and calls it with that.
