"""
Common BDD step definitions used across features.
"""

import pytest
from pytest_bdd import given, when, then, parsers

from declaro_persistum.types import Schema

from tests.bdd.factories.schema_factory import (
    simple_todos_schema,
    simple_users_schema,
    complex_ecommerce_schema,
    SchemaFactory,
)


# =============================================================================
# Schema Setup Steps
# =============================================================================


@given("an empty schema")
def given_empty_schema(bdd_context):
    """Set up an empty schema."""
    bdd_context.schema = {}


@given("a complex e-commerce schema")
def given_ecommerce_schema(bdd_context):
    """Set up complex e-commerce schema."""
    bdd_context.schema = complex_ecommerce_schema()


# =============================================================================
# Result Verification Steps
# =============================================================================


@then(parsers.parse('I should find exactly {count:d} result'))
@then(parsers.parse('I should find exactly {count:d} results'))
def then_result_count(bdd_context, count: int):
    """Verify result count."""
    assert len(bdd_context.results) == count, f"Expected {count} results, got {len(bdd_context.results)}"


@then("the query should execute successfully")
def then_query_succeeds(bdd_context):
    """Verify query executed without error."""
    assert bdd_context.error is None, f"Query failed with error: {bdd_context.error}"


@then("the results should match the expected filter")
def then_results_match(bdd_context):
    """Verify results match expected filter (placeholder for complex verification)."""
    assert bdd_context.results is not None


@then("the table should be empty")
def then_table_empty(bdd_context):
    """Verify table is empty by querying for all rows."""
    import asyncio

    async def _query():
        async with bdd_context.connection_factory.get_connection() as conn:
            table_name = list(bdd_context.schema.keys())[0]
            if bdd_context.dialect == "sqlite":
                cursor = await conn.execute(f"SELECT * FROM {table_name}")
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            elif bdd_context.dialect == "postgresql":
                rows = await conn.fetch(f"SELECT * FROM {table_name}")
                return [dict(row) for row in rows]
            elif bdd_context.dialect == "turso":
                cursor = conn.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                return list(rows)
            return []

    results = asyncio.run(_query())
    assert len(results) == 0, f"Expected empty table, got {len(results)} rows"


# =============================================================================
# Error Handling Steps
# =============================================================================


@then(parsers.parse('an error should be raised with message "{message}"'))
def then_error_with_message(bdd_context, message: str):
    """Verify error was raised with message."""
    assert bdd_context.error is not None, "Expected an error to be raised"
    assert message in str(bdd_context.error), f"Expected '{message}' in error: {bdd_context.error}"


@then("no error should be raised")
def then_no_error(bdd_context):
    """Verify no error was raised."""
    assert bdd_context.error is None, f"Unexpected error: {bdd_context.error}"
