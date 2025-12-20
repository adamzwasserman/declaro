"""
Common BDD step definitions used across features.
"""

import pytest
from pytest_bdd import given, when, then, parsers

from declaro_persistum.query.table import table, set_default_schema
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


@given("a schema with a \"todos\" table")
def given_todos_schema(bdd_context):
    """Set up a todos schema."""
    bdd_context.schema = simple_todos_schema()
    set_default_schema(bdd_context.schema)
    bdd_context.table_proxy = table("todos")


@given("a schema with a \"users\" table")
def given_users_schema(bdd_context):
    """Set up a users schema."""
    bdd_context.schema = simple_users_schema()
    set_default_schema(bdd_context.schema)
    bdd_context.table_proxy = table("users")


@given(parsers.parse('the {table_name} table has columns: {columns}'))
def given_table_columns(bdd_context, table_name: str, columns: str):
    """Verify or set up table columns."""
    column_names = [c.strip() for c in columns.split(",")]
    if bdd_context.schema and table_name in bdd_context.schema:
        # Verify columns exist
        existing_columns = set(bdd_context.schema[table_name]["columns"].keys())
        for col in column_names:
            if col not in existing_columns:
                # Add missing column
                bdd_context.schema[table_name]["columns"][col] = {"type": "text"}
        set_default_schema(bdd_context.schema)


@given("an empty schema")
def given_empty_schema(bdd_context):
    """Set up an empty schema."""
    bdd_context.schema = {}
    set_default_schema(bdd_context.schema)


@given("a complex e-commerce schema")
def given_ecommerce_schema(bdd_context):
    """Set up complex e-commerce schema."""
    bdd_context.schema = complex_ecommerce_schema()
    set_default_schema(bdd_context.schema)


# =============================================================================
# Table Setup Steps
# =============================================================================


@given(parsers.parse('a {table_name} table with Django-style interface'))
def given_django_table(bdd_context, table_name: str):
    """Set up a table with Django-style interface."""
    if not bdd_context.schema:
        bdd_context.schema = simple_users_schema() if table_name == "users" else simple_todos_schema()
        set_default_schema(bdd_context.schema)
    bdd_context.table_proxy = table(table_name)


@given(parsers.parse('a {table_name} table with Prisma-style interface'))
def given_prisma_table(bdd_context, table_name: str):
    """Set up a table with Prisma-style interface."""
    if not bdd_context.schema:
        bdd_context.schema = simple_users_schema() if table_name == "users" else simple_todos_schema()
        set_default_schema(bdd_context.schema)
    bdd_context.table_proxy = table(table_name)


# =============================================================================
# SQL Verification Steps
# =============================================================================


@then(parsers.parse('the SQL should be "{expected_sql}"'))
def then_sql_should_be(bdd_context, expected_sql: str):
    """Verify exact SQL match."""
    assert bdd_context.sql == expected_sql, f"Expected SQL: {expected_sql}, Got: {bdd_context.sql}"


@then(parsers.parse('the SQL should contain "{substring}"'))
def then_sql_contains(bdd_context, substring: str):
    """Verify SQL contains substring."""
    assert substring in bdd_context.sql, f"Expected '{substring}' in SQL: {bdd_context.sql}"


@then(parsers.parse('the SQL should contain "{sub1}" and "{sub2}"'))
def then_sql_contains_both(bdd_context, sub1: str, sub2: str):
    """Verify SQL contains both substrings."""
    assert sub1 in bdd_context.sql, f"Expected '{sub1}' in SQL: {bdd_context.sql}"
    assert sub2 in bdd_context.sql, f"Expected '{sub2}' in SQL: {bdd_context.sql}"


@then("there should be no parameters")
def then_no_params(bdd_context):
    """Verify no parameters."""
    assert not bdd_context.params, f"Expected no params, got: {bdd_context.params}"


@then(parsers.parse('the parameters should include {param_name} = "{param_value}"'))
def then_param_equals(bdd_context, param_name: str, param_value: str):
    """Verify parameter value."""
    # Find parameter with matching prefix (params may have suffix like _0, _1)
    found = False
    for key, value in bdd_context.params.items():
        if key.startswith(param_name.rstrip("_")):
            assert str(value) == param_value, f"Expected {param_name}={param_value}, got {key}={value}"
            found = True
            break
    assert found, f"Parameter {param_name} not found in {bdd_context.params}"


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
    # This step would be customized per scenario
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
                return list(rows)  # Just need the count, not dict conversion
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
