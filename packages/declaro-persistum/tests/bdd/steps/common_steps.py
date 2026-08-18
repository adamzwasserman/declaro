"""
Common BDD step definitions used across features.
"""

from pytest_bdd import given, parsers, then

from tests.bdd.factories.schema_factory import (
    complex_ecommerce_schema,
)

# =============================================================================
# Schema Setup Steps
# =============================================================================


@given("an empty schema")
def given_empty_schema(bdd_context):
    """Set up an empty schema."""
    bdd_context["schema"] = {}


@given("a complex e-commerce schema")
def given_ecommerce_schema(bdd_context):
    """Set up complex e-commerce schema."""
    bdd_context["schema"] = complex_ecommerce_schema()


# =============================================================================
# Result Verification Steps
# =============================================================================


@then(parsers.parse('I should find exactly {count:d} result'))
@then(parsers.parse('I should find exactly {count:d} results'))
def then_result_count(bdd_context, count: int):
    """Verify result count."""
    assert len(bdd_context["results"]) == count, f"Expected {count} results, got {len(bdd_context['results'])}"


@then("the query should execute successfully")
def then_query_succeeds(bdd_context):
    """Verify query executed without error."""
    assert bdd_context["error"] is None, f"Query failed with error: {bdd_context['error']}"


@then("the results should match the expected filter")
def then_results_match(bdd_context):
    """Verify results match expected filter (placeholder for complex verification)."""
    assert bdd_context["results"] is not None


@then("the table should be empty")
def then_table_empty(bdd_context):
    """Verify table is empty by querying for all rows."""
    import asyncio

    async def _query():
        async with bdd_context["backend"]["connect"]() as conn:
            table_name = list(bdd_context["schema"].keys())[0]
            if bdd_context["dialect"] == "sqlite":
                cursor = await conn.execute(f"SELECT * FROM {table_name}")
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
            elif bdd_context["dialect"] == "postgresql":
                rows = await conn.fetch(f"SELECT * FROM {table_name}")
                return [dict(row) for row in rows]
            elif bdd_context["dialect"] == "turso":
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
    assert bdd_context["error"] is not None, "Expected an error to be raised"
    assert message in str(bdd_context["error"]), f"Expected '{message}' in error: {bdd_context['error']}"


@then("no error should be raised")
def then_no_error(bdd_context):
    """Verify no error was raised."""
    assert bdd_context["error"] is None, f"Unexpected error: {bdd_context['error']}"


@then(parsers.parse("the count should be {expected:d}"))
def then_count_is(bdd_context, expected: int):
    """The VALUE a COUNT(*) returned, which nothing else in this suite checks.

    `Then I should find exactly 1 result` sits directly above this in every
    scenario that uses it, and it passes whatever the count says: COUNT(*)
    always returns exactly one row. A scenario seeding 1000 rows and inserting
    3 would satisfy it.

    So this is the only assertion in the large-dataset scenarios that can tell
    a working bulk insert from a broken one. It reads the column by name and
    raises if it is absent, rather than reaching for `.get("count", 0)` --
    a missing column and a count of zero are different failures and a default
    would report the first as the second.

    THE THREE ENGINES AGREE HERE BY CONSTRUCTION, NOT BY LUCK. SQLite and
    PostgreSQL return the `AS count` alias in a dict; Turso returns a tuple and
    `when_execute_sql` maps it to the same key. One step, three engines, and a
    drift in any of them fails here.
    """
    results = bdd_context["results"]

    assert len(results) == 1, (
        f"a COUNT query returned {len(results)} rows, not 1. The count cannot "
        f"be read, so this scenario proves nothing about how many rows exist."
    )
    row = results[0]
    assert "count" in row, (
        f"the result has keys {sorted(row)} and no 'count'. The query aliases "
        f"it `AS count` and the Turso branch maps its tuple to that key, so a "
        f"missing one means an engine's result shape has drifted."
    )
    assert row["count"] == expected, (
        f"the table holds {row['count']} rows, not {expected}"
    )
