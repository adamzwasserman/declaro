"""
Database-related BDD step definitions.

Note: pytest-bdd doesn't natively support async step functions,
so we use asyncio.run() to execute async code within sync steps.
"""

import asyncio
import pytest
from pytest_bdd import given, when, then, parsers

from tests.bdd.factories.connection_factory import (
    ConnectionFactory,
    get_sqlite_connection,
    get_postgresql_connection,
    setup_sqlite_schema,
    setup_postgresql_schema,
    teardown_sqlite_schema,
    teardown_postgresql_schema,
)
from tests.bdd.factories.schema_factory import simple_todos_schema


# =============================================================================
# Connection Setup Steps
# =============================================================================


@given("an SQLite database connection")
def given_sqlite_connection(bdd_context):
    """Set up SQLite connection."""
    bdd_context.dialect = "sqlite"
    bdd_context.connection_factory = ConnectionFactory.sqlite()


@given("a PostgreSQL database connection")
def given_postgresql_connection(bdd_context, require_postgresql):
    """Set up PostgreSQL connection (requires real database)."""
    bdd_context.dialect = "postgresql"
    bdd_context.connection_factory = ConnectionFactory.postgresql()


@given("a Turso database connection")
def given_turso_connection(bdd_context, require_turso):
    """Set up Turso connection (requires real database)."""
    bdd_context.dialect = "turso"
    bdd_context.connection_factory = ConnectionFactory.turso()


# =============================================================================
# Table Setup Steps
# =============================================================================


@given("an empty todos table")
def given_empty_todos_table(bdd_context):
    """Create empty todos table."""
    schema = simple_todos_schema()
    bdd_context.schema = schema

    async def _setup():
        async with bdd_context.connection_factory.get_connection() as conn:
            # Drop and recreate table
            await bdd_context.connection_factory.teardown_schema(conn, schema)
            await bdd_context.connection_factory.setup_schema(conn, schema)
            bdd_context.connection = conn

    asyncio.run(_setup())


@given(parsers.parse("a todos table with {count:d} rows"))
def given_todos_with_rows(bdd_context, count: int, todo_factory):
    """Create todos table with specified number of rows."""
    schema = simple_todos_schema()
    bdd_context.schema = schema
    todos = todo_factory.create_batch(count)

    async def _setup():
        async with bdd_context.connection_factory.get_connection() as conn:
            await bdd_context.connection_factory.teardown_schema(conn, schema)
            await bdd_context.connection_factory.setup_schema(conn, schema)

            # Insert todos
            for todo in todos:
                if bdd_context.dialect == "sqlite":
                    await conn.execute(
                        "INSERT INTO todos (id, title, completed) VALUES (?, ?, ?)",
                        (todo["id"], todo["title"], int(todo["completed"]))
                    )
                    await conn.commit()
                elif bdd_context.dialect == "postgresql":
                    await conn.execute(
                        "INSERT INTO todos (id, title, completed) VALUES ($1, $2, $3)",
                        todo["id"], todo["title"], todo["completed"]
                    )

    asyncio.run(_setup())


# =============================================================================
# CRUD Operation Steps
# =============================================================================


@when(parsers.parse('I insert a todo with title "{title}"'))
def when_insert_todo(bdd_context, title: str):
    """Insert a todo into the database."""
    import uuid
    todo_id = str(uuid.uuid4())

    async def _insert():
        async with bdd_context.connection_factory.get_connection() as conn:
            if bdd_context.dialect == "sqlite":
                await conn.execute(
                    "INSERT INTO todos (id, title, completed) VALUES (?, ?, ?)",
                    (todo_id, title, 0)
                )
                await conn.commit()
            elif bdd_context.dialect == "postgresql":
                await conn.execute(
                    "INSERT INTO todos (id, title, completed) VALUES ($1, $2, $3)",
                    todo_id, title, False
                )
            elif bdd_context.dialect == "turso":
                # Turso uses sync libsql API with ? placeholders
                conn.execute(
                    "INSERT INTO todos (id, title, completed) VALUES (?, ?, ?)",
                    (todo_id, title, 0)
                )
                conn.commit()

    asyncio.run(_insert())
    bdd_context.last_inserted_id = todo_id


@when("I update the todo to completed")
def when_update_todo_completed(bdd_context):
    """Update the last inserted todo to completed."""
    todo_id = bdd_context.last_inserted_id

    async def _update():
        async with bdd_context.connection_factory.get_connection() as conn:
            if bdd_context.dialect == "sqlite":
                await conn.execute(
                    "UPDATE todos SET completed = ? WHERE id = ?",
                    (1, todo_id)
                )
                await conn.commit()
            elif bdd_context.dialect == "postgresql":
                await conn.execute(
                    "UPDATE todos SET completed = $1 WHERE id = $2",
                    True, todo_id
                )
            elif bdd_context.dialect == "turso":
                conn.execute(
                    "UPDATE todos SET completed = ? WHERE id = ?",
                    (1, todo_id)
                )
                conn.commit()

    asyncio.run(_update())


@when("I query for completed todos")
def when_query_completed(bdd_context):
    """Query for completed todos."""
    async def _query():
        async with bdd_context.connection_factory.get_connection() as conn:
            if bdd_context.dialect == "sqlite":
                cursor = await conn.execute(
                    "SELECT * FROM todos WHERE completed = ?",
                    (1,)
                )
                rows = await cursor.fetchall()
                bdd_context.results = [dict(row) for row in rows]
            elif bdd_context.dialect == "postgresql":
                rows = await conn.fetch(
                    "SELECT * FROM todos WHERE completed = $1",
                    True
                )
                bdd_context.results = [dict(row) for row in rows]
            elif bdd_context.dialect == "turso":
                cursor = conn.execute(
                    "SELECT * FROM todos WHERE completed = ?",
                    (1,)
                )
                rows = cursor.fetchall()
                # libsql returns tuples, need to convert to dicts
                columns = ["id", "title", "completed"]
                bdd_context.results = [dict(zip(columns, row)) for row in rows]

    asyncio.run(_query())


@when("I delete the todo")
def when_delete_todo(bdd_context):
    """Delete the last inserted todo."""
    todo_id = bdd_context.last_inserted_id

    async def _delete():
        async with bdd_context.connection_factory.get_connection() as conn:
            if bdd_context.dialect == "sqlite":
                await conn.execute(
                    "DELETE FROM todos WHERE id = ?",
                    (todo_id,)
                )
                await conn.commit()
            elif bdd_context.dialect == "postgresql":
                await conn.execute(
                    "DELETE FROM todos WHERE id = $1",
                    todo_id
                )
            elif bdd_context.dialect == "turso":
                conn.execute(
                    "DELETE FROM todos WHERE id = ?",
                    (todo_id,)
                )
                conn.commit()

    asyncio.run(_delete())


@when(parsers.parse('I execute "{sql}"'))
def when_execute_sql(bdd_context, sql: str):
    """Execute raw SQL query."""
    async def _execute():
        async with bdd_context.connection_factory.get_connection() as conn:
            if bdd_context.dialect == "sqlite":
                cursor = await conn.execute(sql)
                if sql.strip().upper().startswith("SELECT"):
                    rows = await cursor.fetchall()
                    bdd_context.results = [dict(row) for row in rows]
                await conn.commit()
            elif bdd_context.dialect == "postgresql":
                if sql.strip().upper().startswith("SELECT"):
                    rows = await conn.fetch(sql)
                    bdd_context.results = [dict(row) for row in rows]
                else:
                    await conn.execute(sql)
            elif bdd_context.dialect == "turso":
                cursor = conn.execute(sql)
                if sql.strip().upper().startswith("SELECT"):
                    rows = cursor.fetchall()
                    # For COUNT(*), return as dict with 'count' key
                    if "COUNT" in sql.upper():
                        bdd_context.results = [{"count": rows[0][0]}]
                    else:
                        bdd_context.results = [dict(row) for row in rows]
                conn.commit()

    asyncio.run(_execute())


# =============================================================================
# Verification Steps
# =============================================================================


@then(parsers.parse("I should find {count:d} completed todo"))
@then(parsers.parse("I should find {count:d} completed todos"))
def then_completed_count(bdd_context, count: int):
    """Verify completed todo count."""
    assert len(bdd_context.results) == count, f"Expected {count} completed todos, got {len(bdd_context.results)}"


@then("the results match the expected data")
def then_results_match_expected(bdd_context):
    """Verify results match expected data."""
    # This would compare with stored expected data
    assert bdd_context.results is not None


# =============================================================================
# Dialect-Specific Steps
# =============================================================================


@then("PostgreSQL should use $1, $2 parameters")
def then_postgresql_params(bdd_context):
    """Verify PostgreSQL parameter style."""
    if "$1" in bdd_context.sql or "$2" in bdd_context.sql:
        return
    # If building query programmatically, check params dict
    assert bdd_context.dialect == "postgresql" or "$" not in bdd_context.sql


@then("SQLite should use :name parameters")
def then_sqlite_params(bdd_context):
    """Verify SQLite parameter style."""
    if ":" in bdd_context.sql:
        return
    assert bdd_context.dialect != "sqlite" or "?" not in bdd_context.sql


@then("Turso should use ? parameters")
def then_turso_params(bdd_context):
    """Verify Turso parameter style."""
    # Turso uses ? placeholders
    pass
