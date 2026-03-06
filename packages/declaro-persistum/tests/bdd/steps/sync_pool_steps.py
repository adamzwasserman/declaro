"""
BDD step definitions for sync pool auto-migration tests.
"""

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest
from pytest_bdd import given, when, then, parsers, scenarios

# Load all scenarios from the feature file
scenarios("../features/pool/sync_auto_migration.feature")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sync_pool_context():
    """Context for sync pool tests."""
    return {
        "temp_db_path": None,
        "temp_db_path_2": None,
        "schema_path": None,
        "pool": None,
        "pool_2": None,
        "connection": None,
        "pydantic_models": [],
        "existing_table_schema": None,
        "migration_warning_logged": False,
    }


@pytest.fixture
def temp_db_file(sync_pool_context):
    """Create a temporary database file."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    sync_pool_context["temp_db_path"] = path
    yield path
    # Cleanup
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def temp_schema_file(sync_pool_context):
    """Create a temporary schema file."""
    fd, path = tempfile.mkstemp(suffix=".py")
    os.close(fd)
    sync_pool_context["schema_path"] = path
    yield path
    # Cleanup
    if os.path.exists(path):
        os.remove(path)


# =============================================================================
# Given Steps
# =============================================================================


@given("a temporary database file")
def given_temp_db(temp_db_file, sync_pool_context):
    """Set up temporary database file."""
    sync_pool_context["temp_db_path"] = temp_db_file


@given(parsers.parse('a Pydantic model "{model_name}" with fields:'))
def given_pydantic_model(model_name: str, datatable, sync_pool_context, temp_schema_file):
    """Create a Pydantic model definition."""
    fields = []
    primary_key_field = None

    # datatable is list of lists; first row is headers
    headers = datatable[0]
    field_idx = headers.index("field")
    type_idx = headers.index("type")
    pk_idx = headers.index("primary_key") if "primary_key" in headers else -1

    for row in datatable[1:]:  # Skip header row
        field_name = row[field_idx]
        field_type = row[type_idx]
        is_pk = pk_idx >= 0 and row[pk_idx].lower() == "true"

        # Map types
        if field_type == "str":
            py_type = "str"
        elif field_type == "int":
            py_type = "int"
        elif field_type.startswith("Literal"):
            py_type = field_type  # Keep as-is for Literal types
        else:
            py_type = field_type

        if is_pk:
            primary_key_field = field_name
            fields.append(f'    {field_name}: {py_type} = Field(json_schema_extra={{"primary_key": True, "nullable": False}})')
        else:
            fields.append(f'    {field_name}: {py_type} = Field(json_schema_extra={{"nullable": False}})')

    # Generate table name from model name (lowercase + 's')
    table_name = model_name.lower() + "s"

    model_code = f'''
from typing import Literal, Optional
from pydantic import BaseModel, Field

class {model_name}(BaseModel):
    """Auto-generated test model."""
    __tablename__ = "{table_name}"

{chr(10).join(fields)}
'''

    with open(temp_schema_file, "w") as f:
        f.write(model_code)

    sync_pool_context["schema_path"] = temp_schema_file
    sync_pool_context["pydantic_models"].append({
        "name": model_name,
        "table_name": table_name,
        "fields": datatable[1:],  # Store without header
    })


@given(parsers.parse('a database with existing table "{table_name}":'))
def given_existing_table(table_name: str, datatable, sync_pool_context, temp_db_file):
    """Create an existing table in the database."""
    import sqlite3

    # datatable is list of lists; first row is headers
    headers = datatable[0]
    col_idx = headers.index("column")
    type_idx = headers.index("type")

    columns = []
    for row in datatable[1:]:  # Skip header
        col_name = row[col_idx]
        col_type = row[type_idx]
        columns.append(f"{col_name} {col_type}")

    create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"

    conn = sqlite3.connect(temp_db_file)
    conn.execute(create_sql)
    conn.commit()
    conn.close()

    sync_pool_context["existing_table_schema"] = {
        "table_name": table_name,
        "columns": datatable[1:],  # Store without header
    }


@given(parsers.parse('a database with existing table "{table_name}" containing data:'))
def given_existing_table_with_data(table_name: str, datatable, sync_pool_context, temp_db_file):
    """Create an existing table with data."""
    import sqlite3

    if not datatable or len(datatable) < 2:
        return

    # datatable is list of lists; first row is headers
    columns = datatable[0]
    col_defs = [f"{col} TEXT" for col in columns]

    create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"

    conn = sqlite3.connect(temp_db_file)
    conn.execute(create_sql)

    # Insert data (skip header row)
    for row in datatable[1:]:
        placeholders = ", ".join(["?" for _ in columns])
        conn.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})", row)

    conn.commit()
    conn.close()

    sync_pool_context["existing_data"] = datatable[1:]  # Store without header


@given("a Pydantic model \"Task\" with incompatible schema change")
def given_incompatible_schema(sync_pool_context, temp_schema_file):
    """Create a model with incompatible schema change (e.g., rename column)."""
    model_code = '''
from pydantic import BaseModel, Field

class Task(BaseModel):
    """Model with renamed column - incompatible change."""
    __tablename__ = "tasks"

    task_id_renamed: str = Field(json_schema_extra={"primary_key": True, "nullable": False})
    title_renamed: str = Field(json_schema_extra={"nullable": False})
'''

    with open(temp_schema_file, "w") as f:
        f.write(model_code)

    sync_pool_context["schema_path"] = temp_schema_file


# =============================================================================
# When Steps
# =============================================================================


@when("I create a sync SQLite pool with auto_migrate enabled")
def when_create_sync_sqlite_pool_with_migrate(sync_pool_context):
    """Create sync SQLite pool with auto-migration."""
    from declaro_persistum import SyncConnectionPool
    from declaro_persistum.migrations import apply_migrations_sync

    db_path = sync_pool_context["temp_db_path"]
    schema_path = sync_pool_context["schema_path"]

    pool = SyncConnectionPool.sqlite(db_path)
    sync_pool_context["pool"] = pool

    # Apply migrations
    if schema_path and os.path.exists(schema_path):
        result = apply_migrations_sync(pool, "sqlite", schema_path)
        sync_pool_context["migration_result"] = result
        if result.get("error") and "Ambiguous" in str(result.get("error", "")):
            sync_pool_context["migration_warning_logged"] = True


@when("I create a sync SQLite pool with auto_migrate disabled")
def when_create_sync_sqlite_pool_no_migrate(sync_pool_context):
    """Create sync SQLite pool without auto-migration."""
    from declaro_persistum import SyncConnectionPool

    db_path = sync_pool_context["temp_db_path"]
    pool = SyncConnectionPool.sqlite(db_path)
    sync_pool_context["pool"] = pool
    # Don't apply migrations


@when("I create a sync Turso pool with auto_migrate enabled")
def when_create_sync_turso_pool_with_migrate(sync_pool_context):
    """Create sync Turso pool with auto-migration."""
    pytest.importorskip("turso")
    from declaro_persistum import SyncConnectionPool
    from declaro_persistum.migrations import apply_migrations_sync

    db_path = sync_pool_context["temp_db_path"]
    schema_path = sync_pool_context["schema_path"]

    pool = SyncConnectionPool.turso(db_path)
    sync_pool_context["pool"] = pool

    # Apply migrations
    if schema_path and os.path.exists(schema_path):
        result = apply_migrations_sync(pool, "turso", schema_path)
        sync_pool_context["migration_result"] = result


@when("I acquire a connection from the sync pool")
def when_acquire_sync_connection(sync_pool_context):
    """Acquire a connection from the sync pool."""
    pool = sync_pool_context["pool"]
    conn = pool.acquire()
    sync_pool_context["connection"] = conn


@when("I create an async SQLite pool with auto_migrate enabled")
def when_create_async_sqlite_pool(sync_pool_context):
    """Create async SQLite pool with auto-migration."""
    import asyncio
    from declaro_persistum import ConnectionPool
    from declaro_persistum.migrations import apply_migrations_async

    # Use a second temp db for comparison
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    sync_pool_context["temp_db_path_2"] = path

    schema_path = sync_pool_context["schema_path"]

    async def _create():
        pool = await ConnectionPool.sqlite(path)
        if schema_path and os.path.exists(schema_path):
            await apply_migrations_async(pool, "sqlite", schema_path)
        return pool

    pool = asyncio.run(_create())
    sync_pool_context["pool_2"] = pool


@when("I create a sync SQLite pool with auto_migrate enabled on a separate database")
def when_create_sync_pool_separate_db(sync_pool_context):
    """Create sync pool on a third database for comparison."""
    from declaro_persistum import SyncConnectionPool
    from declaro_persistum.migrations import apply_migrations_sync

    # Use yet another temp db
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    sync_pool_context["temp_db_path_3"] = path

    schema_path = sync_pool_context["schema_path"]

    pool = SyncConnectionPool.sqlite(path)
    if schema_path and os.path.exists(schema_path):
        apply_migrations_sync(pool, "sqlite", schema_path)

    sync_pool_context["pool_3"] = pool


# =============================================================================
# Then Steps
# =============================================================================


@then(parsers.parse('the "{table_name}" table should exist in the database'))
def then_table_exists(table_name: str, sync_pool_context):
    """Verify table exists."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    result = cursor.fetchone()
    assert result is not None, f"Table '{table_name}' should exist but doesn't"


@then(parsers.parse('the "{table_name}" table should not exist in the database'))
def then_table_not_exists(table_name: str, sync_pool_context):
    """Verify table does not exist."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    result = cursor.fetchone()
    assert result is None, f"Table '{table_name}' should not exist but does"


@then(parsers.parse('the "{table_name}" table should have columns:'))
def then_table_has_columns(table_name: str, datatable, sync_pool_context):
    """Verify table has expected columns."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = {row[1]: row[2].upper() for row in cursor.fetchall()}

    # datatable is list of lists; first row is headers
    headers = datatable[0]
    col_idx = headers.index("column")
    type_idx = headers.index("type")

    for row in datatable[1:]:  # Skip header
        expected_col = row[col_idx]
        expected_type = row[type_idx].upper()
        assert expected_col in columns, f"Column '{expected_col}' not found in table '{table_name}'"
        assert columns[expected_col] == expected_type, \
            f"Column '{expected_col}' has type '{columns[expected_col]}', expected '{expected_type}'"


@then(parsers.parse('the "{table_name}" lookup table should exist'))
def then_lookup_table_exists(table_name: str, sync_pool_context):
    """Verify enum lookup table exists."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    result = cursor.fetchone()
    assert result is not None, f"Lookup table '{table_name}' should exist"


@then(parsers.parse('the "{table_name}" table should contain values:'))
def then_table_contains_values(table_name: str, datatable, sync_pool_context):
    """Verify lookup table contains expected values."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(f"SELECT value FROM {table_name}")
    values = {row[0] for row in cursor.fetchall()}

    # datatable is list of lists; first row is headers
    headers = datatable[0]
    value_idx = headers.index("value")

    for row in datatable[1:]:  # Skip header
        expected_value = row[value_idx]
        assert expected_value in values, \
            f"Value '{expected_value}' not found in table '{table_name}'"


@then(parsers.parse('the "{table_name}" table should have a foreign key to "{ref_table}"'))
def then_table_has_fk(table_name: str, ref_table: str, sync_pool_context):
    """Verify table has foreign key reference."""
    conn = sync_pool_context["connection"]
    cursor = conn.execute(f"PRAGMA foreign_key_list({table_name})")
    fks = [row[2] for row in cursor.fetchall()]  # referenced table names
    assert ref_table in fks, \
        f"Table '{table_name}' should have FK to '{ref_table}', found: {fks}"


@then("the pool should log a migration warning")
def then_migration_warning_logged(sync_pool_context):
    """Verify migration warning was logged."""
    # Check if ambiguity was detected
    result = sync_pool_context.get("migration_result", {})
    has_warning = (
        sync_pool_context.get("migration_warning_logged", False) or
        result.get("error") is not None
    )
    assert has_warning, "Expected migration warning but none was logged"


@then("the existing data should remain intact")
def then_data_intact(sync_pool_context):
    """Verify existing data wasn't lost."""
    import sqlite3
    db_path = sync_pool_context["temp_db_path"]

    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT * FROM tasks")
    rows = cursor.fetchall()
    conn.close()

    expected_data = sync_pool_context.get("existing_data", [])
    assert len(rows) == len(expected_data), \
        f"Expected {len(expected_data)} rows, found {len(rows)}"


@then("both databases should have identical schemas")
def then_schemas_identical(sync_pool_context):
    """Verify async and sync pools produce identical schemas."""
    import sqlite3

    def get_schema(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        schema = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        return schema

    async_db = sync_pool_context.get("temp_db_path_2")
    sync_db = sync_pool_context.get("temp_db_path_3")

    if async_db and sync_db:
        async_schema = get_schema(async_db)
        sync_schema = get_schema(sync_db)

        assert async_schema == sync_schema, \
            f"Schemas differ:\nAsync: {async_schema}\nSync: {sync_schema}"
