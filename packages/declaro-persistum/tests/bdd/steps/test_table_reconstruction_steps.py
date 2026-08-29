"""
BDD step definitions for table reconstruction testing.

This tests the table_reconstruction abstraction for SQLite/Turso ALTER COLUMN.
"""

import asyncio
import contextlib
from typing import Any, TypedDict, cast

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions.pragma_compat import (
    pragma_foreign_key_list,
    pragma_index_list,
    pragma_table_info,
)
from declaro_persistum.abstractions.reconstruction import (
    execute_reconstruction_async,
    get_reconstruction_columns,
)
from declaro_persistum.abstractions.table_reconstruction import (
    _get_full_table_schema,
    alter_column_default,
    alter_column_property,
    reconstruct_table,
)
from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.applier.spelling import column_definition
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE, MigrationError
from declaro_persistum.types import NOTHING, Column, Operation

# Load scenarios from feature file
scenarios("../features/table_reconstruction.feature")


# Event loop fixture for async operations
@pytest.fixture(scope="function")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


class ReconContext(TypedDict, total=False):
    """What one reconstruction scenario knows so far.

    Twelve data fields and a `run(coro)` wrapper, previously a `Context`
    class defined inside the fixture. Same treatment as `PragmaContext` and
    `Backend`: the facts are data, and `run` is the one piece of behaviour,
    injected.

    `schema_snapshot` and `rows_snapshot` start as None deliberately. None
    means "never captured", and the rollback assertions treat that as a
    FAILURE rather than a pass — a scenario that forgot to snapshot before a
    step expected to fail must not quietly report that nothing changed.
    """

    conn: Any
    table_name: str | None
    columns: dict
    error: Exception | None
    table_data: list
    indexes: list
    initial_row_count: int
    schema_snapshot: Any
    rows_snapshot: Any
    run: Any


# Context fixture
@pytest.fixture
def recon_context(event_loop):
    """A scenario context, with `run` bound to this scenario's loop."""
    ctx: ReconContext = {
        "conn": None,
        "table_name": None,
        "columns": {},
        "error": None,
        "table_data": [],
        "indexes": [],
        "initial_row_count": 0,
        "schema_snapshot": None,
        "rows_snapshot": None,
        "run": event_loop.run_until_complete,
    }
    yield ctx

    if ctx["conn"]:
        # Each scenario opens its own database, so a connection that will not
        # close leaves nothing for the next one to trip over.
        with contextlib.suppress(Exception):
            event_loop.run_until_complete(ctx["conn"].close())


# ============================================
# Helpers
# ============================================


async def _build_full_schema(conn, table_name: str) -> dict[str, Column]:
    """Build complete column schema including UNIQUE and FK constraints."""
    rows = await pragma_table_info(conn, table_name)
    columns: dict[str, Column] = {}

    for row in rows:
        cid, name, type_str, notnull, dflt_value, pk = row
        col_def: Column = {
            "type": type_str or "TEXT",
            "nullable": not bool(notnull),
        }
        if pk:
            col_def["primary_key"] = True
        if dflt_value is not None:
            col_def["default"] = dflt_value
        columns[name] = col_def

    # UNIQUE constraints
    index_list = await pragma_index_list(conn, table_name)
    for idx_row in index_list:
        if idx_row[3] == "u" and idx_row[2]:
            cursor = await conn.execute(f'PRAGMA index_info("{idx_row[1]}")')
            idx_info = await cursor.fetchall()
            if len(idx_info) == 1:
                col_name = idx_info[0][2]
                if col_name in columns:
                    columns[col_name]["unique"] = True  # type: ignore

    # FK constraints
    fk_list = await pragma_foreign_key_list(conn, table_name)
    for fk_row in fk_list:
        from_col, ref_table, ref_col = fk_row[3], fk_row[2], fk_row[4]
        on_delete = fk_row[6]
        if from_col in columns:
            columns[from_col]["references"] = f"{ref_table}.{ref_col}"  # type: ignore
            if on_delete and on_delete.upper() not in ("NO ACTION", "NONE", ""):
                columns[from_col]["on_delete"] = on_delete.lower()  # type: ignore

    return columns


def _enable_fk(recon_context):
    """Enable foreign keys on the connection."""
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))


def _snapshot_table(recon_context, table_name=None):
    """Capture schema and rows before a step that is expected to fail.

    "The original table is unchanged" and "the original data is intact"
    previously asserted only that an error had been raised — the first
    re-checked recon_context["error"] and the second was a bare pass. Neither
    looked at the table, so a reconstruction that failed *after* mutating
    the table would have satisfied both. These snapshots give them real
    prior state to compare against.
    """
    t = table_name or recon_context["table_name"]
    if not t:
        return

    schema = recon_context["run"](pragma_table_info(recon_context["conn"], t))
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT * FROM "{t}" ORDER BY rowid')
    )
    rows = recon_context["run"](cursor.fetchall())

    recon_context["schema_snapshot"] = [tuple(r) for r in schema]
    recon_context["rows_snapshot"] = [tuple(r) for r in rows]


def _track_row_count(recon_context, table_name=None):
    """Update initial_row_count from current table."""
    t = table_name or recon_context["table_name"]
    if t:
        cursor = recon_context["run"](recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{t}"'))
        result = recon_context["run"](cursor.fetchone())
        recon_context["initial_row_count"] = result[0]


# ============================================
# Background
# ============================================


@given("a database connection")
def given_database_connection(recon_context):
    """Set up database connection."""
    if recon_context["conn"] is None:
        import aiosqlite

        recon_context["conn"] = recon_context["run"](aiosqlite.connect(":memory:"))


# ============================================
# Table Setup
# ============================================


@given(parsers.re(r'a table "(?P<table_name>\w+)" with columns:'))
def given_table_with_columns(recon_context, table_name, datatable):
    """Create a table from a Gherkin datatable, through the package's own renderer.

    THIS BUILT THE SQL ITSELF, forty-five lines of `parts.append` with one
    branch per column property: NOT NULL, UNIQUE, DEFAULT, REFERENCES, and two
    passes to work out the primary key. That is a second implementation of
    `column_definition`, which is the function these scenarios exist to test
    the behaviour of, so a bug in the real one could be masked by the same bug
    written differently here.

    It builds the column DECLARATION now -- the same dict shape the schema
    files use -- and asks the renderer for the SQL.
    """
    recon_context["table_name"] = table_name
    headers = [cell.strip() for cell in datatable[0]]
    rows = [{headers[i]: cell.strip() for i, cell in enumerate(row)} for row in datatable[1:]]

    columns = {row["name"]: _a_column_from(row) for row in rows}

    # A LONE `id NOT NULL` IS THE PRIMARY KEY when nothing says otherwise,
    # which is what the scenarios written before this assumed.
    declared = [name for name, col in columns.items() if col.get("primary_key")]
    implied = [
        row["name"]
        for row in rows
        if row["name"] == "id" and row.get("nullable", "true").lower() == "false"
    ]
    keys = declared or implied

    # ONE KEY GOES ON ITS COLUMN, MORE THAN ONE GOES ON THE TABLE, and it has
    # to be one or the other: SQLite refuses a table carrying both, with
    # `table "x" has more than one primary key`. Measured 2026-08-29 when this
    # first version left the column-level key in place alongside the clause.
    spans_columns = len(keys) > 1
    columns = {
        name: {**col, "primary_key": name in keys[:1]}
        if not spans_columns
        else {k: v for k, v in col.items() if k != "primary_key"}
        for name, col in columns.items()
    }

    parts = [column_definition(name, col, "sqlite") for name, col in columns.items()]
    parts += [f"PRIMARY KEY ({', '.join(chr(34) + c + chr(34) for c in keys)})"] * (
        1 if spans_columns else 0
    )

    _rows(recon_context, f'CREATE TABLE "{table_name}" ({", ".join(parts)})')
    _commit(recon_context)


def _a_column_from(row: dict) -> Column:
    """One datatable row, as the column declaration a schema file would carry."""
    declared: dict = {"type": row.get("type", "text").lower()}
    for key, value in (
        ("nullable", row.get("nullable", "true").lower() == "false"),
        ("unique", row.get("unique", "").lower() == "true"),
        ("primary_key", row.get("primary_key", "").lower() == "true"),
    ):
        # Only what the row actually said. A `nullable: false` is a fact; the
        # absence of a `unique` column is not a declaration that it is false.
        declared.update({key: value} if value else {})
    declared.update({"nullable": False} if row.get("nullable", "").lower() == "false" else {})
    default = row.get("default", "").strip()
    declared.update({"default": default} if default else {})
    reference = row.get("references", "").strip()
    declared.update(
        {"references": reference if "." in reference else f"{reference}.id"} if reference else {}
    )
    return cast("Column", declared)


# THE TWO ROWS EACH TABLE STARTS WITH, one entry per table. This was a chain
# of seven `elif` on the table name, each running two inserts written out in
# full, and the audit read the whole chain as fifty-five lines of setup for one
# step. The table name and the values were the only things that differed.
#
# THE VALUES MATTER AND ARE NOT FILLER. `posts` carries a NULL title, because
# a rebuild that drops a nullable column has to keep the NULL; `orders` uses
# integers where the others use text.
_STARTING_ROWS: dict[str, tuple[str, tuple[tuple, ...]]] = {
    "users": (
        "(id, email, name)",
        ((1, "alice@test.com", "Alice"), (2, "bob@test.com", "Bob")),
    ),
    "products": (
        "(id, name, description)",
        ((1, "Product 1", "Description 1"), (2, "Product 2", "Description 2")),
    ),
    "orders": ("(id, status, amount)", ((1, 1, 100), (2, 2, 200))),
    "metrics": ("(id, value)", (("1", "42"), ("2", "99"))),
    "posts": ("(id, title)", (("1", "Post1"), ("2", None))),
    "data": ("(id, value)", ((1, "test"),)),
}


@given(parsers.re(r"the table contains data:"))
def given_table_contains_data(recon_context):
    """Put the rows this table starts every scenario with into it.

    A TABLE NOBODY WROTE ROWS FOR INSERTS NOTHING, which is what the old chain
    did by falling off the end of its `elif`s. It is the same answer, and now
    it is the one written in the table rather than the one left over.
    """
    columns, rows = _STARTING_ROWS.get(recon_context["table_name"], ("", ()))
    for row in rows:
        marks = ", ".join("?" for _ in row)
        _rows(
            recon_context,
            f'INSERT INTO "{recon_context["table_name"]}" {columns} VALUES ({marks})',
            *row,
        )
    _commit(recon_context)
    _track_row_count(recon_context)


@given(parsers.parse("the table contains {count:d} rows of test data"))
def given_table_with_test_data(recon_context, count):
    """Insert N rows of test data."""
    for i in range(count):
        insert_sql = (
            f'INSERT INTO "{recon_context["table_name"]}" (id, email, created_at) VALUES (?, ?, ?)'
        )
        recon_context["run"](
            recon_context["conn"].execute(insert_sql, (i + 1, f"user{i}@test.com", "2024-01-01"))
        )
    _commit(recon_context)
    _track_row_count(recon_context)


@given(parsers.parse('an index "{index_name}" on column "{column}"'))
def given_index_on_column(recon_context, index_name, column):
    """Create index on column."""
    create_idx_sql = f'CREATE INDEX "{index_name}" ON "{recon_context["table_name"]}" ("{column}")'
    recon_context["run"](recon_context["conn"].execute(create_idx_sql))
    recon_context["indexes"].append(index_name)


@given(parsers.parse('a unique index "{index_name}" on column "{column}"'))
def given_unique_index(recon_context, index_name, column):
    """Create unique index."""
    create_idx_sql = (
        f'CREATE UNIQUE INDEX "{index_name}" ON "{recon_context["table_name"]}" ("{column}")'
    )
    recon_context["run"](recon_context["conn"].execute(create_idx_sql))
    recon_context["indexes"].append(index_name)


@given("the tables contain related data")
def given_related_data(recon_context):
    """Insert related data for FK tests (no datatable variant)."""
    recon_context["run"](
        recon_context["conn"].execute("INSERT INTO authors (id, name) VALUES (1, 'Author 1')")
    )
    recon_context["run"](
        recon_context["conn"].execute("INSERT INTO authors (id, name) VALUES (2, 'Author 2')")
    )
    recon_context["run"](
        recon_context["conn"].execute(
            "INSERT INTO books (id, title, author_id) VALUES (1, 'Book 1', 1)"
        )
    )
    recon_context["run"](
        recon_context["conn"].execute(
            "INSERT INTO books (id, title, author_id) VALUES (2, 'Book 2', 2)"
        )
    )
    _commit(recon_context)
    _track_row_count(recon_context)


@given("the tables contain related data:")
def given_related_data_table(recon_context, datatable):
    """Insert related data across multiple tables from datatable."""
    headers = [cell.strip() for cell in datatable[0]]

    for row in datatable[1:]:
        d = {headers[i]: row[i].strip() for i in range(len(headers))}
        table = d.pop("table")
        non_empty = {k: v for k, v in d.items() if v}
        if not non_empty:
            continue

        cols = ", ".join(f'"{k}"' for k in non_empty)
        placeholders = ", ".join("?" for _ in non_empty)
        values = [int(v) if v.isdigit() else v for v in non_empty.values()]

        recon_context["run"](
            recon_context["conn"].execute(
                f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
                values,
            )
        )

    _commit(recon_context)
    _track_row_count(recon_context)


@given("the books table contains invalid foreign key data:")
def given_books_invalid_fk_data(recon_context, datatable):
    """Insert FK-violating data into books (books has no FK constraint yet)."""
    headers = [cell.strip() for cell in datatable[0]]

    for row in datatable[1:]:
        d = {headers[i]: row[i].strip() for i in range(len(headers))}
        cols = ", ".join(f'"{k}"' for k in d)
        placeholders = ", ".join("?" for _ in d)
        values = [int(v) if v.isdigit() else v for v in d.values()]

        recon_context["run"](
            recon_context["conn"].execute(
                f'INSERT INTO "books" ({cols}) VALUES ({placeholders})',
                values,
            )
        )

    _commit(recon_context)


@given("the table contains hierarchical data:")
def given_hierarchical_data(recon_context, datatable):
    """Insert hierarchical data (NULL-aware) into current table."""
    headers = [cell.strip() for cell in datatable[0]]

    for row in datatable[1:]:
        d = {headers[i]: row[i].strip() for i in range(len(headers))}
        values = [None if v.upper() == "NULL" else v for v in d.values()]
        # Convert numeric strings
        values = [int(v) if isinstance(v, str) and v.isdigit() else v for v in values]

        cols = ", ".join(f'"{k}"' for k in d)
        placeholders = ", ".join("?" for _ in d)

        recon_context["run"](
            recon_context["conn"].execute(
                f'INSERT INTO "{recon_context["table_name"]}" ({cols}) VALUES ({placeholders})',
                values,
            )
        )

    _commit(recon_context)
    recon_context["initial_row_count"] = len(datatable) - 1


@given("the table contains data with incompatible types:")
def given_incompatible_types_data(recon_context, datatable):
    """Insert data that will be incompatible for type conversion."""
    headers = [cell.strip() for cell in datatable[0]]

    for row in datatable[1:]:
        d = {headers[i]: row[i].strip() for i in range(len(headers))}
        cols = ", ".join(f'"{k}"' for k in d)
        placeholders = ", ".join("?" for _ in d)
        values = [int(v) if v.isdigit() else v for v in d.values()]

        recon_context["run"](
            recon_context["conn"].execute(
                f'INSERT INTO "{recon_context["table_name"]}" ({cols}) VALUES ({placeholders})',
                values,
            )
        )

    _commit(recon_context)
    recon_context["initial_row_count"] = len(datatable) - 1


@given("the books table has orphaned foreign keys (data corruption):")
def given_books_orphaned_fk_data(recon_context, datatable):
    """Insert orphaned FK data into books (disabling FK to simulate corruption)."""
    headers = [cell.strip() for cell in datatable[0]]

    # Disable FK to allow inserting orphaned data (simulating data corruption)
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = OFF"))

    for row in datatable[1:]:
        d = {headers[i]: row[i].strip() for i in range(len(headers))}
        cols = ", ".join(f'"{k}"' for k in d)
        placeholders = ", ".join("?" for _ in d)
        values = [int(v) if v.isdigit() else v for v in d.values()]

        recon_context["run"](
            recon_context["conn"].execute(
                f'INSERT INTO "books" ({cols}) VALUES ({placeholders})',
                values,
            )
        )

    _commit(recon_context)
    # Re-enable FK so reconstruction's FK check is triggered
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))


@given("tables with foreign key relationships")
def given_fk_relationships(recon_context):
    """Create tables with FK relationships."""
    recon_context["run"](
        recon_context["conn"].execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
        )
    )
    recon_context["run"](
        recon_context["conn"].execute(
            """CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                data TEXT,
                FOREIGN KEY(parent_id) REFERENCES parents(id)
            )"""
        )
    )
    recon_context["table_name"] = "children"


@given("the table has 0 rows")
def given_table_has_zero_rows(recon_context):
    """Assert the emptiness rather than trusting it.

    The body was `pass` over a comment saying "it is after creation". That is
    true today and is exactly the kind of thing a later Background step
    changes. A Given that states a precondition and checks nothing lets the
    scenario report on a table it never had.
    """
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    count = recon_context["run"](cursor.fetchone())[0]

    assert count == 0, (
        f'"{recon_context["table_name"]}" holds {count} rows, and this scenario '
        f"is about reconstructing an EMPTY table"
    )


@given("the table contains data")
def given_table_has_data(recon_context):
    """Insert one generic row."""
    recon_context["run"](
        recon_context["conn"].execute(
            f'INSERT INTO "{recon_context["table_name"]}" (id, value) VALUES (1, "test")'
        )
    )
    _commit(recon_context)
    _track_row_count(recon_context)


@given("data that satisfies the foreign key constraints")
def given_valid_fk_data(recon_context):
    """Insert valid FK data."""
    recon_context["run"](
        recon_context["conn"].execute("INSERT INTO parents (id, name) VALUES (1, 'Parent 1')")
    )
    recon_context["run"](
        recon_context["conn"].execute(
            "INSERT INTO children (id, parent_id, data) VALUES (1, 1, 'Child 1')"
        )
    )
    _commit(recon_context)
    _track_row_count(recon_context)


@given("foreign keys are enabled")
def given_fk_enabled(recon_context):
    """Enable foreign keys."""
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))


@given("a table requiring reconstruction")
def given_table_for_recon(recon_context):
    """Create a simple table for reconstruction."""
    recon_context["run"](
        recon_context["conn"].execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)"
        )
    )
    recon_context["table_name"] = "test_table"


# ============================================
# Actions
# ============================================


@when(parsers.parse('I alter column "{column}" to be NOT NULL'))
def when_alter_column_not_null(recon_context, column):
    """Change column to NOT NULL. Handles table.column format."""
    if "." in column:
        table_name, column_name = column.split(".", 1)
        recon_context["table_name"] = table_name
    else:
        table_name = recon_context["table_name"]
        column_name = column

    # Captured before the attempt so that, if it fails, the rollback
    # assertions have real prior state to compare against.
    _snapshot_table(recon_context, table_name)

    try:
        _enable_fk(recon_context)
        recon_context["run"](
            alter_column_property(recon_context["conn"], table_name, column_name, "nullable", False)
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" to be NOT NULL with default "{default}"'))
def when_alter_not_null_with_default(recon_context, column, default):
    """Change column to NOT NULL and set a default value."""
    try:
        _enable_fk(recon_context)
        columns = recon_context["run"](
            _get_full_table_schema(recon_context["conn"], recon_context["table_name"])
        )
        if column not in columns:
            raise ValueError(f"Column '{column}' not found in '{recon_context['table_name']}'")
        columns[column]["nullable"] = False
        columns[column]["default"] = default
        recon_context["run"](
            reconstruct_table(
                recon_context["conn"],
                recon_context["table_name"],
                columns,
                preserve_data=NOTHING,
                manage_foreign_keys=NOTHING,
            )
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" to be nullable'))
def when_alter_column_nullable(recon_context, column):
    """Change column to nullable."""
    try:
        _enable_fk(recon_context)
        recon_context["run"](
            alter_column_property(
                recon_context["conn"], recon_context["table_name"], column, "nullable", True
            )
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" to type "{new_type}"'))
def when_alter_column_property(recon_context, column, new_type):
    """Change column type. Pre-validates data for strict type enforcement."""
    # Captured before the attempt so that, if it fails, the rollback
    # assertions have real prior state to compare against.
    _snapshot_table(recon_context)
    try:
        _enable_fk(recon_context)

        # Pre-validate: for INTEGER target, check data can be converted
        if new_type.upper() == "INTEGER":
            cursor = recon_context["run"](
                recon_context["conn"].execute(
                    f'SELECT "{column}" FROM "{recon_context["table_name"]}"'
                )
            )
            rows = recon_context["run"](cursor.fetchall())
            for row in rows:
                val = row[0]
                if val is not None:
                    try:
                        int(str(val))
                    except (ValueError, TypeError) as err:
                        raise ValueError(
                            f"Cannot convert value '{val}' in column '{column}' to INTEGER"
                        ) from err

        recon_context["run"](
            alter_column_property(
                recon_context["conn"], recon_context["table_name"], column, "type", new_type
            )
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" to have default {default_val}'))
def when_alter_column_default(recon_context, column, default_val):
    """Add/change default value."""
    try:
        _enable_fk(recon_context)
        recon_context["run"](
            alter_column_default(
                recon_context["conn"], recon_context["table_name"], column, default_val
            )
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" to have no default'))
def when_alter_column_no_default(recon_context, column):
    """Remove default value."""
    try:
        _enable_fk(recon_context)
        recon_context["run"](
            alter_column_default(recon_context["conn"], recon_context["table_name"], column, None)
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when("I reconstruct with entirely new column names")
def when_reconstruct_new_columns(recon_context):
    """Reconstruct with different columns."""
    new_columns = {
        "new_id": {"type": "INTEGER", "primary_key": True},
        "new_val": {"type": "TEXT", "nullable": True},
    }
    try:
        _enable_fk(recon_context)
        recon_context["run"](
            reconstruct_table(
                recon_context["conn"],
                recon_context["table_name"],
                new_columns,
                preserve_data=NOTHING,
                manage_foreign_keys=NOTHING,
            )
        )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


@when(parsers.parse('I alter column "{column}" nullability'))
def when_alter_nullability(recon_context, column):
    """Generic nullability change."""
    _enable_fk(recon_context)
    recon_context["run"](
        alter_column_property(
            recon_context["conn"], recon_context["table_name"], column, "nullable", False
        )
    )
    _commit(recon_context)


@when(parsers.parse('I alter column "{column}" in "{table}" to be nullable'))
def when_alter_column_in_table(recon_context, column, table):
    """Alter column in specific table."""
    _enable_fk(recon_context)
    recon_context["run"](
        alter_column_property(recon_context["conn"], table, column, "nullable", True)
    )
    _commit(recon_context)


@when("I perform table reconstruction")
def when_perform_reconstruction(recon_context):
    """Perform table reconstruction."""
    columns = {
        "id": {"type": "INTEGER", "primary_key": True},
        "value": {"type": "TEXT", "nullable": False},
    }
    _enable_fk(recon_context)
    recon_context["run"](
        reconstruct_table(
            recon_context["conn"],
            recon_context["table_name"],
            columns,
            preserve_data=NOTHING,
            manage_foreign_keys=NOTHING,
        )
    )
    _commit(recon_context)


@when("reconstruction fails during data copy")
def when_reconstruction_fails(recon_context):
    """Force a genuine failure in the data-copy phase.

    Insert a row whose "value" is NULL, then reconstruct with "value" NOT
    NULL: the INSERT ... SELECT that copies rows into the temp table
    violates the new constraint, so the copy aborts.

    The previous injection pre-created a table named "{table}_new" and
    relied on a name collision. Reconstruction now names its temp table
    _declaro_tmp_{table}_{uuid8} and drops any leftover temp table before
    creating it, so that collision became impossible and nothing failed —
    the scenario went on asserting a rollback that was never triggered.
    """
    table = recon_context["table_name"]

    recon_context["run"](
        recon_context["conn"].execute(f'INSERT INTO "{table}" (id, value) VALUES (2, NULL)')
    )
    _commit(recon_context)

    # Snapshot after the setup insert: this is the state the rollback must
    # restore, and it is what the following Then steps compare against.
    _snapshot_table(recon_context)
    _track_row_count(recon_context)

    try:
        recon_context["run"](
            alter_column_property(recon_context["conn"], table, "value", "nullable", False)
        )
    except Exception as e:
        recon_context["error"] = e


@when("I reconstruct a table with column changes")
def when_reconstruct_table(recon_context):
    """Reconstruct a table with column changes."""
    _enable_fk(recon_context)
    recon_context["run"](
        alter_column_property(recon_context["conn"], "children", "data", "nullable", False)
    )
    _commit(recon_context)


def _rows(recon_context, sql: str, *params: object) -> list:
    """Every row one query gives back.

    THE THREE LINES THIS REPLACES WERE WRITTEN IN EVERY STEP. Each database
    call went through `recon_context["run"](...)` twice, once to execute and
    once to fetch, and the audit read the repetition as thirty to a hundred
    lines of setup per step.
    """
    cursor = recon_context["run"](recon_context["conn"].execute(sql, params))
    return list(recon_context["run"](cursor.fetchall()))


def _one_row(recon_context, sql: str, *params: object):
    """The first row, or None when the query found nothing."""
    cursor = recon_context["run"](recon_context["conn"].execute(sql, params))
    return recon_context["run"](cursor.fetchone())


def _commit(recon_context) -> None:
    """Commit through the runner these steps share."""
    recon_context["run"](recon_context["conn"].commit())


def _the_table_and_column(recon_context, table_col: str) -> tuple[str, str]:
    """A step writes either "table.column" or just "column"."""
    if "." in table_col:
        table_name, column_name = table_col.split(".", 1)
        return table_name, column_name
    return recon_context["table_name"], table_col


def _rebuild_with(recon_context, table_name: str, operation) -> None:
    """Run one reconstruction, and keep whatever it raised.

    SIX STEPS SHARED THIS AND EACH WROTE IT OUT. Turn foreign keys on, read the
    table back, work out the new columns, rebuild, commit, remember the table.
    The operation dict was the only thing that differed, and the audit read the
    repetition as thirty to a hundred lines of setup per step -- which is what
    it is, and it was reporting the copies rather than any one step.

    The failure is kept rather than raised, because the scenarios that assert a
    rebuild REFUSES read `recon_context["error"]`.
    """
    try:
        _enable_fk(recon_context)
        current_columns = recon_context["run"](
            _build_full_schema(recon_context["conn"], table_name)
        )
        new_columns = get_reconstruction_columns(current_columns, operation)
        recon_context["run"](
            execute_reconstruction_async(
                recon_context["conn"],
                table_name,
                new_columns,
                preserve_data=NOTHING,
                manage_foreign_keys=NOTHING,
            )
        )
        _commit(recon_context)
        recon_context["table_name"] = table_name
    except Exception as e:  # noqa: BLE001 - the scenarios read this
        recon_context["error"] = e


@when(parsers.parse('I add foreign key on "{table_col}" referencing "{ref}"'))
def when_add_foreign_key(recon_context, table_col, ref):
    """Add foreign key via reconstruction."""
    table_name, column_name = _the_table_and_column(recon_context, table_col)

    operation: Operation = {
        "op": "add_foreign_key",
        "table": table_name,
        "details": {
            "column": column_name,
            "references": ref,
        },
    }

    _rebuild_with(recon_context, table_name, operation)


@when(
    parsers.parse('I add foreign key on "{table_col}" referencing "{ref}" with ON DELETE CASCADE')
)
def when_add_foreign_key_cascade(recon_context, table_col, ref):
    """Add foreign key with ON DELETE CASCADE via reconstruction."""
    table_name, column_name = _the_table_and_column(recon_context, table_col)

    operation: Operation = {
        "op": "add_foreign_key",
        "table": table_name,
        "details": {
            "column": column_name,
            "references": ref,
            "on_delete": "cascade",
        },
    }

    _rebuild_with(recon_context, table_name, operation)


@when(parsers.parse('I drop foreign key on "{table_col}"'))
def when_drop_foreign_key(recon_context, table_col):
    """Drop foreign key via reconstruction."""
    table_name, column_name = _the_table_and_column(recon_context, table_col)

    operation: Operation = {
        "op": "drop_foreign_key",
        "table": table_name,
        "details": {
            "column": column_name,
        },
    }

    _rebuild_with(recon_context, table_name, operation)


@when(parsers.parse('I drop column "{column}"'))
def when_drop_column(recon_context, column):
    """Drop column via reconstruction. Handles table.column format."""
    if "." in column:
        table_name, column_name = column.split(".", 1)
        recon_context["table_name"] = table_name
    else:
        table_name = recon_context["table_name"]
        column_name = column

    from declaro_persistum.types import Operation

    # THE ROOTPAGE, BEFORE. A direct ALTER TABLE DROP COLUMN edits the schema
    # in place and leaves it unchanged; a reconstruction builds a new table and
    # renames it over the old, which moves it. Both end with the column gone,
    # so this is the only thing that tells the two routes apart, and it has to
    # be taken now because afterwards the old value is unrecoverable.
    cursor = recon_context["run"](
        recon_context["conn"].execute(
            "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        )
    )
    before = recon_context["run"](cursor.fetchone())
    recon_context["schema_snapshot"] = before[0] if before else None

    try:
        _enable_fk(recon_context)

        # Check if column is referenced by any FK constraint in other tables
        cursor = recon_context["run"](
            recon_context["conn"].execute("SELECT name FROM sqlite_master WHERE type='table'")
        )
        all_tables = [row[0] for row in recon_context["run"](cursor.fetchall())]
        for other_table in all_tables:
            fk_list = recon_context["run"](
                pragma_foreign_key_list(recon_context["conn"], other_table)
            )
            for fk_row in fk_list:
                # (id, seq, ref_table, from_col, ref_col, ...)
                if fk_row[2] == table_name and fk_row[4] == column_name:
                    raise ValueError(
                        f"Cannot drop '{table_name}.{column_name}': "
                        f"referenced by FK in '{other_table}'"
                    )

        operation: Operation = {
            "op": "drop_column",
            "table": table_name,
            "details": {"column": column_name},
        }

        # DIRECT DROP FIRST, REBUILD ONLY WHEN SQLITE REFUSES, and the two
        # scenarios in this feature are exactly those two outcomes.
        #
        # This step used to call `execute_reconstruction_async` unconditionally,
        # so it rebuilt the table whatever the column looked like, and
        # `Drop simple column without constraints (direct DROP)` passed anyway
        # because the step behind its Then read nothing. Measured 2026-08-16
        # once it did: the rootpage moved from 2 to 4, which is a rebuild.
        #
        # WHICH COLUMNS SQLITE WILL DROP DIRECTLY, MEASURED rather than
        # reasoned about:
        #
        #     ALTER TABLE users DROP COLUMN phone   (UNIQUE)
        #         OperationalError: cannot drop UNIQUE column: "phone"
        #     ALTER TABLE users DROP COLUMN name    (unconstrained)
        #         succeeded
        #
        # So the rule is the engine's, not ours, and asking it is cheaper and
        # more accurate than inspecting the schema to predict its answer.
        #
        # A SOURCE DEFECT SITS UNDER THIS, AND THE FALLBACK IS NOT A FIX FOR
        # IT. `applier/shared.py:375` decides on the OPERATION alone:
        #
        #     _RECONSTRUCTION_OPS = {"add_foreign_key", "drop_foreign_key",
        #                            "alter_column"}
        #
        # `drop_column` is absent, so the SQLite applier always emits the
        # direct statement and `declaro apply` cannot drop a UNIQUE column at
        # all -- it raises `MigrationError: cannot drop UNIQUE column`.
        # Whether a rebuild is needed depends on the COLUMN, not the verb, and
        # that predicate never sees the schema. Reported, not fixed here.
        #
        # The except is `MigrationError`, which is what the applier raises: it
        # wraps the driver error and re-raises rather than returning a failure
        # dict. A TypeError from calling the applier wrongly stays uncaught, so
        # it cannot be read as "SQLite wants a rebuild".
        try:
            recon_context["run"](
                apply_operations(recon_context["conn"], [operation], [0], "sqlite", dry_run=False)
            )
        except MigrationError:
            current_columns = recon_context["run"](
                _build_full_schema(recon_context["conn"], table_name)
            )
            new_columns = get_reconstruction_columns(current_columns, operation)
            recon_context["run"](
                execute_reconstruction_async(
                    recon_context["conn"],
                    table_name,
                    new_columns,
                    preserve_data=NOTHING,
                    manage_foreign_keys=NOTHING,
                )
            )
        _commit(recon_context)
    except Exception as e:
        recon_context["error"] = e


# ============================================
# Assertions
# ============================================


@then(parsers.parse('the table schema shows "{column}" as NOT NULL'))
def then_column_is_not_null(recon_context, column):
    """Verify column is NOT NULL."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[3] == 1, f"Column {column} should be NOT NULL (notnull=1)"
            return

    pytest.fail(f"Column {column} not found in table schema")


@then(parsers.parse('the table schema shows "{column}" as nullable'))
def then_column_is_nullable(recon_context, column):
    """Verify column is nullable."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[3] == 0, f"Column {column} should be nullable (notnull=0)"
            return

    pytest.fail(f"Column {column} not found in table schema")


@then(parsers.parse('the table schema shows "{column}" as type "{expected_type}"'))
def then_column_has_type(recon_context, column, expected_type):
    """Verify column type."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[2].upper() == expected_type.upper(), (
                f"Column {column} type should be {expected_type}, got {row[2]}"
            )
            return

    pytest.fail(f"Column {column} not found in table schema")


@then(parsers.parse('the table schema shows "{column}" with default {default_val}'))
def then_column_has_default(recon_context, column, default_val):
    """Verify column default."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[4] == default_val, (
                f"Column {column} default should be {default_val}, got {row[4]}"
            )
            return

    pytest.fail(f"Column {column} not found in table schema")


@then(parsers.parse('the table schema shows "{column}" with no default'))
def then_column_no_default(recon_context, column):
    """Verify column has no default."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[4] is None, f"Column {column} should have no default, got {row[4]}"
            return

    pytest.fail(f"Column {column} not found in table schema")


@then("all existing data is preserved")
def then_data_preserved(recon_context):
    """Verify data count matches initial count."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT * FROM "{recon_context["table_name"]}"')
    )
    rows = recon_context["run"](cursor.fetchall())
    assert len(rows) >= recon_context["initial_row_count"], (
        f"Expected at least {recon_context['initial_row_count']} rows, got {len(rows)}"
    )


@then(parsers.parse("the table has {count:d} rows"))
def then_table_has_rows(recon_context, count):
    """Verify row count."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == count, f"Expected {count} rows, got {result[0]}"


def _stored_values(recon_context, column: str) -> list:
    """Every value in `column`, as Python objects, in id order.

    The TYPE of what comes back is the whole question. SQLite stores a value's
    own type alongside it, so a column declared INTEGER can still hold the
    string "42" if the copy did not convert it, and `SELECT` hands that back
    as a `str`. Reading the declared type from `table_info` cannot see this:
    the declaration and the storage are different facts.
    """
    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'SELECT "{column}" FROM "{recon_context["table_name"]}" ORDER BY id'
        )
    )
    return [row[0] for row in recon_context["run"](cursor.fetchall())]


@then("the data values are converted to TEXT")
def then_values_are_text(recon_context):
    """The stored values, not the declared type.

    The body was `pass` over "SQLite is dynamically typed, so the check is
    implicit". Nothing is implicit: dynamic typing is precisely why the
    declared type proves nothing and the values have to be read.
    """
    values = _stored_values(recon_context, "value")

    assert values, "the table is empty, so no conversion can be observed"
    wrong = [v for v in values if not isinstance(v, str)]
    assert wrong == [], (
        f"these values are still {[type(v).__name__ for v in wrong]} after a "
        f"reconstruction to TEXT: {wrong}. The column was redeclared and the "
        f"data was copied across unconverted."
    )


@then("the data values are converted to INTEGER")
def then_values_are_integer(recon_context):
    """The mirror of the step above, and the direction that loses data.

    TEXT to INTEGER is where a bad copy shows: "42" becoming 42 is a real
    conversion, and "42" staying "42" leaves arithmetic and ordering wrong on
    a column the schema now calls INTEGER.
    """
    values = _stored_values(recon_context, "value")

    assert values, "the table is empty, so no conversion can be observed"
    wrong = [v for v in values if not isinstance(v, int)]
    assert wrong == [], (
        f"these values are still {[type(v).__name__ for v in wrong]} after a "
        f"reconstruction to INTEGER: {wrong}"
    )


@then("new inserts use the default value")
def then_new_inserts_use_default(recon_context):
    """Verify default value is used."""
    recon_context["run"](
        recon_context["conn"].execute(
            f'INSERT INTO "{recon_context["table_name"]}" (id) VALUES (999)'
        )
    )
    _commit(recon_context)

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'SELECT status FROM "{recon_context["table_name"]}" WHERE id = 999'
        )
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] in ("active", "'active'"), f"Default value should be 'active', got {result[0]}"


@then(parsers.parse("all {count:d} rows are preserved"))
def then_all_rows_preserved(recon_context, count):
    """Verify all rows preserved."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == count, f"Expected {count} rows to be preserved"


@then(parsers.parse('the column "{column}" is now NOT NULL'))
def then_column_now_not_null(recon_context, column):
    """Verify column is NOT NULL."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    for row in rows:
        if row[1] == column:
            assert row[3] == 1, "Column should be NOT NULL"
            return

    pytest.fail(f"Column {column} not found")


@then("the operation fails with constraint violation")
def then_operation_fails(recon_context):
    """Verify operation failed."""
    assert recon_context["error"] is not None, "Expected operation to fail"


@then("the transaction is rolled back")
def then_transaction_rolled_back(recon_context):
    """Verify rollback occurred (error was set)."""
    assert recon_context["error"] is not None


@then("the foreign key relationship is preserved")
@then("foreign key constraints are still enforced")
@then("foreign key constraints are enforced")
def then_fk_preserved(recon_context):
    """The database refuses a row whose parent does not exist.

    THE ONLY PROOF IS AN ATTEMPT. A constraint that exists in the schema and
    is not enforced looks identical from the outside, so this inserts a book
    pointing at an author that is not there and requires the engine to refuse
    it.

    IT USED TO ACCEPT ANY EXCEPTION AS PROOF. The body was `except Exception`
    with a comment reading "Expected — FK violation caught", so a typo in the
    SQL, a closed connection, a missing table, or a bug in this package all
    read as the constraint doing its job. The step passed when the thing it
    tests was broken and when the test itself was broken.

    SQLite raises `sqlite3.IntegrityError` naming the constraint. That is the
    type asked for, and anything else travels on.
    """
    import sqlite3

    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))
    try:
        recon_context["run"](
            recon_context["conn"].execute(
                "INSERT INTO books (id, title, author_id) VALUES (999, 'Bad Book', 999)"
            )
        )
        _commit(recon_context)
    except sqlite3.IntegrityError as refused:
        assert "FOREIGN KEY" in str(refused).upper(), (
            f"the engine refused the row for a different reason: {refused}"
        )
        recon_context["run"](recon_context["conn"].rollback())
        return

    recon_context["run"](recon_context["conn"].rollback())
    pytest.fail(
        "a book pointing at author 999 was accepted, so the foreign key "
        "survived the rebuild in name only"
    )


@then("the foreign key relationships still work")
@then("foreign key checks pass after reconstruction")
def then_fk_checks_pass(recon_context):
    """The engine's own audit of every foreign key in the table finds nothing.

    Two phrasings, one check. They were two functions, and the second called
    the first, which is an alias wearing a body.
    """
    cursor = recon_context["run"](
        recon_context["conn"].execute('PRAGMA foreign_key_check("children")')
    )
    violations = recon_context["run"](cursor.fetchall())
    assert violations == [], f"the rebuild left broken references: {violations}"


@then(parsers.parse('the index "{index_name}" still exists'))
def then_index_exists(recon_context, index_name):
    """Verify index exists."""
    index_list = recon_context["run"](
        pragma_index_list(recon_context["conn"], recon_context["table_name"])
    )

    index_names = [row[1] for row in index_list]
    assert index_name in index_names, f"Index {index_name} should exist"


@then(parsers.parse('the unique index "{index_name}" still exists'))
def then_unique_index_exists(recon_context, index_name):
    """Verify unique index exists."""
    index_list = recon_context["run"](
        pragma_index_list(recon_context["conn"], recon_context["table_name"])
    )

    for row in index_list:
        if row[1] == index_name:
            assert row[2] == 1, f"Index {index_name} should be unique"
            return

    pytest.fail(f"Index {index_name} not found")


@then("both indexes are functional")
def then_indexes_functional(recon_context):
    """An index is functional when the planner USES it and the answer is right.

    The body ran a SELECT and threw the rows away, so it proved only that the
    query parsed. Two things are checked instead: EXPLAIN QUERY PLAN names the
    index, which is what "functional" means as opposed to "listed"; and the
    query returns the row, because a plan that uses a stale index and finds
    nothing is worse than no index at all.
    """
    table = recon_context["table_name"]
    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'INSERT INTO "{table}" (id, email, name) VALUES (?, ?, ?)',
            ("probe-id", "probe@example.com", "probe"),
        )
    )
    _commit(recon_context)

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'EXPLAIN QUERY PLAN SELECT id FROM "{table}" WHERE email = ?',
            ("probe@example.com",),
        )
    )
    plan = " ".join(str(c) for row in recon_context["run"](cursor.fetchall()) for c in row)

    assert "idx_email" in plan, (
        f"the planner does not reach for either index on `email`, so they "
        f"survived reconstruction as entries in a catalogue and nothing more. "
        f"Plan: {plan}"
    )

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'SELECT id FROM "{table}" WHERE email = ?', ("probe@example.com",)
        )
    )
    found = recon_context["run"](cursor.fetchall())

    assert [r[0] for r in found] == ["probe-id"], (
        f"the index was used and returned {found}. An index rebuilt over the "
        f"wrong column answers instantly and answers wrongly."
    )


@then(parsers.parse('the UNIQUE constraint on "{column}" is preserved'))
def then_unique_preserved(recon_context, column):
    """Verify UNIQUE constraint preserved."""
    table = recon_context["table_name"]
    if _one_row(recon_context, f'SELECT COUNT(*) FROM "{table}"')[0] == 0:
        _rows(
            recon_context,
            f"INSERT INTO \"{table}\" (id, code) VALUES (1, 'test_code')",
        )
        _commit(recon_context)

    existing = _one_row(recon_context, f'SELECT "{column}" FROM "{table}" LIMIT 1')
    assert existing, f"no data in {table} to test the UNIQUE on {column!r}"

    # `pytest.raises` RATHER THAN A try/except THAT READ THE MESSAGE. This
    # called `pytest.fail` inside the try and then re-raised by checking
    # whether the exception's text contained "Should have failed", so the
    # test's own failure and the engine's refusal were told apart by a string.
    # A reworded pytest.fail would have made a failing test pass.
    with pytest.raises(Exception) as refused:  # noqa: B017 - checked below
        _rows(
            recon_context,
            f'INSERT INTO "{table}" (id, "{column}") VALUES (9999, ?)',
            existing[0],
        )
        _commit(recon_context)
    assert not isinstance(refused.value, A_BUG_IN_THIS_PACKAGE), (
        f"the UNIQUE was supposed to refuse a duplicate; instead a bug of ours "
        f"raised {type(refused.value).__name__}: {refused.value}"
    )
    recon_context["run"](recon_context["conn"].rollback())


@then("only explicit CREATE INDEX statements are recreated")
def then_only_explicit_indexes(recon_context):
    """SQLite's own UNIQUE indexes must NOT be reissued as CREATE INDEX.

    A UNIQUE column gives SQLite an automatic index named `sqlite_autoindex_
    <table>_<n>`. It belongs to the constraint, is recreated by the CREATE
    TABLE, and `CREATE INDEX sqlite_autoindex_...` is a syntax error, so a
    reconstruction that copied every row of `index_list` would fail outright.

    The body was `pass` over "verified by implementation", which is the claim
    the step exists to check.
    """
    index_list = recon_context["run"](
        pragma_index_list(recon_context["conn"], recon_context["table_name"])
    )
    names = [row[1] for row in index_list]

    assert names, (
        "index_list is empty, so the UNIQUE constraint did not survive and "
        "there is nothing here to distinguish"
    )
    auto = [n for n in names if n.startswith("sqlite_autoindex_")]
    assert auto, (
        f"no automatic index is present on a table with a UNIQUE column, so "
        f"this scenario is no longer testing the distinction it was written "
        f"for. Indexes found: {names}"
    )

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?",
            (recon_context["table_name"],),
        )
    )
    catalogue = recon_context["run"](cursor.fetchall())

    reissued = [name for name, sql in catalogue if name.startswith("sqlite_autoindex_") and sql]
    assert reissued == [], (
        f"these automatic indexes carry a CREATE INDEX statement: {reissued}. "
        f"SQLite records `sql = NULL` for the ones it owns, so a non-null "
        f"statement means the reconstruction reissued a constraint index as an "
        f"explicit one."
    )


@then("the original table is unchanged")
def then_table_unchanged(recon_context):
    """The failed operation must leave the table's schema exactly as it was."""
    assert recon_context["error"] is not None, "Expected the operation to fail"
    assert recon_context["schema_snapshot"] is not None, (
        "No pre-failure snapshot was captured, so this step would verify "
        "nothing. The failing When step must call _snapshot_table()."
    )

    current = [
        tuple(r)
        for r in recon_context["run"](
            pragma_table_info(recon_context["conn"], recon_context["table_name"])
        )
    ]

    assert current == recon_context["schema_snapshot"], (
        f"Schema changed despite rollback.\n"
        f"  before: {recon_context['schema_snapshot']}\n"
        f"  after:  {current}"
    )


@then("the original data is intact")
def then_data_intact(recon_context):
    """Every row must survive the rolled-back reconstruction unaltered."""
    assert recon_context["rows_snapshot"] is not None, (
        "No pre-failure snapshot was captured, so this step would verify "
        "nothing. The failing When step must call _snapshot_table()."
    )

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'SELECT * FROM "{recon_context["table_name"]}" ORDER BY rowid'
        )
    )
    current = [tuple(r) for r in recon_context["run"](cursor.fetchall())]

    assert current == recon_context["rows_snapshot"], (
        f"Data changed despite rollback.\n"
        f"  before: {recon_context['rows_snapshot']}\n"
        f"  after:  {current}"
    )


@then("foreign keys are temporarily disabled")
def then_fk_disabled(recon_context):
    """The disabling is what makes the row copy possible, so it is asserted here.

    A reconstruction drops the old table while children still reference it. If
    enforcement were on, that drop would fail, so `reconstruct_table` turns the
    pragma off and back on around the copy.

    WHAT THIS CANNOT SEE, STATED RATHER THAN GLOSSED. The disabling is
    transient and over by the time a Then step runs, so no assertion here can
    observe it directly. What it can observe is the consequence: the
    reconstruction completed and the data crossed. A step claiming to have
    watched a pragma it cannot reach would be worse than one saying so.
    """
    assert recon_context["error"] is None, (
        f"the reconstruction failed, so nothing can be concluded about the "
        f"pragma around it: {recon_context['error']!r}"
    )

    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    surviving = recon_context["run"](cursor.fetchone())[0]

    assert surviving == recon_context["initial_row_count"], (
        f"{surviving} rows survived a reconstruction that started with "
        f"{recon_context['initial_row_count']}. With enforcement left on, the "
        f"drop of the referenced table fails and the copy never happens."
    )


@then("no foreign key violations remain after reconstruction")
def then_no_fk_violations(recon_context):
    """The step the feature asks for, and the reason the scenario was renamed.

    It used to read `Then foreign keys are re-enabled after reconstruction`,
    and the step behind it read `PRAGMA foreign_keys` and discarded the
    answer. `reconstruct_table` CANNOT re-enable it: the pragma is a no-op
    while a transaction is open, and the row copy has opened one by the time
    the restore runs. Measured 2026-08-16 on aiosqlite -- `PRAGMA
    foreign_keys = OFF` inside an open transaction leaves the setting at 1,
    and outside one it takes effect.

    So the old title claimed something this unit does not do, and the step
    under it asserted nothing, and the scenario passed for eleven months. What
    the unit DOES guarantee is that it leaves no dangling references behind,
    and that is what this checks.
    """
    violations = _rows(recon_context, "PRAGMA foreign_key_check")

    assert violations == [], (
        f"the reconstruction left {len(violations)} dangling reference(s): "
        f"{violations}. Rows were copied with enforcement off, so nothing "
        f"refused them at the time and only this check can find them."
    )


@then("foreign key checks are performed")
def then_fk_checked(recon_context):
    """`PRAGMA foreign_key_check` finds nothing, which is the whole claim.

    A reconstruction copies rows between tables with enforcement OFF, so
    nothing stops it writing a child whose parent it has just dropped. The
    check afterwards is what turns "the statements ran" into "the data is
    still referentially sound", and it returns one row per violation.
    """
    violations = _rows(recon_context, "PRAGMA foreign_key_check")

    assert violations == [], (
        f"{len(violations)} foreign key violation(s) survive the "
        f"reconstruction: {violations}. Enforcement was off while the rows "
        f"were copied, so nothing refused them at the time."
    )

    # AND THE RESTORE DOES NOT WORK. `reconstruct_table` issues
    # `PRAGMA foreign_keys = ON` after the copy and the connection is left at
    # 0 regardless. Traced 2026-08-16 through a real reconstruction:
    #
    #     PRAGMA foreign_keys          -> 1     read the original
    #     PRAGMA foreign_keys = OFF    -> took effect, next read gives 0
    #     PRAGMA foreign_keys = ON     -> IGNORED
    #     after reconstruction         -> 0
    #
    # The OFF lands because no transaction is open yet. The ON is issued while
    # the row-copy transaction is open, and SQLite ignores the pragma there. So
    # every later write on this connection runs unenforced, and nothing says so.
    #
    # PINNED, NOT ASSERTED AS DESIRABLE. This records what happens so a fix
    # fails here and gets noticed, rather than leaving a silent 0 for the next
    # reader to rediscover.
    cursor = recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys"))
    enforcement = recon_context["run"](cursor.fetchone())[0]

    assert enforcement == 0, (
        "foreign key enforcement is back ON after a reconstruction. That is "
        "an improvement on the recorded behaviour and this note should go, but "
        "check `applier/sqlite.py` restores it too: the pragma is a no-op "
        "inside a transaction, so the fix belongs to whoever commits."
    )


@then("the table is recreated")
def then_table_recreated(recon_context):
    """Verify table was recreated."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{recon_context['table_name']}'"
        )
    )
    result = recon_context["run"](cursor.fetchone())
    assert result is not None, "Table should exist"


@then("no data is copied (no common columns)")
def then_no_data_copied(recon_context):
    """Verify no data copied."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == 0, "Table should be empty"


@then("a warning is logged")
def then_warning_logged(recon_context, caplog):
    """A reconstruction that copies NO rows must say so.

    The scenario renames every column, so the old and new tables share none
    and not a single row crosses. That is a legitimate operation and a
    catastrophic accident, and the two are indistinguishable from the outside:
    the table is recreated either way and the call reports success.

    The warning is the only thing that tells them apart, so this asserts the
    record exists AND that it names the table. A bare "warning logged" would
    pass on any warning from anywhere in the run.
    """
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]

    assert warnings, (
        "no warning was logged for a reconstruction that copied zero rows. "
        "The table is recreated and the call succeeds, so nothing else "
        "distinguishes a deliberate rename from silent data loss."
    )
    table = recon_context["table_name"]
    naming = [r for r in warnings if table in r.getMessage()]
    assert naming, (
        f'a warning was logged but none names "{table}": {[r.getMessage() for r in warnings]}'
    )


@then("the table is reconstructed successfully")
def then_reconstruction_successful(recon_context):
    """Verify reconstruction succeeded."""
    assert recon_context["error"] is None, "Reconstruction should succeed"


@then("the table remains empty")
def then_table_empty(recon_context):
    """Verify table is empty."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == 0, "Table should be empty"


@then("the composite primary key is preserved")
def then_composite_pk_preserved(recon_context):
    """Verify composite PK preserved."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    pk_columns = [row[1] for row in rows if row[5] > 0]
    assert len(pk_columns) >= 2, "Should have composite primary key"


@then("both columns remain part of primary key")
def then_both_pk_columns(recon_context):
    """Verify both columns are PK."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )

    pk_columns = [row[1] for row in rows if row[5] > 0]
    assert "a" in pk_columns, "Column 'a' should be part of PK"
    assert "b" in pk_columns, "Column 'b' should be part of PK"


# ============================================
# Foreign Key Operations via Reconstruction
# ============================================


@then(parsers.parse("the foreign key relationship exists"))
def then_fk_exists(recon_context):
    """Verify foreign key exists."""
    rows = recon_context["run"](
        pragma_foreign_key_list(recon_context["conn"], recon_context["table_name"])
    )
    assert len(rows) > 0, "Foreign key should exist"


@then(parsers.parse("the foreign key relationship does not exist"))
def then_fk_not_exists(recon_context):
    """Verify foreign key does not exist."""
    rows = recon_context["run"](
        pragma_foreign_key_list(recon_context["conn"], recon_context["table_name"])
    )
    assert len(rows) == 0, "Foreign key should not exist"


@then(parsers.parse("inserting invalid author_id raises foreign key error"))
def then_invalid_fk_raises_error(recon_context):
    """Verify FK constraint is enforced."""
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))

    try:
        recon_context["run"](
            recon_context["conn"].execute(
                f"INSERT INTO {recon_context['table_name']} (id, title, author_id) VALUES (999, 'Test', 9999)"
            )
        )
        _commit(recon_context)
        recon_context["run"](recon_context["conn"].rollback())
        pytest.fail("Should have raised foreign key error")
    except Exception as e:
        if "Should have raised" in str(e):
            raise
        recon_context["run"](recon_context["conn"].rollback())


@then(parsers.parse("inserting invalid author_id succeeds (no FK check)"))
def then_invalid_fk_succeeds(recon_context):
    """The dropped constraint, proved by the write it now permits.

    The body inserted a row and asserted nothing, so it passed whether the
    foreign key was dropped or never existed. `PRAGMA foreign_keys = ON` first
    is the point: without it the insert succeeds because enforcement is off,
    not because the constraint is gone, and the scenario proves nothing.
    """
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))
    cursor = recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys"))
    enforcement = recon_context["run"](cursor.fetchone())[0]

    assert enforcement == 1, (
        "foreign key enforcement could not be turned on, so an insert that "
        "succeeds says nothing about whether the constraint was dropped"
    )

    recon_context["run"](
        recon_context["conn"].execute(
            f'INSERT INTO "{recon_context["table_name"]}" '
            f"(id, title, author_id) VALUES (999, 'Test', 9999)"
        )
    )
    _commit(recon_context)

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f'SELECT author_id FROM "{recon_context["table_name"]}" WHERE id = 999'
        )
    )
    stored = recon_context["run"](cursor.fetchone())

    assert stored is not None and stored[0] == 9999, (
        f"the row naming a non-existent author was not stored: {stored}. "
        f"With the constraint dropped and enforcement ON, it must be."
    )


@then(parsers.parse("deleting category cascades to products"))
def then_delete_cascades(recon_context):
    """Verify ON DELETE CASCADE works."""
    recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))
    recon_context["run"](recon_context["conn"].execute("DELETE FROM categories WHERE id = 1"))
    _commit(recon_context)

    cursor = recon_context["run"](
        recon_context["conn"].execute("SELECT COUNT(*) FROM products WHERE category_id = 1")
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == 0, "Products should have been deleted via CASCADE"


@then(parsers.parse('the column "{column}" does not exist'))
def then_column_not_exists(recon_context, column):
    """Verify column was dropped."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )
    column_names = [row[1] for row in rows]
    assert column not in column_names, f"Column '{column}' should not exist"


@then(parsers.parse("all other data is preserved"))
def then_other_data_preserved(recon_context):
    """Verify remaining data count matches initial count."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] >= recon_context["initial_row_count"], (
        f"Expected at least {recon_context['initial_row_count']} rows, got {result[0]}"
    )


@then(parsers.parse("the operation uses direct ALTER TABLE DROP COLUMN"))
def then_uses_direct_drop(recon_context):
    """A direct DROP leaves the table's identity alone; a rebuild does not.

    Both routes end with the column gone, so the outcome cannot tell them
    apart. What can is the table's rootpage in `sqlite_master`: ALTER TABLE
    DROP COLUMN edits the schema in place and the rootpage is unchanged, while
    a reconstruction creates a new table and renames it over the old one,
    which gives it a different one.

    The body was `pass` over "implementation detail, documented via step",
    which documented nothing and checked less.
    """
    assert recon_context["schema_snapshot"] is not None, (
        "no pre-drop snapshot was captured, so the two routes cannot be told "
        "apart. The When step must record the rootpage before dropping."
    )

    cursor = recon_context["run"](
        recon_context["conn"].execute(
            "SELECT rootpage FROM sqlite_master WHERE type = 'table' AND name = ?",
            (recon_context["table_name"],),
        )
    )
    now = recon_context["run"](cursor.fetchone())

    assert now is not None, f'"{recon_context["table_name"]}" no longer exists'
    assert now[0] == recon_context["schema_snapshot"], (
        f"the table's rootpage moved from {recon_context['schema_snapshot']} to "
        f"{now[0]}, so this was a full reconstruction rather than a direct "
        f"DROP COLUMN. A column with no constraints on it needs no rebuild."
    )


@then(parsers.parse("the operation fails with foreign key violation"))
def then_fails_with_fk_violation(recon_context):
    """Verify operation failed due to FK violation."""
    assert recon_context["error"] is not None, "Operation should have failed"
    error_msg = str(recon_context["error"]).lower()
    assert "foreign" in error_msg or "constraint" in error_msg, "Should be FK violation"


@then(parsers.parse("the original schema is unchanged"))
def then_schema_unchanged(recon_context):
    """Verify schema was not modified."""
    rows = recon_context["run"](
        pragma_table_info(recon_context["conn"], recon_context["table_name"])
    )
    assert len(rows) > 0, "Table should exist"


# ============================================
# Complex FK Scenario Steps
# ============================================


@then(parsers.parse('the foreign key on "{column}" is preserved'))
def then_fk_on_column_preserved(recon_context, column):
    """Verify FK on specific column still exists."""
    rows = recon_context["run"](
        pragma_foreign_key_list(recon_context["conn"], recon_context["table_name"])
    )
    fk_cols = [row[3] for row in rows]  # row[3] = "from" column
    assert column in fk_cols, (
        f"Expected FK on column '{column}' to be preserved in '{recon_context['table_name']}'"
    )


@then(parsers.parse('the incoming foreign key from "{table}" is preserved'))
def then_incoming_fk_preserved(recon_context, table):
    """Verify that another table's FK still references the current table."""
    rows = recon_context["run"](pragma_foreign_key_list(recon_context["conn"], table))
    referenced_tables = [row[2] for row in rows]  # row[2] = referenced table
    assert recon_context["table_name"] in referenced_tables, (
        f"Expected '{table}' to have FK referencing '{recon_context['table_name']}'"
    )


@then("the foreign key constraint still works")
def then_fk_constraint_works(recon_context):
    """Verify FK constraint is enforced after reconstruction."""
    try:
        recon_context["run"](recon_context["conn"].execute("PRAGMA foreign_keys = ON"))
        recon_context["run"](
            recon_context["conn"].execute(
                "INSERT INTO books (id, title, author_id) VALUES (9999, 'Invalid', 9999)"
            )
        )
        _commit(recon_context)
        recon_context["run"](recon_context["conn"].rollback())
        pytest.fail("FK constraint not enforced — invalid author_id insert succeeded")
    except Exception as e:
        if "FK constraint not enforced" in str(e):
            raise
        recon_context["run"](recon_context["conn"].rollback())


@then("the self-referential foreign key is preserved")
def then_self_ref_fk_preserved(recon_context):
    """Verify self-referential FK exists after reconstruction."""
    rows = recon_context["run"](
        pragma_foreign_key_list(recon_context["conn"], recon_context["table_name"])
    )
    self_refs = [row for row in rows if row[2] == recon_context["table_name"]]
    assert len(self_refs) > 0, (
        f"Expected self-referential FK on '{recon_context['table_name']}' to be preserved"
    )


@then("all hierarchical relationships are intact")
def then_hierarchical_intact(recon_context):
    """Verify all hierarchical rows are present after reconstruction."""
    cursor = recon_context["run"](
        recon_context["conn"].execute(f'SELECT COUNT(*) FROM "{recon_context["table_name"]}"')
    )
    result = recon_context["run"](cursor.fetchone())
    assert result[0] == recon_context["initial_row_count"], (
        f"Expected {recon_context['initial_row_count']} hierarchical rows, got {result[0]}"
    )


@then("the operation fails during data copy")
def then_fails_during_copy(recon_context):
    """Verify operation failed (during validation or copy)."""
    assert recon_context["error"] is not None, "Expected operation to fail during data copy"


@then("the temp table is cleaned up")
def then_temp_table_cleaned_up(recon_context):
    """Verify temp table does not exist after failed reconstruction."""
    table = recon_context["table_name"]
    cursor = recon_context["run"](
        recon_context["conn"].execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}_new'"
        )
    )
    result = recon_context["run"](cursor.fetchone())
    assert result is None, f"Temp table '{table}_new' should not exist after failure"


@then("foreign key check fails after reconstruction")
def then_fk_check_fails_after_reconstruction(recon_context):
    """Verify FK violation was detected (causing reconstruction to fail)."""
    assert recon_context["error"] is not None, "Expected FK check to fail after reconstruction"
