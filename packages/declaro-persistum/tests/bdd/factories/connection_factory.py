"""
Connection factories for managing database connections in tests.
"""

import os
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, TypedDict

from declaro_persistum.types import Schema

# =============================================================================
# Connection URLs
# =============================================================================

# Use a temp file for SQLite tests so tables persist across connections
_SQLITE_TEMP_DB = os.path.join(tempfile.gettempdir(), "declaro_test.db")

# Same for Turso — a local file so tables persist across connections.
_TURSO_TEMP_DB = os.path.join(tempfile.gettempdir(), "declaro_test_turso.db")






def get_turso_url() -> str:
    """Get the local Turso database path for BDD runs.

    Deliberately a local temp file, and deliberately NOT read from
    TEST_TURSO_URL. That variable holds a *remote* libsql:// URL, and it was
    being handed to turso.connect(), which takes a local filesystem path —
    the engine tried to open a file literally named "libsql://..." and failed
    with "IoError: open: NotFound". Pointing these BDD scenarios at a real
    remote database is also not something a default test run should do.
    """
    return _TURSO_TEMP_DB


def resolve_url(variable: str, *, default: str) -> str:
    """Read an environment variable, telling unset apart from set-to-nothing.

    THIS REPLACES `os.environ.get(variable, default)`, which collapsed three
    cases into two. Unset means a developer machine and the default is right.
    Set to a real value means an operator chose. Set to an EMPTY value means an
    operator meant to choose and did not, and answering that with the default
    runs the suite against localhost while reporting a pass against whatever
    they thought they had configured.

    `default` is keyword-only and required. A default for the default would
    put the same silence one level up.
    """
    configured = os.environ.get(variable)
    if configured is None:
        return default
    if not configured.strip():
        raise ValueError(
            f"{variable} is set to an empty value. Unset it to use the default "
            f"({default}), or set it to a real one. An empty setting is an "
            f"operator's mistake and this refuses to guess which they meant."
        )
    return configured


def postgresql_url() -> str:
    """Where the PostgreSQL scenarios connect.

    Reads `TEST_POSTGRESQL_URL`, the same variable `tests/conftest.py` reads
    for `require_postgresql`, and shares its default constant. Two names for
    one setting would let the fixture probe one server while the scenarios ran
    against another, and the suite would report a pass.
    """
    from tests.conftest import DEFAULT_POSTGRESQL_URL

    return resolve_url("TEST_POSTGRESQL_URL", default=DEFAULT_POSTGRESQL_URL)


def sqlite_path() -> str:
    """The local SQLite file for BDD runs. Deliberately not configurable.

    NOT READ FROM `TEST_SQLITE_URL`, for the reason `turso_path` records
    below: these engines take a filesystem path and the variable holds a
    remote url. A temp FILE rather than `:memory:`, because steps in one
    scenario open separate connections and an in-memory database is private to
    each one, so a Given that created a table would be invisible to its When.
    """
    return os.path.join(tempfile.gettempdir(), "declaro_test.db")


def turso_path() -> str:
    """The local Turso database for BDD runs. Deliberately not configurable.

    `TEST_TURSO_URL` holds a REMOTE `libsql://` url, and it was being handed to
    `turso.connect()`, which takes a local filesystem path. The engine tried to
    open a file literally named "libsql://..." and failed with
    `IoError: open: NotFound`. Pointing these scenarios at a real remote
    database is also not something a default test run should do.

    A separate file from `sqlite_path`, so neither engine's scenarios can be
    made to pass by the other engine's leftovers.
    """
    return os.path.join(tempfile.gettempdir(), "declaro_test_turso.db")


# =============================================================================
# SQLite Connection
# =============================================================================

@asynccontextmanager
async def get_sqlite_connection() -> AsyncGenerator[Any, None]:
    """Get an SQLite connection."""
    import aiosqlite

    url = sqlite_path()
    async with aiosqlite.connect(url) as conn:
        conn.row_factory = aiosqlite.Row
        yield conn




async def teardown_sqlite_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in SQLite."""
    for table_name in schema.keys():
        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    await conn.commit()


# =============================================================================
# Turso Connection - Sync API
# =============================================================================



def teardown_turso_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in Turso (sync API)."""
    for table_name in schema.keys():
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()


# =============================================================================
# PostgreSQL Connection
# =============================================================================

@asynccontextmanager
async def get_postgresql_connection() -> AsyncGenerator[Any, None]:
    """Get a PostgreSQL connection."""
    import asyncpg

    url = postgresql_url()
    conn = await asyncpg.connect(url)
    try:
        yield conn
    finally:
        await conn.close()




async def teardown_postgresql_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in PostgreSQL."""
    # Drop in reverse order to handle foreign keys
    for table_name in reversed(list(schema.keys())):
        await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")



# =============================================================================
# Building the DDL: pure functions, executed at the boundary below (Rule 4)
# =============================================================================
#
# THIS REPLACES THREE FUNCTIONS AND ELEVEN SILENT FALLBACKS. `setup_sqlite_
# schema` and `setup_turso_schema` held the same forty lines twice, and each
# read `col_def.get("type", "text")` -- so a column with no declared type
# became TEXT and every scenario built on it tested a table nobody wrote.
#
# The DDL is built by `column_sql` and `create_table_sql`, which take data and
# return a string. Nothing here opens a connection, so the whole translation
# is testable with `f(input) == expected` and no database.

# PostgreSQL type names to SQLite ones. SQLite has no uuid, no timestamptz, no
# boolean and no jsonb, so a schema written in PostgreSQL's vocabulary needs
# translating before it reaches it.
TO_SQLITE_TYPE = {
    "uuid": "TEXT",
    "timestamptz": "TEXT",
    "timestamp": "TEXT",
    "boolean": "INTEGER",
    "serial": "INTEGER",
    "bigserial": "INTEGER",
    "jsonb": "TEXT",
    "json": "TEXT",
    "numeric": "REAL",
    "real": "REAL",
    "float": "REAL",
}

# SQLite has neither function. A `now()` default reaching it raises
# `unknown function: now()`, which is why `examples/todo_app_native` supplies
# its own timestamp rather than letting the default fire.
TO_SQLITE_DEFAULT = {
    "now()": "(datetime('now'))",
    "gen_random_uuid()": "(lower(hex(randomblob(16))))",
}

# Turso is SQLite-compatible for both, so one entry serves two engines rather
# than a second copy of the tables above.
TYPE_TRANSLATIONS: dict[str, dict[str, str]] = {
    "sqlite": TO_SQLITE_TYPE,
    "turso": TO_SQLITE_TYPE,
    "postgresql": {},
}

DEFAULT_TRANSLATIONS: dict[str, dict[str, str]] = {
    "sqlite": TO_SQLITE_DEFAULT,
    "turso": TO_SQLITE_DEFAULT,
    "postgresql": {},
}


def column_sql(name: str, column: dict[str, Any], dialect: str) -> str:
    """One column's DDL fragment. Pure.

    A COLUMN WITH NO TYPE RAISES. It used to become TEXT, which is the input
    side of silent failure: the builder could not tell "declared text" from
    "forgot to declare", and a scenario would go green against a table that
    did not match its own schema.

    A TYPE OUTSIDE THE TRANSLATION TABLE PASSES THROUGH, and that is a branch
    rather than a default. SQLite accepts any type name and assigns affinity
    by rule, so "not translated" has a defined meaning here, unlike "not
    declared" above.
    """
    if dialect not in TYPE_TRANSLATIONS:
        raise ValueError(
            f"Unsupported dialect: {dialect}. "
            f"Builders exist for: {', '.join(sorted(TYPE_TRANSLATIONS))}"
        )
    if "type" not in column:
        raise ValueError(
            f"column {name!r} declares no type. A schema is the input to every "
            f"scenario that uses it, so an undeclared type is a broken fixture "
            f"rather than a column to guess at."
        )

    declared = column["type"]
    types = TYPE_TRANSLATIONS[dialect]
    base = declared.lower().split("(")[0]
    if base in types:
        rendered = types[base]
    elif dialect == "postgresql":
        rendered = declared
    else:
        rendered = declared.upper()

    parts = [name, rendered]
    if column.get("primary_key"):
        parts.append("PRIMARY KEY")
    if column.get("nullable") is False:
        parts.append("NOT NULL")
    if column.get("unique"):
        parts.append("UNIQUE")
    if "default" in column:
        defaults = DEFAULT_TRANSLATIONS[dialect]
        value = column["default"]
        parts.append(f"DEFAULT {defaults[value] if value in defaults else value}")
    if "references" in column and dialect == "postgresql":
        target = column["references"].replace(".", "(") + ")"
        on_delete = column.get("on_delete")
        clause = f"REFERENCES {target}"
        if on_delete:
            clause += f" ON DELETE {on_delete.upper()}"
        parts.append(clause)

    return " ".join(parts)


def create_table_sql(name: str, table: dict[str, Any], dialect: str) -> str:
    """One CREATE TABLE. Pure.

    A TABLE WITH NO COLUMNS RAISES, for the same reason a column with no type
    does. `table_def.get("columns", {})` answered an empty table, which is
    valid SQL and not what anybody declared.
    """
    columns = table.get("columns")
    if not columns:
        raise ValueError(
            f"table {name!r} declares no columns, so there is nothing to create"
        )

    parts = [column_sql(col, spec, dialect) for col, spec in columns.items()]
    if "primary_key" in table:
        parts.append(f"PRIMARY KEY ({', '.join(table['primary_key'])})")

    return f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(parts)})"


# --- the boundary: the same statements, executed ---------------------------


async def setup_sqlite_schema(conn: Any, schema: Schema) -> None:
    """Create every table. Async, because aiosqlite is."""
    for name, table in schema.items():
        await conn.execute(create_table_sql(name, table, "sqlite"))
    await conn.commit()


def setup_turso_schema(conn: Any, schema: Schema) -> None:
    """The same statements, synchronously, because pyturso's sync API is."""
    for name, table in schema.items():
        conn.execute(create_table_sql(name, table, "turso"))
    conn.commit()


async def setup_postgresql_schema(conn: Any, schema: Schema) -> None:
    """No commit: asyncpg autocommits outside an explicit transaction."""
    for name, table in schema.items():
        await conn.execute(create_table_sql(name, table, "postgresql"))

# =============================================================================
# Connection Factory
# =============================================================================


# A backend is DATA: the three callables a scenario needs, carried in a
# TypedDict. This replaces `ConnectionFactory`, a class whose three
# constructors and three methods did nothing but choose between the
# module-level functions above and then forward to them.
#
# The shape follows honest-persist's opener: (db_id, connect, classify,
# close, size)` — the dependency is injected as a callable, and what the
# scenario holds is a value it can print, compare and pass on.


class Backend(TypedDict):
    """One database backend, as the callables a scenario needs."""

    dialect: str
    connect: Any  # () -> async context manager yielding a connection
    setup: Any  # (conn, schema) -> None | awaitable
    teardown: Any  # (conn, schema) -> None | awaitable


@asynccontextmanager
async def get_turso_connection() -> AsyncGenerator[Any, None]:
    """Open a local Turso database for a scenario.

    This lived inside `ConnectionFactory` and was the only connect function
    with no module-level twin, so it is the one piece the deletion actually
    removed rather than merely unwrapped.
    """
    import turso

    conn = turso.connect(turso_path())
    try:
        yield conn
    finally:
        conn.close()


def sqlite_backend() -> Backend:
    return {
        "dialect": "sqlite",
        "connect": get_sqlite_connection,
        "setup": setup_sqlite_schema,
        "teardown": teardown_sqlite_schema,
    }


def postgresql_backend() -> Backend:
    return {
        "dialect": "postgresql",
        "connect": get_postgresql_connection,
        "setup": setup_postgresql_schema,
        "teardown": teardown_postgresql_schema,
    }


def turso_backend() -> Backend:
    return {
        "dialect": "turso",
        "connect": get_turso_connection,
        "setup": setup_turso_schema,
        "teardown": teardown_turso_schema,
    }


BACKENDS = {
    "sqlite": sqlite_backend,
    "postgresql": postgresql_backend,
    "turso": turso_backend,
}
"""The bounded vocabulary of backends, as a dispatch table (Rule 1)."""


async def apply_step(fn: Any, *args: Any) -> Any:
    """Call a setup/teardown that may be sync or async.

    Turso's schema helpers are synchronous and SQLite's are not. The
    difference belongs here, at the one place that knows it, rather than in
    every step that calls one.
    """
    result = fn(*args)
    if hasattr(result, "__await__"):
        return await result
    return result
