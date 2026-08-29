"""Database connections for the todo example, as data and functions.

Four backends, one surface. The app code does not change; only this module
knows the differences, and it knows them as TABLE ENTRIES rather than as
branches.

WHY THIS FILE IS WRITTEN THIS WAY. An example is the most-copied code a library
ships. A reader opens it before the README and long before `src/`, and whatever
shape it uses becomes the shape of their application. This file used to teach
the shape declaro-persistum exists to replace:

    12  dispatch chains   `if config.dialect == "sqlite": ... elif ...`
     2  implicit defaults `get_sqlite_config(database_path=None)`
     2  broad handlers    every failure flattened to `(False, "message")`
     1  data class        `@dataclass class DatabaseConfig`, three fields

The package itself dispatches with `APPLIERS`, `INSPECTORS`, `CONNECTORS`,
`BULK_LOADERS`, `WRITERS` and more. A chain that runs out of branches does
nothing and says nothing; a table raises, naming the engines that exist.

IT ALSO IMPORTED `libsql_experimental` FOR TURSO CLOUD. That package was
eradicated from this repository and replaced by pyturso, and it is not a
dependency of anything here, so the import failed and the `except` reported the
backend as unavailable. The Turso Cloud button was dead and blamed the reader's
environment. Turso Cloud is pyturso with a `remote_url`.

This file is byte-identical across all three example apps, and a test holds it
that way.
"""

from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, TypedDict

from declaro_persistum.boundary import boundary

# ---------------------------------------------------------------------------
# Configuration is data
# ---------------------------------------------------------------------------


class DatabaseConfig(TypedDict):
    """Which engine, where, and what to call it on screen.

    A TypedDict rather than a dataclass: three fields, no behaviour, nothing to
    attach a method to. It can be printed, compared with `==`, stored and sent
    over a wire, none of which is true of an object graph.
    """

    dialect: str
    connection_string: str
    display_name: str
    auth_token: str | None


DEFAULT_SQLITE_PATH = str(Path(__file__).parent / "todos.db")
DEFAULT_TURSO_EMBEDDED_PATH = str(Path(__file__).parent / "todos_turso.db")


# THE TWO ENGINES THAT WANT ONLY A PATH, one row each. Both builders returned
# the same record and differed by the dialect and the word on screen, so those
# are the row and the builder is written once.
#
# THE PATH IS REQUIRED, and that is the point of the shape rather than a
# detail. It used to be `database_path: str | None = None` falling back to a
# constant, so the function could not tell "I chose the default" from "I
# forgot to pass one". A caller wanting the default says so by name.
_FROM_A_PATH: dict[str, tuple[str, str]] = {
    "get_sqlite_config": ("sqlite", "SQLite"),
    "get_turso_embedded_config": ("turso_embedded", "Turso Embedded"),
}


def _built_from_a_path(name: str) -> Callable[[str], DatabaseConfig]:
    """Build the function that describes one path-based engine."""

    dialect, shown = _FROM_A_PATH[name]

    def build(database_path: str) -> DatabaseConfig:
        return DatabaseConfig(
            dialect=dialect,
            auth_token=None,
            connection_string=database_path,
            display_name=f"{shown} ({Path(database_path).name})",
        )

    build.__name__ = name
    build.__qualname__ = name
    build.__doc__ = f"{shown} at `database_path`. The path is required."
    return build


get_sqlite_config = _built_from_a_path("get_sqlite_config")
get_turso_embedded_config = _built_from_a_path("get_turso_embedded_config")


def get_postgres_config(
    host: str, port: int, user: str, password: str, database: str
) -> DatabaseConfig:
    return DatabaseConfig(
        dialect="postgresql",
        auth_token=None,
        connection_string=f"postgresql://{user}:{password}@{host}:{port}/{database}",
        display_name=f"PostgreSQL ({database}@{host})",
    )


def get_turso_cloud_config(url: str, auth_token: str) -> DatabaseConfig:
    """Turso Cloud, which is pyturso with a remote url.

    NOT libsql. `import libsql_experimental` lived here and that package is not
    installed, so this backend reported itself unavailable on every machine.
    """
    return DatabaseConfig(
        dialect="turso_cloud",
        auth_token=auth_token,
        connection_string=url,
        display_name=f"Turso Cloud ({url.split('//')[-1][:24]})",
    )


# Kept: `get_turso_config` is the older name for the cloud builder.
get_turso_config = get_turso_cloud_config

SQLITE_CONFIG = get_sqlite_config(DEFAULT_SQLITE_PATH)
TURSO_EMBEDDED_CONFIG = get_turso_embedded_config(DEFAULT_TURSO_EMBEDDED_PATH)

# THERE IS NO CURRENT DATABASE HERE. This module held one in `_current_db`,
# with `set_database` writing it and `get_database` reading it, so every
# function that opened a connection depended on module state no caller could
# see. Two functions in this file wrote it, and `test_connection` had to save
# the old value and put it back in a `finally` because it borrowed the shared
# one to try a different engine.
#
# Which database the app is talking to is the APP's, and the app keeps it in
# `app.state`. Everything here takes the config it is to use.


# ---------------------------------------------------------------------------
# One entry per engine
# ---------------------------------------------------------------------------


def _for_dialect(table: dict[str, Any], dialect: str, what: str) -> Any:
    """Read a table, naming what exists when the key does not.

    This is the whole difference between a table and a chain. A chain that runs
    out of branches falls off the end: `get_connection` yields nothing and the
    caller gets an error about whatever it did next.
    """
    if dialect in table:
        return table[dialect]
    raise ValueError(f"Unknown dialect {dialect!r}. {what} exist for: {', '.join(sorted(table))}")


# WHERE THIS APP TOUCHES A DATABASE, marked as the package marks its own.
# `@boundary` does nothing at runtime; it is a declaration, so a reader does
# not have to work out for themselves which of these opens a socket. Every
# function below it here either opens a connection, runs a statement or ends a
# transaction, and nothing else in this file does.
@boundary
@asynccontextmanager
async def _connect_sqlite(config: DatabaseConfig):
    import aiosqlite

    async with aiosqlite.connect(config["connection_string"]) as conn:
        yield conn


@boundary
@asynccontextmanager
async def _connect_postgresql(config: DatabaseConfig):
    import asyncpg

    conn = await asyncpg.connect(config["connection_string"])
    try:
        yield conn
    finally:
        await conn.close()


@boundary
@asynccontextmanager
async def _connect_turso_embedded(config: DatabaseConfig):
    import turso

    conn = turso.connect(config["connection_string"])
    try:
        yield conn
    finally:
        conn.close()


@boundary
@asynccontextmanager
async def _connect_turso_cloud(config: DatabaseConfig):
    """pyturso with a remote url, which is what Turso Cloud is."""
    import turso.aio.sync

    conn = await turso.aio.sync.connect(
        str(Path(__file__).parent / "todos_cloud.db"),
        config["connection_string"],
        auth_token=config["auth_token"],
    )
    try:
        yield conn
    finally:
        await conn.close()


CONNECTORS = {
    "sqlite": _connect_sqlite,
    "postgresql": _connect_postgresql,
    "turso_embedded": _connect_turso_embedded,
    "turso_cloud": _connect_turso_cloud,
}

# The drivers each backend needs, by import name. `check_driver_available`
# reads this instead of a chain of imports, so adding an engine is a row.
DRIVERS = {
    "sqlite": ("aiosqlite", "aiosqlite"),
    "postgresql": ("asyncpg", "asyncpg"),
    "turso_embedded": ("turso", "pyturso"),
    "turso_cloud": ("turso", "pyturso"),
}

# `now()` is PostgreSQL's; SQLite and Turso spell it `datetime('now')`.
_SQLITE_TIMESTAMP = "TEXT DEFAULT (datetime('now'))"
_POSTGRES_TIMESTAMP = "TIMESTAMPTZ DEFAULT now()"

TIMESTAMP_TYPES = {
    "sqlite": _SQLITE_TIMESTAMP,
    "turso_embedded": _SQLITE_TIMESTAMP,
    "turso_cloud": _SQLITE_TIMESTAMP,
    "postgresql": _POSTGRES_TIMESTAMP,
}

# Whether the driver's `execute` is awaited. The two Turso paths differ from
# each other, which is exactly the kind of thing a chain gets wrong.
AWAITS_EXECUTE = {
    "sqlite": True,
    "postgresql": True,
    "turso_embedded": False,
    "turso_cloud": True,
}

# PostgreSQL commits per statement outside a transaction; the others need one.
NEEDS_COMMIT = {
    "sqlite": True,
    "postgresql": False,
    "turso_embedded": True,
    "turso_cloud": True,
}


@asynccontextmanager
async def get_connection(config: DatabaseConfig):
    """A connection to `config`'s database, AND what engine it is.

    THE ENGINE TRAVELS WITH THE CONNECTION, because it is true of the
    connection. Every function that touched a connection used to ask
    `get_database()` again for its dialect, so a function's behaviour depended
    on module state no caller could see, and calling one with a connection
    from a different engine would have read the wrong row.

    This is the ONLY place engine-specific code lives, and it is a lookup.
    """
    connector = _for_dialect(CONNECTORS, config["dialect"], "Connectors")
    async with connector(config) as conn:
        yield conn, config["dialect"]


@boundary
async def _run(conn: Any, sql: str, dialect: str) -> None:
    """Execute one statement on `conn`, then commit it."""
    if _for_dialect(AWAITS_EXECUTE, dialect, "Drivers"):
        await conn.execute(sql)
    else:
        conn.execute(sql)
    await commit(conn, dialect)


@boundary
async def commit(conn: Any, dialect: str) -> None:
    """End the transaction on the engine `conn` belongs to.

    THIS IS THE LADDER THE THREE EXAMPLE APPS EACH WROTE THREE TIMES:

        dialect = get_database().dialect
        if dialect == "sqlite":
            await conn.commit()
        elif dialect in ("turso_cloud", "turso_embedded"):
            conn.commit()

    PostgreSQL appears in none of the nine copies. It works, because asyncpg
    commits each statement when there is no open transaction, but the code
    does not say so -- it runs off the end of the chain and returns. A reader
    who copies the ladder into an application that opens a transaction loses
    the write and is told nothing.

    Here PostgreSQL is a row saying `False`, which is an answer rather than an
    absence, and an engine nobody thought about raises.
    """
    if not _for_dialect(NEEDS_COMMIT, dialect, "Drivers"):
        return
    if _for_dialect(AWAITS_EXECUTE, dialect, "Drivers"):
        await conn.commit()
    else:
        conn.commit()


def todos_table_sql(dialect: str) -> str:
    """The CREATE TABLE for one engine. Returns SQL, runs nothing."""
    stamp = _for_dialect(TIMESTAMP_TYPES, dialect, "Timestamp types")
    return (
        "CREATE TABLE IF NOT EXISTS todos ("
        "id TEXT PRIMARY KEY, "
        "title TEXT NOT NULL, "
        "completed INTEGER DEFAULT 0, "
        f"created_at {stamp}, "
        f"updated_at {stamp})"
    )


async def init_schema(config: DatabaseConfig) -> None:
    """Create the todos table on `config`'s engine."""
    async with get_connection(config) as (conn, dialect):
        await _run(conn, todos_table_sql(config["dialect"]), dialect)


async def test_connection(config: DatabaseConfig) -> tuple[bool, str]:
    """Try to reach `config`, and say what went wrong if it cannot.

    THE HANDLERS ARE NARROW AND THE TYPE SURVIVES. This was
    `except Exception as e: return False, f"Connection failed: {e}"`, which
    flattened a missing driver, a wrong password and a typo in a hostname into
    one string. A caller reading the boolean could not tell which, and only one
    of the three is worth retrying.

    A bug of OURS is not caught at all: a TypeError here means this function
    was called wrongly, and reporting that as "the database is unreachable"
    sends the reader to check a database that is fine.
    """
    try:
        async with get_connection(config) as (conn, dialect):
            await _run(conn, "SELECT 1", dialect)
        return True, "Connection successful"
    except ImportError as exc:
        return False, f"Driver not installed: {exc}"
    except OSError as exc:
        return False, f"Could not reach the database: {exc}"


def check_driver_available(dialect: str) -> tuple[bool, str]:
    """Whether this backend's driver is installed.

    One row per engine rather than a chain of imports, so an engine that is not
    in the table raises instead of returning "unknown dialect" as if it were an
    ordinary answer.
    """
    import importlib.util

    module, package = _for_dialect(DRIVERS, dialect, "Drivers")
    if importlib.util.find_spec(module) is None:
        return False, f"{package} is not installed"
    return True, f"{package} installed"
