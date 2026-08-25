"""
Shared test fixtures for declaro_persistum.

Test levels:
- precommit: Fast sanity checks (30s max total, mocked DBs)
- stress: Full stress testing (requires real databases)
"""

import asyncio
import hashlib
import os

import pytest
from dotenv import load_dotenv

# Load .env file for test credentials
load_dotenv()

from declaro_persistum.types import Schema

# =============================================================================
# Test Level Configuration
# =============================================================================


@pytest.fixture(autouse=True)
def precommit_timeout(request: pytest.FixtureRequest) -> None:
    """Enforce 5-second timeout per test for precommit tests."""
    if request.node.get_closest_marker("precommit"):
        request.node.add_marker(pytest.mark.timeout(5))


# NOT A REAL DATABASE, and it used to be `declarotodo`, which is one. Every
# name a test suite for a migration engine reaches is a name that suite may
# empty, so the only default it may carry is one it makes itself.
DEFAULT_POSTGRESQL_URL = "postgresql://postgres:postgres@localhost/postgres"
_PROBE_SERVER = "postgresql://postgres:postgres@localhost"


def probe_url() -> str:
    """A PostgreSQL database this pytest process owns.

    TWENTY-ONE TEST FILES HARD-CODED ONE DATABASE NAME and created the same
    tables in it: `t`, `users`, `widgets`, `parents`, `children`. One runner
    was fine. Two were not, and on 2026-08-22 three agents working in
    parallel each hit `DuplicateTableError: relation "widgets" already
    exists` on a different test, then each spent a run proving their own
    change innocent before finding the shared database.

    The table names are fixed on purpose, because the tests are about what
    the differ does to a named table. So the boundary has to be the database,
    which is what `postgresql_schema` already does per test. This is the same
    thing per process, for the files that build their own connection.

    `TEST_POSTGRESQL_URL` NAMES THE SERVER, NOT THE DATABASE. It used to name
    the database and win outright, and this package's `.env` sets it to
    `declarotodo`, which holds real data. Two things then happened on every
    full test run:

      the session fixture ran DROP DATABASE "declarotodo" and remade it empty

      every test that migrates introspects the database it is pointed at and
      diffs it against a small declared schema, so the differ emitted a
      drop_table for every table it found and the applier ran them

    Both are the suite doing exactly what it was built to do, to a database
    nobody told it was real. Found on 2026-08-25 with `declarotodo` already at
    zero tables. A canary table planted there was gone after one run, by the
    second path, after the first had been closed.

    A migration engine's test suite is destructive by construction. The only
    safe rule is that it works on a database it made itself, so the server is
    taken from the caller and the database name never is.
    """
    return f"{_server_from(os.environ.get('TEST_POSTGRESQL_URL'))}/declaro_probe_{os.getpid()}"


def _server_from(url: str | None) -> str:
    """The server half of a URL a caller gave us, or our own default."""
    return next(
        (given.rpartition("/")[0] for given in [url] if given and "/" in given),
        _PROBE_SERVER,
    )


POSTGRESQL_PROBE_URL = probe_url()


@pytest.fixture(scope="session", autouse=True)
def _a_probe_database_of_our_own():
    """Make this process's probe database, and drop it at the end.

    Without this the twenty-one files that build their own connection would
    point at a database nobody created and skip, which reads exactly like
    PostgreSQL being unavailable. A silent skip is worse than the collision
    it replaced.

    IT DROPS ONLY THE DATABASE IT NAMES ITSELF. It used to drop whatever
    `probe_url()` returned, and `probe_url()` prefers `TEST_POSTGRESQL_URL`.
    This package's `.env` sets that to `declarotodo`, which holds real data,
    so `DROP DATABASE IF EXISTS "declarotodo"` ran at the start of every full
    test run and again at the end. Found on 2026-08-25 with `declarotodo`
    already empty.

    A database someone else named is a database someone else owns. This
    fixture makes one, so this fixture may drop one; anything else it is
    pointed at, it uses and leaves alone.
    """
    import asyncio

    import asyncpg

    server, _, name = POSTGRESQL_PROBE_URL.rpartition("/")

    async def _make() -> bool:
        try:
            admin = await asyncpg.connect(f"{server}/postgres")
        except (OSError, asyncpg.PostgresError):
            return False
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
            await admin.execute(f'CREATE DATABASE "{name}"')
        finally:
            await admin.close()
        return True

    async def _drop() -> None:
        admin = await asyncpg.connect(f"{server}/postgres")
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        finally:
            await admin.close()

    # SAFE BECAUSE THE NAME IS OURS. `probe_url` builds it from this process
    # id and never from what a caller passed, so there is no database reached
    # from here that somebody else owns.
    made = asyncio.run(_make())
    yield
    [asyncio.run(_drop()) for _ in [made] if made]


def refuse_without_postgresql(url: str, unreachable: BaseException) -> None:
    """PostgreSQL is a supported backend, so its absence is a failure.

    A SKIP HERE READS AS A PASS. Measured 2026-08-25: with the server down the
    suite reports 2680 passed, 200 skipped, and 186 of those skips are tests
    that had been passing. Nobody counts the `s` characters, so a run on a
    machine without PostgreSQL says the package is tested while the whole
    PostgreSQL applier, its inspector, its type mapping and the alter-column
    path only that engine has go unexercised.

    That has already produced a wrong claim here: a session reported seven
    times that PostgreSQL was unavailable, because the tests that would have
    said otherwise had quietly skipped.

    `require_postgresql` below has failed rather than skipped since August and
    its docstring gives the reason. These sites connect inline rather than
    through the fixture, and this is the same decision for them.
    """
    pytest.fail(
        f"PostgreSQL is a supported backend and is not reachable at {url}: "
        f"{type(unreachable).__name__}: {unreachable}. Start it, or point "
        f"TEST_POSTGRESQL_URL at one that is running. This is a failure and "
        f"not a skip because a skipped backend reads as a tested one."
    )


@pytest.fixture
def require_postgresql() -> None:
    """Require PostgreSQL for stress tests — by OPENING one.

    This used to check two proxies and no server:

        import asyncpg                     # is the driver installed
        if not pg_url:                     # a string that has a default,
            pytest.fail(...)               # so this could never be true

    Neither touches a database. The second is a guard whose condition is
    false by construction. On 2026-08-12 two PostgreSQL scenarios reported
    green, and they were green only because a server happened to be running
    on that machine — the fixture would have said the environment was fine
    either way, and the scenarios would have errored further in, blaming
    whatever they touched first.

    A check written against a proxy — a driver import, a string's length —
    tells you about the proxy. The only thing that establishes a database is
    there is connecting to it, so that is what this does.
    """
    try:
        import asyncpg
    except ImportError:
        pytest.fail(
            "PostgreSQL (asyncpg) is REQUIRED for stress tests. "
            "Install with: uv pip install asyncpg"
        )

    url = f"{_server_from(os.environ.get('TEST_POSTGRESQL_URL'))}/postgres"

    async def _probe() -> None:
        conn = await asyncpg.connect(url, timeout=5)
        try:
            await conn.execute("SELECT 1")
        finally:
            await conn.close()

    try:
        asyncio.run(_probe())
    except Exception as e:
        pytest.fail(
            f"PostgreSQL is REQUIRED for stress tests and is not reachable at "
            f"{url}: {type(e).__name__}: {e}. Start it, or point "
            f"TEST_POSTGRESQL_URL at one that is running."
        )


@pytest.fixture
def require_turso() -> None:
    """Require Turso for stress tests - skip if not configured."""
    try:
        import turso  # noqa: F401
    except ImportError:
        pytest.skip("Turso (pyturso) not installed. Install with: uv sync --extra turso")

    # Also check if Turso is configured with a real URL
    turso_url = os.environ.get("TEST_TURSO_URL", "")
    if not turso_url:
        pytest.skip("TEST_TURSO_URL environment variable not set - skipping Turso tests")


@pytest.fixture
def require_sqlite() -> None:
    """Require SQLite for tests."""
    try:
        import aiosqlite  # noqa: F401
    except ImportError:
        pytest.fail("SQLite (aiosqlite) is REQUIRED. Install with: uv pip install aiosqlite")


# Sample schemas for testing


@pytest.fixture
def empty_schema() -> Schema:
    """Empty schema."""
    return {}


@pytest.fixture
def simple_schema() -> Schema:
    """Simple schema with one table."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            }
        }
    }


@pytest.fixture
def complex_schema() -> Schema:
    """Complex schema with multiple tables and relationships."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "name": {"type": "text"},
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "indexes": {
                "users_email_idx": {"columns": ["email"], "unique": True},
                "users_created_at_idx": {"columns": ["created_at"]},
            },
        },
        "orders": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "user_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "users.id",
                    "on_delete": "cascade",
                },
                "total": {"type": "numeric(10,2)", "nullable": False, "check": "total >= 0"},
                "status": {
                    "type": "text",
                    "nullable": False,
                    "default": "'pending'",
                    "check": "status IN ('pending', 'confirmed', 'shipped', 'delivered')",
                },
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "indexes": {
                "orders_user_id_idx": {"columns": ["user_id"]},
                "orders_status_idx": {"columns": ["status"]},
            },
        },
        "order_items": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "order_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "orders.id",
                    "on_delete": "cascade",
                },
                "product_name": {"type": "text", "nullable": False},
                "quantity": {"type": "integer", "nullable": False, "check": "quantity > 0"},
                "price": {"type": "numeric(10,2)", "nullable": False},
            },
        },
    }


@pytest.fixture
def schema_with_composite_pk() -> Schema:
    """Schema with composite primary key."""
    return {
        "user_roles": {
            "columns": {
                "user_id": {"type": "uuid", "nullable": False},
                "role_id": {"type": "uuid", "nullable": False},
                "granted_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "primary_key": ["user_id", "role_id"],
        }
    }


# `mock_pg_connection` and `mock_sqlite_connection` were here. Both returned
# classes that had already been deleted, so requesting either raised NameError,
# and nothing requested either. Deleted rather than rebuilt: a mock connection
# is the machinery a test needs when the code under test mixes I/O with the
# logic being asserted (Rule 10), and both SQLite and Postgres are available to
# these tests for real. `test_the_bulk_loaders_actually_load` and
# `test_fk_ordering` are what that looks like instead.


# Temporary directory fixtures


@pytest.fixture
def temp_schema_dir(tmp_path):
    """Create a temporary schema directory structure."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()
    (schema_dir / "tables").mkdir()
    return schema_dir


@pytest.fixture
def temp_schema_with_users(temp_schema_dir):
    """Create a temporary schema directory with a users table."""
    users_toml = temp_schema_dir / "tables" / "users.toml"
    users_toml.write_text('''
[users]
primary_key = ["id"]

[users.columns.id]
type = "uuid"
nullable = false
default = "gen_random_uuid()"

[users.columns.email]
type = "text"
nullable = false
unique = true

[users.columns.created_at]
type = "timestamptz"
nullable = false
default = "now()"
''')
    return temp_schema_dir


# =============================================================================
# One schema per test, so no two tests can see each other's tables
# =============================================================================


@pytest.fixture
async def postgresql_schema(request: pytest.FixtureRequest):
    """A PostgreSQL database of this test's own, dropped afterwards.

    TWENTY-FOUR TEST FILES SHARED ONE DATABASE and collided by name: `t` is
    created by 83 places in this suite, `users` by 22, `orders` by 8. Each
    depended on the others cleaning up, so the suite passed in whatever order
    it happened to run in. Measured 2026-08-21: seed 1 passed and seeds 2 and
    3 failed the same three scenarios.

    A SCHEMA WAS NOT ENOUGH, and that is worth recording. Pointing
    `search_path` at a private schema isolates writes, but the inspector reads
    `pg_tables WHERE schemaname = 'public'`, so a migration still saw every
    table other tests had left there and reported them as dropped:

        Ambiguous changes detected: Table 't' removed and 'orders' added
        Skipping migration - manual intervention required

    A whole database is the boundary that matches what the code reads.

    Composition inside a test is untouched. The steps of one scenario still
    meet the state the previous step left; what is gone is one test seeing
    another's.
    """
    import asyncpg

    # ONE SOURCE FOR WHERE THE SUITE MAY WRITE. This read the environment
    # directly and so reached whatever database a caller had named, where
    # `probe_url` reaches only the one this process made.
    url = POSTGRESQL_PROBE_URL
    name = "t_" + hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:16]
    base, _, _ = url.rpartition("/")

    try:
        admin = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        refuse_without_postgresql(url, exc)

    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()

    yield f"{base}/{name}", "public"

    try:
        admin = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError):
        return
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    finally:
        await admin.close()
