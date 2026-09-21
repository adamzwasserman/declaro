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

# Load .env file for test credentials. THE IMPORT BELOW IS AFTER IT ON
# PURPOSE: anything imported from here on may read the environment at
# import time, and this is the line that puts the credentials in it.
load_dotenv()


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


# =============================================================================
# The declaration's own shapes
# =============================================================================
#
# WHAT THESE ARE FOR. `docs/design/persistum.hd` declares what this package is
# built out of, and until 2026-09-21 no fixture produced any of it. A test
# reaching for a schema got a finished one, which is the one thing the design
# says nobody hands you: a schema is derived from what the code declares, by
# `schema_from_declared`, so a fixture that hands one over skips the function
# that makes it.
#
# These are plain dicts on purpose. They are the records the declaration names,
# written out in the test suite so a test can say "given these models" without
# reaching into a package that does not have them yet.


@pytest.fixture
def declared(a_person_model, an_order_model) -> dict:
    """Everything the code declares, in one answer, the way `schema_read_declared`
    hands it over: models and views together, and what it means to lose."""
    return {
        "models": [a_person_model, an_order_model],
        "views": [],
        "dropping_tables": [],
        "dropping_views": [],
    }


@pytest.fixture
def a_room() -> dict:
    """An empty waiting room. Both lists are empty whenever nobody is
    mid-flight, which is most of the time."""
    return {"arrivals": [], "waiting": []}


@pytest.fixture
def a_handle():
    """A real driver connection, held and never read.

    Real rather than a stand-in, because a stand-in that gets the driver
    contract wrong manufactures bugs: this package has twice chased a defect
    that lived only in a double. Nothing here reads it, which is what `handle`
    means in the declaration.
    """
    import sqlite3

    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def a_database(a_handle, a_room) -> dict:
    """What opening gives a caller, as the declaration describes it: the engine
    decided once, the connection it reads on, one writer per drainer, the room,
    the shutdown policy and the primary it derives replication from."""
    return {
        "engine": "sqlite",
        "connection": a_handle,
        "writers": [a_handle],
        "room": a_room,
        "shutdown": "exit_immediately",
        "primary": None,
    }


@pytest.fixture
def a_replicated_database(a_database) -> dict:
    """The same, with a primary, which is the one thing replication is derived
    from. A second flag saying "replicated" could disagree with this one."""
    return {
        **a_database,
        "primary": "libsql://team-app.turso.io",
        "shutdown": "replicate_then_exit",
        "writers": [],
    }


# =============================================================================
# The shapes the replaced design used are gone
# =============================================================================
#
# Seven fixtures handed a test a finished `Schema` and two wrote TOML schema
# files. Both shapes belonged to the source deleted on 2026-09-21: a schema is
# derived from what the code declares, by `schema_from_declared`, and the models
# are the only description there is.


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
