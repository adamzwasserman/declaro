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
    """Require Turso, and FAIL rather than skip when it is missing.

    A SKIP HERE READS AS A PASS, which is the argument `refuse_without_postgresql`
    already makes twice in this file. This fixture used to skip on a missing
    driver and again on missing credentials, so a run with no Turso reported the
    package tested while nothing Turso does was exercised.
    """
    try:
        import turso  # noqa: F401
    except ImportError:
        pytest.fail(
            "Turso (pyturso) is REQUIRED and is not installed. "
            "Install with: uv sync --extra turso. This is a failure and not a "
            "skip because a skipped backend reads as a tested one."
        )

    missing = [
        name for name in ("TEST_TURSO_URL", "TEST_TURSO_AUTH_TOKEN") if not os.environ.get(name, "")
    ]
    if missing:
        # APPROVED-IF 2026-09-23 09:40 — credentials are absent or present, and
        # absence is a reason to stop rather than a row in a table of answers.
        pytest.fail(
            f"Turso is REQUIRED and {', '.join(missing)} is not set in .env. "
            f"This is a failure and not a skip because a skipped backend reads "
            f"as a tested one."
        )


@pytest.fixture
def require_sqlite() -> None:
    """Require SQLite for tests."""
    try:
        import aiosqlite  # noqa: F401
    except ImportError:
        pytest.fail("SQLite (aiosqlite) is REQUIRED. Install with: uv pip install aiosqlite")


# Sample schemas for testing


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


# A REPLICA IS MADE BY THE FIXTURE, NOT BY THE DOOR. Added 2026-09-22 after
# `session_open_connection` could not be asked about a replicated database at
# all: the driver's `turso.aio.sync.connect(path, remote_url, auth_token=)` wants
# a local file, a primary and a token, and the door is handed a target, an engine
# and a number.
#
# MEASURED THE SAME DAY, WHICH IS WHAT MAKES THIS POSSIBLE. A replica is four
# files: the database, its changes, its info and its write-ahead log. Once they
# exist, `turso.aio.connect(path)` reopens them by path alone, with no url and no
# token. So building the replica is setup, the way creating a table is setup,
# and the door opens what is already on disk exactly as it opens SQLite.
#
# THE FOUR FILES ARE ONE ATOMIC UNIT. Deleting a subset breaks the next open, so
# the whole folder goes at the end or none of it does.
@pytest.fixture
def a_real_turso_replica(tmp_path):
    """A live replica of the staging primary, on disk, ready to be opened.

    Skips with the name of what was missing rather than passing quietly: 186
    passing tests once went silent as skips with a server down.
    """
    import asyncio

    import turso.aio.sync

    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    missing = [
        name
        for name, value in (("TEST_TURSO_URL", url), ("TEST_TURSO_AUTH_TOKEN", token))
        if not value
    ]
    if missing:
        # APPROVED-IF 2026-09-22 20:10 — credentials are absent or present, and
        # absence is a reason to stop rather than a row in a table of answers.
        pytest.skip(f"no Turso replica: {', '.join(missing)} is not set in .env")

    path = tmp_path / "replica.db"

    async def _fill() -> None:
        made = await asyncio.wait_for(turso.aio.sync.connect(str(path), url, auth_token=token), 30)
        await made.close()

    asyncio.run(_fill())
    return str(path)


# =============================================================================
# The four things no function in the library builds
# =============================================================================
#
# EVERY OTHER SHAPE COMES FROM A FUNCTION, SO NO FIXTURE MAY HAND ONE OVER.
# Worked out on 2026-09-23 by walking every type in `docs/design/persistum.hd`
# and asking which of them something in the library produces. Twenty-four are
# produced directly and most of the rest sit nested inside one of those: a
# `Model` comes out of `schema_read_declared`, a `Doorbell` out of `crew_make`,
# a `Report` is assembled by the opener. A fixture that writes one of those out
# by hand is handing the function under test its own answer, and it goes stale
# the day the shape changes. That happened to the database handle under four
# files this week and nothing noticed.
#
# Four things have no producer anywhere, and they are what belongs here: the
# classes a host declares, a question as a caller types it, rows as a driver
# hands them back, and a real database.


# --- the classes a host declares -------------------------------------------
#
# THE CLASS NAME AND THE TABLE NAME ARE DIFFERENT, ON PURPOSE AND ALWAYS. A
# caller writes `Person` because that is the only name it has; the rows live in
# `people`. On 2026-09-22 every model in the suite was named after its own
# table, so the two words were one word, and the library shipped with nothing
# trading one for the other: every question a caller wrote by class name reached
# the engine as `SELECT ... FROM Person`. 541 tests passed.
#
# DECLARED INSIDE A CALL AND NEVER AT MODULE LEVEL. `reader fn
# schema_read_declared` reads every Pydantic class alive in the process, so a
# class held at import time joins every other test's migration for the whole run.


@pytest.fixture
def a_person_class():
    """`class Person` over the table `people`, declared the way a host declares one."""
    from pydantic import BaseModel

    # honest-code-allow: L1.21.5 — a host's model IS a Pydantic class.
    # `HOW-IT-SHOULD-WORK.md:34`: "You write Pydantic models. That is your
    # only description of the database." `reader fn schema_read_declared`
    # reads exactly these, so a fixture that declared anything else would be
    # testing an input the library never receives.
    class Person(BaseModel):
        __tablename__ = "people"
        id: int
        name: str

    return Person


@pytest.fixture
def an_order_class():
    """`class Order` over `orders`, pointing at `people`, so a join has a key to find.

    The foreign key is the whole of what `fn join_between` reads, so a pair of
    classes with no key between them cannot exercise it either way.
    """
    from pydantic import BaseModel

    # honest-code-allow: L1.21.5 — a host's model IS a Pydantic class.
    # `HOW-IT-SHOULD-WORK.md:34`: "You write Pydantic models. That is your
    # only description of the database." `reader fn schema_read_declared`
    # reads exactly these, so a fixture that declared anything else would be
    # testing an input the library never receives.
    class Order(BaseModel):
        __tablename__ = "orders"
        id: int
        person_id: int
        total: int

    return Order


# --- a question, as a caller types it ---------------------------------------
#
# ONE MEANING IN FOUR GRAMMARS. `HOW-IT-SHOULD-WORK.md:59` says the four are
# spellings of one question and produce the same rows, so a fixture that offers
# only one grammar can never show that. The words here are the caller's own; no
# function in the library builds an `Asked`.


@pytest.fixture
def the_same_question_four_ways() -> dict[str, dict]:
    """`people.select("id", "name").where({"name": "bob"})`, in all four grammars.

    Keyed by grammar so a scenario can walk them and assert one answer.
    """
    return {
        "pseudo_sql": {
            "model": "Person",
            "wanted": "all",
            "select": ["id", "name"],
            "where": {"name": "bob"},
        },
        "django": {
            "model": "Person",
            "wanted": "all",
            "values": ["id", "name"],
            "filter": {"name": "bob"},
        },
        "prisma": {
            "model": "Person",
            "wanted": "all",
            "select": ["id", "name"],
            "where": {"name": "bob"},
        },
        "sqlalchemy": {
            "model": "Person",
            "wanted": "all",
            "with_only": ["id", "name"],
            "filter_by": {"name": "bob"},
        },
    }


# --- rows, as a driver hands them back --------------------------------------


@pytest.fixture
def rows_a_driver_handed_back() -> list[dict]:
    """Two whole rows of `people`, column name to value, the way every driver
    answers once the cursor has been drained."""
    return [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


# --- a real database, on each engine ----------------------------------------
#
# SQLITE HAD NO FIXTURE AND FOUR STEP FILES EACH MADE THEIR OWN TEMP FILE.
# PostgreSQL has `postgresql_schema` and Turso has `a_real_turso_replica`; this
# is the third, so all three engines are reached the same way.


@pytest.fixture
def a_real_sqlite_database(tmp_path) -> str:
    """A SQLite file this test owns, gone with the temp folder afterwards.

    A path rather than an open connection, because opening one is
    `boundary_out fn session_open_connection`'s job and a fixture that opened it
    would be answering the question under test.
    """
    return str(tmp_path / "declaro.db")


# =============================================================================
# The two shapes twelve step files were each building their own way
# =============================================================================
#
# ONE PLACE, SO A CHANGE BREAKS LOUDLY. Twelve step files wrote a `Model` out by
# hand and ten wrote a `Column`, between them through seven private `_a_model`
# helpers and six private `_a_column` helpers, each slightly different. On
# 2026-09-22 `Column` lost `facets` and `Operation` lost `detail`, and not one of
# the twelve knew. That is the shape that already bit this suite once: eight
# files hand-built a `Room`, the room changed under them in a day, and the suite
# stayed green because nothing compares a fixture against the declaration.
#
# NOTHING TAKES A DEFAULT, AND THAT IS THE WHOLE POINT. Every field is required
# at every call site, so a field added to either record fails twelve files at
# once rather than filling itself in with an empty list nobody chose. It is
# noisier to call and it is the noise that does the work.
#
# A MODEL THAT COMES OUT OF `reader fn schema_read_declared` IS NOT THIS. These
# are for the pure functions that TAKE a model, so a scenario can say "given a
# model shaped like this" without a live process full of Pydantic classes.


def a_column(*, name: str, declared_type: str, renamed_from: str | None, references: dict | None):
    """One `Column`, every field said out loud."""
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": renamed_from,
        "references": references,
    }


def a_reference(*, table: str, column: str, on_delete: str):
    """Where a foreign key points, and what a delete does to the child."""
    return {"table": table, "column": column, "on_delete": on_delete}


def a_model(
    *,
    name: str,
    table: str,
    columns: list,
    dropping: list,
    indexes: list,
    constraints: list,
):
    """One `Model`, every field said out loud.

    `name` AND `table` ARE DIFFERENT WORDS AND A CALLER SHOULD KEEP THEM SO. A
    caller writes the class's name; the rows live under the table's. Naming them
    alike is what hid a whole family of defect until 2026-09-22.
    """
    return {
        "name": name,
        "table": table,
        "columns": columns,
        "dropping": dropping,
        "indexes": indexes,
        "constraints": constraints,
    }
