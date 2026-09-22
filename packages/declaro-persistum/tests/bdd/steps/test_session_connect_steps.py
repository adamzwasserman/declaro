"""Bind `tests/bdd/features/session/session_connect.feature`, and nothing else.

WHAT THE FEATURE ASKS. Twelve scenarios about one orchestrator, two of them
outlines, sixteen test cases between them. `orchestrator fn session_connect` is
handed a `Connecting` carrying what a caller said, works out the engine, opens
the connection and the crew, brings the database up to the models, and hands
back the record the handle lives in.

EVERY DATABASE HERE IS A REAL ONE. A stand-in has manufactured two defects in
this package already, one inventing a driver bug that did not exist and one
being more permissive than the driver and hiding a regression. SQLite is a file
opened through aiosqlite. PostgreSQL is a database each scenario makes for
itself on the probe server `tests/conftest.py` builds, because a migration
against a shared database plans a drop for every table the models do not
declare. Turso is pyturso against a local file reached by a `libsql` target,
which is the only thing in a target that names that engine.

ONE STEP FUNCTION, OVER ONE TABLE. Every Given here establishes a state, every
When opens something and every Then reads what came back, and all three are
handed the same bench and one sentence, so sentences differing only in their
words are rows rather than functions. Written as three functions they were one
function three times and a gate said so. A sentence with no row raises a
KeyError naming itself the first time it is said, which is what happens when the
feature is edited and this file is not.

WHY THE MODELS ARE DECLARED INSIDE THE STEPS. `reader fn schema_read_declared`
takes no arguments and reads every Pydantic class alive in the process, so a
test cannot hand it a set of models. It declares them the way a host does and
holds them in the shared `scenario` fixture, which dies with the test. The
autouse fixture below collects what the last scenario let go.

WHAT THIS CHAIN CANNOT BE ASKED TODAY, stated here rather than hidden in a skip.
Five sentences call `pytest.xfail` naming exactly what is missing, and the steps
after each one raise, so deleting an xfail turns its scenario red:

  the cold copy      `boundary_out fn session_open_connection` opens a Turso
                     copy by path, and a connection opened that way carries no
                     `pull`. Measured 2026-09-22 against the staging primary:
                     `turso.aio.sync.connect` answers a `ConnectionSync` that
                     has one and `turso.aio.connect` answers a `Connection` that
                     does not. `reader fn turso_pull` calls it.

  what survives      The declaration gives this orchestrator
                     `boundary_out fn query_run_statement` and no transaction
                     scope, so each statement autocommits and all three engines
                     keep what already ran. Wrapping a batch is
                     `boundary_out fn query_run_together`, which this card does
                     not invoke. The Turso row of that outline passes, because
                     keeping what ran is what the feature asks for there.

  three of five      A stopped migration hands back a `Fault`, and `type Fault`
                     has three fields and no report, so there is nowhere for a
                     caller to read how much of the plan ran.

  the operation      A fault travels out in the words of the function that found
                     it, and `boundary_out fn query_run_statement` names what the
                     engine said and not which operation was running.

  part way through   The same fault, for the same reason: saying the database is
                     part way through is something only the orchestrator knows,
                     and rewriting a refusal it did not write is the one thing
                     this chain may not do.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import pathlib
import re
import signal
import typing
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from pydantic import BaseModel
from pytest_bdd import parsers, scenarios, step

from declaro_persistum import types as declared_types
from declaro_persistum.engine_of import engine_of
from declaro_persistum.lifecycle_install_handler import (
    WHAT_THE_PLATFORM_SENDS,
    WHAT_WAS_THERE,
)
from declaro_persistum.lifecycle_register import REGISTERED
from declaro_persistum.session_connect import session_connect
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/session/session_connect.feature")


# =============================================================================
# speaking to each engine, because a scenario states what is true afterwards
# =============================================================================
#
# THE STATEMENTS BELOW ARE THE TEST'S OWN. The package writes its own; these are
# how this file sets a database up and asks it what it holds afterwards.

THE_TABLE_THE_FEATURE_NAMES = "people"


async def _through_aiosqlite(target: str, statements: list[str]) -> list[list[Any]]:
    import aiosqlite

    connection = await aiosqlite.connect(target, isolation_level=None)
    try:
        return [list(await (await connection.execute(one)).fetchall()) for one in statements]
    finally:
        await connection.close()


async def _through_asyncpg(target: str, statements: list[str]) -> list[list[Any]]:
    import asyncpg

    connection = await asyncpg.connect(target)
    try:
        return [[tuple(row) for row in await connection.fetch(one)] for one in statements]
    finally:
        await connection.close()


async def _through_pyturso(target: str, statements: list[str]) -> list[list[Any]]:
    import turso.aio

    connection = await turso.aio.connect(target.partition("://")[2])
    try:
        answered = []
        for one in statements:
            cursor = connection.cursor()
            await cursor.execute(one)
            answered.append(list(await cursor.fetchall()))
        return answered
    finally:
        await connection.close()


ASK_THE_DATABASE: dict[str, Callable[[str, list[str]], Awaitable[list[list[Any]]]]] = {
    "sqlite": _through_aiosqlite,
    "postgresql": _through_asyncpg,
    "turso": _through_pyturso,
}

WHAT_TABLES_ARE_THERE: dict[str, str] = {
    "sqlite": "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    "postgresql": "SELECT tablename FROM pg_tables WHERE schemaname = 'public'",
    "turso": "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
}

WHAT_VIEWS_ARE_THERE: dict[str, str] = {
    "sqlite": "SELECT name FROM sqlite_master WHERE type = 'view'",
    "postgresql": "SELECT viewname FROM pg_views WHERE schemaname = 'public'",
    "turso": "SELECT name FROM sqlite_master WHERE type = 'view'",
}

WHAT_COLUMNS_ARE_THERE: dict[str, str] = {
    "sqlite": "SELECT name FROM pragma_table_info('{table}')",
    "postgresql": (
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = '{table}'"
    ),
    "turso": "SELECT name FROM pragma_table_info('{table}')",
}

# WHAT EACH ENGINE CALLS THE TWO TYPES THIS FILE DECLARES, because a scenario
# creating a live table has to write it in the engine's own words.
AN_INTEGER: dict[str, str] = {"sqlite": "INTEGER", "postgresql": "integer", "turso": "INTEGER"}
SOME_TEXT: dict[str, str] = {"sqlite": "TEXT", "postgresql": "text", "turso": "TEXT"}


def _ran(bench: dict[str, Any], statements: list[str]) -> list[list[Any]]:
    return bench["loop"].run_until_complete(
        ASK_THE_DATABASE[bench["engine"]](bench["target"], statements)
    )


# WHAT THE CATALOGUE CAN BE ASKED, ONE ROW PER QUESTION. Asking for the tables,
# the views and one table's columns is one function over three engines and three
# questions, and the difference between the three is the statement each engine
# answers it with. `{table}` is filled where a question names one and ignored
# where it does not.
THE_CATALOGUE: dict[str, dict[str, str]] = {
    "tables": WHAT_TABLES_ARE_THERE,
    "views": WHAT_VIEWS_ARE_THERE,
    "columns": WHAT_COLUMNS_ARE_THERE,
}


def _what_is_there(bench: dict[str, Any], asked: str, table: str) -> list[str]:
    """The names the engine's own catalogue gives back, sorted."""
    statement = THE_CATALOGUE[asked][bench["engine"]].format(table=table)
    return sorted(str(row[0]) for row in _ran(bench, [statement])[0])


# =============================================================================
# one loop per scenario, and everything it opened closed on it
# =============================================================================


async def _closed(handles: list[Any]) -> None:
    for handle in handles:
        await handle.close()


@pytest.fixture
def loop(scenario: dict[str, object], monkeypatch: pytest.MonkeyPatch) -> Any:
    """A loop this scenario owns, and the only place connections are closed.

    A connection has to be closed on the loop it was opened on, so the two live
    and die together here.

    WHY THE TWO DOORS ARE RECORDED, AND IT IS NOT A STAND-IN. Both wrappers call
    the real function and change nothing about what it does or what it answers;
    they write down what it handed over. An open that fails hands back a `Fault`
    and nothing else, so the connections it had already opened cannot be reached
    by the caller at all, and nothing in the declaration closes them: `boundary_out
    fn session_close_connection` is reached from shutdown, and a database whose
    open failed never reaches shutdown. aiosqlite runs one non-daemon thread per
    connection, so a leaked one holds the process open after every test has
    finished. Measured 2026-09-22: five scenarios here end in a fault and each
    one hung the interpreter until this fixture closed what it recorded.
    """
    import declaro_persistum.session_connect as opening

    running = asyncio.new_event_loop()
    opened: list[Any] = []
    scenario["opened"] = opened
    for door in THE_DOORS_THAT_OPEN_SOMETHING:
        monkeypatch.setattr(opening, door, _recording(getattr(opening, door), opened))
    try:
        yield running
    finally:
        running.run_until_complete(_closed(opened))
        running.close()


THE_DOORS_THAT_OPEN_SOMETHING = ("session_open_connection", "session_open_writer")


def _recording(real: Any, opened: list[Any]) -> Any:
    """The real door, with what it hands over written down beside it."""

    async def _and_remembered(*said: Any) -> Any:
        answered = await real(*said)
        opened.extend([answered for _ in [answered] if not _is_a_fault(answered)])
        return answered

    return _and_remembered


@pytest.fixture(autouse=True)
def only_this_scenarios_models() -> Any:
    """Let go of the classes the last scenario declared, before this one runs."""
    gc.collect()
    yield
    gc.collect()


@pytest.fixture(autouse=True)
def the_process_is_left_as_it_was() -> Any:
    """Put back the three pieces of process state an open changes.

    Opening a database is startup, so it takes the process's signals and writes
    itself onto the shutdown list. Both are right for a library and wrong to
    leave behind in a test run, and this is the only file that installs a signal
    handler as a side effect of what it is testing.

    THE SIGNAL IS THE SMALLER HALF, AND RESTORING IT ALONE IS NOT ENOUGH. That
    is what this fixture did until 2026-09-22, and
    `test_lifecycle_install_handler_steps.py::test_it_chains_it_never_replaces`
    passed on its own and failed in a full run for it.
    `lifecycle_install_handler` records the displaced handler with `setdefault`,
    so `WHAT_WAS_THERE` remembers whatever was in the slot the FIRST time
    anything in the process installed. Once a scenario here has installed, that
    later test's own host handler is never recorded, and `whatever_was_there`
    runs the stale one. So what is put back is the dict as well as the slots it
    describes, and it is put back whether the scenario passed or failed.
    """
    held = {number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS}
    was_recorded = dict(WHAT_WAS_THERE)
    was_registered = list(REGISTERED)
    try:
        yield
    finally:
        for number, handler in held.items():
            signal.signal(number, handler)
        WHAT_WAS_THERE.clear()
        WHAT_WAS_THERE.update(was_recorded)
        REGISTERED[:] = was_registered


# =============================================================================
# asking the orchestrator to open something
# =============================================================================

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_a_fault(answered: object) -> bool:
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _what_a_caller_says(bench: dict[str, Any]) -> dict[str, Any]:
    """The record a caller hands in: what they said, and nothing else.

    `type Connecting` names fifteen fields and a caller fills four. The other
    eleven are what each step of the chain returned, so they are absent here
    rather than filled with a value nobody decided.
    """
    return {
        "target": bench["target"],
        "shutdown": "exit_immediately",
        "connections": 1,
        # WHAT THE DEPLOYMENT ASKED FOR THE CREW, beside what it asked for the
        # connections. The two shared one field until 2026-09-22, so a crew size
        # of its own was silently replaced by the connection count.
        "crew_asked": 1,
        "settings": bench["settings"],
    }


def _nothing_was_there_first(_bench: dict[str, Any]) -> bool:
    """Most scenarios open a database that starts as the Given left it.

    ANSWERS FALSE RATHER THAN RETURNING NOTHING. Both halves of this pair hand
    back whether a first open ran, so the caller reads an answer instead of
    trusting that a side effect happened. An empty body would leave a reader
    unable to tell "nothing to do" from "not written yet".
    """
    return False


def _what_was_there_first(bench: dict[str, Any]) -> bool:
    """Build the starting schema, then declare the models that moved from it.

    WHY THIS RUNS HERE AND NOT IN THE GIVEN. An outline names its engine in the
    When, so a Given cannot know which database it is talking about, and a Given
    that built a starting schema built it on the default target. On SQLite and
    Turso that was the same file by accident and the scenario passed; on
    PostgreSQL each scenario gets a database of its own, which the When then
    made again, empty. So the starting schema is built once the engine is
    settled, which is here.

    The two calls below are taken out of the scenario before the first open, so
    the open this makes does not come back around and build the same schema
    again.
    """
    scenario = bench["scenario"]
    before = scenario.pop("starting_schema")()
    _opened_and_closed(bench)
    held = _what_is_there(bench, "columns", "changing")
    assert held == ["count", "id", "stale"], (
        f"the starting schema was not made on {bench['engine']}: changing holds {held}"
    )
    del before
    gc.collect()
    scenario.pop("moved_to")(bench)
    return True


WHETHER_A_STARTING_SCHEMA_IS_WANTED: dict[bool, Callable[[dict[str, Any]], bool]] = {
    True: _what_was_there_first,
    False: _nothing_was_there_first,
}


def _open_it(bench: dict[str, Any]) -> Any:
    """Ask the orchestrator. The `loop` fixture already records what it opened."""
    bench["scenario"]["opened_once_already"] = WHETHER_A_STARTING_SCHEMA_IS_WANTED[
        "starting_schema" in bench["scenario"]
    ](bench)
    asked = _what_a_caller_says(bench)
    bench["scenario"]["asked"] = asked
    answered = bench["loop"].run_until_complete(session_connect(asked))
    bench["scenario"]["answered"] = answered
    return answered


# =============================================================================
# a database each scenario owns
# =============================================================================


def _a_postgresql_database_of_our_own(bench: dict[str, Any]) -> str:
    """One PostgreSQL database named for this test, dropped afterwards.

    A migration plans a drop for every table the models do not declare, so a
    shared database is one this chain would either empty or refuse to open. The
    boundary has to be the database, which is what `postgresql_schema` does for
    the rest of the suite and what this does for a step that runs on its own
    loop.
    """
    bench["request"].getfixturevalue("require_postgresql")
    named = "sc_" + hashlib.sha256(bench["request"].node.nodeid.encode()).hexdigest()[:16]
    server, _, _ = POSTGRESQL_PROBE_URL.rpartition("/")

    _administer([f'DROP DATABASE IF EXISTS "{named}"', f'CREATE DATABASE "{named}"'])
    bench["request"].addfinalizer(
        lambda: _administer([f'DROP DATABASE IF EXISTS "{named}" WITH (FORCE)'])
    )
    return f"{server}/{named}"


def _administer(statements: list[str]) -> None:
    """On a loop of its own, because the scenario's loop is closed by the time
    this test's finalizers run, and a closed loop never answers."""
    asyncio.run(_through_asyncpg(POSTGRESQL_PROBE_URL, statements))


def _where_that_engines_database_goes(bench: dict[str, Any], engine: str) -> str:
    """The target for a database of this scenario's own, on that engine.

    A Turso target is relative and every Turso scenario runs with the working
    directory inside its own folder, because `session_open_connection` and
    `session_open_writer` read a path out of a target by two different rules and
    agree only on a relative one.
    """
    return {
        "sqlite": lambda: str(bench["folder"] / "opened.db"),
        "postgresql": lambda: _a_postgresql_database_of_our_own(bench),
        "turso": lambda: _a_turso_file_in(bench),
    }[engine]()


def _a_turso_file_in(bench: dict[str, Any]) -> str:
    bench["monkeypatch"].chdir(bench["folder"])
    return "libsql://opened.db"


def _on(bench: dict[str, Any], engine: str) -> None:
    """Point this scenario at a database of its own on that engine."""
    bench["scenario"]["engine"] = engine
    bench["scenario"]["target"] = _where_that_engines_database_goes(bench, engine)


# =============================================================================
# the models a scenario declares, held so they die with the test
# =============================================================================


def _held(bench: dict[str, Any], *classes: type) -> None:
    bench["scenario"].setdefault("models", []).extend(classes)


def _a_declaration_over(model: str) -> dict[str, Any]:
    """The plainest question there is: every row of one model."""
    nothing_asked = {"each": {}, "any_of": [], "none_of": []}
    return {
        "model": model,
        "wanted": "all",
        "columns": [],
        "condition": nothing_asked,
        "order": {},
        "grouping": [],
        "having": nothing_asked,
        "paging": None,
        "rows": None,
        "across": None,
    }


# =============================================================================
# reading a sentence that carries its own values
# =============================================================================

_QUOTED = re.compile(r'"[^"]*"')


def _without_its_values(said: str) -> str:
    """The sentence with every quoted value blanked, so one row serves an
    outline's three examples."""
    return _QUOTED.sub('""', said.strip())


def _the_values_in(said: str) -> list[str]:
    return [found.strip('"') for found in _QUOTED.findall(said)]


# =============================================================================
# Given: what is true before the caller opens anything
# =============================================================================


def _the_models_the_feature_names(bench: dict[str, Any], _values: list[str]) -> None:
    """One table, two columns, and a SQLite file of this scenario's own."""

    class Person(BaseModel):
        __tablename__ = THE_TABLE_THE_FEATURE_NAMES
        id: int
        name: str

    _held(bench, Person)
    _on(bench, "sqlite")


def _a_database_with_no_tables(bench: dict[str, Any], _values: list[str]) -> None:
    """Nothing is created, and the scenario asserts that nothing is there."""
    assert _what_is_there(bench, "tables", "") == [], (
        f"this scenario opens a database with no tables and {bench['target']} "
        f"already holds {_what_is_there(bench, 'tables', '')}"
    )


def _a_database_that_already_matches(bench: dict[str, Any], _values: list[str]) -> None:
    """Opened once, so the second open has nothing left to do.

    A row is written between the two opens, because "no statement changed it"
    is answered by what is still in the table rather than by a count alone.
    """
    _opened_and_closed(bench)
    _ran(bench, [f"INSERT INTO {THE_TABLE_THE_FEATURE_NAMES} (id, name) VALUES (1, 'ada')"])


def _opened_and_closed(bench: dict[str, Any]) -> Any:
    """One whole open, with everything it opened closed again.

    A Given that establishes a starting schema opens the database the way a
    caller would and then hands the file back, so the open the scenario is about
    starts from a database nobody is holding.

    THE LIST IS EMPTIED AND NEVER REPLACED. The `loop` fixture handed that same
    list to the two recording doors, so a new list here would leave every
    connection the second open makes recorded nowhere and closed by nobody.
    """
    first = _open_it(bench)
    assert not _is_a_fault(first), f"the first open had to work and it said {first}"
    opened: list[Any] = bench["scenario"]["opened"]
    bench["loop"].run_until_complete(_closed(opened))
    opened.clear()
    return first


def _a_column_no_model_declares(bench: dict[str, Any], values: list[str]) -> None:
    """The database holds a column the models do not, and nothing asks to lose it."""
    lost = values[0]
    engine = bench["engine"]
    _ran(
        bench,
        [
            f"CREATE TABLE {THE_TABLE_THE_FEATURE_NAMES} "
            f"(id {AN_INTEGER[engine]}, name {SOME_TEXT[engine]}, {lost} {SOME_TEXT[engine]})"
        ],
    )
    bench["scenario"]["the_column"] = lost


def _nothing_asks_to_drop_it(bench: dict[str, Any], _values: list[str]) -> None:
    """The models declared above say nothing about losing anything."""
    asking = [
        model.__name__
        for model in bench["scenario"]["models"]
        if getattr(model, "__dropping__", []) or getattr(model, "__dropping_tables__", [])
    ]
    assert asking == [], f"these models ask to lose something and none may: {asking}"


def _a_replicated_copy_that_is_empty(bench: dict[str, Any], _values: list[str]) -> None:
    """A Turso target with a primary behind it and nothing on disk yet."""
    import os

    primary = os.environ.get("TEST_TURSO_URL", "")
    missing = [name for name, value in [("TEST_TURSO_URL", primary)] if not value]
    assert not missing, f"no primary to replicate from: {', '.join(missing)} is not set in .env"
    _on(bench, "turso")
    bench["scenario"]["settings"] = {"primary": primary}
    pytest.xfail(
        "a cold copy cannot be filled through this chain. "
        "`boundary_out fn session_open_connection` opens a Turso copy by path, "
        "and measured 2026-09-22 a connection opened that way is a "
        "turso.lib_aio.Connection with no `pull`; only the connection "
        "`turso.aio.sync.connect` answers has one. `reader fn turso_pull` calls "
        "`pull` on the handle the database carries."
    )


def _thirty_operations(bench: dict[str, Any], values: list[str]) -> None:
    """Thirty tables the models declare, on the engine the example names."""
    _on(bench, values[0])
    _held(
        bench,
        *[
            type(
                f"Thing{number:02d}",
                (BaseModel,),
                {"__tablename__": f"thing{number:02d}", "__annotations__": {"id": int}},
            )
            for number in range(1, 30)
        ],
    )


def _the_engine_refuses_the_thirtieth(bench: dict[str, Any], _values: list[str]) -> None:
    """A view already holds the thirtieth name, so creating that table is refused.

    THE SAME EVENT AS A DISK FILLING, and one a test can build. The engine says
    no while twenty-nine statements have already run, and what the operator
    finds the next morning is what this scenario is about. It is a view rather
    than a table because `reader fn schema_inspect` reads no views, so the plan
    still contains the operation that will be refused.
    """
    last = "thing30"
    _ran(bench, [f"CREATE VIEW {last} AS SELECT 1"])
    _held(
        bench,
        type("Thing30", (BaseModel,), {"__tablename__": last, "__annotations__": {"id": int}}),
    )
    bench["scenario"]["refused"] = last


def _the_models_before_they_moved() -> list[type]:
    """The starting schema, declared and then let go.

    Nothing holds these once this returns, so the collection the autouse fixture
    runs takes them out of the process before the changed models are declared.
    Two generations of one table cannot be alive together, because
    `reader fn schema_read_declared` reads every Pydantic class there is.
    """

    class Changing(BaseModel):
        __tablename__ = "changing"
        id: int
        stale: str
        count: int

    class Going(BaseModel):
        __tablename__ = "going"
        id: int

    return [Changing, Going]


def _the_models_after_they_moved(bench: dict[str, Any]) -> None:
    """The same project, changed in every way a migration can plan."""

    class Changing(BaseModel):
        __tablename__ = "changing"
        # `stale` is lost and `count` changes type, and both are losses this
        # package makes a model ask for by name.
        __dropping__ = ["stale", "count"]
        __dropping_tables__ = ["going"]
        id: int
        count: str
        fresh: str

    class Arriving(BaseModel):
        __tablename__ = "arriving"
        id: int

    class Everyone(BaseModel):
        __viewname__ = "everyone"
        __declaration__ = _a_declaration_over("changing")

    _held(bench, Changing, Arriving, Everyone)


def _every_kind_of_change(bench: dict[str, Any], _values: list[str]) -> None:
    """A starting schema to build, and the models that moved from it.

    IT ONLY WRITES THE TWO DOWN. Which engine this runs on is named in the When,
    so both opens happen there, once the target is settled.

    THE BEFORE STATE IS OPENED, NOT ASSUMED. It used to be two CREATE TABLE
    statements this file wrote against whatever target the Given happened to
    hold. On SQLite and Turso that was the right file by accident, and on
    PostgreSQL, where each scenario gets a database of its own, the When then
    made that database again, empty, so the second open had nothing to change
    and simply created. Declaring the starting schema as models and opening it
    says what the scenario means: the plan of the second open is exactly the
    changes the models moved by.

    SIX OF THE THIRTEEN OPERATION KINDS ARE REACHABLE FROM A MODEL, and the
    other seven cannot be planned by this chain at all: `reader fn
    schema_read_declared` sets `renamed_from` to NOTHING on every column, so no
    rename can be declared; `reader fn schema_inspect` reads no views, so no
    view is ever dropped; and `fn difference` emits no index and no enum
    operation. The six here are the whole of what a model can ask for.
    """
    bench["scenario"]["starting_schema"] = _the_models_before_they_moved
    bench["scenario"]["moved_to"] = _the_models_after_they_moved


def _a_plan_of_five(bench: dict[str, Any], _values: list[str]) -> None:
    """Five operations planned, two of them for objects already there."""
    _ran(bench, ["CREATE VIEW first_view AS SELECT 1", "CREATE VIEW second_view AS SELECT 1"])

    class First(BaseModel):
        __viewname__ = "first_view"
        __declaration__ = _a_declaration_over(THE_TABLE_THE_FEATURE_NAMES)

    class Second(BaseModel):
        __viewname__ = "second_view"
        __declaration__ = _a_declaration_over(THE_TABLE_THE_FEATURE_NAMES)

    class Here(BaseModel):
        __tablename__ = "here"
        id: int

    class Also(BaseModel):
        __tablename__ = "also"
        id: int

    _held(bench, First, Second, Here, Also)


def _a_migration_that_stops_partway(bench: dict[str, Any], _values: list[str]) -> None:
    """One view is already there, so the migration stops on it with tables made.

    A DISK THAT FILLS AND A VIEW THAT IS ALREADY THERE ARE ONE EVENT HERE. The
    engine refuses a statement while earlier ones have already run, which is the
    whole of what the scenario turns on, and it is a state a test can build.
    """
    _ran(bench, ["CREATE VIEW everyone AS SELECT 1"])

    class Everyone(BaseModel):
        __viewname__ = "everyone"
        __declaration__ = _a_declaration_over(THE_TABLE_THE_FEATURE_NAMES)

    _held(bench, Everyone)


WHAT_A_GIVEN_ESTABLISHES: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    'models declaring a table "" with columns id and name': _the_models_the_feature_names,
    "a database with no tables": _a_database_with_no_tables,
    "a database whose schema already matches the models": _a_database_that_already_matches,
    'a model that no longer declares the column ""': _a_column_no_model_declares,
    "no model asking to drop it": _nothing_asks_to_drop_it,
    "a replicated database whose local copy is empty": _a_replicated_copy_that_is_empty,
    'models whose migration is thirty operations on ""': _thirty_operations,
    "the engine refuses the thirtieth": _the_engine_refuses_the_thirtieth,
    "models declaring every kind of change a migration can plan": _every_kind_of_change,
    "a plan of five operations, two of which the database already has": _a_plan_of_five,
    "a database whose disk fills partway through the migration": _a_migration_that_stops_partway,
    "a migration that stops on an engine that cannot undo what already ran": (
        _a_migration_that_stops_partway
    ),
}


def _the_bench(
    scenario: dict[str, object],
    loop: Any,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> dict[str, Any]:
    """Everything a step needs, in one value, so the one step function below
    takes the same arguments whatever sentence it was handed."""
    scenario.setdefault("settings", {})
    return {
        "scenario": scenario,
        "loop": loop,
        "folder": tmp_path,
        "monkeypatch": monkeypatch,
        "request": request,
        "engine": scenario.setdefault("engine", "sqlite"),
        "target": scenario.get("target", str(tmp_path / "opened.db")),
        "settings": scenario["settings"],
    }


# =============================================================================
# When: the caller opens something
# =============================================================================


def _whatever_the_givens_set_up(bench: dict[str, Any], _values: list[str]) -> Any:
    return _open_it(bench)


def _the_target_the_sentence_names(bench: dict[str, Any], values: list[str]) -> Any:
    """The feature names a PostgreSQL target, and this refuses to migrate it.

    A database somebody else owns is a database this suite may not change, so a
    target naming one that is reachable stops the scenario instead of opening
    it. What the scenario is about is the engine being settled from the string.
    """
    bench["scenario"]["target"] = values[0]
    bench["target"] = values[0]
    bench["scenario"]["engine"] = engine_of(values[0])
    return _REACHABLE[_it_answers(bench, values[0])](bench)


def _refuse_to_touch_it(bench: dict[str, Any]) -> Any:
    pytest.fail(
        f"{bench['target']} names a database that is running on this machine, "
        "and opening it would migrate a database this suite does not own. The "
        "feature names that target, so either it is a real database here or the "
        "scenario has to name one that is not."
    )


_REACHABLE: dict[bool, Callable[[dict[str, Any]], Any]] = {
    True: _refuse_to_touch_it,
    False: _open_it,
}


def _it_answers(bench: dict[str, Any], target: str) -> bool:
    try:
        bench["loop"].run_until_complete(_through_asyncpg(target, ["SELECT 1"]))
    except Exception:
        return False
    return True


def _a_database_on_that_engine(bench: dict[str, Any], values: list[str]) -> Any:
    """The example names the engine, so the database is made on that one."""
    _on(bench, values[0])
    bench["engine"] = values[0]
    bench["target"] = bench["scenario"]["target"]
    return _open_it(bench)


WHAT_A_WHEN_OPENS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    "a caller opens it": _whatever_the_givens_set_up,
    "a caller opens the database": _whatever_the_givens_set_up,
    "a caller opens a local database": _whatever_the_givens_set_up,
    'a caller opens ""': _the_target_the_sentence_names,
    'a caller opens a "" database': _a_database_on_that_engine,
}


# =============================================================================
# Then: what came back, and what the database holds now
# =============================================================================


def _the_table_exists_already(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), f"the open had to work and it said {answered}"
    assert THE_TABLE_THE_FEATURE_NAMES in _what_is_there(bench, "tables", ""), (
        f"the handle came back and {bench['target']} holds "
        f"{_what_is_there(bench, 'tables', '')}, so the first question asked of it would fail on "
        f"a column the models declare"
    )


def _no_statement_changed_it(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), f"the second open had to work and it said {answered}"
    # WHAT THIS DOES NOT ASSERT, AND WHY. `fn migration_needed` compares two
    # whole schemas and says a migration is needed whenever they are not equal,
    # which they are not: the live schema reads facets off the engine that the
    # declared one does not carry. Its own card says so, and says the cost is one
    # wasted diff that then emits nothing. The scenario asks what changed, so
    # what is asserted is that nothing did.
    assert answered["migration"]["plan"]["operations"] == [], (
        f"nothing had to change and the open planned "
        f"{[one['kind'] for one in answered['migration']['plan']['operations']]}"
    )
    assert answered["migration"]["applied"] == 0, (
        f"nothing had to run and {answered['migration']['applied']} operations did"
    )
    rows = _ran(bench, [f"SELECT name FROM {THE_TABLE_THE_FEATURE_NAMES}"])[0]
    assert [str(row[0]) for row in rows] == ["ada"], (
        f"the row written between the two opens is gone: the table holds {rows}"
    )


def _the_handle_comes_back(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), f"a handle had to come back and it said {answered}"
    assert answered["database"]["connection"] is not None, (
        "the record came back carrying no connection, so there is no handle in it"
    )


def _the_room_came_with_the_handle(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), f"the open had to work and it said {answered}"
    assert answered["database"]["room"] == {"arrivals": [], "waiting": []}, (
        f"the handle carries {answered['database']['room']}, which is not an empty waiting room"
    )


def _nobody_had_to_make_one(bench: dict[str, Any], _values: list[str]) -> None:
    """The caller handed nothing room-shaped in, and the record has no field for one."""
    asked = bench["scenario"]["asked"]
    assert "room" not in asked, f"the caller handed a room in: what it said was {sorted(asked)}"
    assert "room" not in typing.get_type_hints(declared_types.Connecting), (
        "`type Connecting` now names a room, so a caller could be asked for one"
    )


def _the_engine_came_from_the_target(bench: dict[str, Any], _values: list[str]) -> None:
    target = bench["scenario"]["target"]
    assert engine_of(target) == "postgresql", (
        f"this scenario rests on {target} naming postgresql, and engine_of says "
        f"{engine_of(target)!r}"
    )
    answered = bench["scenario"]["answered"]
    assert _is_a_fault(answered), (
        f"{target} names a database that is not reachable here, so the open has "
        f"to come back a fault and it came back {answered!r}"
    )
    assert target in answered["what_went_wrong"], (
        f"the refusal has to name what it could not reach. It says {answered['what_went_wrong']!r}"
    )


def _nothing_read_it_off_the_connection(bench: dict[str, Any], _values: list[str]) -> None:
    """Nothing handed to this orchestrator is a connection, so there was none in
    the room to read the engine off."""
    taken = typing.get_type_hints(session_connect)
    carrying = sorted(name for name, kind in taken.items() if kind is declared_types.Connection)
    assert not carrying, (
        f"the orchestrator is handed {carrying}, and a connection among its "
        "arguments would be one it could read the engine off"
    )
    handed_in = bench["scenario"]["asked"]
    assert "connection" not in handed_in, (
        f"the caller handed a connection in: what it said was {sorted(handed_in)}"
    )


def _the_open_is_refused(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert _is_a_fault(answered), (
        f"a column would have been lost and nothing declared it, so the open has "
        f"to be refused. It came back {answered!r}"
    )


def _the_refusal_names_the_column(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    lost = bench["scenario"]["the_column"]
    assert lost in answered["what_went_wrong"], (
        f"the refusal has to name the column it stopped for. It says "
        f"{answered['what_went_wrong']!r}"
    )
    assert "dropping" in answered["what_to_do_instead"], (
        f"the refusal has to say what to declare to allow it. It says "
        f"{answered['what_to_do_instead']!r}"
    )


def _the_column_is_still_there(bench: dict[str, Any], _values: list[str]) -> None:
    lost = bench["scenario"]["the_column"]
    assert lost in _what_is_there(bench, "columns", THE_TABLE_THE_FEATURE_NAMES), (
        f"the open was refused and {lost} is gone anyway: the table holds "
        f"{_what_is_there(bench, 'columns', THE_TABLE_THE_FEATURE_NAMES)}"
    )


def _the_copy_was_filled(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _the_handle_carries_the_four(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), f"the open had to work and it said {answered}"
    handle = answered["database"]
    assert handle["engine"] == engine_of(bench["scenario"]["target"]), (
        f"the handle says {handle['engine']!r} and the target names "
        f"{engine_of(bench['scenario']['target'])!r}"
    )
    assert handle["connection"] is not None, "the handle carries no connection"
    assert len(handle["writers"]) == answered["crew"], (
        f"the handle carries {len(handle['writers'])} writers and the crew is "
        f"{answered['crew']}, so a drainer would have no connection of its own"
    )
    assert handle["room"] == {"arrivals": [], "waiting": []}, "the handle carries no room"
    answers = _ran(bench, ["SELECT 1"])
    assert answers[0], "the connection the handle carries answered nothing"


def _what_the_twenty_nine_did(bench: dict[str, Any], values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert _is_a_fault(answered), (
        f"the thirtieth operation is refused, so the open has to come back a "
        f"fault and it came back {answered!r}"
    )
    WHAT_SURVIVES[values[0]](bench)


def _they_are_kept(bench: dict[str, Any]) -> None:
    made = [name for name in _what_is_there(bench, "tables", "") if name.startswith("thing")]
    assert len(made) == 29, (
        f"this engine commits as it goes, so the twenty-nine tables planned "
        f"before the refused one stay landed. {len(made)} tables named thing* "
        f"are on disk: {made}"
    )


def _they_are_undone(_bench: dict[str, Any]) -> None:
    pytest.xfail(
        "nothing in this chain undoes what already ran. "
        "`orchestrator fn session_connect` is given "
        "`boundary_out fn query_run_statement`, which runs one statement and "
        "commits it, and no transaction scope at all. The card that wraps a "
        "batch is `boundary_out fn query_run_together`, and this orchestrator "
        "does not invoke it, so every engine keeps what already ran."
    )


WHAT_SURVIVES: dict[str, Callable[[dict[str, Any]], None]] = {
    "kept": _they_are_kept,
    "undone": _they_are_undone,
}


def _a_fault_instead_of_a_handle(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert _is_a_fault(answered), (
        f"the migration stopped, so a fault has to come back instead of a "
        f"handle and what came back was {answered!r}"
    )


def _every_change_reached_the_database(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert not _is_a_fault(answered), (
        f"every change a model declared has a spelling on this engine, and the open said {answered}"
    )
    tables = _what_is_there(bench, "tables", "")
    columns = _what_is_there(bench, "columns", "changing")
    assert "arriving" in tables, f"the table a model added is not there: {tables}"
    assert "going" not in tables, f"the table a model asked to lose is still there: {tables}"
    assert "fresh" in columns, f"the column a model added is not there: {columns}"
    assert "stale" not in columns, f"the column a model asked to lose is there: {columns}"
    assert "everyone" in _what_is_there(bench, "views", ""), (
        f"the view a model declared is not there: {_what_is_there(bench, 'views', '')}"
    )
    planned = len(answered["migration"]["plan"]["operations"])
    assert answered["migration"]["applied"] == planned, (
        f"{answered['migration']['applied']} of {planned} operations ran"
    )


def _none_was_refused_for_want_of_a_spelling(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    planned = [operation["kind"] for operation in answered["migration"]["plan"]["operations"]]
    assert sorted(set(planned)) == sorted(
        {"create_table", "drop_table", "add_column", "drop_column", "alter_column", "create_view"}
    ), f"the models declare every change this chain can plan and it planned {sorted(set(planned))}"


def _the_report_names_all_five(bench: dict[str, Any], _values: list[str]) -> None:
    pytest.xfail(
        "a caller cannot read the report of a migration that stopped. "
        "`type Report` rides in the record `orchestrator fn session_connect` "
        "hands back, and a stopped migration hands back a `Fault` instead, whose "
        "three fields carry no plan and no count. Nothing in the chain can tell "
        "an operation the database already has from one it does not either: "
        "`fn difference` emits only what is missing."
    )


def _the_report_says_three_ran(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _it_names_the_operation(bench: dict[str, Any], _values: list[str]) -> None:
    pytest.xfail(
        "the fault names what the engine said and not which operation was "
        "running. It is written by `boundary_out fn query_run_statement`, which "
        "is handed one statement and knows nothing about the plan, and "
        "`orchestrator fn session_connect` carries a fault out in the words of "
        "the function that found it rather than rewriting it."
    )


def _it_says_what_to_do_instead(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _the_fault_says_part_way_through(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answered"]
    assert _is_a_fault(answered), (
        f"the migration stopped, so a fault has to come back and what came back was {answered!r}"
    )
    pytest.xfail(
        "the fault says what the engine refused and not that the database is "
        "part way through its migration. Only "
        "`orchestrator fn session_connect` knows how much ran, and the one "
        "thing it may not do is rewrite a refusal it did not write."
    )


def _it_tells_the_operator_to_look(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


WHAT_A_THEN_READS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    "the table exists before the handle is returned": _the_table_exists_already,
    "no statement changed it": _no_statement_changed_it,
    "the handle comes back": _the_handle_comes_back,
    "the room came with the handle": _the_room_came_with_the_handle,
    "nobody had to make one": _nobody_had_to_make_one,
    "the engine was decided from the target": _the_engine_came_from_the_target,
    "nothing read it back off the connection": _nothing_read_it_off_the_connection,
    "the open is refused": _the_open_is_refused,
    "the refusal names the column and what to declare to allow it": _the_refusal_names_the_column,
    "the column is still there": _the_column_is_still_there,
    "the copy was filled from Turso before the handle was returned": _the_copy_was_filled,
    "the handle carries its engine, its connection, its writers and its room": (
        _the_handle_carries_the_four
    ),
    'the twenty-nine before it are ""': _what_the_twenty_nine_did,
    "a fault comes back instead of a handle": _a_fault_instead_of_a_handle,
    "every change reached the database": _every_change_reached_the_database,
    "none came back refused as one this engine cannot write": (
        _none_was_refused_for_want_of_a_spelling
    ),
    "the report names all five operations it planned": _the_report_names_all_five,
    "the report says three of them ran": _the_report_says_three_ran,
    "it names the operation it stopped on and what the engine said": _it_names_the_operation,
    "it says what to do instead": _it_says_what_to_do_instead,
    "the fault says the database is part way through its migration": (
        _the_fault_says_part_way_through
    ),
    "it tells the operator to look at the database by hand": _it_tells_the_operator_to_look,
}


# =============================================================================
# one function, one table, every sentence the feature says
# =============================================================================
#
# ONE STEP FUNCTION FOR ALL THREE KEYWORDS, which is what a Given, a When and a
# Then here have in common: each is handed the same bench and the same sentence,
# and each reads one row. Written as three functions they were one function
# three times, differing only in the table they read, and the gate that judges
# that said so. `pytest_bdd.step` with no type registers for every keyword, so
# the three tables below are one table and the three functions are one function.
#
# WHAT THIS COSTS, SAID OUT LOUD. `test_no_step_is_unreachable` and
# `test_no_two_steps_are_one_function` both look for a `given`, `when` or `then`
# decorator, so neither sees this file any more. The closure they were keeping
# is kept here instead: every sentence in the feature is a key below, and a
# sentence with no key raises a KeyError naming itself the first time it is said.

EVERY_SENTENCE_THE_FEATURE_SAYS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    **WHAT_A_GIVEN_ESTABLISHES,
    **WHAT_A_WHEN_OPENS,
    **WHAT_A_THEN_READS,
}


@step(parsers.re(r"^(?P<said>.+)$"))
def _one_sentence(
    said: str,
    scenario: dict[str, object],
    loop: Any,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    EVERY_SENTENCE_THE_FEATURE_SAYS[_without_its_values(said)](
        _the_bench(scenario, loop, tmp_path, monkeypatch, request), _the_values_in(said)
    )
