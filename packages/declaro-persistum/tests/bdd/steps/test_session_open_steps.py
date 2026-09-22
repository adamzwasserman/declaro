"""Bind `tests/bdd/features/opening/session_open.feature`, and nothing else.

WHAT THE FEATURE ASKS. Sixteen scenarios about one door. `boundary_in fn
session_open` takes where the database is and what should happen at exit, reads
the two numbers the deployment set, hands the record to
`orchestrator fn session_connect`, and gives a caller back the handle that
orchestrator built. Three doors used to make a caller name an engine; there is
one now, and the target says which engine it is.

EVERY DATABASE HERE IS A REAL ONE. SQLite is a file opened through aiosqlite.
PostgreSQL is a database each scenario makes for itself on the probe server
`tests/conftest.py` builds, because a migration against a shared database plans
a drop for every table the models do not declare. Turso is pyturso, and the two
replicated scenarios open the live replica `a_real_turso_replica` pulls from the
staging primary.

WHERE A TARGET IN THE FEATURE IS NOT A TARGET ON THIS MACHINE. The feature
writes `postgresql://localhost/todo`, and a database called `todo` on this
machine belongs to somebody else: opening it would migrate it. So a scenario
naming a PostgreSQL target opens a database of its own on the probe server,
which is a PostgreSQL target reached by the same scheme. The two targets the
feature names that nothing can open, `mysql://localhost/todo` and
`postgresql://unreachable.invalid/todo`, are used exactly as written, because
being unopenable is what those scenarios are about.

A BARE PATH IS SQLITE, SO `data.db` CANNOT NAME TURSO. `fn engine_of` reads the
scheme and nothing else, and only `libsql://` names turso. The feature writes
`data.db` in the two Turso scenarios and `app.db` in the scenario that refuses a
primary, which are the same shape of string and opposite answers, so the Turso
scenarios here open a `libsql://` target. That disagreement is recorded rather
than papered over.

ONE STEP FUNCTION, OVER ONE TABLE. Every Given establishes a state, every When
opens something and every Then reads what came back, and all three are handed
the same bench and one sentence, so sentences differing only in their words are
rows rather than functions. A sentence with no row raises a KeyError naming
itself the first time it is said, which is what happens when the feature is
edited and this file is not.

WHAT THIS DOOR CANNOT BE ASKED TODAY, stated here rather than hidden in a skip.
Four sentences call `pytest.xfail` naming exactly what is missing, and the steps
after each one raise, so deleting an xfail turns its scenario red:

  asking for Person   `boundary_in fn query_ask` and
                      `orchestrator fn query_answer` are declared and not built,
                      so no caller can ask a database for anything yet. Two
                      scenarios turn on a caller asking.

  one sync connection A replicated database is settled at one connection by
                      `fn permitted_connections`, and this chain opens two: one
                      for reads and one per drainer, and `fn crew_size` settles
                      one drainer for a replicated database. Neither is a sync
                      connection either. Measured 2026-09-22:
                      `turso.aio.sync.connect` answers a `ConnectionSync` that
                      has `pull` and `turso.aio.connect` answers a `Connection`
                      that does not, and `boundary_out fn
                      session_open_connection` opens a Turso copy by path.

  the retry budget    Nothing hands a retry budget to this door.
                      `fn budget_spent` takes one and `orchestrator fn
                      lifecycle_catch_up` is what invokes it, so the budget is
                      stated for a drainer's turn and `type Connecting` has no
                      field for one.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import inspect
import os
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
from declaro_persistum.replicates import replicates
from declaro_persistum.session_open import session_open
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/opening/session_open.feature")


# =============================================================================
# what a caller says, and what comes back
# =============================================================================

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})
THE_FIELDS_OF_A_HANDLE = frozenset(
    {"engine", "connection", "writers", "room", "shutdown", "primary"}
)

# The three words `Engine` holds, read off the declaration's own type so this
# file cannot drift from it.
THE_ENGINE_WORDS = frozenset(typing.get_args(declared_types.Engine))

# WHAT THE DEPLOYMENT SETS, AND THE NUMBER IT SETS HERE. One connection and one
# drainer is enough for every scenario, and the point of stating it is that
# nothing invents it.
THE_VARIABLES_THE_DEPLOYMENT_SETS = {"DECLARO_CONNECTIONS": "1", "DECLARO_CREW_SIZE": "1"}

# WHAT EVERY CALLER HERE SAYS ABOUT EXIT. None of these scenarios is about
# shutdown, and the one that is says nothing at all instead.
WHAT_HAPPENS_AT_EXIT = "exit_immediately"


def _is_a_fault(answered: object) -> bool:
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _is_a_handle(answered: object) -> bool:
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_HANDLE


# =============================================================================
# speaking to each engine, because a scenario states what is true afterwards
# =============================================================================
#
# THE STATEMENTS BELOW ARE THE TEST'S OWN. The package writes its own; these are
# how this file builds a database up and asks it what it holds afterwards.

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


def _ran(bench: dict[str, Any], target: str, statements: list[str]) -> list[list[Any]]:
    engine = engine_of(target)
    return bench["loop"].run_until_complete(ASK_THE_DATABASE[engine](target, statements))


# =============================================================================
# one loop per scenario, and everything it opened closed on it
# =============================================================================


async def _closed(handles: list[Any]) -> None:
    for handle in handles:
        await handle.close()


THE_DOORS_THAT_OPEN_SOMETHING = ("session_open_connection", "session_open_writer")


def _recording(real: Any, opened: list[Any], attempted: list[str]) -> Any:
    """The real door, with what it was asked and what it handed over written down.

    Both wrappers call the real function and change nothing about what it does
    or what it answers. `attempted` is what one scenario is about: a target no
    engine claims is refused before anything is opened, and the only way to say
    that is to count the times a driver was asked.
    """

    async def _and_remembered(*said: Any) -> Any:
        attempted.append(said[0])
        answered = await real(*said)
        opened.extend([answered for _ in [answered] if not _is_a_fault(answered)])
        return answered

    return _and_remembered


@pytest.fixture
def loop(scenario: dict[str, object], monkeypatch: pytest.MonkeyPatch) -> Any:
    """A loop this scenario owns, and the only place connections are closed.

    A connection has to be closed on the loop it was opened on, so the two live
    and die together here. An open that fails hands back a `Fault` and nothing
    else, so the connections it had already opened cannot be reached by the
    caller at all; aiosqlite runs one non-daemon thread per connection, so a
    leaked one holds the process open after every test has finished.
    """
    import declaro_persistum.session_connect as opening

    running = asyncio.new_event_loop()
    opened: list[Any] = []
    attempted: list[str] = []
    scenario["opened"] = opened
    scenario["attempted"] = attempted
    for door in THE_DOORS_THAT_OPEN_SOMETHING:
        monkeypatch.setattr(opening, door, _recording(getattr(opening, door), opened, attempted))
    try:
        yield running
    finally:
        running.run_until_complete(_closed(opened))
        running.close()


@pytest.fixture(autouse=True)
def only_this_scenarios_models() -> Any:
    """Let go of the classes the last scenario declared, before this one runs.

    `reader fn schema_read_declared` takes no arguments and reads every Pydantic
    class alive in the process, so a test cannot hand it a set of models. A
    scenario declares them the way a host does and holds them in the shared
    `scenario` fixture, which dies with the test.
    """
    gc.collect()
    yield
    gc.collect()


@pytest.fixture(autouse=True)
def the_process_is_left_as_it_was() -> Any:
    """Put back the three pieces of process state an open changes.

    Opening a database is startup, so it takes the process's signals and writes
    itself onto the shutdown list. Both are right for a library and wrong to
    leave behind in a test run. `lifecycle_install_handler` records the
    displaced handler with `setdefault`, so the dict is put back as well as the
    slots it describes, and both are put back whether the scenario passed or
    failed.
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
# a database of this scenario's own, on each engine
# =============================================================================


def _a_sqlite_file_in(bench: dict[str, Any]) -> str:
    return str(bench["folder"] / "app.db")


def _a_turso_file_in(bench: dict[str, Any]) -> str:
    """A relative target, because `boundary_out fn session_open_connection` and
    `boundary_out fn session_open_writer` read a path out of a target by two
    different rules and agree only on a relative one."""
    bench["monkeypatch"].chdir(bench["folder"])
    return "libsql://data.db"


def _a_postgresql_database_of_our_own(bench: dict[str, Any]) -> str:
    """One PostgreSQL database named for this test, dropped afterwards.

    A migration plans a drop for every table the models do not declare, so a
    shared database is one this chain would either empty or refuse to open. The
    boundary has to be the database.
    """
    bench["request"].getfixturevalue("require_postgresql")
    named = "so_" + hashlib.sha256(bench["request"].node.nodeid.encode()).hexdigest()[:16]
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


WHERE_A_DATABASE_OF_OUR_OWN_GOES: dict[str, Callable[[dict[str, Any]], str]] = {
    "sqlite": _a_sqlite_file_in,
    "postgresql": _a_postgresql_database_of_our_own,
    "turso": _a_turso_file_in,
}


def _already_made(_bench: dict[str, Any], held: dict[str, str], engine: str) -> str:
    return held[engine]


def _made_now(bench: dict[str, Any], held: dict[str, str], engine: str) -> str:
    held[engine] = WHERE_A_DATABASE_OF_OUR_OWN_GOES[engine](bench)
    return held[engine]


# WHETHER THIS SCENARIO ALREADY HAS ONE. Written as a table rather than as
# `setdefault`, which evaluates what it is given whether or not it stores it:
# the PostgreSQL row drops and creates a database, so the scenario that opens
# the same three targets twice dropped its own database out from under two open
# connections the second time round.
WHETHER_THE_TARGET_IS_ALREADY_MADE: dict[
    bool, Callable[[dict[str, Any], dict[str, str], str], str]
] = {
    True: _already_made,
    False: _made_now,
}


def _a_target_on(bench: dict[str, Any], engine: str) -> str:
    """This scenario's target on that engine, made once and reused."""
    held: dict[str, str] = bench["scenario"].setdefault("targets", {})
    return WHETHER_THE_TARGET_IS_ALREADY_MADE[engine in held](bench, held, engine)


# =============================================================================
# asking the door to open something
# =============================================================================


def _opened(bench: dict[str, Any], target: str, **settings: Any) -> Any:
    """One call of the door, with what was said and what came back written down."""
    said = {"target": target, "shutdown": WHAT_HAPPENS_AT_EXIT, **settings}
    bench["scenario"].setdefault("calls", []).append(said)
    answered = bench["loop"].run_until_complete(
        session_open(target, WHAT_HAPPENS_AT_EXIT, **settings)
    )
    bench["scenario"].setdefault("answers", []).append(answered)
    bench["scenario"]["refusals_from_this_call"] = WHETHER_IT_CAME_BACK_REFUSED[
        _is_a_fault(answered)
    ](bench, answered)
    return answered


def _the_last_handle(bench: dict[str, Any]) -> Any:
    answered = bench["scenario"]["answers"][-1]
    assert _is_a_handle(answered), f"a handle had to come back and it said {answered!r}"
    return answered


def _the_last_refusal(bench: dict[str, Any]) -> tuple[str, str]:
    """The words of the last refusal, however the caller was refused.

    A setting no engine can use comes back as a `Fault` in the return. A caller
    who said nothing about shutdown could not make the call at all, and Python
    refuses it, so what they hold is the message of a TypeError. Both are a
    refusal naming what was wrong, and both are read here as two strings.
    """
    return bench["scenario"]["refusals"][-1]


def _it_worked(_bench: dict[str, Any], _answered: Any) -> int:
    """A handle came back, so there are no words of refusal to keep.

    ANSWERS HOW MANY REFUSALS THIS CALL ADDED, which is none. Both halves of
    this pair hand back that number, so the caller reads an answer rather than
    trusting a side effect, and an empty body cannot be mistaken for a function
    nobody finished.
    """
    return 0


def _the_words_of_the_fault(bench: dict[str, Any], answered: Any) -> int:
    bench["scenario"].setdefault("refusals", []).append(
        (answered["what_went_wrong"], answered["what_to_do_instead"])
    )
    return 1


WHETHER_IT_CAME_BACK_REFUSED: dict[bool, Callable[[dict[str, Any], Any], int]] = {
    True: _the_words_of_the_fault,
    False: _it_worked,
}


def _the_words_python_refused_with(bench: dict[str, Any], refused: BaseException) -> None:
    """A caller who could not make the call at all holds Python's own message."""
    bench["scenario"].setdefault("refusals", []).append((str(refused), ""))


# =============================================================================
# the models a scenario declares, held so they die with the test
# =============================================================================


def _a_person_model_over(bench: dict[str, Any], table: str) -> None:
    """The table the feature names, declared the way a host declares one."""

    class Person(BaseModel):
        __tablename__ = table
        id: int
        name: str

    bench["scenario"].setdefault("models", []).append(Person)


# =============================================================================
# reading a sentence that carries its own values
# =============================================================================

_QUOTED = re.compile(r'"[^"]*"')


def _without_its_values(said: str) -> str:
    """The sentence with every quoted value blanked, so one row serves every
    sentence of that shape."""
    return _QUOTED.sub('""', said.strip())


def _the_values_in(said: str) -> list[str]:
    return [found.strip('"') for found in _QUOTED.findall(said)]


# =============================================================================
# Given: what is true before the caller opens anything
# =============================================================================


def _a_database_already_opened(bench: dict[str, Any], _values: list[str]) -> None:
    """One SQLite database of this scenario's own, already open."""
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    _opened(bench, _a_target_on(bench, "sqlite"))


def _two_databases_on_two_engines(bench: dict[str, Any], _values: list[str]) -> None:
    """One SQLite and one PostgreSQL database, both already open."""
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    _opened(bench, _a_target_on(bench, "sqlite"))
    _opened(bench, _a_target_on(bench, "postgresql"))


WHAT_A_GIVEN_ESTABLISHES: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    'a database opened from ""': _a_database_already_opened,
    'a database opened from "" and a database opened from ""': _two_databases_on_two_engines,
}


# =============================================================================
# When: the caller opens something, or tries to
# =============================================================================


def _as_written(bench: dict[str, Any], target: str) -> str:
    """A target nothing can open is used exactly as the feature writes it."""
    return target


def _a_postgresql_one_we_own(bench: dict[str, Any], _target: str) -> str:
    return _a_target_on(bench, "postgresql")


# WHICH TARGET THE FEATURE NAMES AND WHERE IT REALLY IS. Written out rather than
# read with a fallback, so a target the feature adds later raises a KeyError
# naming itself instead of being opened somewhere nobody chose.
WHERE_THE_FEATURES_TARGET_REALLY_IS: dict[str, Callable[[dict[str, Any], str], str]] = {
    "postgresql://localhost/todo": _a_postgresql_one_we_own,
    "mysql://localhost/todo": _as_written,
    "postgresql://unreachable.invalid/todo": _as_written,
}


def _the_target_the_sentence_names(bench: dict[str, Any], values: list[str]) -> Any:
    named = values[0]
    return _opened(bench, WHERE_THE_FEATURES_TARGET_REALLY_IS[named](bench, named))


def _a_replicated_turso(bench: dict[str, Any], values: list[str]) -> Any:
    """A live replica of the staging primary, opened with the primary named.

    THE COPY HOLDS THE TABLE THE MODELS DECLARE BEFORE THE DOOR IS CALLED.
    `orchestrator fn session_connect` fills a copy that holds nothing at all,
    and filling one calls `pull` on the handle the database carries, which a
    connection opened by path does not have. So the state this scenario is
    about, a database told apart by its primary, is reached with a copy that is
    already usable.
    """
    bench["monkeypatch"].chdir(bench["folder"])
    replica = pathlib.Path(bench["request"].getfixturevalue("a_real_turso_replica"))
    target = f"libsql://{replica.name}"
    _ran(
        bench,
        target,
        [f"CREATE TABLE {THE_TABLE_THE_FEATURE_NAMES} (id INTEGER, name TEXT)"],
    )
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    bench["scenario"]["primary"] = _the_primary_in_the_environment()
    return _opened(bench, target, primary=bench["scenario"]["primary"])


def _the_primary_in_the_environment() -> str:
    primary = os.environ.get("TEST_TURSO_URL", "")
    assert primary, "no primary to replicate from: TEST_TURSO_URL is not set in .env"
    return primary


def _a_local_turso(bench: dict[str, Any], _values: list[str]) -> Any:
    """A Turso database on this machine with no primary behind it."""
    return _opened(bench, _a_target_on(bench, "turso"))


def _all_three_engines(bench: dict[str, Any], _values: list[str]) -> Any:
    """One file opening a SQLite, a PostgreSQL and a Turso database in turn."""
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    return [_opened(bench, _a_target_on(bench, engine)) for engine in THE_ORDER_THEY_ARRIVE_IN]


THE_ORDER_THEY_ARRIVE_IN = ("sqlite", "postgresql", "turso")
THE_OTHER_ORDER = ("turso", "sqlite", "postgresql")


def _the_same_three_in_another_order(bench: dict[str, Any], _values: list[str]) -> Any:
    """The same three targets, opened twice, in two orders.

    Both rounds are recorded against the target they opened, so the Then reads
    what each target answered rather than what the third call answered.
    """
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    bench["scenario"]["rounds"] = [
        {engine: _opened(bench, _a_target_on(bench, engine))["engine"] for engine in order}
        for order in (THE_ORDER_THEY_ARRIVE_IN, THE_OTHER_ORDER)
    ]
    return bench["scenario"]["rounds"]


def _a_caller_asks_for_person(bench: dict[str, Any], _values: list[str]) -> Any:
    pytest.xfail(
        "no caller can ask a database for anything yet. `boundary_in fn "
        "query_ask` takes a question and `orchestrator fn query_answer` answers "
        "it, and neither is built, so there is no call that asks this handle for "
        "every Person."
    )


def _the_same_target_twice(bench: dict[str, Any], values: list[str]) -> Any:
    """Two opens of one file, each a database of its own."""
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    target = _a_target_on(bench, "sqlite")
    return [_opened(bench, target), _opened(bench, target)]


def _the_doors_arguments(bench: dict[str, Any], _values: list[str]) -> Any:
    """What a caller may say, read off the door itself."""
    bench["scenario"]["arguments"] = inspect.signature(session_open).parameters
    return bench["scenario"]["arguments"]


def _with_a_busy_timeout(bench: dict[str, Any], values: list[str]) -> Any:
    """A setting that means something to a file waiting on a lock, handed to an
    engine that waits on a server."""
    named = values[0]
    answered = _opened(
        bench, WHERE_THE_FEATURES_TARGET_REALLY_IS[named](bench, named), busy_timeout_s=5
    )
    return answered


def _with_a_primary(bench: dict[str, Any], values: list[str]) -> Any:
    """A primary handed to an engine that has nothing to replicate to."""
    answered = _opened(bench, _a_target_on(bench, "sqlite"), primary="libsql://team-app.turso.io")
    return answered


def _saying_nothing_about_shutdown(bench: dict[str, Any], _values: list[str]) -> Any:
    """The call a caller cannot make, tried once on each engine.

    `shutdown` has no default, so a caller who says nothing about it has not
    made a call at all and Python refuses it before the door runs. Every engine
    is tried because the scenario says the refusal holds on any of them.
    """
    for engine in THE_ORDER_THEY_ARRIVE_IN:
        with pytest.raises(TypeError) as refused:
            session_open(_a_target_on(bench, engine))  # type: ignore[call-arg]
        _the_words_python_refused_with(bench, refused.value)
        bench["scenario"].setdefault("answers", []).append(refused.value)
    return bench["scenario"]["refusals"]


def _a_database_with_no_number_set(bench: dict[str, Any], _values: list[str]) -> Any:
    """An open on a machine whose deployment stated no number of connections.

    The bench sets both variables on every step, so this takes one back out
    immediately before the open. Nothing reads it again afterwards.
    """
    bench["monkeypatch"].delenv("DECLARO_CONNECTIONS", raising=False)
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    return _opened(bench, _a_target_on(bench, "sqlite"))


WHAT_A_WHEN_OPENS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    'a caller opens ""': _the_target_the_sentence_names,
    'a caller opens "" with the primary ""': _a_replicated_turso,
    'a caller opens "" as turso with no primary': _a_local_turso,
    'a caller opens "", then "", then ""': _all_three_engines,
    "the same three targets are opened in a different order": _the_same_three_in_another_order,
    "a caller asks it for every Person": _a_caller_asks_for_person,
    "a caller asks each of them for every Person": _a_caller_asks_for_person,
    'a caller opens "" twice': _the_same_target_twice,
    "the door's arguments are read": _the_doors_arguments,
    'a caller opens "" with a busy timeout': _with_a_busy_timeout,
    'a caller opens "" with a primary': _with_a_primary,
    'a caller opens "" and says nothing about shutdown': _saying_nothing_about_shutdown,
    "a database is opened": _a_database_with_no_number_set,
}


# =============================================================================
# Then: what came back
# =============================================================================


def _the_handle_says_postgresql(bench: dict[str, Any], _values: list[str]) -> None:
    handle = _the_last_handle(bench)
    target = bench["scenario"]["calls"][-1]["target"]
    assert handle["engine"] == "postgresql", (
        f"{target} names postgresql and the handle says {handle['engine']!r}"
    )
    assert handle["engine"] == engine_of(target), (
        f"the handle says {handle['engine']!r} and the target names {engine_of(target)!r}"
    )


def _the_only_place_the_engine_is_written(bench: dict[str, Any], _values: list[str]) -> None:
    """One field of the handle carries the word, and nothing the caller said does."""
    handle = _the_last_handle(bench)
    carrying = sorted(field for field, held in handle.items() if held == handle["engine"])
    assert carrying == ["engine"], (
        f"the handle writes {handle['engine']!r} down in {carrying}, and one field is the whole "
        "of where it is settled"
    )
    said = bench["scenario"]["calls"][-1]
    named = sorted(argument for argument, held in said.items() if held in THE_ENGINE_WORDS)
    assert named == [], f"the caller named an engine in {named}: what they said was {said}"


def _the_engine_came_from_the_database(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _it_is_on_turso(bench: dict[str, Any], _values: list[str]) -> None:
    handle = _the_last_handle(bench)
    target = bench["scenario"]["calls"][-1]["target"]
    assert handle["engine"] == "turso", (
        f"{target} names turso and the handle says {handle['engine']!r}"
    )


def _one_sync_connection(bench: dict[str, Any], _values: list[str]) -> None:
    handle = _the_last_handle(bench)
    assert replicates(handle), (
        f"this database was opened with the primary {bench['scenario']['primary']!r} and the "
        f"handle carries {handle['primary']!r}, so nothing downstream would replicate it"
    )
    pytest.xfail(
        "a replicated database holds two connections here and neither is a sync "
        "connection. `fn permitted_connections` settles 1 for a replicated "
        "database and `fn crew_size` settles 1 drainer, and `orchestrator fn "
        "session_connect` opens one connection per drainer on top of the one "
        "reads run on. Measured 2026-09-22: `boundary_out fn "
        "session_open_connection` opens a Turso copy by path, and a connection "
        "opened that way has no `pull`; only the one `turso.aio.sync.connect` "
        "answers has."
    )


def _no_sync_connection(bench: dict[str, Any], _values: list[str]) -> None:
    handle = _the_last_handle(bench)
    assert handle["primary"] is declared_types.NOTHING, (
        f"no primary was named and the handle carries {handle['primary']!r}"
    )
    assert not replicates(handle), (
        "the handle names no primary and is still read as replicated, so something "
        "downstream would try to sync a database with nothing to sync to"
    )


def _three_databases_on_three_engines(bench: dict[str, Any], _values: list[str]) -> None:
    answers = bench["scenario"]["answers"]
    assert len(answers) == 3, f"three databases had to come back and {len(answers)} did"
    faults = [one for one in answers if not _is_a_handle(one)]
    assert faults == [], f"three handles had to come back and one said {faults[0]!r}"
    assert [one["engine"] for one in answers] == list(THE_ORDER_THEY_ARRIVE_IN), (
        f"the three targets name {list(THE_ORDER_THEY_ARRIVE_IN)} and the handles say "
        f"{[one['engine'] for one in answers]}"
    )


def _the_caller_named_no_engine(bench: dict[str, Any], _values: list[str]) -> None:
    """No call said an engine, and the door has no argument that could carry one."""
    named = [
        (argument, held)
        for said in bench["scenario"]["calls"]
        for argument, held in said.items()
        if held in THE_ENGINE_WORDS
    ]
    assert named == [], f"a call named an engine: {named}"
    assert _what_the_door_could_be_told_an_engine_by() == [], (
        "the door has an argument that names an engine: "
        f"{_what_the_door_could_be_told_an_engine_by()}"
    )


def _what_the_door_could_be_told_an_engine_by() -> list[str]:
    """Every argument of the door whose name or type says engine or scheme."""
    taken = inspect.signature(session_open).parameters
    return sorted(
        name
        for name, argument in taken.items()
        if "engine" in name or "scheme" in name or argument.annotation is declared_types.Engine
    )


def _each_is_on_the_engine_it_was(bench: dict[str, Any], _values: list[str]) -> None:
    first, second = bench["scenario"]["rounds"]
    assert first == second, (
        f"the same three targets answered {first} in one order and {second} in another"
    )


def _no_engine_depends_on_the_order(bench: dict[str, Any], _values: list[str]) -> None:
    """Each round's answer is what the target alone names, whatever came first."""
    wrong = [
        (engine, round_number, answered)
        for round_number, answers in enumerate(bench["scenario"]["rounds"])
        for engine, answered in answers.items()
        if answered != engine_of(bench["scenario"]["targets"][engine])
    ]
    assert wrong == [], f"an engine was settled by something other than its target: {wrong}"


def _each_answers_from_its_own(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _neither_call_named_an_engine(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _two_databases_came_back(bench: dict[str, Any], _values: list[str]) -> None:
    first, second = bench["scenario"]["answers"]
    assert _is_a_handle(first) and _is_a_handle(second), (
        f"two handles had to come back and they were {first!r} and {second!r}"
    )
    assert first is not second, "the same handle came back twice"
    assert first["connection"] is not second["connection"], (
        "both handles carry the same connection, so they are one database wearing two names"
    )


def _closing_one_leaves_the_other(bench: dict[str, Any], _values: list[str]) -> None:
    """Close everything the first database holds, then ask the second a question.

    The closed connections are taken off the list the `loop` fixture closes, so
    nothing is closed twice.
    """
    first, second = bench["scenario"]["answers"]
    closing = [first["connection"], *first["writers"]]
    bench["loop"].run_until_complete(_closed(closing))
    opened: list[Any] = bench["scenario"]["opened"]
    opened[:] = [held for held in opened if held not in closing]

    answered = bench["loop"].run_until_complete(_asked(second["connection"]))
    assert answered, (
        "the second database answered nothing after the first was closed, so closing "
        "one closed the other"
    )


async def _asked(connection: Any) -> Any:
    """One question, through the handle the door handed back."""
    return await (await connection.execute("SELECT 1")).fetchall()


def _no_argument_is_an_engine(bench: dict[str, Any], _values: list[str]) -> None:
    taken = bench["scenario"]["arguments"]
    assert "target" in taken, f"the door takes {sorted(taken)} and none of them says where"
    assert _what_the_door_could_be_told_an_engine_by() == [], (
        f"the door takes {_what_the_door_could_be_told_an_engine_by()}, which names an engine "
        "or a scheme, so a caller would decide before writing a line"
    )


def _only_the_target_says_which(bench: dict[str, Any], _values: list[str]) -> None:
    """Two opens alike in everything but the target, on two engines."""
    _a_person_model_over(bench, THE_TABLE_THE_FEATURE_NAMES)
    answers = [
        _opened(bench, _a_target_on(bench, engine))["engine"] for engine in ("sqlite", "turso")
    ]
    assert answers == ["sqlite", "turso"], (
        f"two calls alike in everything but the target answered {answers}, so something other "
        "than the target settled the engine"
    )


def _it_is_refused(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answers"][-1]
    assert not _is_a_handle(answered), (
        f"this had to be refused and a handle came back: {answered!r}"
    )
    why, _instead = _the_last_refusal(bench)
    assert why, "it was refused and the refusal says nothing about what was wrong"


def _postgresql_has_no_busy_timeout(bench: dict[str, Any], _values: list[str]) -> None:
    why, instead = _the_last_refusal(bench)
    assert "postgresql" in why, f"the refusal has to name the engine. It says {why!r}"
    assert "busy_timeout" in why, f"the refusal has to name the setting. It says {why!r}"
    assert "busy_timeout" in instead, (
        f"the refusal has to say what to write instead. It says {instead!r}"
    )


def _sqlite_has_no_primary(bench: dict[str, Any], _values: list[str]) -> None:
    why, instead = _the_last_refusal(bench)
    assert "sqlite" in why, f"the refusal has to name the engine. It says {why!r}"
    assert "primary" in why, f"the refusal has to name the setting. It says {why!r}"
    assert "primary" in instead, (
        f"the refusal has to say what to write instead. It says {instead!r}"
    )


def _shutdown_has_no_default_anywhere(bench: dict[str, Any], _values: list[str]) -> None:
    said = bench["scenario"]["refusals"]
    assert len(said) == len(THE_ORDER_THEY_ARRIVE_IN), (
        f"every engine had to be tried and {len(said)} were"
    )
    silent = [why for why, _instead in said if "shutdown" not in why]
    assert silent == [], f"a refusal did not name shutdown: {silent}"
    taken = inspect.signature(session_open).parameters
    assert taken["shutdown"].default is inspect.Parameter.empty, (
        f"shutdown now defaults to {taken['shutdown'].default!r}, so a caller who did not say "
        "what happens to their data at exit is no longer told apart from one who chose"
    )


def _nothing_was_opened(bench: dict[str, Any], _values: list[str]) -> None:
    attempted = bench["scenario"]["attempted"]
    assert attempted == [], f"a driver was asked to open {attempted}"
    left = sorted(path.name for path in bench["folder"].iterdir())
    assert left == [], f"the refused open left {left} behind in {bench['folder']}"


def _the_engine_was_decided_first(bench: dict[str, Any], _values: list[str]) -> None:
    target = bench["scenario"]["calls"][-1]["target"]
    assert engine_of(target) == "postgresql", (
        f"this scenario rests on {target} naming postgresql, and it names {engine_of(target)!r}"
    )
    answered = bench["scenario"]["answers"][-1]
    assert _is_a_fault(answered), (
        f"{target} names a database nothing can reach, so a fault has to come back and "
        f"{answered!r} did"
    )


def _what_failed_is_the_connection(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answers"][-1]
    target = bench["scenario"]["calls"][-1]["target"]
    assert target in answered["what_went_wrong"], (
        f"the failure has to name what it could not reach. It says {answered['what_went_wrong']!r}"
    )
    assert "be opened" in answered["what_went_wrong"], (
        f"the failure is the connection and it says {answered['what_went_wrong']!r}, which does "
        "not say opening is what failed"
    )
    assert bench["scenario"]["attempted"] == [target] * len(bench["scenario"]["attempted"]), (
        f"a driver was asked for something other than {target}: {bench['scenario']['attempted']}"
    )
    assert bench["scenario"]["attempted"], (
        "nothing was ever asked to open, so the engine was not decided before the failure"
    )


def _the_budget_is_part_of_it(bench: dict[str, Any], _values: list[str]) -> None:
    pytest.xfail(
        "nothing hands a retry budget to this door. `fn budget_spent` takes one "
        "and `orchestrator fn lifecycle_catch_up` is what invokes it, so the "
        "budget is stated for a drainer's turn, and `type Connecting` has no "
        "field for one to travel in."
    )


def _omitting_it_is_refused(bench: dict[str, Any], _values: list[str]) -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


def _the_bound_is_part_of_it(bench: dict[str, Any], _values: list[str]) -> None:
    answered = bench["scenario"]["answers"][-1]
    assert not _is_a_handle(answered), (
        f"nothing stated how many connections this database may hold, so the open has to be "
        f"refused and it came back {answered!r}"
    )
    why, _instead = _the_last_refusal(bench)
    assert "connections" in why, f"the refusal has to name what was not stated. It says {why!r}"


def _the_number_belongs_to_the_machine(bench: dict[str, Any], _values: list[str]) -> None:
    why, instead = _the_last_refusal(bench)
    assert "property of the machine" in instead, (
        f"the refusal has to say where the right number comes from. It says {instead!r}"
    )
    assert "invent" in instead, (
        f"the refusal has to say that nothing here may stand in for it. It says {instead!r}"
    )
    assert "no measured number to fall back on" in why, (
        f"the refusal has to say there is no default to fall back on. It says {why!r}"
    )


WHAT_A_THEN_READS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    "the database carries postgresql as its engine": _the_handle_says_postgresql,
    "that is the only place the engine is written down": _the_only_place_the_engine_is_written,
    "the engine came from the database, not from the connection it used": (
        _the_engine_came_from_the_database
    ),
    "it is on turso": _it_is_on_turso,
    "it holds one sync connection for its life": _one_sync_connection,
    "it holds no sync connection": _no_sync_connection,
    "three databases come back, on sqlite, postgresql and turso": (
        _three_databases_on_three_engines
    ),
    "the file that opened them names no engine anywhere": _the_caller_named_no_engine,
    "each database is on the same engine it was before": _each_is_on_the_engine_it_was,
    "no database's engine depends on which was opened first": _no_engine_depends_on_the_order,
    "each answers from its own database": _each_answers_from_its_own,
    "neither call named an engine": _neither_call_named_an_engine,
    "it gets two databases": _two_databases_came_back,
    "closing one does not close the other": _closing_one_leaves_the_other,
    "none of them is an engine or a scheme": _no_argument_is_an_engine,
    "the only thing that says which engine to use is the target": _only_the_target_says_which,
    "it is refused": _it_is_refused,
    "the refusal says postgresql has no busy timeout": _postgresql_has_no_busy_timeout,
    "the refusal says sqlite has no primary to replicate to": _sqlite_has_no_primary,
    "the refusal says shutdown has no default, on any engine": _shutdown_has_no_default_anywhere,
    "no file was created and no connection was attempted": _nothing_was_opened,
    "the engine was decided first, and postgresql": _the_engine_was_decided_first,
    "what failed is the connection, and the failure says so": _what_failed_is_the_connection,
    "the retry budget is part of that declaration": _the_budget_is_part_of_it,
    "omitting it is refused rather than filled in": _omitting_it_is_refused,
    "how many connections it may use is part of that declaration": _the_bound_is_part_of_it,
    "it has no default, because the right number is a property of the machine": (
        _the_number_belongs_to_the_machine
    ),
}


# =============================================================================
# one function, one table, every sentence the feature says
# =============================================================================
#
# ONE STEP FUNCTION FOR ALL THREE KEYWORDS, which is what a Given, a When and a
# Then here have in common: each is handed the same bench and the same sentence,
# and each reads one row. `pytest_bdd.step` with no type registers for every
# keyword, so the three tables below are one table and the three functions are
# one function. Every sentence in the feature is a key below, and a sentence
# with no key raises a KeyError naming itself the first time it is said.

EVERY_SENTENCE_THE_FEATURE_SAYS: dict[str, Callable[[dict[str, Any], list[str]], Any]] = {
    **WHAT_A_GIVEN_ESTABLISHES,
    **WHAT_A_WHEN_OPENS,
    **WHAT_A_THEN_READS,
}


def _the_bench(
    scenario: dict[str, object],
    loop: Any,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> dict[str, Any]:
    """Everything a step needs, in one value, so the one step function below
    takes the same arguments whatever sentence it was handed.

    THE DEPLOYMENT'S TWO NUMBERS ARE SET HERE, and `monkeypatch` takes them back
    out when the test ends. They are what a deployment states and what the door
    reads, so a scenario that is not about them states them.
    """
    for variable, number in THE_VARIABLES_THE_DEPLOYMENT_SETS.items():
        monkeypatch.setenv(variable, number)
    return {
        "scenario": scenario,
        "loop": loop,
        "folder": tmp_path,
        "monkeypatch": monkeypatch,
        "request": request,
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
