"""Bind `tests/bdd/features/session/session_open_connection.feature`, and nothing else.

WHAT THE FEATURE ASKS. Six scenarios about one outward call. Three of them read
what comes back from `boundary_out fn session_open_connection`: a connection, a
refusal naming the target, and the engine having been settled from the target
rather than from the handle. Three more, moved here on 2026-09-21 from
`permitted_connections.feature`, are about time passing on the connection it
opened: a read that does not wait for a writer, five reads in flight together,
and reads that do not queue behind the sync connection.

EVERY DATABASE HERE IS A REAL ONE. A stand-in has manufactured two defects in
this package already, one inventing a driver bug that did not exist and one
being more permissive than the driver and hiding a regression. So SQLite is a
file on disk opened through aiosqlite, and PostgreSQL is the probe database
`tests/conftest.py` builds for this process, reached through the
`require_postgresql` fixture, which fails rather than skips: 186 passing tests
once went quiet as skips with the server down.

WHY TURSO IS NOT OPENED HERE, and this is the one thing in the feature this
function cannot be asked. `turso.aio.sync.connect(path, remote_url, auth_token=)`
takes a local file to replicate into and a token, and
`session_open_connection(target, engine, connections)` is handed neither. Measured
2026-09-22 against the staging primary in `.env`: with the token a replica opens
in 1.85s and answers `select 1`; without it the primary answers 401. So the
driver works and the credentials work, and what is missing is in the argument
list. The replicated scenario is `pytest.xfail` for that reason, which prints
the gap instead of hiding it, and its remaining steps raise so that deleting the
xfail turns the scenario red rather than green.

WHAT "OVERLAP" IS MEASURED AS, stated plainly because the word can mean two
things. `docs/architecture.md` says the concurrency this package buys is
"overlapping waiting, not executing in parallel". So the five reads are asserted
to be in flight together: every one of them starts before any of them finishes.
A control runs the same five one after another and is asserted to produce
disjoint spans, so the measurement can tell the two apart rather than passing on
anything. On PostgreSQL the five also execute at the same time, because the
handle is the driver's own pool sized by the number of connections permitted; on
SQLite one connection runs them one at a time internally, and what the scenario
forbids is this package queueing them.
"""

from __future__ import annotations

import asyncio
import inspect
import pathlib
import time
import typing
from collections.abc import Awaitable, Callable

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import types as declared_types
from declaro_persistum.engine_of import engine_of
from declaro_persistum.session_open_connection import session_open_connection
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/session/session_open_connection.feature")


# =============================================================================
# The two engines this door can be handed a target for
# =============================================================================

Handle = object

# HOW LONG THE WRITE HOLDS ITS TRANSACTION. Long enough that a read blocked
# behind it finishes after it, short enough that six scenarios stay quick.
THE_WRITE_HOLDS_FOR = 0.4

# FIVE CALLERS, SO FIVE CONNECTIONS ARE PERMITTED. The scenario states the
# number of callers and nothing else, and the bound a deployment would state for
# five callers is five.
FIVE_CALLERS = 5

A_TABLE_ONLY_THESE_SCENARIOS_TOUCH = "session_open_connection_waiting"
_MAKE_THAT_TABLE = f"CREATE TABLE IF NOT EXISTS {A_TABLE_ONLY_THESE_SCENARIOS_TOUCH} (n INTEGER)"
_A_WRITE_TO_IT = f"INSERT INTO {A_TABLE_ONLY_THESE_SCENARIOS_TOUCH} (n) VALUES (1)"
_A_READ_OF_IT = f"SELECT count(*) FROM {A_TABLE_ONLY_THESE_SCENARIOS_TOUCH}"

# A READ THAT TAKES TIME, in each engine's own words. SQLite counts to four
# hundred thousand, which is about an eighth of a second of engine work;
# PostgreSQL sleeps, which is the same wait without the arithmetic.
A_READ_THAT_TAKES_TIME: dict[str, str] = {
    "sqlite": (
        "WITH RECURSIVE counting(i) AS ("
        "SELECT 1 UNION ALL SELECT i + 1 FROM counting WHERE i < 400000"
        ") SELECT count(*) FROM counting"
    ),
    "postgresql": "SELECT pg_sleep(0.2)",
}

# WHAT ONLY THAT ENGINE ANSWERS. A connection that answers `sqlite_version()`
# is SQLite's, and one whose `version()` says PostgreSQL is PostgreSQL's. The
# database is asked rather than the object, because reading which module an
# object's class lives in is the thing `docs/architecture.md` refuses.
WHAT_THE_ENGINE_CALLS_ITSELF: dict[str, tuple[str, str]] = {
    "sqlite": ("SELECT sqlite_version()", "."),
    "postgresql": ("SELECT version()", "PostgreSQL"),
}


def _a_local_database(engine: str, folder: pathlib.Path) -> str:
    """Where a local database of that engine is, as a target."""
    return {
        "sqlite": str(folder / "reads.db"),
        "postgresql": POSTGRESQL_PROBE_URL,
    }[engine]


THE_ENGINES_THIS_DOOR_CAN_BE_HANDED = ("sqlite", "postgresql")

# A HOST THAT DOES NOT EXIST. `.invalid` is reserved by RFC 2606 and never
# resolves, so this fails in the resolver on any machine and does not depend on
# whatever happens to be listening here.
A_HOST_THAT_DOES_NOT_EXIST = "postgresql://postgres:postgres@nosuchhost.invalid:5432/nothing"


# =============================================================================
# Speaking two drivers, because both are real
# =============================================================================


async def _read_through_aiosqlite(handle: Handle, statement: str) -> list:
    cursor = await handle.execute(statement)
    return await cursor.fetchall()


async def _read_through_asyncpg(handle: Handle, statement: str) -> list:
    return await handle.fetch(statement)


READ: dict[str, Callable[[Handle, str], Awaitable[list]]] = {
    "sqlite": _read_through_aiosqlite,
    "postgresql": _read_through_asyncpg,
}


async def _a_slow_write_through_aiosqlite(target: str, finishing: dict[str, float]) -> None:
    """One write holding the file's write lock, on a connection of its own.

    A drainer's connection is a separate connection, which is what
    `fn session_open_writer` opens. This is that shape, built here because the
    scenario needs a writer and not because it needs a double.

    WHY `BEGIN EXCLUSIVE` AND NOT `BEGIN IMMEDIATE`, measured 2026-09-22. An
    immediate transaction takes a RESERVED lock, and a RESERVED lock does not
    hold readers off in either journal mode, so the read answered first whatever
    the connection had been opened with and the scenario proved nothing. An
    exclusive transaction holds every reader off in the default journal mode and
    holds none of them off in WAL, which is the difference this scenario is
    about.
    """
    import aiosqlite

    writer = await aiosqlite.connect(target, isolation_level=None)
    try:
        await writer.execute("BEGIN EXCLUSIVE")
        await writer.execute(_A_WRITE_TO_IT)
        await asyncio.sleep(THE_WRITE_HOLDS_FOR)
        await writer.execute("COMMIT")
    finally:
        finishing["sqlite"] = time.perf_counter()
        await writer.close()


async def _a_slow_write_through_asyncpg(target: str, finishing: dict[str, float]) -> None:
    """The same write, held open in a PostgreSQL transaction."""
    import asyncpg

    writer = await asyncpg.connect(target)
    try:
        transaction = writer.transaction()
        await transaction.start()
        await writer.execute(_A_WRITE_TO_IT)
        await asyncio.sleep(THE_WRITE_HOLDS_FOR)
        await transaction.commit()
    finally:
        finishing["postgresql"] = time.perf_counter()
        await writer.close()


A_WRITE_IN_FLIGHT: dict[str, Callable[[str, dict[str, float]], Awaitable[None]]] = {
    "sqlite": _a_slow_write_through_aiosqlite,
    "postgresql": _a_slow_write_through_asyncpg,
}


# =============================================================================
# One loop per scenario, and everything it opened closed on it
# =============================================================================


async def _close_them_all(opened: list[Handle]) -> None:
    [await handle.close() for handle in opened]


@pytest.fixture
def loop(scenario: dict[str, object]):
    """A loop this scenario owns, and the only place connections are closed.

    A connection has to be closed on the loop it was opened on, so the two live
    and die together here. `scenario["opened"]` is what every step appends to.
    """
    running = asyncio.new_event_loop()
    scenario["opened"] = []
    try:
        yield running
    finally:
        running.run_until_complete(_close_them_all(scenario["opened"]))
        running.close()


def _opened_by_the_door(
    loop: asyncio.AbstractEventLoop,
    scenario: dict[str, object],
    target: str,
    engine: str,
    connections: int,
) -> object:
    """Ask the door, and remember anything it handed back so it gets closed."""
    answered = loop.run_until_complete(session_open_connection(target, engine, connections))
    [scenario["opened"].append(answered) for _ in [answered] if not isinstance(answered, dict)]
    return answered


def _is_a_fault(answered: object) -> bool:
    return isinstance(answered, dict) and set(answered) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


# =============================================================================
# it hands back a connection, and a host that does not exist is a fault
# =============================================================================
#
# ONE FUNCTION FOR BOTH SENTENCES. They differ in which target is asked for and
# in nothing else, so the difference is a row and the rest is shared. A wording
# the table does not hold raises a KeyError naming itself.

_WHAT_IS_ASKED_FOR: dict[str, Callable[[pathlib.Path], dict[str, str]]] = {
    "a local database": lambda folder: {
        engine: _a_local_database(engine, folder) for engine in THE_ENGINES_THIS_DOOR_CAN_BE_HANDED
    },
    "a host that does not exist": lambda _folder: {"postgresql": A_HOST_THAT_DOES_NOT_EXIST},
}


@when(
    parsers.re(
        r"the driver is asked for a connection to "
        r"(?P<where>a local database|a host that does not exist)"
    )
)
def _the_driver_is_asked_for_a_connection(
    scenario: dict[str, object],
    loop: asyncio.AbstractEventLoop,
    tmp_path: pathlib.Path,
    require_postgresql: None,
    where: str,
) -> None:
    targets = _WHAT_IS_ASKED_FOR[where](tmp_path)
    scenario["targets"] = targets
    scenario["answered"] = {
        engine: _opened_by_the_door(loop, scenario, target, engine, 1)
        for engine, target in targets.items()
    }


@then("a connection comes back")
def _a_connection_comes_back(scenario: dict[str, object], loop: asyncio.AbstractEventLoop) -> None:
    """Not a refusal, and not an object that only looks like a handle: each one
    is asked for a row and answers."""
    answered = scenario["answered"]
    refused = {engine: a for engine, a in answered.items() if _is_a_fault(a)}
    assert refused == {}, (
        f"these targets name a local database that is running and the door "
        f"refused them anyway: {refused}"
    )
    answers = {
        engine: loop.run_until_complete(READ[engine](handle, "SELECT 1"))
        for engine, handle in answered.items()
    }
    empty = [engine for engine, rows in answers.items() if not rows]
    assert empty == [], f"these connections came back and then answered nothing: {empty}"


@then("a fault comes back naming the target")
def _a_fault_comes_back_naming_the_target(scenario: dict[str, object]) -> None:
    for engine, answered in scenario["answered"].items():
        target = scenario["targets"][engine]
        assert _is_a_fault(answered), (
            f"{target} cannot be reached, so the answer has to be a fault and it was {answered!r}"
        )
        assert target in answered["what_went_wrong"], (
            f"the fault has to name the target it could not reach. It says "
            f"{answered['what_went_wrong']!r}, which does not contain {target!r}"
        )


@then("no handle is returned")
def _no_handle_is_returned(scenario: dict[str, object]) -> None:
    """A fault instead of a handle, not a fault beside one. Nothing that came
    back may be usable as a connection, so nothing that came back may answer to
    `execute` or `fetch`."""
    handed_back = [
        (engine, answered)
        for engine, answered in scenario["answered"].items()
        if hasattr(answered, "execute") or hasattr(answered, "fetch")
    ]
    assert handed_back == [], (
        f"the target could not be reached and something usable as a connection "
        f"came back anyway: {handed_back}"
    )


# =============================================================================
# nothing asks the connection what engine it is
# =============================================================================


@when("a connection is opened")
def _a_connection_is_opened(
    scenario: dict[str, object],
    loop: asyncio.AbstractEventLoop,
    tmp_path: pathlib.Path,
    require_postgresql: None,
) -> None:
    scenario["targets"] = {
        engine: _a_local_database(engine, tmp_path)
        for engine in THE_ENGINES_THIS_DOOR_CAN_BE_HANDED
    }
    scenario["answered"] = {
        engine: _opened_by_the_door(loop, scenario, target, engine, 1)
        for engine, target in scenario["targets"].items()
    }


@then("the engine came from the target, not from the connection")
def _the_engine_came_from_the_target(
    scenario: dict[str, object], loop: asyncio.AbstractEventLoop
) -> None:
    """Two halves, because the engine could be worked out in two places.

    The engine the target names is the engine that answered, asked of the
    database itself. And nothing the door is handed is a connection, so there
    was none in the room to read the answer off.
    """
    for engine, handle in scenario["answered"].items():
        target = scenario["targets"][engine]
        assert engine_of(target) == engine, (
            f"this scenario is built on {target} naming {engine}, and engine_of "
            f"says {engine_of(target)!r}"
        )
        statement, said_by_that_engine = WHAT_THE_ENGINE_CALLS_ITSELF[engine]
        spoke = str(loop.run_until_complete(READ[engine](handle, statement))[0][0])
        assert said_by_that_engine in spoke, (
            f"{target} names {engine}, so the connection that came back has to "
            f"be {engine}'s, and it calls itself {spoke!r}"
        )

    # WHAT THIS ASSERTS IS THAT NOTHING HANDED IN IS A CONNECTION, not that the
    # argument list is one particular list. It named `["target", "connections"]`
    # until 2026-09-22, and then went red when `engine` was added, on a change
    # that made the claim more true rather than less: the engine now arrives as a
    # word decided upstream instead of being worked out here.
    taken = typing.get_type_hints(session_open_connection)
    carrying = sorted(name for name, kind in taken.items() if kind is declared_types.Connection)

    assert not carrying, (
        f"the door is handed {carrying}, and a connection among its arguments "
        "would be a connection this function could read the engine off, which is "
        "the one thing `docs/architecture.md` refuses"
    )


# =============================================================================
# a read does not wait for a writer
# =============================================================================


@given("a write in flight")
def _a_write_in_flight(
    scenario: dict[str, object],
    loop: asyncio.AbstractEventLoop,
    tmp_path: pathlib.Path,
    require_postgresql: None,
) -> None:
    """One write holding its transaction open on a connection of its own, on
    every engine this door can be handed."""
    targets = {
        engine: _a_local_database(engine, tmp_path)
        for engine in THE_ENGINES_THIS_DOOR_CAN_BE_HANDED
    }
    handles = {
        engine: _opened_by_the_door(loop, scenario, target, engine, 1)
        for engine, target in targets.items()
    }
    [
        loop.run_until_complete(READ[engine](handle, _MAKE_THAT_TABLE))
        for engine, handle in handles.items()
    ]
    finishing: dict[str, float] = {}
    scenario["handles"] = handles
    scenario["write_finished_at"] = finishing
    scenario["writes"] = {
        engine: loop.run_until_complete(
            _started(A_WRITE_IN_FLIGHT[engine](targets[engine], finishing))
        )
        for engine in targets
    }


async def _started(work: Awaitable[None]) -> asyncio.Task:
    """A task running on this loop, given a head start so the write holds its
    lock before anything reads."""
    running = asyncio.ensure_future(work)
    await asyncio.sleep(0.05)
    return running


@when("a caller reads")
def _a_caller_reads(scenario: dict[str, object], loop: asyncio.AbstractEventLoop) -> None:
    scenario["read_answered_at"] = {
        engine: _timed(loop, READ[engine](handle, _A_READ_OF_IT))
        for engine, handle in scenario["handles"].items()
    }


def _timed(loop: asyncio.AbstractEventLoop, work: Awaitable[list]) -> float:
    """When that read answered, by this machine's clock."""
    loop.run_until_complete(work)
    return time.perf_counter()


@then("the read answers without waiting for the write to finish")
def _the_read_answers_without_waiting(
    scenario: dict[str, object], loop: asyncio.AbstractEventLoop
) -> None:
    loop.run_until_complete(asyncio.gather(*scenario["writes"].values()))
    for engine, read_at in scenario["read_answered_at"].items():
        wrote_at = scenario["write_finished_at"][engine]
        assert read_at < wrote_at, (
            f"on {engine} the write committed {read_at - wrote_at:.3f}s before "
            "the read answered, so the read waited for the writer"
        )


# =============================================================================
# reads run beside each other
# =============================================================================


@when("five callers read at once, each taking time")
def _five_callers_read_at_once(
    scenario: dict[str, object],
    loop: asyncio.AbstractEventLoop,
    tmp_path: pathlib.Path,
    require_postgresql: None,
) -> None:
    """Five at once, and the same five one after another as the control.

    Without the control the reading below cannot tell overlap from a clock that
    moved too little to see.
    """
    handles = {
        engine: _opened_by_the_door(
            loop, scenario, _a_local_database(engine, tmp_path), engine, FIVE_CALLERS
        )
        for engine in THE_ENGINES_THIS_DOOR_CAN_BE_HANDED
    }
    scenario["at_once"] = {
        engine: loop.run_until_complete(_five_at_once(engine, handle))
        for engine, handle in handles.items()
    }
    scenario["one_after_another"] = {
        engine: loop.run_until_complete(_five_one_after_another(engine, handle))
        for engine, handle in handles.items()
    }


async def _one_read(engine: str, handle: Handle, spans: list[tuple[float, float]]) -> None:
    started = time.perf_counter()
    await READ[engine](handle, A_READ_THAT_TAKES_TIME[engine])
    spans.append((started, time.perf_counter()))


async def _five_at_once(engine: str, handle: Handle) -> list[tuple[float, float]]:
    spans: list[tuple[float, float]] = []
    await asyncio.gather(*[_one_read(engine, handle, spans) for _ in range(FIVE_CALLERS)])
    return spans


async def _five_one_after_another(engine: str, handle: Handle) -> list[tuple[float, float]]:
    spans: list[tuple[float, float]] = []
    for _ in range(FIVE_CALLERS):
        await _one_read(engine, handle, spans)
    return spans


def _every_one_started_before_any_finished(spans: list[tuple[float, float]]) -> bool:
    return max(start for start, _end in spans) < min(end for _start, end in spans)


@then("the five overlap rather than running one after another")
def _the_five_overlap(scenario: dict[str, object]) -> None:
    for engine, spans in scenario["at_once"].items():
        assert len(spans) == FIVE_CALLERS, f"on {engine} only {len(spans)} of five read"
        assert _every_one_started_before_any_finished(spans), (
            f"on {engine} the five reads did not overlap: their spans are "
            f"{[(round(s - spans[0][0], 3), round(e - spans[0][0], 3)) for s, e in spans]}, "
            "and one starting after another finished is a read that queued"
        )
        control = scenario["one_after_another"][engine]
        assert not _every_one_started_before_any_finished(control), (
            f"on {engine} five reads run one after another also read as "
            "overlapping, so the reading above cannot tell the two apart"
        )


# =============================================================================
# reads do not queue behind the sync connection
# =============================================================================
#
# THE ONE SENTENCE THIS DOOR CANNOT BE ASKED, and why it is `xfail` rather than
# a skip or a quiet pass. A skip says the line was not worth running; a pass
# says it was satisfied. Deleting the xfail turns the scenario red, because the
# two steps after it raise.


@given("a replicated database")
def _a_replicated_database() -> None:
    """What a replica takes, read off the installed driver rather than claimed.

    The two lists are printed side by side so the gap is the reason and not an
    assertion somebody made once and left behind.
    """
    import turso.aio.sync

    replica = inspect.signature(turso.aio.sync.connect)
    needed = [
        name
        for name, parameter in replica.parameters.items()
        if parameter.default is inspect.Parameter.empty
    ]
    taken = list(inspect.signature(session_open_connection).parameters)
    pytest.xfail(
        f"session_open_connection takes {taken}, and opening a Turso replica "
        f"takes {needed} plus an auth token. Measured 2026-09-22 against the "
        "staging primary in .env: with the token a replica opens in 1.85s and "
        "answers, without it the primary answers 401. The driver and the "
        "credentials are there; the local file and the token are not in this "
        "door's arguments, so it cannot open a replicated database at all."
    )


# EVERY STEP IS DEFINED, INCLUDING THE TWO AFTER THE XFAIL. They never run
# today, and they raise rather than assert, so removing the xfail above turns
# the scenario red and names what is missing instead of passing on an empty
# body. Registered from a table because two functions with one body are one
# function.

AFTER_THE_XFAIL: tuple[tuple[str, str], ...] = (
    ("when", "many callers read and write at once"),
    ("then", "the reads did not queue behind it"),
)

REGISTER = {"given": given, "when": when, "then": then}


def _unreachable_until_a_replica_can_be_opened(sentence: str) -> Callable[[], None]:
    def step() -> None:
        raise NotImplementedError(
            f"CODE NOT WRITTEN: {sentence!r} needs a replicated database, and "
            "session_open_connection(target, engine, connections) is handed neither "
            "local file a replica lives in nor the token the primary wants."
        )

    return step


for _step_type, _sentence in AFTER_THE_XFAIL:
    REGISTER[_step_type](_sentence)(_unreachable_until_a_replica_can_be_opened(_sentence))
