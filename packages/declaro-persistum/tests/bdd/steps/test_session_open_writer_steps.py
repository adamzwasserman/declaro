"""Bind `session/session_open_writer.feature` to `session_open_writer`, and to
nothing else.

WHAT THIS FUNCTION IS ASKED. `boundary_out fn session_open_writer` takes a
target and hands back one connection, or a fault. It is the first function in
this package that opens anything, so every scenario here runs against a real
database on this machine rather than against a stand-in. Twice a stand-in has
manufactured a defect here: one invented a pyturso bug that did not exist, and
one was more permissive than the driver and would have hidden a regression. So
SQLite, PostgreSQL and Turso are all opened for real, and every scenario that
opens anything opens all three.

WHY THE STEPS COUNT WRITERS THAT THIS FUNCTION DOES NOT COUNT. The function
opens ONE connection. How many drainers a deployment runs is `crew_size`, and
how many times to call this is `orchestrator fn session_connect`, which is not
written. So the composition lives here, in the open of a scenario: `crew_size`
says how many on a local database, and a replicated database opens none,
because `docs/architecture.md` says a replica holds one connection by
construction and that one is the connection it reads on.

WHY THE TESTS OPEN THEIR OWN READING CONNECTION. The third scenario compares a
writer against the connection the database reads on, and
`boundary_out fn session_open_connection` opens that one and is not written
either. The driver opens it here instead, which is the real driver and not a
double, and the scenario asserts only that the two are different objects.

EVERY CONNECTION OPENED HERE IS CLOSED HERE, on the one loop that opened it. An
asyncpg connection belongs to the loop that made it, so the scenario gets a loop
of its own and hands it to every step.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import aiosqlite
import asyncpg
import pytest
import turso.aio
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_size import crew_size
from declaro_persistum.permitted_connections import permitted_connections
from declaro_persistum.session_open_writer import session_open_writer
from declaro_persistum.types import NOTHING
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/session/session_open_writer.feature")


# --- what the sentences mean ----------------------------------------------

HOW_MANY = {"no": 0, "one": 1, "four": 4}
"""The counts this feature spells out in words."""

A_PRIMARY = "libsql://team-app.turso.io"
"""An address a replica pulls from. Nothing here connects to it: the scenario
that names it opens nothing at all, which is what it is there to say."""


# --- the three engines, opened for real ------------------------------------
#
# The driver calls below are the test's own. They are what a reading connection
# is opened with, and what one row is read with to prove a writer is live rather
# than merely an object. `query_run_statement` will own the second of those; it
# is not written, and a scenario asserting that four connections came back
# without ever asking one of them for a row would pass on four closed handles.


def _a_turso_file(target: str) -> str:
    """The local file a turso target names, which is the target without the
    scheme `engine_of` read the engine out of."""
    return target.partition("://")[2]


_CONNECT: dict[str, Callable[[str], Any]] = {
    "sqlite": lambda target: aiosqlite.connect(target),
    "postgresql": lambda target: asyncpg.connect(target),
    "turso": lambda target: turso.aio.connect(_a_turso_file(target)),
}


async def _a_connection_to_read_on(engine: str, target: str) -> Any:
    """The driver's own connect, called from inside the loop.

    IT HAS TO BE CALLED IN THERE. pyturso's `connect` is a plain function that
    asks for the current loop as it builds the connection, so calling it to make
    an argument for `run_until_complete` raises `RuntimeError: There is no
    current event loop`. aiosqlite and asyncpg do not care, which is exactly
    what would have made this look like a pyturso defect.
    """
    return await _CONNECT[engine](target)


async def _a_row_through_a_cursor(connection: Any) -> Any:
    """aiosqlite and pyturso both answer with a cursor, and an unfetched one
    proves nothing, so the row is fetched."""
    cursor = await connection.execute("SELECT 1")
    return await cursor.fetchall()


async def _a_row_straight_back(connection: Any) -> Any:
    """asyncpg answers with the rows themselves."""
    return await connection.fetch("SELECT 1")


_ONE_ROW: dict[str, Callable[[Any], Coroutine[Any, Any, Any]]] = {
    "sqlite": _a_row_through_a_cursor,
    "turso": _a_row_through_a_cursor,
    "postgresql": _a_row_straight_back,
}


# --- what a scenario runs on -----------------------------------------------


@pytest.fixture
def loop():
    """A loop this scenario owns, closed when the scenario ends.

    Every connection is opened and closed on this one, because an asyncpg
    connection belongs to the loop that made it and closing it on another is a
    failure at teardown, attributed to whatever test is running by then.
    """
    made = asyncio.new_event_loop()
    try:
        yield made
    finally:
        made.close()


async def _close_them_all(connections: list[Any]) -> None:
    """What is opened here is closed here. Built inside the loop, because a
    coroutine made outside a running loop has nowhere to run."""
    await asyncio.gather(*[connection.close() for connection in connections])


@pytest.fixture
def opened(loop) -> Any:
    """Every connection a step opened, closed before the loop goes.

    This fixture asks for `loop`, so pytest tears it down first and the closes
    run on a loop that is still open.
    """
    connections: list[Any] = []
    yield connections
    loop.run_until_complete(_close_them_all(connections))


@pytest.fixture
def targets(tmp_path, require_postgresql) -> dict[str, str]:
    """One target per engine, each naming a database this test owns.

    `require_postgresql` fails rather than skips when the server is down. A
    skipped backend reads as a tested one, and 186 passing tests once went quiet
    as skips that way.
    """
    return {
        "sqlite": str(tmp_path / "crew.db"),
        "postgresql": POSTGRESQL_PROBE_URL,
        "turso": f"libsql://{tmp_path / 'crew-turso.db'}",
    }


UNREACHABLE = {
    "sqlite": "/no-such-folder-session-open-writer/app.db",
    "postgresql": "postgresql://postgres@no-such-host.invalid/postgres",
    "turso": "libsql:///no-such-folder-session-open-writer/app.db",
}
"""A target of each engine that no machine can open: a folder that is not there,
and a host that does not resolve."""


# --- opening a database, which is what session_connect will do -------------


def _a_replica_opens_none(_scenario: dict[str, Any]) -> int:
    """`docs/architecture.md`: a replicated database holds one connection by
    construction, and that one is the connection it reads on, so a drainer there
    has nothing of its own to be opened."""
    return 0


def _a_local_database_opens_its_crew(scenario: dict[str, Any]) -> int:
    """One writer per drainer, and `crew_size` says how many drainers."""
    return crew_size(scenario["asked"], False)


_WRITERS_TO_OPEN: dict[bool, Callable[[dict[str, Any]], int]] = {
    True: _a_replica_opens_none,
    False: _a_local_database_opens_its_crew,
}


async def _one_writer(engine: str, target: str, timeline: list[str]) -> Any:
    writer = await session_open_writer(target, engine)
    assert not isinstance(writer, dict), (
        f"opening a writer on the {engine} target {target} came back with a "
        f"fault rather than a connection: {writer}"
    )
    timeline.append(f"{engine}: a writer was opened")
    return writer


async def _a_handle(
    engine: str, target: str, wanted: int, primary: str | None, timeline: list[str]
) -> dict[str, Any]:
    """What a caller keeps: the crew opened first, then the handle carrying it."""
    writers = [await _one_writer(engine, target, timeline) for _ in range(wanted)]
    handle = {
        "engine": engine,
        "connection": NOTHING,
        "writers": writers,
        "room": {"arrivals": [], "waiting": []},
        "shutdown": "exit_immediately",
        "primary": primary,
    }
    timeline.append(f"{engine}: the handle came back")
    return handle


async def _every_handle(
    targets: dict[str, str], wanted: int, primary: str | None, timeline: list[str]
) -> dict[str, dict[str, Any]]:
    return {
        engine: await _a_handle(engine, target, wanted, primary, timeline)
        for engine, target in targets.items()
    }


def _open_the_database(
    scenario: dict[str, Any], loop: Any, opened: list[Any], targets: dict[str, str]
) -> None:
    """Open every engine, and leave the handles where the Then can read them."""
    timeline: list[str] = []
    wanted = _WRITERS_TO_OPEN[scenario["primary"] is not NOTHING](scenario)
    handles = loop.run_until_complete(_every_handle(targets, wanted, scenario["primary"], timeline))
    opened.extend([writer for handle in handles.values() for writer in handle["writers"]])
    scenario["handles"] = handles
    scenario["timeline"] = timeline
    scenario["targets"] = targets


# --- the scenarios ---------------------------------------------------------


@given(parsers.parse("a deployment asking for a crew of {number}"))
def a_deployment_asking_for_a_crew(scenario: dict[str, Any], number: str) -> None:
    """A deployment that names no primary is running on a local database, which
    is the convention `tests/bdd/conftest.py` writes down."""
    scenario["asked"] = HOW_MANY[number]
    scenario["primary"] = NOTHING


@given("a replicated database")
def a_replicated_database(scenario: dict[str, Any]) -> None:
    """A primary is the whole of what makes a database replicated, and this
    scenario states no crew size because a replica overrides one anyway."""
    scenario["primary"] = A_PRIMARY
    scenario["asked"] = NOTHING


@when("a local database is opened")
@when("it is opened")
def the_database_is_opened(
    scenario: dict[str, Any], loop: Any, opened: list[Any], targets: dict[str, str]
) -> None:
    _open_the_database(scenario, loop, opened, targets)


@when(parsers.parse("a local database is opened with a crew of {number}"))
def a_local_database_is_opened_with_a_crew(
    scenario: dict[str, Any],
    loop: Any,
    opened: list[Any],
    targets: dict[str, str],
    number: str,
) -> None:
    scenario["asked"] = HOW_MANY[number]
    scenario["primary"] = NOTHING
    _open_the_database(scenario, loop, opened, targets)


@when("a writer is asked for against a target that cannot be opened")
def a_writer_is_asked_for_against_a_target_that_cannot_be_opened(
    scenario: dict[str, Any], loop: Any
) -> None:
    """Three engines, three targets no machine can open, and nothing is closed
    afterwards because nothing was opened."""
    scenario["refusals"] = {
        engine: loop.run_until_complete(session_open_writer(target, engine))
        for engine, target in UNREACHABLE.items()
    }


@then(parsers.parse("{number} writer connections were opened"))
@then(parsers.parse("{number} writer connection was opened"))
def that_many_writer_connections_were_opened(
    scenario: dict[str, Any], loop: Any, number: str
) -> None:
    """The count, and that no two of them are one connection handed back twice.

    A shared connection would make the crew one drainer wearing several names,
    which is the whole of what the crew is for. And each one is asked for a row,
    because a connection that was never opened is still an object.
    """
    wanted = HOW_MANY[number]
    for engine, handle in scenario["handles"].items():
        writers = handle["writers"]
        assert len(writers) == wanted, (
            f"{engine} opened {len(writers)} writers and the scenario says {wanted}"
        )
        assert len({id(writer) for writer in writers}) == wanted, (
            f"{engine} handed back {len({id(w) for w in writers})} distinct "
            f"connections for {wanted} drainers, so at least two drainers share one"
        )
        for writer in writers:
            answered = loop.run_until_complete(_ONE_ROW[engine](writer))
            assert len(answered) == 1, (
                f"a {engine} writer answered {answered!r} to one row, so the "
                f"connection came back without being open"
            )


@then("they were opened before the handle came back")
def they_were_opened_before_the_handle_came_back(scenario: dict[str, Any]) -> None:
    """Opening them at open is what makes reuse possible, so the order is the
    claim: every writer this engine holds was opened before its handle existed."""
    timeline = scenario["timeline"]
    for engine, handle in scenario["handles"].items():
        came_back = timeline.index(f"{engine}: the handle came back")
        opens = [
            at for at, event in enumerate(timeline) if event == f"{engine}: a writer was opened"
        ]
        assert len(opens) == len(handle["writers"]), (
            f"{engine} recorded {len(opens)} opens for {len(handle['writers'])} writers"
        )
        assert opens and max(opens) < came_back, (
            f"{engine} handed the handle back at {came_back} and opened writers at "
            f"{opens}, so a drainer's connection is opened after the caller has "
            f"the handle rather than at open"
        )


@then("the reason is that a replica holds one connection by construction")
def the_reason_is_one_connection_by_construction(scenario: dict[str, Any]) -> None:
    """The reason is a stated number rather than a comment: a replica permits
    one connection and runs one drainer, and that one connection is the one the
    database reads on, so there is none left for a writer to hold."""
    assert permitted_connections(NOTHING, True) == 1
    assert crew_size(NOTHING, True) == 1
    for engine, handle in scenario["handles"].items():
        assert handle["writers"] == [], f"{engine} opened {len(handle['writers'])} writers"
        assert handle["primary"] == A_PRIMARY, engine


@then("the writer is a different connection from the one the database reads on")
def the_writer_is_not_the_reading_connection(
    scenario: dict[str, Any], loop: Any, opened: list[Any]
) -> None:
    """A reading connection opened by the driver itself, because
    `session_open_connection` is what opens that one and is not written."""
    for engine, handle in scenario["handles"].items():
        reading = loop.run_until_complete(
            _a_connection_to_read_on(engine, scenario["targets"][engine])
        )
        opened.append(reading)
        assert [id(writer) for writer in handle["writers"]] != [id(reading)], (
            f"the {engine} writer is the connection reads run on, so a drainer "
            f"and every reader share one connection"
        )


@then("a fault comes back naming the target")
def a_fault_comes_back_naming_the_target(scenario: dict[str, Any]) -> None:
    """A failure comes back in the return, and the fault says which target."""
    for engine, refusal in scenario["refusals"].items():
        assert isinstance(refusal, dict), f"{engine} came back with {refusal!r}, not a fault"
        assert refusal["kind"] == "refused", (engine, refusal)
        assert UNREACHABLE[engine] in refusal["what_went_wrong"], (engine, refusal)
        assert refusal["what_to_do_instead"], (engine, refusal)


@then("no connection is handed back")
def no_connection_is_handed_back(scenario: dict[str, Any]) -> None:
    """A fault and a connection are the two halves of the return, and a caller
    that got one did not get the other."""
    for engine, refusal in scenario["refusals"].items():
        assert set(refusal) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
            f"{engine} came back with {sorted(refusal)}, which carries more than a fault"
        )
