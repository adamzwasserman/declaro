"""Bind `tests/bdd/features/session/session_close_connection.feature`.

WHAT THE FEATURE ASKS. The connection handed over comes back shut and nothing
comes back with it. A driver that refuses the close is quoted rather than
swallowed. One that is already shut closes again quietly.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO. The feature says
nothing about engines, because the result is the same on each, and a driver
nobody closed is a driver nobody has seen close. So each scenario opens SQLite,
PostgreSQL and Turso, closes each, and asks each the same question.

NOTHING HERE IS A STAND-IN, EXCEPT THE ONE THAT HAS TO BE. SQLite is
`aiosqlite`, PostgreSQL is `asyncpg` on the probe database this pytest process
made, and Turso is `pyturso` on a file of its own. A driver that refuses to close
is the one thing no real driver will do on demand, so that scenario hands over an
object whose `close` raises, and the assertion is about what reaches the caller
rather than about any driver's behaviour.

HOW "IT IS SHUT" IS ASKED, WHICH IS NOT BY READING A FLAG. Each driver is asked
to run a statement afterwards. A connection that is really closed refuses; one
that only says it is closed answers, and a flag would have read the same either
way.
"""

from __future__ import annotations

import asyncio
import pathlib
import tempfile
from collections.abc import Awaitable, Callable

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.session_close_connection import session_close_connection
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/session/session_close_connection.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

WHAT_THE_DRIVER_SAID = "this handle will not let go"


# --- the three engines, opened for real --------------------------------------
#
# ONE ROW PER ENGINE, TWO ENTRIES EACH: how a connection is opened, and how it is
# asked to do something afterwards. The second is how "it is shut" is read: a
# closed connection refuses, whatever it says about itself.


async def _open_sqlite(folder: pathlib.Path):
    import aiosqlite

    return await aiosqlite.connect(folder / "sqlite.db")


async def _use_sqlite(connection) -> None:
    await connection.execute("SELECT 1")


async def _open_turso(folder: pathlib.Path):
    import turso.aio

    return await turso.aio.connect(str(folder / "turso.db"))


async def _use_turso(connection) -> None:
    cursor = connection.cursor()
    await cursor.execute("SELECT 1")


async def _open_postgresql(_folder: pathlib.Path):
    import asyncpg

    return await asyncpg.connect(POSTGRESQL_PROBE_URL)


async def _use_postgresql(connection) -> None:
    await connection.fetch("SELECT 1")


THE_THREE_ENGINES: dict[str, dict[str, Callable]] = {
    "sqlite": {"open": _open_sqlite, "use": _use_sqlite},
    "turso": {"open": _open_turso, "use": _use_turso},
    "postgresql": {"open": _open_postgresql, "use": _use_postgresql},
}


class _AHandleThatWillNotClose:
    """The one thing no real driver will do on demand.

    A CLASS BECAUSE A CONNECTION IS AN OBJECT WITH A METHOD, which is the whole
    of what this door asks of one. It stands in for a driver refusing, not for a
    driver, so there is no contract here to get wrong.
    """

    async def close(self) -> None:
        raise OSError(WHAT_THE_DRIVER_SAID)


@pytest.fixture
def scenario() -> dict:
    return {}


async def _against_every_engine(
    work: Callable[[str, object], Awaitable[dict]],
) -> dict[str, dict]:
    """Open each engine in a folder of its own, run `work`, and hand back what it
    said. Nothing is closed here: closing is what is being tested."""
    answers: dict[str, dict] = {}
    for engine, driver in THE_THREE_ENGINES.items():
        with tempfile.TemporaryDirectory() as made:
            connection = await driver["open"](pathlib.Path(made))
            answers[engine] = await work(engine, connection)
    return answers


async def _still_open(engine: str, connection) -> bool:
    """Whether the driver will still do anything on this connection."""
    try:
        await THE_THREE_ENGINES[engine]["use"](connection)
    except Exception:  # noqa: BLE001
        return False
    return True


# --- what each scenario establishes ------------------------------------------


async def _one_that_is_open(engine: str, connection) -> dict:
    return {"connection": connection, "engine": engine, "closed_before": False}


async def _one_already_closed(engine: str, connection) -> dict:
    """Shut by the driver itself, which is what a second shutdown pass finds."""
    await connection.close()
    return {"connection": connection, "engine": engine, "closed_before": True}


_WHAT_EACH_GIVEN_ESTABLISHES: dict[str, Callable] = {
    "a connection that is open": _one_that_is_open,
    "a connection that has already been closed": _one_already_closed,
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_ESTABLISHES, key=len, reverse=True)))))
def _what_was_opened(scenario: dict, sentence: str) -> None:
    scenario["setup"] = _WHAT_EACH_GIVEN_ESTABLISHES[sentence]


@given("a connection the driver refuses to close")
def _one_that_refuses(scenario: dict) -> None:
    scenario["refuses"] = True


@when("it is closed")
def _it_is_closed(scenario: dict) -> None:
    async def _closed() -> dict:
        async def _close_one(engine: str, connection) -> dict:
            held = await scenario["setup"](engine, connection)
            answer = await session_close_connection(connection)
            return {**held, "answer": answer, "still_open": await _still_open(engine, connection)}

        return await _against_every_engine(_close_one)

    async def _refused() -> dict:
        handle = _AHandleThatWillNotClose()
        return {"refusing": {"answer": await session_close_connection(handle)}}  # type: ignore[arg-type]

    scenario["outcomes"] = asyncio.run(
        {True: _refused, False: _closed}[scenario.get("refuses", False)]()
    )


# --- what must be true afterwards --------------------------------------------


@then("the connection is shut")
def _the_connection_is_shut(scenario: dict) -> None:
    """Asked of the driver, not of a flag: a closed connection refuses work."""
    _every_engine_answered(scenario)
    for engine, outcome in scenario["outcomes"].items():
        assert not outcome["still_open"], (
            f"on {engine} the connection still runs statements after being "
            f"closed, so the operating system thread behind it is still there"
        )


@then("nothing came back with it")
def _nothing_came_back(scenario: dict) -> None:
    _every_engine_answered(scenario)
    for engine, outcome in scenario["outcomes"].items():
        assert outcome["answer"] is None, (
            f"on {engine} closing answered {outcome['answer']!r}, and a close "
            f"that worked answers with nothing"
        )


@then("a fault comes back naming what the driver said")
def _a_fault_names_the_driver(scenario: dict) -> None:
    answer = scenario["outcomes"]["refusing"]["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a driver refusing the close answered {answer!r}, and a failure comes "
        f"back as a Fault in the return"
    )
    assert WHAT_THE_DRIVER_SAID in answer["what_went_wrong"], (
        f"the fault does not carry what the driver said, so an operator reading "
        f"the shutdown cannot tell why: {answer['what_went_wrong']!r}"
    )


def _every_engine_answered(scenario: dict) -> None:
    """A loop over nothing runs no assertion and publishes a pass."""
    outcomes = scenario["outcomes"]
    assert set(outcomes) == set(THE_THREE_ENGINES), (
        f"closing was asked of {sorted(outcomes)}, and this file exists to close "
        f"every driver: {sorted(THE_THREE_ENGINES)}"
    )
