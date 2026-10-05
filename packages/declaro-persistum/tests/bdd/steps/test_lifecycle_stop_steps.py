"""Bind `tests/bdd/features/lifecycle/lifecycle_stop.feature`.

WHAT THE FEATURE ASKS. The signal runs shutdown to completion, it hands over
nothing and so both registered databases are reached, and a shutdown that could
not finish comes back as a value rather than raising inside a signal handler.

THE DATABASES ARE PUT ON THE REGISTER THE WAY A CALLER PUTS THEM THERE, through
`fn lifecycle_register`, and taken off again afterwards. The register is module
state that outlives one scenario, so a scenario that left something on it is the
next one's mystery.

EVERY SCENARIO RUNS ON REAL SQLITE CONNECTIONS, so "its connections were closed"
is asked by running a statement on them.

THE CHANNEL IS A STAND-IN FOR THE PRIMARY, because what these scenarios turn on
is whether it was asked and whether it took what it was given.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_register import REGISTERED, lifecycle_register
from declaro_persistum.lifecycle_stop import lifecycle_stop
from declaro_persistum.types import Database

scenarios("../features/lifecycle/lifecycle_stop.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


class _AChannelThatMayRefuse:
    """The primary, standing in so a scenario can say whether it took it."""

    def __init__(self, refuses: bool) -> None:
        self._refuses = refuses
        self.asked = 0

    async def push(self) -> None:
        self.asked = self.asked + 1
        _WHETHER_IT_TAKES_IT[self._refuses]()

    async def pull(self) -> bool:
        return False


def _turn_it_down() -> bool:
    raise OSError("the primary would not take it")


def _take_it() -> bool:
    return True


_WHETHER_IT_TAKES_IT = {True: _turn_it_down, False: _take_it}


# WHAT EACH SENTENCE PUTS ON THE REGISTER: how many databases, and whether their
# channel ever takes what it is given.
WHAT_EACH_GIVEN_SETS: dict[str, dict] = {
    "a database that asked to replicate at exit": {"databases": 1, "refuses": False},
    "two databases that asked to replicate at exit": {"databases": 2, "refuses": False},
    "a database whose channel refuses every attempt": {"databases": 1, "refuses": True},
    # A REGISTER HOLDING SOMETHING MALFORMED. Every driver failure is caught and
    # turned into a value before it reaches this door, so this is the path that
    # actually raises, and without it "nothing was raised" asserts nothing.
    "a register holding something that is not a database": {
        "databases": 1,
        "refuses": False,
        "malformed": True,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    """One scenario, with the register emptied and every connection closed.

    THE REGISTER IS MODULE STATE THAT OUTLIVES A SCENARIO, so leaving a database
    on it hands the next scenario a connection that is already closed.

    WHAT THIS SCENARIO OPENED, THIS SCENARIO CLOSES, and that is the test's job
    rather than the library's. Shutdown closes a database it finished with; a
    database whose crew it could not stop it leaves alone, so these scenarios own
    the closing. Measured 2026-10-02: without this, the malformed-crew scenario
    left two connections open, aiosqlite gave each one a thread it never marks as
    a daemon, and `uv run pytest` never exited at all.
    """
    held: dict = {"tmp": tmp_path}
    yield held
    REGISTERED.clear()
    asyncio.run(_close_what_this_scenario_opened(held.get("databases", [])))


async def _close_what_this_scenario_opened(databases: list[Database]) -> None:
    """Every connection this scenario made, closed whether shutdown reached it.

    A connection shutdown already closed refuses a second close on some drivers
    and takes it on others, so the refusal is the answer here rather than a
    failure: what matters is that none is left open.
    """
    for db in databases:
        for connection in [*db["writers"], *db["connections"]]:
            with contextlib.suppress(Exception):
                await connection.close()  # type: ignore[attr-defined]


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_registered(scenario: dict, sentence: str) -> None:
    scenario.update(WHAT_EACH_GIVEN_SETS[sentence])


@when("the platform says stop")
def _the_platform_says_stop(scenario: dict) -> None:
    scenario.update(asyncio.run(_a_signal(scenario)))


async def _one_database(scenario: dict, n: int) -> Database:
    import aiosqlite

    path = scenario["tmp"] / f"stop{n}.db"
    return Database(
        target=f"sqlite:///{path}",
        engine="sqlite",
        connections=[await aiosqlite.connect(path, isolation_level=None)],  # type: ignore[list-item]
        writers=[await aiosqlite.connect(path, isolation_level=None)],  # type: ignore[list-item]
        channel=_AChannelThatMayRefuse(scenario["refuses"]),  # type: ignore[typeddict-item]
        crew=crew_make(),
        shutdown="replicate_then_exit",
    )


async def _a_signal(scenario: dict) -> dict:
    """Register what the scenario said, then let the signal find it."""
    databases = [await _one_database(scenario, n) for n in range(scenario["databases"])]
    for db in databases:
        lifecycle_register(_WHETHER_IT_IS_MALFORMED[scenario.get("malformed", False)](db))

    raised = None
    try:
        answer = await lifecycle_stop()
    except BaseException as came_out:  # noqa: BLE001
        answer, raised = None, came_out
    return {
        "answer": answer,
        "raised": raised,
        "databases": databases,
        "asked": [db["channel"].asked for db in databases],
        "still_open": await _which_are_still_open(
            databases[: int(not scenario.get("malformed", False))]
        ),
    }


async def _it_still_answers(connection: object) -> bool:
    """Whether the driver will still do anything on this connection.

    THE REFUSAL IS THE EVIDENCE AND IT IS TURNED INTO ONE, not swallowed. A
    `try` whose `except` does nothing reads as work that failed being reported
    as success; here the failing is the answer, so it comes back as `False`.
    """
    try:
        await connection.execute("SELECT 1")  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        return False
    return True


async def _which_are_still_open(databases: list[Database]) -> list[str]:
    """Asked of the driver: a closed connection refuses work, whatever it says."""
    return [
        db["target"]
        for db in databases
        for connection in [*db["writers"], *db["connections"]]
        if await _it_still_answers(connection)
    ]


def _break_its_crew(db: Database) -> Database:
    """A database whose crew is not a crew, which is what `fn crew_stop` meets
    when something that is not a database reaches the register."""
    return {**db, "crew": "not a crew"}  # type: ignore[typeddict-item, return-value]


def _leave_it_alone(db: Database) -> Database:
    return db


_WHETHER_IT_IS_MALFORMED = {True: _break_its_crew, False: _leave_it_alone}


@then("its local commits went up and its connections were closed")
def _it_finished(scenario: dict) -> None:
    assert scenario["asked"] == [1], (
        f"the channel was asked {scenario['asked']} times, and a signal that "
        f"does not push is a process exiting with its commits still here"
    )
    assert scenario["still_open"] == [], (
        f"{scenario['still_open']} still answer statements after the signal"
    )


@then("both were shut down, though the signal named neither")
def _both_were_reached(scenario: dict) -> None:
    """The signal carries nothing, so the register is the only way either is
    found. Reaching one and not the other is half a shutdown."""
    assert scenario["asked"] == [1, 1], (
        f"the two channels were asked {scenario['asked']} times, and a signal "
        f"that reaches one database leaves the other's commits on the machine"
    )
    assert scenario["still_open"] == [], (
        f"{scenario['still_open']} still answer statements after the signal"
    )


@then("a fault comes back and nothing was raised")
def _a_fault_came_back(scenario: dict) -> None:
    """Raising inside a signal handler tells nobody anything and leaves the rest
    of the databases unreached."""
    assert scenario["raised"] is None, (
        f"the signal raised {scenario['raised']!r} out to the platform, where "
        f"there is no caller to catch it and the process is already leaving"
    )
    answer = scenario["answer"]
    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a shutdown that could not finish answered {answer!r}"
    )
