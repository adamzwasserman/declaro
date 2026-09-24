"""Bind `tests/bdd/features/crew/crew_start.feature`, and nothing else.

WHAT THE FEATURE ASKS. One drainer per writer connection, each given a writer of
its own and running the turn it was handed. No writers starts nobody and is not
a failure. A crew already running is refused rather than doubled.

NOTHING HERE TOUCHES A DATABASE. The call is handed a database and a turn, and
the writers on that database are markers: the door passes each one to a drainer
and never opens anything. What a drainer does with its connection is the turn's
business, and the turn in these scenarios writes down what it was given.

THE TURN IS A STAND-IN AND IT IS THE RIGHT KIND. It is not standing in for a
driver, where a wrong contract has manufactured defects in this package three
times; it is standing in for `orchestrator fn lifecycle_catch_up`, whose whole
contract with this door is that it is a coroutine taking one `Draining`. A real
one would need a live database to answer the one question these scenarios ask,
which is which connection each drainer was handed.

EVERY SCENARIO RUNS INSIDE ONE EVENT LOOP, because a task belongs to the loop
that made it, and the drainers are stopped before the loop ends so that nothing
is left running between scenarios.
"""

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_start import crew_start
from declaro_persistum.types import Crew, Database, Draining

scenarios("../features/crew/crew_start.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# How long a drainer is given to reach its first sleep before the scenario reads
# what it was handed.
LONG_ENOUGH_TO_HAVE_STARTED = 0.05


@pytest.fixture
def scenario() -> dict:
    return {}


def _a_writer(n: int) -> str:
    """A marker where a connection would be, told apart by its own name.

    NOTHING IS OPENED, because the question is which drainer got which, and two
    real connections to one database are indistinguishable by looking at them.
    """
    return f"writer-{n}"


def _a_database(crew: Crew, writers: int) -> Database:
    return Database(
        target="sqlite:///nothing.db",
        engine="sqlite",
        connections=[],
        writers=[_a_writer(n) for n in range(writers)],  # type: ignore[misc]
        channel=None,
        crew=crew,
        shutdown="exit_immediately",
    )


def _a_turn(given_to: list[Draining]):
    """A turn that writes down what it was handed and then sleeps until stopped.

    IT SLEEPS RATHER THAN RETURNING, because a drainer that returned at once
    would leave the crew holding finished tasks and every scenario about what is
    running would read the same whether or not anything had been started.
    """

    async def turn(draining: Draining) -> None:
        given_to.append(draining)
        await draining["stop"].wait()

    return turn


@given(parsers.re(r"^a database opened with (?P<how_many>four|one|no) writers?$"))
def _a_database_with_writers(scenario: dict, how_many: str) -> None:
    scenario["writers"] = {"four": 4, "one": 1, "no": 0}[how_many]


@given("a crew that has already been started")
def _a_crew_already_started(scenario: dict) -> None:
    scenario["start_twice"] = True


@when("the crew is started")
def _the_crew_is_started(scenario: dict) -> None:
    """Start it, read what happened, and stop every drainer before the loop ends."""

    async def _started() -> dict:
        crew = crew_make()
        db = _a_database(crew, scenario["writers"])
        given_to: list[Draining] = []
        turn = _a_turn(given_to)
        first = await crew_start(db, turn)
        await asyncio.sleep(LONG_ENOUGH_TO_HAVE_STARTED)
        answer = {True: lambda: crew_start(db, turn), False: lambda: None}[
            scenario.get("start_twice", False)
        ]()
        answered = await answer if answer is not None else first
        held = list(crew["drainers"])
        crew["stop"].set()
        await asyncio.gather(*held, return_exceptions=True)
        return {"crew": crew, "answer": answered, "given_to": given_to, "held": held}

    scenario.update(asyncio.run(_started()))


@then(parsers.re(r"^(?P<how_many>four|no) drainers? (?:are|is) running$"))
def _how_many_are_running(scenario: dict, how_many: str) -> None:
    wanted = {"four": 4, "no": 0}[how_many]
    assert len(scenario["held"]) == wanted, (
        f"{len(scenario['held'])} drainers were handed to the runtime and the "
        f"database has {scenario['writers']} writers, so there is not one each"
    )


@then("the crew is holding all four")
def _the_crew_is_holding_them(scenario: dict) -> None:
    """A task nothing holds may be collected while it is still running, and
    `fn crew_stop` has nothing to wait for."""
    assert len(scenario["held"]) == scenario["writers"], (
        f"the crew held {len(scenario['held'])} drainers for {scenario['writers']} writers"
    )


@then("no two drainers were given the same connection")
def _each_got_its_own(scenario: dict) -> None:
    """Two drainers on one connection is two writers on one OS thread, which is
    the serialising this crew exists to avoid."""
    handed = [draining["writer"] for draining in scenario["given_to"]]

    assert len(handed) == scenario["writers"], (
        f"{len(handed)} drainers started for {scenario['writers']} writers"
    )
    assert len(set(handed)) == len(handed), (
        f"the drainers were handed {handed}, and two of them share a connection"
    )


@then("the turn runs")
def _the_turn_runs(scenario: dict) -> None:
    """A drainer nothing runs is a crew that holds writes and drains none."""
    assert scenario["given_to"] != [], (
        "the crew was started and the turn it was handed never ran, so every "
        "deposited write waits for a drainer that does nothing"
    )


@then("it comes back a crew rather than a fault")
def _it_is_a_crew(scenario: dict) -> None:
    """A read-only deployment opens no writer, and refusing there turns an
    ordinary configuration into a failed open."""
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) != THE_THREE_FACTS_IN_A_FAULT, (
        f"a database with no writers came back as {answer!r}"
    )
    assert "drainers" in answer, f"what came back is not a crew: {sorted(answer)}"


@then("it is refused, saying the crew is already running")
def _it_is_refused(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"starting a running crew again came back as {answer!r}, and a refusal "
        f"comes back as a Fault in the return"
    )
    assert "running" in answer["what_went_wrong"], (
        f"the refusal does not say the crew is already running: {answer['what_went_wrong']!r}"
    )


@then("no second drainer was started")
def _no_second_drainer(scenario: dict) -> None:
    """A second set running on the same connections with nothing holding them is
    what makes shutdown stop half the crew and report success."""
    assert len(scenario["held"]) == scenario["writers"], (
        f"{len(scenario['held'])} drainers are running over "
        f"{scenario['writers']} writers, so the second start doubled the crew"
    )
    assert len(scenario["given_to"]) == scenario["writers"], (
        f"the turn ran {len(scenario['given_to'])} times for "
        f"{scenario['writers']} writers, so a second drainer was started and "
        f"nothing is holding it"
    )
