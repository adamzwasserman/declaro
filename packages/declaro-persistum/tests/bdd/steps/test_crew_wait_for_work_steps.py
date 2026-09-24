"""Bind `tests/bdd/features/crew/crew_wait_for_work.feature`, and nothing else.

WHAT THE FEATURE ASKS. Three things wake a sleeping drainer and none of them is
a failure: a write arriving, the crew stopping, and the idle limit running out.
Work already waiting means no sleep at all, and the bell is left ready for the
next sleeper.

NOTHING HERE TOUCHES A DATABASE. The call is handed a `Draining`, which carries
a database only so the crew can be reached through it. No statement is run and
no connection is opened, so the writer in that record is a marker rather than a
handle to anything.

EVERY SCENARIO RUNS INSIDE ONE EVENT LOOP, because a bell belongs to the loop
that made it. A crew made on one loop and waited on from another would be
waiting on something nothing is running.

WHAT IS READ IS THAT THE WAIT ENDED AND ROUGHLY WHEN, NEVER HOW. Nothing here
asks which primitive was used or how many tasks were made. A scenario that
watched the machinery would pass while the machinery ran and the drainer slept
through a shutdown.

THE SLEEPING SCENARIOS ARE BOUNDED AND SAY WHY. "It wakes" and "it sleeps" are
claims about time, and the way each fails is by never coming back. So each runs
under a deadline, and the deadline running out is the failure being reported
rather than the whole suite hanging.
"""

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_wait_for_work import crew_wait_for_work
from declaro_persistum.types import Crew, Draining, Statement

scenarios("../features/crew/crew_wait_for_work.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}

# How long a drainer with nothing to do sleeps in the scenarios that ask about
# the idle limit. Short enough that the suite is not held up, long enough that a
# loaded machine does not reach it while something else was supposed to wake it.
A_SHORT_IDLE_LIMIT = 0.2

# What every other scenario sets, so a wait that should have been ended by a
# bell cannot pass by running out instead.
LONGER_THAN_ANY_SCENARIO = 30.0

# How long a wait that should end at once is given before it is called a hang.
LONG_ENOUGH_TO_BE_A_HANG = 5.0


@pytest.fixture
def scenario() -> dict:
    return {}


def _a_drainer_on(crew: Crew, idle: float) -> Draining:
    """What one drainer holds, with a marker where its connection would be.

    THE WRITER IS NEVER TOUCHED. This door takes a `Draining` because that is
    what a drainer holds for its life, and reads the crew out of the database on
    it. A real connection here would be a connection nothing opened a database
    for.
    """
    db = {
        "target": "sqlite:///nothing.db",
        "engine": "sqlite",
        "connections": [],
        "writers": [],
        "channel": None,
        "crew": crew,
        "shutdown": "exit_immediately",
    }
    return Draining(db=db, writer=None, stop=crew["stop"], idle=idle)  # type: ignore[typeddict-item]


# WHAT EACH `Given` SETS UP: the crew a drainer waits on, and how long it may
# sleep. Every row is one line, so the four sentences are a table rather than
# four step functions.
_WHAT_EACH_GIVEN_SETS: dict[str, float] = {
    "a drainer waiting on an empty crew": LONGER_THAN_ANY_SCENARIO,
    "a drainer waiting on an empty crew with a short idle limit": A_SHORT_IDLE_LIMIT,
    "a crew already holding a write": LONGER_THAN_ANY_SCENARIO,
}

# Whether that sentence puts a write on the crew before anybody waits.
_WHETHER_A_WRITE_IS_ALREADY_THERE: dict[str, bool] = {
    "a drainer waiting on an empty crew": False,
    "a drainer waiting on an empty crew with a short idle limit": False,
    "a crew already holding a write": True,
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    """Four sentences, one function over two tables."""
    scenario["idle"] = _WHAT_EACH_GIVEN_SETS[sentence]
    scenario["deposit_first"] = _WHETHER_A_WRITE_IS_ALREADY_THERE[sentence]


async def _waiting(scenario: dict, crew: Crew) -> asyncio.Task:
    """A drainer started and given a turn to reach the sleep.

    THE TURN IS WHAT MAKES THIS A TEST OF WAKING. Without it the task has not run
    when the bell is rung, so it would find the bell already set and a door that
    never slept would pass.
    """
    waiting = asyncio.ensure_future(crew_wait_for_work(_a_drainer_on(crew, scenario["idle"])))
    await asyncio.sleep(0)
    return waiting


def _run(scenario: dict, what) -> None:
    """One event loop for the whole scenario, because a bell belongs to a loop."""

    async def _done() -> dict:
        crew = crew_make()
        return await what(crew)

    scenario.update(asyncio.run(_done()))


# --- what each scenario does -------------------------------------------------


@when("a caller deposits a write")
def _a_caller_deposits(scenario: dict) -> None:
    async def _woke(crew: Crew) -> dict:
        waiting = await _waiting(scenario, crew)
        crew_deposit(crew, A_WRITE)
        await asyncio.wait_for(waiting, LONG_ENOUGH_TO_BE_A_HANG)
        return {"crew": crew, "answer": waiting.result()}

    _run(scenario, _woke)


@when("the crew is told to stop")
def _the_crew_is_told_to_stop(scenario: dict) -> None:
    async def _woke(crew: Crew) -> dict:
        waiting = await _waiting(scenario, crew)
        crew["stop"].set()
        await asyncio.wait_for(waiting, LONG_ENOUGH_TO_BE_A_HANG)
        return {"crew": crew, "answer": waiting.result()}

    _run(scenario, _woke)


@when("a drainer waits for work")
def _a_drainer_waits(scenario: dict) -> None:
    """The write is already there, so this should not sleep at all."""

    async def _ran(crew: Crew) -> dict:
        crew_deposit(crew, A_WRITE)
        started = asyncio.get_running_loop().time()
        answer = await asyncio.wait_for(
            crew_wait_for_work(_a_drainer_on(crew, scenario["idle"])),
            LONG_ENOUGH_TO_BE_A_HANG,
        )
        return {
            "crew": crew,
            "answer": answer,
            "took": asyncio.get_running_loop().time() - started,
        }

    _run(scenario, _ran)


@when("that write is taken and another drainer waits")
def _the_write_is_taken_and_another_waits(scenario: dict) -> None:
    """The crew drained, and a second drainer arriving to an empty one.

    A BELL LEFT SET BY THE FIRST WRITE WOULD WAKE THIS ONE AT ONCE, which is the
    sleeping being decorative: every later drainer would spin through its turn
    without waiting for anything.
    """

    async def _slept(crew: Crew) -> dict:
        crew_deposit(crew, A_WRITE)
        await crew_wait_for_work(_a_drainer_on(crew, scenario["idle"]))
        crew["arrivals"].clear()
        crew["writes"].clear()
        second = asyncio.ensure_future(
            crew_wait_for_work(_a_drainer_on(crew, LONGER_THAN_ANY_SCENARIO))
        )
        await asyncio.sleep(0.1)
        still_asleep = not second.done()
        second.cancel()
        return {"crew": crew, "still_asleep": still_asleep}

    _run(scenario, _slept)


# --- what must be true afterwards --------------------------------------------


@then("it wakes")
def _it_wakes(scenario: dict) -> None:
    """The deadline above is half of this: a door that slept through the bell
    fails there, naming the timeout."""
    assert scenario["answer"] is None, (
        f"the wait came back with {scenario['answer']!r}, and waking is answered "
        f"with nothing however it happened"
    )


@then("it comes back without sleeping")
def _it_comes_back_without_sleeping(scenario: dict) -> None:
    """A write already on the crew is picked up on this turn, not the next.

    A DOOR THAT SLEPT FIRST WOULD HOLD THAT WRITE until the next deposit or the
    idle limit, and the caller would wait out whichever came sooner. The idle
    limit in this scenario is longer than the whole suite, so any sleeping at all
    shows up here as the deadline running out.
    """
    assert scenario["took"] < A_SHORT_IDLE_LIMIT, (
        f"the wait took {scenario['took']:.3f}s with a write already on the crew, "
        f"so it slept before looking"
    )


@then("it wakes on its own when the limit runs out")
def _it_wakes_on_the_limit(scenario: dict) -> None:
    async def _waited() -> object:
        crew = crew_make()
        return await asyncio.wait_for(
            crew_wait_for_work(_a_drainer_on(crew, A_SHORT_IDLE_LIMIT)),
            LONG_ENOUGH_TO_BE_A_HANG,
        )

    scenario["answer"] = asyncio.run(_waited())
    assert scenario["answer"] is None, (
        f"waking on the idle limit answered {scenario['answer']!r}, and waking is "
        f"answered with nothing"
    )


@then("what comes back when the limit runs out is not a fault")
def _the_limit_is_not_a_fault(scenario: dict) -> None:
    """An idle database would otherwise look like a broken one on every turn."""
    _it_wakes_on_the_limit(scenario)
    assert not isinstance(scenario["answer"], dict), (
        f"a drainer that woke with nothing to do came back as {scenario['answer']!r}, "
        f"and a free turn is what `orchestrator fn lifecycle_catch_up` spends syncing"
    )


@then("the second drainer sleeps rather than waking at once")
def _the_second_drainer_sleeps(scenario: dict) -> None:
    assert scenario["still_asleep"], (
        "the second drainer woke at once on an empty crew, so the bell was left "
        "set by a write another drainer already took and every later sleeper "
        "spins through its turn"
    )
