"""Bind `tests/bdd/features/crew/crew_stop.feature`, and nothing else.

WHAT THE FEATURE ASKS. The stop is rung once for everybody and then every
drainer is waited for, rather than asked and left. A drainer that raised on its
way out is reported and the rest still finish. A crew nobody started stops
without complaint, and the crew is left holding nobody.

NOTHING HERE TOUCHES A DATABASE. The call is handed a crew. The drainers are
coroutines that wait on the crew's own stop, which is exactly what a real one
does inside `fn crew_wait_for_work`, and what they do when they wake is the only
thing that differs between scenarios.

WAITING IS READ AS THE ORDER OF TWO FACTS, NEVER AS A CLOCK. "It waits rather
than asking and leaving" is checked by a drainer that records when it finished
and a scenario that records when the call came back: if the call came back
first, it did not wait. A scenario that slept and then looked would pass on a
call that returned at once and got lucky.

EVERY SCENARIO RUNS INSIDE ONE EVENT LOOP, because a task belongs to the loop
that made it.
"""

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_stop import crew_stop
from declaro_persistum.types import Crew

scenarios("../features/crew/crew_stop.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# How long the drainer that "takes a moment to notice" spends between waking and
# finishing. Long enough that a call which did not wait comes back first.
LONG_ENOUGH_TO_LOSE_A_RACE = 0.1

WHAT_THE_FAILING_DRAINER_SAID = "this drainer could not put its connection down"


@pytest.fixture
def scenario() -> dict:
    return {}


def _a_drainer(crew: Crew, finished: list[str], name: str, dawdles: bool, fails: bool):
    """One drainer: wait on the stop, take a moment or not, then end or raise."""

    async def drainer() -> None:
        await crew["stop"].wait()
        await asyncio.sleep({True: LONG_ENOUGH_TO_LOSE_A_RACE, False: 0}[dawdles])
        finished.append(name)
        {True: _raise, False: lambda: None}[fails]()

    return drainer


def _raise() -> None:
    raise RuntimeError(WHAT_THE_FAILING_DRAINER_SAID)


# WHAT EACH `Given` PUTS ON THE CREW: how many drainers, whether one dawdles,
# and whether one fails. Four sentences, one function over a table.
_WHAT_EACH_GIVEN_SETS: dict[str, dict[str, object]] = {
    "a crew of four drainers running": {"how_many": 4, "dawdles": False, "fails": False},
    "a crew of one drainer that takes a moment to notice": {
        "how_many": 1,
        "dawdles": True,
        "fails": False,
    },
    "a crew of two drainers, one of which fails as it stops": {
        "how_many": 2,
        "dawdles": False,
        "fails": True,
    },
    "a crew nobody started": {"how_many": 0, "dawdles": False, "fails": False},
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_SETS, key=len, reverse=True)))))
def _what_was_set_up(scenario: dict, sentence: str) -> None:
    scenario.update(_WHAT_EACH_GIVEN_SETS[sentence])


@when("the crew is stopped")
def _the_crew_is_stopped(scenario: dict) -> None:
    """Start the drainers, stop them, and record the order of the two facts."""

    async def _stopped() -> dict:
        crew = crew_make()
        finished: list[str] = []
        # ONLY THE LAST ONE FAILS, so the scenario can say the others still
        # finished. A crew where every drainer failed would pass a check that
        # only asked whether a fault came back.
        crew["drainers"] = [
            asyncio.ensure_future(
                _a_drainer(
                    crew,
                    finished,
                    f"drainer-{n}",
                    dawdles=bool(scenario["dawdles"]),
                    fails=bool(scenario["fails"]) and n == int(scenario["how_many"]) - 1,  # type: ignore[call-overload]
                )()
            )
            for n in range(int(scenario["how_many"]))  # type: ignore[call-overload]
        ]
        started = list(crew["drainers"])
        await asyncio.sleep(0)
        answer = await crew_stop(crew)
        return {
            "crew": crew,
            "answer": answer,
            "finished": finished,
            "started": started,
            "running_when_it_returned": [one for one in started if not one.done()],
        }

    scenario.update(asyncio.run(_stopped()))


@then("every drainer has finished")
def _every_drainer_finished(scenario: dict) -> None:
    assert len(scenario["finished"]) == scenario["how_many"], (
        f"{len(scenario['finished'])} of {scenario['how_many']} drainers "
        f"finished, so the stop did not reach all of them"
    )


@then("the call did not come back until that drainer had finished")
def _it_waited(scenario: dict) -> None:
    """Read as the order of two facts, not off a clock.

    A CALL THAT RANG THE BELL AND RETURNED would come back with the drainer still
    running, and shutdown would go on to push to the primary with a local write
    still in flight behind it.
    """
    assert scenario["running_when_it_returned"] == [], (
        f"{len(scenario['running_when_it_returned'])} drainers were still "
        f"running when the stop came back, so it asked and left"
    )
    assert scenario["finished"] != [], (
        "the drainer never finished at all, so the stop never reached it"
    )


@then("a fault comes back saying one drainer did not stop cleanly")
def _a_fault_names_the_drainer(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a drainer raised on its way out and the stop answered {answer!r}, and "
        f"an operator reading the shutdown needs to know a connection may be open"
    )
    assert WHAT_THE_FAILING_DRAINER_SAID in answer["what_went_wrong"], (
        f"the fault does not carry what the drainer said: {answer['what_went_wrong']!r}"
    )


@then("the other drainer still finished")
def _the_other_still_finished(scenario: dict) -> None:
    """One drainer failing is one connection possibly left open. The rest
    stopping is what makes the shutdown worth finishing."""
    assert len(scenario["finished"]) == scenario["how_many"], (
        f"only {scenario['finished']} finished, so the first failure came back "
        f"while the others were still running, and they were still running when "
        f"the process died"
    )


@then("nothing comes back, and it is not a fault")
def _nothing_comes_back(scenario: dict) -> None:
    """A process that opened a database and wrote nothing still shuts down."""
    answer = scenario["answer"]

    assert answer is None, (
        f"stopping a crew nobody started answered {answer!r}, which makes an "
        f"ordinary shutdown read as a failure"
    )


@then("the crew holds no drainers")
def _the_crew_holds_nobody(scenario: dict) -> None:
    """So a stopped crew can be started again, and nothing holds a finished task."""
    assert scenario["crew"]["drainers"] == [], (
        f"the crew still holds {len(scenario['crew']['drainers'])} drainers "
        f"after every one of them finished"
    )
