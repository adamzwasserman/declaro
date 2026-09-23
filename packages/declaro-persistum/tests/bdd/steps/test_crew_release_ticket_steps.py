"""Bind `tests/bdd/features/crew/crew_release_ticket.feature`, and nothing else.

WHAT THE FEATURE ASKS. A caller parked on a ticket gets the outcome the drainer
posts for it. The ticket comes off the crew when it does. Two callers in flight
together each get their own, and a ticket the crew does not hold is refused at
once rather than waited on.

NOTHING HERE TOUCHES A DATABASE. The call is handed a crew and a ticket. The
write is over by the time anything collects, and what comes back is the record
of what it did.

EVERY SCENARIO RUNS INSIDE ONE EVENT LOOP, because a waiter belongs to the loop
that made it. A deposit on one loop and a collection on another would be waiting
on something nothing is running, and the wait would never end.

THE CALLER PARKS BEFORE THE DRAINER POSTS, WHICH IS THE ORDER THAT MATTERS. A
scenario that posted first and collected afterwards would pass on a function
that never waits at all, because the outcome would already be there. So the
collection is started as its own task, the loop is given a turn to let it park,
and only then does the drainer post.

THE FOURTH SCENARIO IS BOUNDED AND SAYS WHY. "Refused at once rather than
waiting" is a claim about time, and the way it fails is by never coming back. So
it runs under a deadline, and the deadline running out is the failure being
reported rather than the whole suite hanging.
"""

from __future__ import annotations

import asyncio

import pytest
from pytest_bdd import scenarios, then, when

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_post_receipt import crew_post_receipt
from declaro_persistum.crew_release_ticket import crew_release_ticket
from declaro_persistum.receipt_for import receipt_for
from declaro_persistum.types import Crew, Fault, Receipt, Statement, Ticket

scenarios("../features/crew/crew_release_ticket.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}
ANOTHER_WRITE: Statement = {
    "text": "DELETE FROM tasks WHERE id = :id",
    "parameters": {"id": 7},
}

ROWS_IT_TOUCHED = 3
ATTEMPTS_IT_TOOK = 1

# How long a collection that should answer at once is given before the scenario
# calls it a hang. Generous enough that a loaded machine does not fail it, short
# enough that a wait nothing ends is reported rather than sat through.
LONG_ENOUGH_TO_BE_A_HANG = 2.0

A_TICKET_NOBODY_DEPOSITED: Ticket = "0123456789abcdef0123456789abcdef"

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


@pytest.fixture
def scenario() -> dict:
    return {}


def _deposited(crew: Crew, statement: Statement) -> Ticket:
    """One write handed over, which is the only way a waiter comes to exist."""
    ticket = crew_deposit(crew, statement)
    assert isinstance(ticket, str), f"the deposit was refused: {ticket}"
    return ticket


def _what_the_drainer_says(ticket: Ticket, failure: Fault | None) -> Receipt:
    return receipt_for(ticket, ROWS_IT_TOUCHED, failure, ATTEMPTS_IT_TOOK)


A_WRITE_THE_ENGINE_REFUSED = Fault(
    kind="refused",
    what_went_wrong="The engine refused the statement: no such table: tasks",
    what_to_do_instead="Declare the table the write names, or write to one that exists.",
)


async def _parked(crew: Crew, ticket: Ticket) -> asyncio.Task:
    """A caller collecting, started and given a turn to park on its waiter.

    THE TURN IS WHAT MAKES THIS A TEST OF WAITING. Without it the task has not
    run at all when the drainer posts, so the outcome is already there when the
    collection first looks, and a function that never waits would pass.
    """
    collecting = asyncio.ensure_future(crew_release_ticket(crew, ticket))
    await asyncio.sleep(0)
    return collecting


@when("a caller waits on a ticket and the drainer posts its outcome")
def _a_caller_waits_and_the_drainer_posts(scenario: dict) -> None:
    async def _collected() -> dict:
        crew = crew_make()
        ticket = _deposited(crew, A_WRITE)
        collecting = await _parked(crew, ticket)
        posted = _what_the_drainer_says(ticket, None)
        crew_post_receipt(crew, ticket, posted)
        return {
            "crew": crew,
            "ticket": ticket,
            "posted": posted,
            "answer": await asyncio.wait_for(collecting, LONG_ENOUGH_TO_BE_A_HANG),
        }

    scenario.update(asyncio.run(_collected()))


@when("an outcome has been handed back")
def _an_outcome_has_been_handed_back(scenario: dict) -> None:
    _a_caller_waits_and_the_drainer_posts(scenario)


@when("two callers wait on two tickets and one write fails")
def _two_callers_and_one_failure(scenario: dict) -> None:
    """Two writes in flight together, and the engine refuses the second.

    BOTH ARE PARKED BEFORE EITHER OUTCOME IS POSTED. A scenario that ran them
    one after the other would say nothing about two callers being told apart.
    """

    async def _collected() -> dict:
        crew = crew_make()
        first = _deposited(crew, A_WRITE)
        second = _deposited(crew, ANOTHER_WRITE)
        collecting = {
            "first": await _parked(crew, first),
            "second": await _parked(crew, second),
        }
        crew_post_receipt(crew, first, _what_the_drainer_says(first, None))
        crew_post_receipt(crew, second, _what_the_drainer_says(second, A_WRITE_THE_ENGINE_REFUSED))
        return {
            "crew": crew,
            "tickets": {"first": first, "second": second},
            "answers": {
                name: await asyncio.wait_for(task, LONG_ENOUGH_TO_BE_A_HANG)
                for name, task in collecting.items()
            },
        }

    scenario.update(asyncio.run(_collected()))


@when("a caller collects on a ticket that was never deposited")
def _a_caller_collects_an_unknown_ticket(scenario: dict) -> None:
    async def _collected() -> dict:
        crew = crew_make()
        return {
            "crew": crew,
            "ticket": A_TICKET_NOBODY_DEPOSITED,
            "answer": await asyncio.wait_for(
                crew_release_ticket(crew, A_TICKET_NOBODY_DEPOSITED),
                LONG_ENOUGH_TO_BE_A_HANG,
            ),
        }

    scenario.update(asyncio.run(_collected()))


@then("the outcome comes back to that caller")
def _the_outcome_comes_back(scenario: dict) -> None:
    assert scenario["answer"] == scenario["posted"], (
        f"the caller was given {scenario['answer']}, and the drainer posted {scenario['posted']}"
    )


@then("the crew holds nothing for that ticket")
def _the_crew_holds_nothing(scenario: dict) -> None:
    """A collected ticket is spent, and a crew that kept them grows forever."""
    crew = scenario["crew"]
    ticket = scenario["ticket"]
    held = {
        "arrivals": [one for one in crew["arrivals"] if one == ticket],
        "writes": [one for one in crew["writes"] if one == ticket],
        "waiting": [one for one in crew["waiting"] if one == ticket],
    }

    assert held == {"arrivals": [], "writes": [], "waiting": []}, (
        f"the crew still holds {held} for a ticket whose outcome the caller has"
    )


@then("only that ticket's caller is told")
def _only_that_tickets_caller_is_told(scenario: dict) -> None:
    """Two callers, two outcomes, and neither gets the other's.

    THIS IS WHAT THE TICKET IS FOR. Two callers sharing one would each collect
    whichever outcome arrived first, so a write that failed would be reported to
    somebody whose write landed, and the caller whose write failed would be told
    it succeeded.
    """
    first, second = scenario["answers"]["first"], scenario["answers"]["second"]

    assert first["ok"] is True, f"the caller whose write landed was told it failed: {first}"
    assert second["ok"] is False, (
        f"the caller whose write the engine refused was told it landed: {second}"
    )
    assert second["failure"] == A_WRITE_THE_ENGINE_REFUSED, (
        f"the failing caller was given {second['failure']}, and the engine said "
        f"{A_WRITE_THE_ENGINE_REFUSED}"
    )
    assert (first["id"], second["id"]) == (
        scenario["tickets"]["first"],
        scenario["tickets"]["second"],
    ), f"each caller's receipt should name its own ticket: {first} and {second}"


@then("it is refused at once rather than waiting")
def _it_is_refused_at_once(scenario: dict) -> None:
    """A ticket nobody deposited has no outcome coming, so waiting is the wrong answer.

    THE DEADLINE ABOVE IS HALF THIS ASSERTION. A function that waits fails there,
    naming the timeout; this half says the answer that came back is a refusal
    rather than a receipt somebody invented.
    """
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"collecting a ticket the crew does not hold gave back {answer!r}, and a "
        f"refusal comes back as a Fault in the return"
    )
    assert scenario["ticket"] in answer["what_went_wrong"], (
        f"the refusal does not name the ticket that was asked for, so a caller "
        f"holding several cannot tell which: {answer['what_went_wrong']!r}"
    )
