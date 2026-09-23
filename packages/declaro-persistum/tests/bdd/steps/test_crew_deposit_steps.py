"""Bind `tests/bdd/features/crew/crew_deposit.feature`, and nothing else.

WHAT THE FEATURE ASKS. Handing a write to the crew gives back a ticket at once
and runs nothing. A write with no statement is refused at the door. One write
reaches the engine once. Two callers get two tickets and the writes keep their
order.

NOTHING HERE TOUCHES A DATABASE, AND THE SIGNATURE IS THE EVIDENCE. The call is
handed a crew and a statement. A connection is not among its arguments, so there
is nowhere for the write to have run before the ticket came back.

THE STAND-IN ENGINE IS A LIST. The third scenario asks how many times the engine
was asked, so the drain below hands each write to a function that writes down
what it was asked and runs nothing. A real engine would answer the same question
and need a live database to do it, which would make a fact about an arrival
order depend on a connection.

WHY THE FOURTH SCENARIO NEEDS TWO CALLERS. One deposit cannot show that two
callers get different tickets, and it cannot show that two writes keep their
order. Both failures lose a caller's write and neither is visible from one
deposit: two callers holding one ticket collect each other's outcome, and two
writes drained back to front land in the database the wrong way round.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.next_in_order import next_in_order
from declaro_persistum.types import NOTHING, Crew, Statement
from declaro_persistum.writer_waiting import writer_waiting

scenarios("../features/crew/crew_deposit.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}
ANOTHER_WRITE: Statement = {
    "text": "DELETE FROM tasks WHERE id = :id",
    "parameters": {"id": 7},
}
NOTHING_TO_RUN: Statement = {"text": "", "parameters": {}}

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _a_stand_in_engine(asked: list[Statement]) -> Callable[[Statement], int]:
    """An engine that writes down what it was asked to run and runs nothing."""

    def run(statement: Statement) -> int:
        asked.append(statement)
        return 1

    return run


def _the_crew_drains(crew: Crew, engine: Callable[[Statement], int]) -> None:
    """One pass: every write waiting, one at a time, in arrival order.

    A drainer asks whose write is next, runs the statement it finds under that
    ticket, and takes the ticket out. The taking is here because
    `fn next_in_order` is pure and cannot remove anything.
    """
    while (ticket := next_in_order(crew)) is not NOTHING:
        engine(crew["writes"][ticket])
        crew["arrivals"].remove(ticket)


# ONE FUNCTION OVER A TABLE. The four openings differ in what the caller hands
# over and in nothing else.
WHAT_THE_CALLER_HANDS_OVER: dict[str, tuple[Statement, ...]] = {
    "a caller deposits a write": (A_WRITE,),
    "a caller deposits a write with nothing to run": (NOTHING_TO_RUN,),
    "a caller deposits one write and the crew drains once": (A_WRITE,),
    "two callers each deposit a write": (A_WRITE, ANOTHER_WRITE),
}

# Which openings drain afterwards, so the third scenario can count what the
# engine was asked without every other scenario draining too.
DRAINS_AFTERWARDS = frozenset({"a caller deposits one write and the crew drains once"})


@when(parsers.re(r"^(?P<opening>(?:a|two) caller.+)$"), target_fixture="scene")
def _handed_over(opening: str) -> dict[str, object]:
    async def _done() -> dict[str, object]:
        crew = crew_make()
        tickets = [crew_deposit(crew, write) for write in WHAT_THE_CALLER_HANDS_OVER[opening]]
        asked: list[Statement] = []
        [
            _the_crew_drains(crew, _a_stand_in_engine(asked))
            for _ in [opening]
            if opening in DRAINS_AFTERWARDS
        ]
        return {"crew": crew, "tickets": tickets, "asked": asked}

    return asyncio.run(_done())


@then("it is handed a ticket without waiting for the write")
def _a_ticket_came_back_at_once(scene: dict) -> None:
    ticket = scene["tickets"][0]
    crew = scene["crew"]

    assert isinstance(ticket, str) and ticket, (
        f"the caller was handed {ticket!r}, which names no write"
    )

    # NOTHING WAS WAITED FOR, AND THE SIGNATURE IS WHY. The call is handed a
    # crew and a write. A connection is not among its arguments, so there is
    # nowhere for the write to have run before the ticket came back.
    assert list(inspect.signature(crew_deposit).parameters) == ["crew", "statement"], (
        "crew_deposit takes the crew and the write; anything else here is a "
        "connection it could have run the write on before answering"
    )
    assert not inspect.iscoroutinefunction(crew_deposit), (
        "the deposit is a coroutine, so a caller has to await it to get its "
        "ticket, which is the wait this function exists to remove"
    )

    # And the write is still sitting there, so the ticket came back ahead of it.
    assert crew["writes"][next_in_order(crew)] == A_WRITE, (
        f"the crew's next write is {crew['writes'].get(next_in_order(crew))!r}"
    )


@then("it is refused before it takes a ticket")
def _refused(scene: dict) -> None:
    answer = scene["tickets"][0]

    assert not isinstance(answer, str), (
        f"a write with nothing to run was given the ticket {answer!r}, so a "
        f"caller is now waiting on an outcome the crew can never produce"
    )
    assert set(answer) == THE_THREE_FACTS_IN_A_FAULT, f"the refusal came back as {answer!r}"
    assert answer["kind"] == "refused", (
        f"a write with nothing in it is refused, not {answer['kind']!r}: a "
        f"caller retrying a conflict would retry this forever"
    )
    assert answer["what_to_do_instead"], "the refusal tells the caller nothing to do instead"


@then("the crew is given nothing to do")
def _the_crew_is_empty(scene: dict) -> None:
    crew = scene["crew"]

    assert next_in_order(crew) is NOTHING, (
        f"the crew has {next_in_order(crew)!r} to run, from a write that was refused"
    )
    assert not writer_waiting(crew), (
        "the crew says somebody is mid-flight, so every sync pass is dropped "
        "for a caller who was refused and went home"
    )
    assert not crew["waiting"], f"a refused write left a caller parked: {crew['waiting']}"


@then("the engine was asked to run it once")
def _asked_once(scene: dict) -> None:
    assert scene["asked"] == [A_WRITE], (
        f"the engine was asked to run {scene['asked']!r}. One deposit is one "
        f"write: twice is the caller's row inserted twice, and never is a caller "
        f"waiting on a ticket for a write nobody can find."
    )


@then("each caller is given a ticket of its own")
def _two_tickets(scene: dict) -> None:
    first, second = scene["tickets"]

    assert isinstance(first, str) and isinstance(second, str), (
        f"one of the two callers was refused: {first!r} and {second!r}"
    )
    assert first != second, (
        f"both callers hold the ticket {first!r}, so each collects on the "
        f"other's write and one of the two outcomes is unreachable"
    )
    assert scene["crew"]["arrivals"] == [first, second], (
        f"the crew holds {scene['crew']['arrivals']!r}, so a ticket a caller "
        f"holds is not one the crew knows anybody is waiting on"
    )


@then("the crew is handed the two writes in the order they arrived")
def _in_order(scene: dict) -> None:
    crew = scene["crew"]
    first, second = scene["tickets"]

    # THE ORDER IS A LIST OF TICKETS AND THE WRITES ARE LOOKED UP BY THEM, which
    # is what lets a write leave the middle without the rest losing their owners.
    in_order = [crew["writes"][ticket] for ticket in crew["arrivals"]]
    assert in_order == [A_WRITE, ANOTHER_WRITE], (
        f"the arrival order is {in_order!r}, and a write that drains out of "
        f"order lands in the database behind one that was written later"
    )

    handed: list[Statement] = []
    _the_crew_drains(crew, _a_stand_in_engine(handed))
    assert handed == [A_WRITE, ANOTHER_WRITE], f"the crew was handed {handed!r}"

    # EACH TICKET NAMES ITS OWN WRITE, and that is the join. This survives the
    # drain above, which empties the arrival order and leaves the writes behind.
    assert crew["writes"][first] == A_WRITE, (
        f"the first caller's ticket names {crew['writes'][first]!r}, so its "
        "outcome would be posted against somebody else's write"
    )
    assert crew["writes"][second] == ANOTHER_WRITE, (
        f"the second caller's ticket names {crew['writes'][second]!r}"
    )


# --- the bell -----------------------------------------------------------------


@given("a worker asleep on a crew holding nothing", target_fixture="sleeper")
def _a_worker_asleep() -> dict[str, object]:
    """A crew nobody has used, and the bell a worker would sleep on.

    The sleeping and the deposit have to happen on one event loop, so the work
    is done in the Then below and this only settles what it starts from.
    """
    return {"crew": crew_make()}


@then("the worker wakes")
def _the_worker_wakes(sleeper: dict) -> None:
    """Sleep on the bell, hand a write over, and see whether the sleeper woke.

    Bounded, because a deposit that never rings hangs the suite rather than
    failing it, and a hang says nothing about which write was lost.
    """
    crew = sleeper["crew"]

    async def _woke() -> bool:
        asleep = asyncio.ensure_future(crew["arrived"].wait())
        await asyncio.sleep(0)
        assert not asleep.done(), "the bell was already rung before any deposit"
        crew_deposit(crew, A_WRITE)
        await asyncio.wait_for(asleep, 5.0)
        return asleep.done()

    assert asyncio.run(_woke()), (
        "a worker slept on the bell, a caller handed over a write, and the "
        "worker did not wake. Every deposit would sit until the idle limit ran "
        "out, and a worker that keeps looking instead can be starved by a busy "
        "loop while the caller waits with no time limit at all."
    )
