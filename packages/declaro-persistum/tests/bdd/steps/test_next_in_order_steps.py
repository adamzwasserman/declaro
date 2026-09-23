"""Bind `tests/bdd/features/core/next_in_order.feature`, and nothing else.

WHAT THE FEATURE ASKS. Which write is next, in the order they arrived, and
NOTHING when the crew is holding none.

IT ANSWERS AND IT DOES NOT TAKE, which is the third scenario. A pure function
cannot remove anything, so the drain loop below does what a drainer does: it
asks, takes the answer out itself, and asks again. A version that popped would
pass an order test and silently let two drainers run one write.

THE CREW IS BUILT BY HANDING WRITES TO IT rather than by writing its fields out,
for the same reason every other file does it that way.

WHY THE SIGNATURE IS ASSERTED. The feature says the writes come out in arrival
order whoever is asking. One argument is the only evidence this function can
give for the second half of that, and a second argument would remove it.
"""

from __future__ import annotations

import asyncio
import inspect

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.next_in_order import next_in_order
from declaro_persistum.types import NOTHING, Crew, Statement

scenarios("../features/core/next_in_order.feature")

# Three writes telling themselves apart, compared whole so a wrong answer names
# which one came back.
THREE_WRITES: tuple[Statement, ...] = (
    {"text": "insert into cards (id, title) values (:id, :title)", "parameters": {"id": 1}},
    {"text": "update cards set title = :title where id = :id", "parameters": {"id": 1}},
    {"text": "delete from cards where id = :id", "parameters": {"id": 1}},
)


def _a_crew_holding(writes: tuple[Statement, ...]) -> Crew:
    """A crew holding these writes, handed over in this order."""

    async def _made() -> Crew:
        crew = crew_make()
        for write in writes:
            ticket = crew_deposit(crew, write)
            assert isinstance(ticket, str), f"a write was refused: {ticket!r}"
        return crew

    return asyncio.run(_made())


WHAT_EACH_OPENING_HOLDS = {
    "three writes handed to the crew in a known order": THREE_WRITES,
    "a crew holding nothing": (),
}


@given(
    parsers.re(
        r"^(?P<opening>three writes handed to the crew in a known order"
        r"|a crew holding nothing)$"
    ),
    target_fixture="crew",
)
def _the_crew(opening: str) -> Crew:
    return _a_crew_holding(WHAT_EACH_OPENING_HOLDS[opening])


@then("they come out in that order")
def _in_that_order(crew: Crew) -> None:
    # NOBODY IS NAMED IN THE ASKING. One argument, no default, so two drainers
    # asking the same crew ask the identical question and cannot be answered
    # differently.
    taken = inspect.signature(next_in_order).parameters
    assert list(taken) == ["crew"], f"the answer would depend on more than the crew: {list(taken)}"
    assert [p.default for p in taken.values()] == [inspect.Parameter.empty]

    # The drain loop, with the taking-out done here, because the function does
    # not do it.
    for position, expected in enumerate(THREE_WRITES):
        answered = next_in_order(crew)
        assert crew["writes"][answered] == expected, (
            f"write {position} out of the crew is {crew['writes'].get(answered)}, "
            f"and the write handed over {position} is {expected}"
        )
        crew["arrivals"].pop(0)

    assert next_in_order(crew) is NOTHING, "the crew is empty and still answers with a write"


@then("NOTHING comes back, which is not a failure")
def _nothing_comes_back(crew: Crew) -> None:
    # No raise, no Fault: the call returns, and what it returns is the named
    # absence. There being no work is an ordinary state.
    answered = next_in_order(crew)

    assert answered is NOTHING, f"a crew holding nothing answered {answered!r}"
    assert not crew["arrivals"] and not crew["writes"], f"the crew was changed: {crew}"


@then("asking twice answers the same both times, and the crew still holds three")
def _asking_does_not_take(crew: Crew) -> None:
    held = len(crew["arrivals"])
    first = next_in_order(crew)
    again = next_in_order(crew)

    assert again == first, (
        f"two drainers asked the same crew and were told {first} and {again}, "
        f"so both would run a write the other is already running"
    )
    assert len(crew["arrivals"]) == held == 3, (
        f"asking took the write out: the crew held {held} and now holds "
        f"{len(crew['arrivals'])}. Taking it out belongs to the drainer."
    )
