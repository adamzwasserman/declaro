"""Bind `tests/bdd/features/core/next_in_order.feature`, and nothing else.

WHAT THE FUNCTION ANSWERS. `next_in_order(room)` is handed a room and says which
write is next. `docs/architecture.md` line 140: a room holds the arrival order
and the callers waiting on tickets. Line 142: a crew is N drainers over one
room, and each takes one write at a time in arrival order. So the whole of this
function is the order, and the two scenarios ask for the two answers it has: the
first write deposited, and NOTHING when none were.

IT ANSWERS, IT DOES NOT TAKE. `docs/design/persistum.hd` line 779 declares it a
pure `fn` with no side effect, and a pure function cannot remove anything from a
room. So the drain loop below does what the boundary does: it asks, it takes the
answer out itself, and it asks again. Each Then asserts that the room still
holds what it held before the call, because a function that popped the list
would pass an order test and silently make two drainers race.

WHY THE SIGNATURE IS ASSERTED. The feature says the writes come out in the order
they arrived, whoever is draining. `next_in_order` is handed the room and
nothing else, so there is no drainer for the answer to depend on. That is the
only evidence this function can give for the second half of the claim, and a
second parameter would remove it.
"""

from __future__ import annotations

import inspect

from pytest_bdd import given, scenarios, then

from declaro_persistum.next_in_order import next_in_order
from declaro_persistum.types import NOTHING, Room, Statement

scenarios("../features/core/next_in_order.feature")


# Three writes a caller could have deposited, each one telling itself apart from
# the other two. They are compared whole, so a function answering with the wrong
# one names which one in the failure.
THREE_WRITES: tuple[Statement, ...] = (
    {
        "text": "insert into cards (id, title) values (:id, :title)",
        "parameters": {"id": 1, "title": "first"},
    },
    {
        "text": "update cards set title = :title where id = :id",
        "parameters": {"id": 1, "title": "second"},
    },
    {
        "text": "delete from cards where id = :id",
        "parameters": {"id": 1},
    },
)


def _a_room(arrivals: tuple[Statement, ...]) -> Room:
    """A room holding these arrivals and nobody waiting on a ticket."""
    return {"arrivals": list(arrivals), "waiting": []}


@given("three writes deposited in a known order")
def _three_writes_deposited_in_a_known_order(scenario: dict[str, object]) -> None:
    scenario["room"] = _a_room(THREE_WRITES)


@given("an empty room")
def _an_empty_room(scenario: dict[str, object]) -> None:
    scenario["room"] = _a_room(())


@then("they come out in that order")
def _they_come_out_in_that_order(scenario: dict[str, object]) -> None:
    room: Room = scenario["room"]  # type: ignore[assignment]

    # NOBODY IS NAMED IN THE ASKING. One parameter, no default, so two drainers
    # asking the same room ask the identical question and cannot be answered
    # differently.
    parameters = inspect.signature(next_in_order).parameters
    assert list(parameters) == ["room"], (
        f"the answer would depend on more than the room: {list(parameters)}"
    )
    assert [p.default for p in parameters.values()] == [inspect.Parameter.empty]

    # The drain loop, with the taking-out done here, because the function does
    # not do it. Each turn asks twice first: a second drainer arriving before
    # the first has taken its write must be given the same write.
    for position, expected in enumerate(THREE_WRITES):
        held = len(room["arrivals"])
        answer = next_in_order(room)
        again = next_in_order(room)

        assert answer == expected, (
            f"write {position} out of the room is {answer}, and the write "
            f"deposited {position} is {expected}"
        )
        assert again == answer, (
            f"two drainers asked the same room and were told {answer} and {again}"
        )
        assert len(room["arrivals"]) == held, (
            f"asking took the write out: the room held {held} arrivals and now "
            f"holds {len(room['arrivals'])}. Taking it out belongs to the drainer."
        )

        room["arrivals"].pop(0)

    assert next_in_order(room) is NOTHING, "the room is drained and still answers with a write"


@then("NOTHING comes back, which is not a failure")
def _nothing_comes_back(scenario: dict[str, object]) -> None:
    room: Room = scenario["room"]  # type: ignore[assignment]

    # No `pytest.raises`, no Fault: the call returns, and what it returns is the
    # named absence. `NOTHING` is `None`, per the declaration.
    answer = next_in_order(room)

    assert answer is NOTHING, f"an empty room answered with {answer!r}"
    assert room == {"arrivals": [], "waiting": []}, f"the empty room was changed: {room}"
