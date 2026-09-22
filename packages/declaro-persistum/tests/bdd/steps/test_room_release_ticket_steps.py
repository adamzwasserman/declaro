"""Bind `tests/bdd/features/room/room_release_ticket.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `boundary_out fn room_release_ticket` is the other
end of a deposit. A caller deposited a write, was handed a ticket and went away
free. When it comes back for the outcome, this is where it waits, and this is
what takes its ticket out of the room afterwards.

THE WAIT IS THE WHOLE FUNCTION, WHICH IS WHY EVERY SCENARIO RUNS A DRAINER
BESIDE IT. A caller that waits and a drainer that posts run on one event loop.
If the wait did not yield, the drainer would never get a turn and the outcome
would never arrive, so a test that posted the outcome first and then waited
would pass against a function that cannot work. Each scenario below starts the
caller first and posts afterwards, which is the order a real caller meets.

NOTHING IS POSTED BY HAND. `boundary_out fn room_post_receipt` is what a drainer
uses, and reaching into `room["receipts"]` here would let this file agree with a
room shape nothing else has. Both halves of the room were written this week and
hand-built rooms in eight files went stale in a day.

WHY THE THIRD SCENARIO NEEDS TWO CALLERS. One caller cannot show that an outcome
goes to the right one. Two callers waiting, one write failing, is the only shape
where the wrong answer is visible: a function that handed out whichever receipt
arrived first would pass every single-caller test and tell the wrong caller its
write had failed.

EVERY WAIT IS BOUNDED. A release that never returns hangs the suite rather than
failing it, so each scenario runs under a timeout and a wait that does not end
is reported as the defect it is.
"""

from __future__ import annotations

import asyncio
from typing import cast

from pytest_bdd import scenarios, then, when

from declaro_persistum.room_deposit import room_deposit
from declaro_persistum.room_make import room_make
from declaro_persistum.room_post_receipt import room_post_receipt
from declaro_persistum.room_release_ticket import room_release_ticket
from declaro_persistum.types import Fault, Receipt, Room, Statement, Ticket

scenarios("../features/room/room_release_ticket.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}

ANOTHER_WRITE: Statement = {
    "text": "DELETE FROM tasks WHERE id = :id",
    "parameters": {"id": 7},
}

# HOW LONG A SCENARIO WILL WAIT BEFORE CALLING IT A HANG. Everything here runs
# in one process with no I/O in it, so a wait this long is a wait that is never
# going to end.
LONG_ENOUGH_TO_BE_A_HANG = 5.0

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

WHAT_A_LANDED_WRITE_LOOKS_LIKE: Receipt = {
    "id": "",
    "ok": True,
    "attempts": 1,
    "failure": None,
}

THE_ENGINE_STAYED_BUSY: Fault = {
    "kind": "unavailable",
    "what_went_wrong": "The engine stayed busy for the whole retry budget.",
    "what_to_do_instead": "Deposit the write again when the database is quieter.",
}

WHAT_A_FAILED_WRITE_LOOKS_LIKE: Receipt = {
    "id": "",
    "ok": False,
    "attempts": 4,
    "failure": THE_ENGINE_STAYED_BUSY,
}


def _a_room_holding(*writes: Statement) -> tuple[Room, list[Ticket]]:
    """A room with these writes waiting, and the tickets their callers hold."""
    room = room_make()
    tickets = []
    for write in writes:
        ticket = room_deposit(room, write)
        assert isinstance(ticket, str), f"the deposit was refused: {ticket!r}"
        tickets.append(ticket)
    return room, tickets


async def _the_caller_waits_while_the_drainer_posts(
    room: Room, posting: dict[Ticket, Receipt]
) -> dict[Ticket, Receipt | Fault]:
    """Start every caller waiting, then post each outcome, and collect them all.

    THE CALLERS GO FIRST AND THE POSTING FOLLOWS. `asyncio.sleep(0)` hands the
    loop over once, which is what gets each waiting caller as far as its first
    await. Posting before that would answer a caller that never had to wait, and
    the wait is what this function is.

    AND THE POSTING RUNS BACKWARDS, WHICH IS THE ONLY REASON THE THIRD SCENARIO
    SAYS ANYTHING. Posting in the order the callers queued makes "give this
    caller its own outcome" and "give this caller whichever outcome came in
    first" the same answer. Measured 2026-09-22: a release rewritten to hand out
    the first receipt in the room passed all four scenarios. Posting in the
    opposite order is what tells the two apart.
    """
    waiting = {
        ticket: asyncio.ensure_future(room_release_ticket(room, ticket)) for ticket in posting
    }
    await asyncio.sleep(0)
    for ticket, receipt in reversed(list(posting.items())):
        room_post_receipt(room, ticket, receipt)
    return {
        ticket: await asyncio.wait_for(task, LONG_ENOUGH_TO_BE_A_HANG)
        for ticket, task in waiting.items()
    }


# --- one caller, one outcome -----------------------------------------------


@when("a caller waits on a ticket and the drainer posts its outcome")
def _one_caller_waits(scenario: dict[str, object]) -> None:
    room, (ticket,) = _a_room_holding(A_WRITE)
    posted = {**WHAT_A_LANDED_WRITE_LOOKS_LIKE, "id": ticket}
    scenario["room"] = room
    scenario["ticket"] = ticket
    scenario["posted"] = posted
    scenario["given"] = asyncio.run(
        _the_caller_waits_while_the_drainer_posts(room, {ticket: posted})
    )[ticket]


@then("the outcome comes back to that caller")
def _the_caller_was_given_it(scenario: dict[str, object]) -> None:
    given = scenario["given"]

    assert given == scenario["posted"], (
        f"the caller was given {given!r} and the drainer posted "
        f"{scenario['posted']!r}. A caller that waited through a write and was "
        f"handed something else has no way to find out what its write did."
    )


# --- the ticket is spent ---------------------------------------------------


@when("an outcome has been handed back")
def _an_outcome_was_handed_back(scenario: dict[str, object]) -> None:
    room, (ticket,) = _a_room_holding(A_WRITE)
    asyncio.run(
        _the_caller_waits_while_the_drainer_posts(
            room, {ticket: {**WHAT_A_LANDED_WRITE_LOOKS_LIKE, "id": ticket}}
        )
    )
    scenario["room"] = room
    scenario["ticket"] = ticket


@then("the room holds nothing for that ticket")
def _the_room_is_empty_of_it(scenario: dict[str, object]) -> None:
    room = cast(Room, scenario["room"])
    ticket = cast(Ticket, scenario["ticket"])

    # ALL THREE, BECAUSE THE TICKET IS A KEY IN ALL THREE. A release that took
    # the arrival out and left the write and the receipt behind would look
    # collected to a drainer and would grow the room for the life of the
    # process, one spent ticket at a time.
    assert ticket not in room["arrivals"], (
        f"the ticket is still in the arrival order, so a drainer will run "
        f"{room['writes'].get(ticket)!r} a second time"
    )
    assert ticket not in room["writes"], f"the room still holds the write under {ticket!r}"
    assert ticket not in room["receipts"], (
        f"the room still holds the outcome under {ticket!r}, and nothing will ever come back for it"
    )
    assert room == room_make(), f"a collected ticket left {room!r} behind in the room"


# --- two callers, and only one of them is being told anything ---------------


@when("two callers wait on two tickets and one write fails")
def _two_callers_wait(scenario: dict[str, object]) -> None:
    room, (first, second) = _a_room_holding(A_WRITE, ANOTHER_WRITE)
    landed = {**WHAT_A_LANDED_WRITE_LOOKS_LIKE, "id": first}
    failed = {**WHAT_A_FAILED_WRITE_LOOKS_LIKE, "id": second}
    scenario["landed"] = landed
    scenario["failed"] = failed
    scenario["given"] = asyncio.run(
        _the_caller_waits_while_the_drainer_posts(room, {first: landed, second: failed})
    )
    scenario["first"] = first
    scenario["second"] = second


@then("only that ticket's caller is told")
def _the_failure_reached_one_caller(scenario: dict[str, object]) -> None:
    given = cast(dict[Ticket, Receipt], scenario["given"])
    first = cast(Ticket, scenario["first"])
    second = cast(Ticket, scenario["second"])

    assert given[second] == scenario["failed"], (
        f"the caller whose write failed was given {given[second]!r}, so the one "
        f"caller who has to know did not find out"
    )
    assert given[first] == scenario["landed"], (
        f"the caller whose write landed was given {given[first]!r}. Telling a "
        f"caller its write failed when it did not sends it to undo a row that "
        f"is really there."
    )
    assert given[first]["ok"] is True and given[second]["ok"] is False, (
        f"both callers were told the same thing: {given!r}"
    )


# --- a ticket with no write behind it --------------------------------------


@when("a caller collects on a ticket that was never deposited")
def _collecting_on_nothing(scenario: dict[str, object]) -> None:
    room = room_make()
    scenario["room"] = room
    scenario["answer"] = asyncio.run(
        asyncio.wait_for(
            room_release_ticket(room, "a ticket nobody deposited under"),
            LONG_ENOUGH_TO_BE_A_HANG,
        )
    )


@then("it is refused at once rather than waiting")
def _it_was_refused_at_once(scenario: dict[str, object]) -> None:
    answer = scenario["answer"]
    room = cast(Room, scenario["room"])

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"collecting on a ticket the room never gave out answered {answer!r}, "
        f"which is not a Fault, so a caller cannot tell it from a write that ran"
    )
    assert answer["kind"] == "refused", (
        f"a ticket nobody deposited under is refused, not {answer['kind']!r}: a "
        f"caller retrying an unavailable would retry this forever"
    )
    assert answer["what_to_do_instead"], "the refusal tells the caller nothing to do instead"
    assert room == room_make(), f"a refused collection left {room!r} behind in the room"
