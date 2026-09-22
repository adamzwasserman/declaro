"""Bind `tests/bdd/features/room/room_post_receipt.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `boundary_out fn room_post_receipt` is how a drainer
tells a caller what happened to its write. The drainer never speaks to the
caller: it leaves the outcome in the room under the ticket the caller is
holding, and the caller collecting takes it from there.

WHAT MAKES A CALLER "GONE", WHICH IS THE ONLY THING THE ROOM CAN ANSWER. A room
knows a ticket because a deposit put the write there under it. A ticket the room
has never heard of, or one whose caller has already collected and taken it out,
is a ticket with nobody behind it. Those are the same state and the room cannot
tell them apart, which is right: in both, there is no one left to hand the
outcome to.

WHY THE SECOND SCENARIO READS THE LOG. An outcome nobody is waiting for has
nowhere to go, and dropping it silently would make a failed write disappear from
a running system without a word. The log is the only place it can still be seen,
so the scenario reads the line rather than the room. `boundary_out fn log_write`
writes through the standard library's logging, so `caplog` is what reads it, and
that is the package's own writer rather than a stand-in.

THE ROOM IS BUILT BY DEPOSITING. A `Room` joins a ticket to its write and its
receipt, and hand-building one is how the shape changed under eight test files
on 2026-09-22 without one of them noticing.
"""

from __future__ import annotations

import logging
from typing import Any, cast

import pytest
from pytest_bdd import scenarios, then, when

from declaro_persistum.room_deposit import room_deposit
from declaro_persistum.room_make import room_make
from declaro_persistum.room_post_receipt import room_post_receipt
from declaro_persistum.types import Receipt, Room, Statement, Ticket

scenarios("../features/room/room_post_receipt.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}

# WHAT THE DRAINER FOUND OUT. A write that landed on the second try, so the
# scenario reads a receipt that carries a number and not only a verdict.
WHAT_HAPPENED: Receipt = {"id": "", "ok": True, "attempts": 2, "failure": None}

A_TICKET_THE_ROOM_NEVER_GAVE_OUT: Ticket = "a ticket nobody deposited under"


def _a_room_holding_one_write() -> tuple[Room, Ticket]:
    """A room with one write waiting, and the ticket its caller is holding."""
    room = room_make()
    ticket = room_deposit(room, A_WRITE)
    assert isinstance(ticket, str), f"the deposit was refused: {ticket!r}"
    return room, ticket


# --- a receipt somebody is waiting for -------------------------------------


@when("a drainer posts an outcome for a ticket")
def _posted_for_a_waiting_caller(scenario: dict[str, object]) -> None:
    room, ticket = _a_room_holding_one_write()
    scenario["room"] = room
    scenario["ticket"] = ticket
    scenario["answer"] = room_post_receipt(room, ticket, {**WHAT_HAPPENED, "id": ticket})


@then("the caller waiting on that ticket is given it")
def _the_waiting_caller_has_it(scenario: dict[str, object]) -> None:
    room = cast(Room, scenario["room"])
    ticket = cast(Ticket, scenario["ticket"])

    assert scenario["answer"] is None, (
        f"posting answered {scenario['answer']!r}. A drainer has nobody to hand "
        f"a failure to, so this returns nothing at all or it returns a Fault."
    )
    assert ticket in room["receipts"], (
        f"the room holds outcomes for {sorted(room['receipts'])} and the caller "
        f"is waiting on {ticket!r}, so it waits for an outcome that is already in"
    )
    assert room["receipts"][ticket] == {**WHAT_HAPPENED, "id": ticket}, (
        f"the caller would be given {room['receipts'][ticket]!r}, which is not "
        f"what the drainer found out"
    )

    # THE WRITE IS STILL THERE, because taking it out belongs to the caller
    # collecting. A drainer that removed it would leave the next caller asking
    # the room about a ticket the room no longer knows.
    assert room["writes"][ticket] == A_WRITE, (
        "posting an outcome took the write out from under the ticket"
    )


# --- a receipt nobody is waiting for ---------------------------------------


@when("a drainer posts an outcome for a ticket whose caller has gone")
def _posted_for_nobody(scenario: dict[str, object], caplog: pytest.LogCaptureFixture) -> None:
    room = room_make()
    scenario["room"] = room
    scenario["ticket"] = A_TICKET_THE_ROOM_NEVER_GAVE_OUT
    with caplog.at_level(logging.DEBUG):
        scenario["answer"] = room_post_receipt(
            room,
            A_TICKET_THE_ROOM_NEVER_GAVE_OUT,
            {**WHAT_HAPPENED, "id": A_TICKET_THE_ROOM_NEVER_GAVE_OUT, "ok": False},
        )
    scenario["lines"] = [record.getMessage() for record in caplog.records]


@then("it is written to the log")
def _the_log_carries_it(scenario: dict[str, object]) -> None:
    lines = cast(list[str], scenario["lines"])
    ticket = cast(Ticket, scenario["ticket"])

    assert lines, (
        "nothing was written. A write that failed for a caller who has gone "
        "would leave no trace anywhere in a running system."
    )
    assert any(ticket in line for line in lines), (
        f"the log says {lines!r}, and none of it names the ticket {ticket!r}, so "
        f"nobody reading it can tell which write this was"
    )


@then("nothing else is kept")
def _the_room_kept_nothing(scenario: dict[str, object]) -> None:
    room = cast(Room, scenario["room"])
    answer: Any = scenario["answer"]

    assert room == room_make(), (
        f"the room kept {room!r}. An outcome filed under a ticket nobody holds "
        f"is never collected and never removed, so the room grows for the life "
        f"of the process."
    )
    assert answer is None, (
        f"posting answered {answer!r}. Nobody is waiting and nothing went wrong "
        f"with the posting itself, so there is no failure to hand anybody."
    )
