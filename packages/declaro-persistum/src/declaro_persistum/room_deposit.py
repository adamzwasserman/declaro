"""A write left in the waiting room, and a ticket handed back at once.

    boundary_out fn room_deposit : (room: Room, statement: Statement) -> Ticket | Fault

`docs/architecture.md` line 138: the caller deposits and is free. This hands
back a ticket immediately and runs nothing, and a crew of drainers empties the
room afterwards, one write at a time, in arrival order. That is the difference
between this and a lock, which makes the caller wait at the moment it writes.

NOTHING HERE TOUCHES A DATABASE, AND THE ARGUMENTS ARE WHY. A room and a
statement arrive, and neither of them is a connection, so there is nowhere for
the write to run before the ticket comes back. The text was spelled upstream and
travels unread, which is also why this module names no engine: what a caller
deposits is already written down.

IT IS PLAIN, NOT AWAITED, AND THAT IS THE WHOLE POINT. Two list appends and a
ticket have nothing to wait for, and a coroutine would put an await back in the
one place the design took it out of: the caller's write path. A caller outside
an event loop can deposit, which is what lets the fast half of this library be
reached from anywhere.

TWO WRITES NEVER SHARE A TICKET. The ticket is the only thing joining a caller
to its outcome, so two callers holding one would each collect on the other's
write and one of the two outcomes would be unreachable. It comes from a fresh
random identifier rather than a count, because a count lives somewhere: either
in this module, where it would be state hidden from every caller, or in the
room, which holds an arrival order and the tickets waiting and nothing else. A
count read off the room's length repeats itself the moment the crew drains.

ORDER IS RECORDED, NOT REPAIRED. `fn next_in_order` reads the arrivals and hands
out the first, so the order writes come out in is the order they were appended
here. Nothing downstream can put back an order this function records wrongly.

A WRITE WITH NOTHING TO RUN IS REFUSED AT THE DOOR. Accepting it would take a
ticket for work that cannot be done: the caller would leave, the drainer would
hand an empty statement to the engine, and the engine's complaint would reach
the caller as a failed receipt long after the deposit returned, naming a
statement the caller can no longer see. Refusing costs the caller one return
value it is already reading, at the moment it still holds the write.

WHICH OF THE TWO HAPPENS IS A TABLE WITH BOTH ROWS IN IT. There is a row for a
write with text and a row for a write without, so nothing is read with a
fallback and neither case is a branch decided at run time.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

from declaro_persistum.types import Fault, Room, Statement, Ticket


# WHAT A CALLER GETS BACK AS ITS TICKET. A fresh identifier per deposit, with no
# order in it: the arrival order lives in the room, and a ticket that carried one
# would be a second answer to a question the room already answers.
def _take_a_ticket(room: Room, statement: Statement) -> Ticket | Fault:
    """Record the arrival, record the caller waiting, hand back its ticket.

    Both lists are appended in the same call and in step, so the write that
    arrived first is the ticket that was taken first. Nothing in a `Room` says
    which ticket belongs to which write, and until something does, that is the
    join a drainer has.
    """
    ticket: Ticket = uuid4().hex
    # ONE KEY, AND THE ORDER IS A LIST OF IT. Two lists appended in step joined a
    # write to its ticket only while nothing left the middle, and releasing a
    # ticket does exactly that.
    room["arrivals"].append(ticket)
    room["writes"][ticket] = statement
    return ticket


def _refuse_an_empty_write(_room: Room, statement: Statement) -> Ticket | Fault:
    """Name what arrived, and leave the room exactly as it was found."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You deposited a write whose text is {statement['text']!r}, with "
            f"{len(statement['parameters'])} parameters. There is nothing for a "
            f"drainer to run, so the ticket would never come back with an outcome."
        ),
        what_to_do_instead=(
            "Deposit the write with the statement you meant to run in it, or make no call at all."
        ),
    )


# Whether the caller brought anything to run. Both rows are here, so a write
# with no text is answered by a row rather than by a default.
_ANSWER: dict[bool, Callable[[Room, Statement], Ticket | Fault]] = {
    True: _take_a_ticket,
    False: _refuse_an_empty_write,
}


def room_deposit(room: Room, statement: Statement) -> Ticket | Fault:
    """Put one write in the room and take a ticket for it.

    Returns at once, having run nothing. Blank text is refused, and whitespace
    is blank: a statement of spaces is a statement the caller did not write.
    """
    return _ANSWER[bool(statement["text"].strip())](room, statement)
