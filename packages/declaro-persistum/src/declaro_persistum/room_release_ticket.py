"""Waiting for one write's outcome, and taking its ticket out of the room.

    boundary_out fn room_release_ticket : (room: Room, ticket: Ticket) -> Receipt | Fault awaited

`docs/architecture.md` line 138: the caller deposits and is free. This is the
other end of that. The caller comes back holding the ticket it was given, waits
here until the drainer has posted what its write did, and leaves with the
outcome. The ticket is spent by the time this returns, so a caller draws its own
atomicity boundary by choosing when to come back.

THE WAIT IS THE WHOLE FUNCTION, AND IT HAS TO YIELD. The drainer that posts the
outcome runs on the same event loop as the caller waiting for it. A wait that
held the loop would stop the drainer getting a turn, and the outcome being
waited for could never be posted. So this is a coroutine, and every caller of it
out to the door is awaited too.

A TICKET THE ROOM DOES NOT KNOW IS REFUSED AT ONCE. Collecting leaves the room
empty of that ticket, so collecting twice asks about a ticket the room has never
heard of, and so does a ticket no deposit ever gave out. Waiting on one would be
waiting for an outcome nobody is ever going to post, and the caller would never
come back. Refusing costs it one return value it is already reading.

IT TAKES ALL THREE OUT, because the ticket is a key in all three. Leaving the
arrival behind would have a drainer run the write a second time. Leaving the
write or the receipt behind would grow the room for the life of the process, one
spent ticket at a time, with nothing left that would ever come and collect them.

WHAT THIS COSTS WHILE IT WAITS, SAID PLAINLY. A `Room` holds an arrival order,
the writes and the outcomes, and nothing in it can say "something has changed".
So a caller that comes back before its write has run gives the loop a turn, looks
again, and gives the loop a turn, for as long as the drainer's engine call takes.
The loop stays live and every other task keeps running, but this one task is
awake for the whole wait rather than asleep until it is wanted. A caller that
comes back after its write has run finds the outcome on the first look and never
waits at all, which is the ordinary case. Fixing the other one means the room
carrying something a waiter can sleep on, and that is a change to the declared
shape of a `Room`, not to this function.

WHICH OF THE TWO HAPPENS IS A TABLE WITH BOTH ROWS IN IT, keyed on whether the
room knows the ticket at all, so neither case is a branch decided at run time
and neither is reached through a fallback.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from declaro_persistum.types import Fault, Receipt, Room, Ticket


async def _waited_for_and_taken_out(room: Room, ticket: Ticket) -> Receipt | Fault:
    """Give the loop a turn until the outcome is posted, then spend the ticket."""
    while ticket not in room["receipts"]:
        # ONE TURN AT A TIME, AND NO INTERVAL OF OUR OWN. `asyncio.sleep(0)`
        # hands the loop over and asks to be resumed, which is the smallest
        # honest wait there is: any number here would be a guess about how long
        # an engine takes, and a guess is what a caller would be paying.
        await asyncio.sleep(0)

    receipt = room["receipts"].pop(ticket)
    room["arrivals"].remove(ticket)
    del room["writes"][ticket]
    return receipt


async def _no_such_ticket(_room: Room, ticket: Ticket) -> Receipt | Fault:
    """Nothing was ever deposited under this, or it has been collected already."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"Nothing in this room is waiting under the ticket {ticket!r}. Either "
            f"no write was ever deposited under it, or its outcome has already "
            f"been collected and a ticket is only good once."
        ),
        what_to_do_instead=(
            "Collect on the ticket a deposit handed you, once. If you have already "
            "collected it, you are holding the outcome and there is nothing more "
            "to come."
        ),
    )


# Whether the room still holds a write under this ticket. Both rows are written
# out, so a ticket with nothing behind it is answered by a row rather than by a
# wait that never ends.
_WHAT_A_TICKET_IS_WORTH: dict[bool, Callable[[Room, Ticket], Awaitable[Receipt | Fault]]] = {
    True: _waited_for_and_taken_out,
    False: _no_such_ticket,
}


async def room_release_ticket(room: Room, ticket: Ticket) -> Receipt | Fault:
    """Wait for this ticket's outcome, hand it over, and take the ticket out.

    A ticket the room does not know comes back refused at once, because no
    outcome for it is ever going to be posted.
    """
    return await _WHAT_A_TICKET_IS_WORTH[ticket in room["writes"]](room, ticket)
