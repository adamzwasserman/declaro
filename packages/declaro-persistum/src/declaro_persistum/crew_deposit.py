"""A write handed to the crew, and a ticket handed back at once.

    boundary_out fn crew_deposit : (crew: Crew, statement: Statement) -> Ticket | Fault

`docs/architecture.md:201`: the caller hands over a write and gets a ticket back
immediately, and a crew of drainers empties the pile afterwards. That is the
difference between this and a lock, which makes the caller wait at the moment it
writes.

NOTHING HERE TOUCHES A DATABASE, AND THE ARGUMENTS ARE WHY. A crew and a
statement arrive, and neither is a connection, so there is nowhere for the write
to run before the ticket comes back.

IT LEAVES A WAITER AND RINGS THE DOORBELL. The waiter is where this caller's
outcome will be put, made now because now is when the caller still exists to
make one for. The ring wakes a sleeping worker; without it the crew has to keep
looking rather than sleeping, and `write_queue.py` recorded what that cost
before the rewrite deleted it: a busy loop starves the worker while a caller
waits with no time limit, which hung about one run in six.

TWO WRITES NEVER SHARE A TICKET. The ticket is the only thing joining a caller
to its outcome, so two callers holding one would each collect the other's. It
comes from a fresh random identifier rather than a count, because a count read
off the crew's length repeats itself the moment the crew drains.

A WRITE WITH NOTHING TO RUN IS REFUSED HERE. Accepting it would take a ticket
for work that cannot be done: the caller leaves, a worker hands an empty
statement to the engine, and the engine's complaint reaches the caller long
after, naming a statement the caller can no longer see.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import uuid4

from declaro_persistum.types import Crew, Fault, Statement, Ticket


def _take_a_ticket(crew: Crew, statement: Statement) -> Ticket | Fault:
    """Record the arrival, leave a waiter, ring the bell, hand back the ticket."""
    ticket: Ticket = uuid4().hex
    crew["arrivals"].append(ticket)
    crew["writes"][ticket] = statement
    crew["waiting"][ticket] = asyncio.get_running_loop().create_future()
    crew["arrived"].set()
    return ticket


def _refuse_an_empty_write(_crew: Crew, statement: Statement) -> Ticket | Fault:
    """Name what arrived, and leave the crew exactly as it was found."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You handed over a write whose text is {statement['text']!r}, with "
            f"{len(statement['parameters'])} values. There is nothing for a "
            f"worker to run, so the ticket would never come back with an outcome."
        ),
        what_to_do_instead=(
            "Hand over the write with the statement you meant to run in it, or make no call at all."
        ),
    )


# Whether the caller brought anything to run. Both rows are here, so a write
# with no text is answered by a row rather than by a default.
_THE_ANSWER: dict[bool, Callable[[Crew, Statement], Ticket | Fault]] = {
    True: _take_a_ticket,
    False: _refuse_an_empty_write,
}


def crew_deposit(crew: Crew, statement: Statement) -> Ticket | Fault:
    """Hand one write to the crew and take a ticket for it.

    Returns at once, having run nothing. Blank text is refused, and whitespace
    is blank: a statement of spaces is a statement the caller did not write.
    """
    return _THE_ANSWER[bool(statement["text"].strip())](crew, statement)
