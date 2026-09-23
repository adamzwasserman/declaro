"""Whose write is next, in arrival order.

    fn next_in_order : (crew: Crew) -> Ticket | NOTHING

`docs/architecture.md:142`: a crew is N drainers over one room, and each takes
one write at a time in arrival order. The order is the whole of this function.

IT ANSWERS AND IT DOES NOT TAKE. A pure function cannot remove anything, so two
drainers asking at the same instant are told the same thing and whichever one
takes it does so at the boundary. A version that popped would pass an order test
and silently let two drainers run one write.

IT ANSWERS WITH THE TICKET AND NOT THE WRITE. A drainer looks the statement up
by the ticket and hands the outcome back under the same ticket, so the statement
alone would leave it with no way to say whose write it had just run.

AN EMPTY CREW ANSWERS `NOTHING`, WHICH IS NOT A FAILURE. There being no work is
an ordinary state and most of the time it is the state.
"""

from __future__ import annotations

from declaro_persistum.types import NOTHING, Crew, Ticket


def next_in_order(crew: Crew) -> Ticket | None:
    """Say which write is next, without taking it."""
    return next(iter(crew["arrivals"]), NOTHING)
