"""A write's outcome handed to the waiter its caller is parked on.

    boundary_out fn crew_post_receipt : (crew: Crew, ticket: Ticket, receipt: Receipt) -> NOTHING | Fault

THE DRAINER NEVER SPEAKS TO THE CALLER. It ran a write and knows what happened;
it does not know who asked, and it does not wait for them to come back. It puts
the outcome where the crew keeps this ticket's waiter and goes on to the next
write. `fn crew_release_ticket` is the caller's side of the same handover.

WHY THE TICKET IS THE KEY. It is the only thing both halves hold. A drainer
looked the write up by it and posts the outcome under it, so nothing has to work
out whose write it just ran.

A CALLER WHO HAS GONE IS NOT A FAULT, AND THAT IS THE WHOLE OF WHY THIS LOGS.
The write ran. It landed or it did not, and the database is whatever the write
made it, whether or not anybody is still holding the ticket. A caller times out,
a task is cancelled, a request is dropped: each leaves a receipt with nowhere to
go, and each of those is an ordinary end to a request rather than something this
package got wrong. So the outcome goes to the log, where an operator reading
afterwards can see the write happened, and the crew is left holding nothing.

IT IS A BOUNDARY BECAUSE IT WRITES TO A WAITER. A waiter is a runtime object,
and putting a value into one wakes whatever is parked on it. A pure function
does no waking.

NOTHING HERE TOUCHES A DATABASE. The write is over by the time anything gets
here, and this hands its outcome on.

A WAITER IS FILLED ONCE. The runtime refuses a second value on the same waiter,
and a second post on one ticket means two drainers ran one write, which is a
defect in this package rather than a driver refusing something. So it comes back
as a fault naming the ticket instead of raising where nobody is reading.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.log_write import log_write
from declaro_persistum.types import Crew, Fault, Receipt, Ticket


def _nobody_is_waiting(crew: Crew, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """Write the outcome down and keep nothing.

    WHAT THE LINE HAS TO CARRY is what an operator cannot get anywhere else:
    that this write ran, whether it landed, and how many rows it touched. The
    ticket is in it because the caller's own logs name the same ticket.
    """
    log_write(
        {
            "event": "receipt_nobody_collected",
            "ticket": ticket,
            "ok": receipt["ok"],
            "touched": receipt["touched"],
            "attempts": receipt["attempts"],
        }
    )
    _forget(crew, ticket)
    return None


def _hand_it_to_the_waiter(crew: Crew, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """Put the outcome in the waiter, which wakes the caller parked on it."""
    waiter = crew["waiting"][ticket]
    return _WHETHER_THE_WAITER_IS_STILL_EMPTY[waiter.done()](crew, ticket, receipt)


def _already_filled(_crew: Crew, ticket: Ticket, _receipt: Receipt) -> Fault:
    """Two outcomes for one ticket, which is two drainers on one write."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"An outcome has already been posted for ticket {ticket!r}, so two "
            f"drainers ran the one write it names and the caller has taken the "
            f"first of the two."
        ),
        what_to_do_instead=(
            "Nothing here. Two drainers holding one write is a defect in this "
            "package, not something a caller did, and the write may have run "
            "twice against the database."
        ),
    )


def _fill_it(crew: Crew, ticket: Ticket, receipt: Receipt) -> None:
    crew["waiting"][ticket].set_result(receipt)
    _forget(crew, ticket)
    return None


_WHETHER_THE_WAITER_IS_STILL_EMPTY: dict[bool, Callable[[Crew, Ticket, Receipt], None | Fault]] = {
    False: _fill_it,
    True: _already_filled,
}


def _forget(crew: Crew, ticket: Ticket) -> None:
    """Take the write off the crew, because it is over.

    THE WAITER STAYS UNTIL THE CALLER COLLECTS IT. `fn crew_release_ticket`
    takes it, and taking it here would leave the caller parked on a waiter
    nothing can reach.
    """
    crew["writes"].pop(ticket, None)
    crew["arrivals"] = [held for held in crew["arrivals"] if held != ticket]


# Whether anybody is still holding this ticket. Both rows are here, so a caller
# that has gone is answered by a row rather than by a lookup that finds nothing.
_WHETHER_A_CALLER_IS_WAITING: dict[bool, Callable[[Crew, Ticket, Receipt], None | Fault]] = {
    True: _hand_it_to_the_waiter,
    False: _nobody_is_waiting,
}


def crew_post_receipt(crew: Crew, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """Hand one write's outcome to whoever is waiting on its ticket.

    The caller parked on that ticket wakes with the receipt. A ticket nobody is
    holding any more has its outcome written to the log instead, which is not a
    failure: the write ran either way, and the caller leaving is an ordinary end
    to a request.
    """
    return _WHETHER_A_CALLER_IS_WAITING[ticket in crew["waiting"]](crew, ticket, receipt)
