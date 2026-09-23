"""The caller's side of the handover: wait on one ticket and take it out.

    boundary_out fn crew_release_ticket : (crew: Crew, ticket: Ticket) -> Receipt | Fault

THIS IS WHERE THE WAITING HAPPENS, WHICH IS WHY A DEPOSIT COULD RETURN AT ONCE.
`fn crew_deposit` takes the write, leaves a waiter and hands back a ticket
without running anything. The caller carries that ticket until it needs the
answer, and comes here for it. Between those two calls the caller is free, and
`docs/architecture.md` says that is the whole difference between a crew and a
lock: a lock makes the caller wait at the moment it writes.

IT AWAITS ONE WAITER AND NO OTHER. The ticket names which. Two callers in flight
together each await their own, so a write that fails tells its own caller and
tells nobody else, which is the property `crew_release_ticket.feature` asks for
under "one ticket, one caller".

A TICKET THE CREW HAS NEVER HEARD OF IS REFUSED AT ONCE. Two ways that happens:
the same ticket collected twice, because collecting takes it off the crew; and a
ticket nobody deposited under. Waiting would be the wrong answer to both. No
outcome is ever going to be posted, so the caller would sit there until
something else killed it, and nothing would say why.

THE TICKET COMES OFF THE CREW WHEN IT IS COLLECTED. It is the caller's claim on
one write, and the claim is spent once the outcome is in the caller's hands. A
crew that kept them would grow for the life of the process.

IT IS A BOUNDARY BECAUSE IT AWAITS A WAITER. A pure function waits for nothing.
Nothing here touches a database: the write ran long before this was called, and
what comes back is the record of what it did.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from declaro_persistum.types import Crew, Fault, Receipt, Ticket


async def _no_such_ticket(crew: Crew, ticket: Ticket) -> Receipt | Fault:
    """Say so now, rather than waiting for an outcome nobody will post."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The crew holds no ticket {ticket!r}. It has {len(crew['waiting'])} "
            f"tickets outstanding, so either this one has already been collected "
            f"or it was never deposited."
        ),
        what_to_do_instead=(
            "Collect a ticket once, with the ticket the deposit handed back. A "
            "ticket is spent when its outcome reaches you, so hold the receipt "
            "rather than collecting again."
        ),
    )


async def _wait_for_the_outcome(crew: Crew, ticket: Ticket) -> Receipt | Fault:
    """Park on this ticket's own waiter until a drainer fills it.

    THE WAITER STAYS ON THE CREW WHILE THE CALLER IS PARKED ON IT, and comes off
    afterwards however the wait ended. Taking it off first looks tidier and is
    wrong in both directions: a caller that got here before the drainer would
    leave nothing for `fn crew_post_receipt` to find, so the outcome would go to
    the log as one nobody collected while its caller sat waiting for it; and a
    caller cancelled while parked would leave its ticket on the crew forever.
    """
    waiter = crew["waiting"][ticket]
    try:
        return await waiter
    finally:
        crew["waiting"].pop(ticket, None)


_WHETHER_THE_CREW_KNOWS_IT: dict[bool, Callable[[Crew, Ticket], Awaitable[Receipt | Fault]]] = {
    True: _wait_for_the_outcome,
    False: _no_such_ticket,
}


async def crew_release_ticket(crew: Crew, ticket: Ticket) -> Receipt | Fault:
    """Wait for this ticket's outcome, and take the ticket off the crew.

    What comes back says whether the write landed, how many rows it touched and
    how many attempts it took. A ticket the crew does not hold is refused at
    once, because no outcome for it is ever going to be posted.
    """
    return await _WHETHER_THE_CREW_KNOWS_IT[ticket in crew["waiting"]](crew, ticket)
