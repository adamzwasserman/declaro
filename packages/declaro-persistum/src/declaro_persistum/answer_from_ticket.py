"""What a write comes back as.

ONE DOOR, TWO HALVES. A read is shaped from rows by `answer_from_rows`, and a
write is shaped from the ticket the room handed back. Both end as an `Answer`,
which is what lets one door serve the reading words and the writing words
without a caller having to know which half it used.

IT DOES NOT WAIT, BECAUSE IT CANNOT. `docs/architecture.md` line 138: a deposit
hands back a ticket at once and runs nothing. The only argument here is that
ticket, so a database, a room and a connection are not available to be asked,
and whether the write has run is not an input. A caller holding the answer holds
a claim on a write, and collects on it later.

IT SHAPES AND IT DOES NOT DECIDE. There is nothing to choose between: a Ticket
is already one of the arms an `Answer` may be. Widening the type is the whole
job, and any test, default or fallback added here would be a second opinion
about a value that arrived correct.
"""

from declaro_persistum.types import Answer, Ticket


def answer_from_ticket(ticket: Ticket) -> Answer:
    """The caller's answer for a deposited write: the ticket it was given."""
    return ticket
