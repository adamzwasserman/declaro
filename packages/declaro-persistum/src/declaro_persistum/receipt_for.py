"""What the engine did with one deposited write, in what a caller reads.

    fn receipt_for : (ticket: Ticket, touched: int, failure: NOTHING | Fault, attempts: int) -> Receipt

`docs/architecture.md:152`: `ok: False` means one thing only, that the engine
stayed busy through the whole retry budget. Anything the engine refused outright
never becomes a receipt, and a caller that mixes the two retries the one that
can never succeed.

`touched` IS THE CALLER'S ANSWER. `HOW-IT-SHOULD-WORK.md:166`: every write says
how many rows it touched, and a change_all that matched nothing returns zero.
Zero is an answer to act on rather than a failure, so a write that matched
nothing succeeds and says zero.

HOW MANY TIMES IT TRIED TRAVELS WITH IT. Without that number a caller told its
write landed cannot tell a first attempt from a fourth, which is the difference
between a quiet database and one about to stop taking writes at all.
"""

from __future__ import annotations

from declaro_persistum.types import Fault, Receipt, Ticket


def receipt_for(ticket: Ticket, touched: int, failure: Fault | None, attempts: int) -> Receipt:
    """Build what a caller reads for its write.

    A receipt carrying a failure did not land, so it touched nothing; the caller
    is handed the count anyway rather than an absence it has to interpret.
    """
    return Receipt(
        id=ticket,
        ok=failure is None,
        touched=touched,
        attempts=attempts,
        failure=failure,
    )
