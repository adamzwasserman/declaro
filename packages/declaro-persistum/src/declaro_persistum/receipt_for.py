"""What the engine did with one deposited write, in the shape a caller reads.

    fn receipt_for : (ticket: Ticket, failure: NOTHING | Fault, attempts: int) -> Receipt

A caller deposits a write, gets a Ticket back at once, works, and later collects.
What it collects is a Receipt, and reading one is how it learns whether the write
landed and how hard that was.

`ok` false says one thing only: the engine stayed busy for the whole retry
budget. A write the engine REFUSED never reaches here. The collecting path raises
that, because a refused statement is a defect in the caller's own code and a
false `ok` would let it pass for a busy database. Telling those two apart is the
caller's whole reason to read a receipt, so this function is total over its two
arguments and does no sorting of its own.

`ok` is not stored a second time. It is read off the failure, which is why a
receipt cannot say it succeeded while carrying a Fault. The Fault itself travels
unchanged, and a Fault carries `what_to_do_instead`, so a caller holding a failed
receipt is told what to do differently rather than only what went wrong.

`attempts` is carried, not counted here. The retry loop is the only code that
knows how many times it went round, so it hands the number over and this function
writes it down. A verdict alone cannot say it: two writes that both landed, one
on the first try and one on the fourth, are the same `ok` true and a different
`attempts`.

Pure: three arguments in, one dict out. No clock, no random source, no
environment, no file and no connection.
"""

from declaro_persistum.types import Fault, Receipt, Ticket


def receipt_for(ticket: Ticket, failure: Fault | None, attempts: int) -> Receipt:
    """Build the Receipt for one deposited write."""
    return {"id": ticket, "ok": failure is None, "attempts": attempts, "failure": failure}
