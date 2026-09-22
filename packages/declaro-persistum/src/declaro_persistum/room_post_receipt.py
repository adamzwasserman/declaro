"""The outcome of a write, left where its caller will find it.

    boundary_out fn room_post_receipt : (room: Room, ticket: Ticket, receipt: Receipt) -> NOTHING | Fault

`docs/architecture.md` line 138: the caller deposits and is free, and a drainer
empties the room afterwards. So by the time a write runs, the caller that wrote
it is somewhere else entirely and the drainer has no way to reach it. It puts
what happened in the room, under the ticket the caller is holding, and the
caller collecting takes it from there. The two never speak.

WHAT THE ROOM CAN ANSWER, AND IT IS THE ONLY QUESTION ASKED HERE. A room knows a
ticket because a deposit put a write there under it. A ticket the room has never
heard of, and a ticket whose caller has already collected and taken it out, are
the same state to a room and there is nothing to tell them apart with. In both,
nobody is left to hand the outcome to, which is what the question "is anybody
waiting" means.

AN OUTCOME NOBODY IS WAITING FOR IS WRITTEN DOWN AND DROPPED. Filing it under a
ticket nobody holds would be filing it forever: no caller will ever collect it,
nothing removes it, and the room grows for the life of the process. Dropping it
silently would make a failed write vanish from a running system without a word.
So it goes to the log, which `boundary_out fn log_write` says is exactly what
the log is for: what a person watching a process needs to see and no caller is
waiting for.

IT RETURNS NOTHING AND NOT A RECEIPT. The drainer is not the audience for this;
it already knows what happened, which is why it is the one calling. The value
here is that the outcome is somewhere the caller can reach.

IT IS PLAIN, NOT AWAITED. Writing one key into a dict has nothing to wait for. A
coroutine would put an await into the drainer's own loop for no gain, and the
drainer's awaits are the engine calls either side of this.

WHICH OF THE TWO HAPPENS IS A TABLE WITH BOTH ROWS IN IT, keyed on whether the
room knows the ticket, so neither case is a branch decided at run time and
neither is reached through a fallback.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.log_write import log_write
from declaro_persistum.types import NOTHING, Fault, Receipt, Room, Ticket


def _left_for_the_caller(room: Room, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """File it under the ticket, and leave the write where it is.

    The write stays because taking it out belongs to the caller collecting.
    Removing it here would leave that caller asking the room about a ticket the
    room no longer knows.
    """
    room["receipts"][ticket] = receipt
    return NOTHING


def _written_down_instead(_room: Room, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """Nobody holds this ticket, so the log is the last place it can be seen."""
    log_write(
        {
            "level": "warning",
            "dropped": "a write's outcome, with no caller left to collect it",
            "ticket": ticket,
            "ok": receipt["ok"],
            "attempts": receipt["attempts"],
        }
    )
    return NOTHING


# Whether the room still holds the write this outcome belongs to. Both rows are
# written out, so an outcome for a caller who has gone is answered by a row
# rather than by a default.
_WHERE_IT_GOES: dict[bool, Callable[[Room, Ticket, Receipt], None | Fault]] = {
    True: _left_for_the_caller,
    False: _written_down_instead,
}


def room_post_receipt(room: Room, ticket: Ticket, receipt: Receipt) -> None | Fault:
    """Leave one write's outcome where the caller waiting on it will find it.

    A ticket the room does not know belongs to a caller that has gone, and its
    outcome is written to the log instead, because there is nobody to give it
    to and nothing would ever take it back out of the room.
    """
    return _WHERE_IT_GOES[ticket in room["writes"]](room, ticket, receipt)
