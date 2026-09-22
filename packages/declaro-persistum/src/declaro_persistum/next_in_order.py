"""Which write the room hands out next.

`docs/architecture.md` line 140: a room holds the arrival order and the callers
waiting on tickets. Line 142: a crew is N drainers over one room, and each one
takes a write at a time in arrival order. Order is the whole of what this
function is for. Two writes that arrived in a known order come back in that
order, and nothing about which drainer asks can move them, because the room is
the only argument and there is nowhere for a drainer to be named.

IT ANSWERS, IT DOES NOT TAKE. `docs/design/persistum.hd` line 779 declares this
a pure `fn`, so it reads the arrivals and leaves them where they are. Taking the
write out is a write to the room, and `room_deposit` and `room_run_write` are
the boundaries that touch it. A version that popped the list would answer the
same way in a test and give two drainers two different writes in production.

AN EMPTY ROOM ANSWERS `NOTHING`. Nobody deposited anything, which is an answer a
drainer acts on by going back to sleep, not a failure. `NOTHING` is the named
absence from the declaration, so the caller reads the word rather than a bare
`None` it has to interpret.

WHY THERE IS NO BRANCH. "The first one, or nothing" is what `next` over an
iterator already says, and the empty case is its second argument rather than a
decision made here at run time.
"""

from __future__ import annotations

from declaro_persistum.types import NOTHING, Room, Statement


def next_in_order(room: Room) -> Statement | None:
    """Answer with the write that arrived first, or `NOTHING` if none did."""
    # THE TICKET, NOT THE WRITE. A drainer looks the statement up by the ticket
    # and posts the receipt against that same ticket, so handing back the
    # statement alone left it with no way to say whose write it had just run.
    return next(iter(room["arrivals"]), NOTHING)
