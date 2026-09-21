"""Whether anybody wants the connection, asked before a sync pass takes it.

    fn writer_waiting : (room: Room) -> bool

WHAT IT GUARDS. `docs/architecture.md:308`: sync is opportunistic and never on
the critical path. A pass runs only when nothing else wants the connection, and
this is the question that decides. Answer it wrong in one direction and one
sync is skipped; the next free turn takes it. Answer it wrong in the other and
a pass takes the connection out from under a caller already waiting on it. The
two mistakes are not the same size, so an occupied room answers True.

A DROPPED PASS IS DROPPED, NOT QUEUED. Ten passes skipped under load produce
one pass when the load clears, not ten, because releasing a backlog onto the
connection at the moment it recovers is the worst possible time. That is why
this returns a bool and counts nothing: there is no arrears to keep.

IT READS BOTH LISTS, AND THE ROOM'S OWN DEFINITION SAYS WHY.
`docs/architecture.md:140` calls a `Room` empty whenever nobody is mid-flight,
and it is mid-flight that "nothing else wants the connection" rules out. An
arrival is a write deposited and not yet drained. A ticket in `waiting` is a
caller the crew has not finished with. Reading only `arrivals` would call a
room empty while a drainer was still on the connection, which is the one answer
that costs a waiting caller its turn.

IT IS A BOOL AND NOT A FAULT. There is no wrong room to be handed. Both lists
are empty most of the time, and empty is an answer rather than a failure, so
nothing here can refuse.

Pure: one dict in, one bool out. No clock, no random source, no environment, no
file and no connection. The connection this question is about is never touched
to answer it, because asking the driver anything would make a fact about a dict
depend on a live database.
"""

from declaro_persistum.types import Room


def writer_waiting(room: Room) -> bool:
    """True when the room holds anybody at all, so the sync pass is dropped."""
    return bool(room["arrivals"] or room["waiting"])
