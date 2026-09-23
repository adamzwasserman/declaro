"""Whether a writer wants the connection.

    fn writer_waiting : (crew: Crew) -> bool

`docs/architecture.md:308`: sync is opportunistic and never on the critical
path, and a pass is dropped when a writer is waiting. Politeness toward writers
is the whole of why sync works that way, and this is the question that politeness
turns on.

A WRITER IS WAITING WHEN THE CREW STILL HOLDS AN UNTAKEN WRITE. The arrival
order is what a drainer takes from, so a ticket still in it is a write nobody
has started. A write already taken is a drainer's business and not a reason to
drop a sync pass.

THE TWO MISTAKES ARE NOT THE SAME SIZE, which is why a caller reads the exact
bool rather than anything that merely behaves like one. A pass dropped by
mistake costs one skipped sync and the next free turn takes it. A pass run by
mistake takes the connection out from under a caller waiting on it.
"""

from __future__ import annotations

from declaro_persistum.types import Crew


def writer_waiting(crew: Crew) -> bool:
    """Say whether a write is still waiting to be taken."""
    return bool(crew["arrivals"])
