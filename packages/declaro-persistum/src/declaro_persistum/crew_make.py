"""A crew with nobody in it yet.

    boundary_out fn crew_make : () -> Crew

A crew holds the writes that have arrived, the caller waiting on each one, the
bell that wakes a worker, the workers themselves and the switch that stops them.
All of it is empty whenever nobody is mid-flight, which is most of the time.

NOTHING IN IT SURVIVES THE PROCESS. `docs/architecture.md:140`: no persistence
and no pending list to survive a crash, because every write that matters has a
caller holding a ticket for it. The write-ahead log is already the queue.

IT IS A BOUNDARY AND NOT A PURE FUNCTION. A doorbell comes from the runtime, and
a pure function reads no runtime.

THE TWO DOORBELLS ARE NOT THE SAME BELL. `arrived` is rung by a deposit and slept
on by a worker with nothing to do. `stop` is rung once, to end every worker.
Sharing one would make a deposit look like a shutdown.
"""

from __future__ import annotations

import asyncio

from declaro_persistum.types import Crew


def crew_make() -> Crew:
    """Build a crew holding nothing, with the bells a worker sleeps on."""
    return Crew(
        arrivals=[],
        writes={},
        waiting={},
        arrived=asyncio.Event(),
        stop=asyncio.Event(),
        drainers=[],
        size=0,
    )
