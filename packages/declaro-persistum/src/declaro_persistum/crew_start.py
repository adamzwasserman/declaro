"""One drainer per writer, handed to the runtime.

    boundary_out fn crew_start : (db: Database, turn: Turn) -> Crew | Fault

ONE PER WRITER, BECAUSE A DRAINER HOLDS ONE CONNECTION FOR ITS LIFE.
`docs/architecture.md` measured what reuse is worth: 250 writes a second against
1,505 when a connection is opened per write. So the writers are opened once at
open and a drainer is given one each, and the count is the count of writers
rather than a second number that could disagree with it.

WHAT A DRAINER DOES IS HANDED IN, AND THIS DOOR NEVER DECIDES IT. A turn is one
pass of the loop, and it comes in as an argument because a boundary starts what
it is given. Working it out here would put the sequence a drainer runs inside a
door, where nothing declares it and nothing can see it; `orchestrator fn
lifecycle_catch_up` is the card that owns that sequence.

WHAT IS HELD, AND WHY IT IS HELD. `fn crew_stop` has to reach every drainer, and
a task nothing holds is a task the runtime may collect while it is still
running. So the crew keeps them, and the crew is what shutdown is handed.

STARTING TWICE IS REFUSED RATHER THAN DONE. A second call would hand the runtime
another drainer per writer and the first set would go on running with nothing
holding them, so `fn crew_stop` would stop half of them and the rest would keep
writing after shutdown said they had stopped. The refusal names how many are
running.

A DATABASE WITH NO WRITERS STARTS NOBODY, AND THAT IS NOT A FAULT. A read-only
deployment opens no writer, and a refusal there would turn an ordinary
configuration into a failed open.

IT IS A BOUNDARY BECAUSE IT HANDS WORK TO THE RUNTIME. Nothing here touches a
database: the connections are passed on unopened, and what runs on them is the
turn's business.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from declaro_persistum.types import Connection, Crew, Database, Draining, Fault

# HOW LONG A DRAINER SLEEPS WITH NOTHING TO DO BEFORE IT TAKES A FREE TURN.
# `docs/architecture.md` gives the free turn to replication, so this is how often
# an idle database pushes rather than how fast a write is picked up: a write
# rings the bell and wakes a sleeper at once, whatever this is.
HOW_LONG_AN_IDLE_DRAINER_SLEEPS = 1.0


def _already_running(db: Database, _turn: Any) -> Crew | Fault:
    """Name how many are already running, and start nobody."""
    crew = db["crew"]
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"This crew is already running {len(crew['drainers'])} drainers over "
            f"{len(db['writers'])} writers, so starting it again would hand the "
            f"runtime a second drainer for every connection."
        ),
        what_to_do_instead=(
            "Stop the crew before starting it again. A database is opened once "
            "and its crew is started once, by the open."
        ),
    )


def _draining(db: Database, writer: Connection) -> Draining:
    """What one drainer holds for its whole life."""
    return Draining(
        db=db,
        writer=writer,
        stop=db["crew"]["stop"],
        idle=HOW_LONG_AN_IDLE_DRAINER_SLEEPS,
    )


def _start_one_per_writer(db: Database, turn: Any) -> Crew | Fault:
    """Hand the runtime one drainer per writer, and keep every one of them."""
    crew = db["crew"]
    crew["drainers"] = [
        asyncio.ensure_future(turn(_draining(db, writer))) for writer in db["writers"]
    ]
    crew["size"] = len(crew["drainers"])
    return crew


# Whether this crew has drainers already. Both rows are here, so a second start
# is answered by a row rather than by a check that happens to come first.
_WHETHER_IT_IS_ALREADY_RUNNING: dict[bool, Callable[[Database, Any], Crew | Fault]] = {
    True: _already_running,
    False: _start_one_per_writer,
}


async def crew_start(db: Database, turn: Any) -> Crew | Fault:
    """Start one drainer per writer, each running the turn it was given.

    What comes back is the database's own crew, now holding the drainers, so
    `fn crew_stop` can reach every one of them. A crew that is already running is
    refused rather than doubled.
    """
    return _WHETHER_IT_IS_ALREADY_RUNNING[db["crew"]["drainers"] != []](db, turn)
