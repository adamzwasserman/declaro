"""Every local commit up the channel before the process dies.

    orchestrator fn lifecycle_shut_down : (shutting_down: ShuttingDown) -> ShuttingDown | Fault

THE ONE PLACE BLOCKING IS CORRECT. Everywhere else in this package a round trip
to the primary yields to real work, because a caller waiting on one is a caller
waiting for somebody else's network. Here it is the opposite: the usual
deployment has disk that does not survive the process, so anything that has not
left the machine when it dies is gone. Politeness at exit is data loss.

THE CREW STOPS FIRST, AND THE ORDER IS THE WHOLE POINT. `fn crew_stop` rings the
bell and waits for every drainer, so no write is still landing when the push
runs. Pushing first would send whatever happened to be committed and leave the
rest, and the rest is exactly the writes a shutdown is racing.

IT KEEPS TRYING. A primary that refused once may take it on the third attempt,
and giving up early is the same as not trying: the commits are equally gone
either way. `fn next_wait` spaces the attempts.

IT STOPS TRYING TOO. The platform kills the process on its own schedule, so past
some number of attempts trying again is time spent not closing anything. What
comes back then is a fault naming the database, so an operator reading the
shutdown knows whose commits are still on the machine.

IT SAYS WHAT IT IS DOING WHILE IT DOES IT. A shutdown that takes thirty seconds
and says nothing is indistinguishable from one that hung. `fn progress_line`
builds the facts and `fn log_write` puts them where an operator reads them.

THE CONNECTIONS CLOSE WHATEVER HAPPENED. A push that never got through is bad
news; a connection left open on top of it is an operating system thread outliving
the process's own idea of being finished. Every database gets its connections
closed, including the one whose channel refused.

A DATABASE WITH NO CHANNEL HAS NOTHING TO PUSH AND THAT IS NOT A FAILURE. A
local-only deployment shuts down by stopping its crew and closing its
connections, which is the whole of what it has to do.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import cast

from declaro_persistum.channel_push import channel_push
from declaro_persistum.clock_read import clock_read
from declaro_persistum.crew_stop import crew_stop
from declaro_persistum.lifecycle_read_registered import lifecycle_read_registered
from declaro_persistum.log_write import log_write
from declaro_persistum.next_wait import next_wait
from declaro_persistum.progress_line import progress_line
from declaro_persistum.replicates import replicates
from declaro_persistum.session_close_connection import session_close_connection
from declaro_persistum.types import Database, Fault, ShuttingDown

# HOW MANY TIMES SHUTDOWN ASKS THE PRIMARY BEFORE IT REPORTS AND CLOSES. The
# platform kills the process on its own schedule, so this is a bound on time
# spent not closing rather than a judgement about the primary.
HOW_MANY_PUSHES_SHUTDOWN_MAKES = 5


async def _push_until_it_lands(db: Database, started_at: float) -> tuple[Fault | None, int]:
    """Push, and go round again while the channel refuses and attempts remain."""
    attempts = 0
    sent: Fault | None = None
    while attempts == 0 or (sent is not None and attempts < HOW_MANY_PUSHES_SHUTDOWN_MAKES):
        await _WHETHER_TO_WAIT_FIRST[attempts > 0](attempts, db["target"])
        attempts = attempts + 1
        sent = await channel_push(db["channel"])
        log_write(progress_line(db, clock_read() - started_at, sent is None))
    return sent, attempts


async def _wait_first(attempts: int, target: str) -> float:
    waited = next_wait(attempts - 1, clock_read(), target)
    await asyncio.sleep(waited)
    return waited


async def _go_straight_in(_attempts: int, _target: str) -> float:
    """The first attempt waits for nothing, and that is a real answer."""
    return 0.0


_WHETHER_TO_WAIT_FIRST: dict[bool, Callable[[int, str], Awaitable[float]]] = {
    True: _wait_first,
    False: _go_straight_in,
}


async def _nothing_to_push(db: Database, started_at: float) -> tuple[Fault | None, int]:
    """A local database has nowhere to send anything, which is not a failure."""
    log_write(progress_line(db, clock_read() - started_at, True))
    return None, 0


_WHETHER_IT_HAS_A_CHANNEL: dict[
    bool, Callable[[Database, float], Awaitable[tuple[Fault | None, int]]]
] = {
    True: _push_until_it_lands,
    False: _nothing_to_push,
}


async def _shut_one_down(db: Database, started_at: float) -> tuple[Fault | None, int]:
    """Stop the crew, send what is local, and close what was opened.

    THE THREE HAPPEN IN THAT ORDER AND NOT ANOTHER. A push before the crew has
    stopped sends whatever happened to be committed and leaves the rest, which
    is exactly the writes a shutdown is racing. A close before the push is
    commits that never left.
    """
    stopped = await crew_stop(db["crew"])
    cleanly = _WHATEVER_THE_CREW_SAID[stopped is None](db, stopped)
    sent, attempts = await _WHETHER_IT_HAS_A_CHANNEL[replicates(db)](db, started_at)
    for connection in [*db["writers"], *db["connections"]]:
        await session_close_connection(connection)
    log_write({"event": "database_shut_down", "shutting_down": db["target"], "cleanly": cleanly})
    return sent, attempts


# BOTH HALVES SAY WHETHER THE CREW STOPPED CLEANLY, so neither is a call made
# for its effect and the shutdown carries the answer rather than a line nobody
# reads back.
def _say_a_drainer_would_not_stop(db: Database, stopped: Fault | None) -> bool:
    """A drainer that raised on its way out may have left a connection open, and
    shutting down on top of that is worth an operator knowing about."""
    log_write(
        {
            "event": "crew_would_not_stop",
            "level": "error",
            "shutting_down": db["target"],
            "why": stopped["what_went_wrong"],  # type: ignore[index]
        }
    )
    return False


def _say_nothing(_db: Database, _stopped: Fault | None) -> bool:
    """Every drainer stopped, which is the ordinary case and needs no line."""
    return True


_WHATEVER_THE_CREW_SAID: dict[bool, Callable[[Database, Fault | None], bool]] = {
    True: _say_nothing,
    False: _say_a_drainer_would_not_stop,
}


async def lifecycle_shut_down(shutting_down: ShuttingDown) -> ShuttingDown | Fault:
    """Stop every crew, send every local commit up, and close what was opened.

    What comes back carries how many pushes it took. A channel that never took
    it comes back as a `Fault` naming that database, and its connections were
    closed anyway: a push that did not get through is bad news, and a connection
    left open on top of it is a thread outliving the process.
    """
    registered = lifecycle_read_registered()
    databases = _every_one_of_them(shutting_down, registered)
    made = [await _shut_one_down(db, shutting_down["started_at"]) for db in databases]
    refused = [(db, sent) for db, (sent, _n) in zip(databases, made, strict=True) if sent]
    return _WHAT_SHUTDOWN_ANSWERS[refused != []](shutting_down, databases, made, refused)


def _every_one_of_them(shutting_down: ShuttingDown, registered: object) -> list[Database]:
    """What registered and what the caller handed over, each once.

    TWO WAYS A SHUTDOWN STARTS AND ONE LIST TO SHUT DOWN. The platform's signal
    reaches `boundary_in fn lifecycle_stop`, which hands over nothing and relies
    on the register; a caller shutting down deliberately hands over its own
    databases and may never have registered them. Taking one and not the other
    leaves connections open in whichever case it was not written for.

    A REGISTER THAT COULD NOT BE READ IS NOT A REASON TO CLOSE NOTHING.
    `fn lifecycle_read_registered` can answer with a fault, and answering with
    that here would leave every connection open on the way out. It is dropped,
    and what the caller named is shut down.
    """
    read = _WHAT_THE_REGISTER_SAID[isinstance(registered, dict)](registered)
    held: list[Database] = []
    for db in [*read, *shutting_down["databases"]]:
        held.extend([db][: int(db["target"] not in [one["target"] for one in held])])
    return held


_WHAT_THE_REGISTER_SAID: dict[bool, Callable[[object], list[Database]]] = {
    True: lambda _registered: [],
    False: lambda registered: cast("list[Database]", registered),
}


def _everything_went_up(
    shutting_down: ShuttingDown,
    databases: list[Database],
    made: list[tuple[Fault | None, int]],
    _refused: list[tuple[Database, Fault]],
) -> ShuttingDown:
    return ShuttingDown(
        databases=databases,
        started_at=shutting_down["started_at"],
        attempts=sum(attempts for _sent, attempts in made),
    )


def _some_of_it_is_still_here(
    _shutting_down: ShuttingDown,
    _databases: list[Database],
    _made: list[tuple[Fault | None, int]],
    refused: list[tuple[Database, Fault]],
) -> Fault:
    return Fault(
        kind="cut_short",
        what_went_wrong=(
            "Shutdown could not get every local commit to a primary. Still on "
            "this machine: "
            + ", ".join(db["target"] for db, _sent in refused)
            + ". The last thing a channel said: "
            + str(refused[0][1]["what_went_wrong"])
        ),
        what_to_do_instead=(
            "Read what the channel said above. Every connection was closed "
            "anyway, and on disk that does not survive the process those "
            "commits are gone with it."
        ),
    )


_WHAT_SHUTDOWN_ANSWERS: dict[bool, Callable[..., ShuttingDown | Fault]] = {
    False: _everything_went_up,
    True: _some_of_it_is_still_here,
}
