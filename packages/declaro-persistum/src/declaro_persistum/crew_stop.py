"""Every drainer told to stop, and waited for.

    boundary_out fn crew_stop : (crew: Crew) -> NOTHING | Fault

IT WAITS, IT DOES NOT ASK AND LEAVE. Shutdown has a deadline the platform sets,
and a drainer still mid-write when the process dies takes that write with it.
Ringing the stop and returning would make shutdown look finished while writes
were still in flight, and `orchestrator fn lifecycle_shut_down` would go on to
push to the primary with local commits still arriving behind it.

THE BELL IS RUNG ONCE, FOR EVERYONE. A drainer sleeping in
`fn crew_wait_for_work` is waiting on that bell, so one ring reaches every
sleeper at once. A drainer mid-write finishes the write it is on and sees the
stop on its next turn, which is why this waits rather than cancelling: a
cancelled drainer is a write abandoned between the statement and the commit.

A DRAINER THAT RAISED ON ITS WAY OUT IS REPORTED, NOT SWALLOWED, AND THE REST
STILL STOP. One drainer failing is one connection that may not have been put
down, which an operator reading the shutdown needs to know; the other three
stopping is what makes the shutdown worth finishing. So every one is waited for
and what any of them raised comes back in the return.

A CREW WITH NOBODY IN IT STOPS WITHOUT COMPLAINT. A process that opened a
database and wrote nothing still shuts down, and a refusal here would make an
ordinary shutdown read as a failure.

THE CREW IS LEFT HOLDING NOBODY, so a crew that has been stopped can be started
again, and so nothing holds a reference to a task that has finished.

IT IS A BOUNDARY BECAUSE IT AWAITS THE RUNTIME'S OWN TASKS. Nothing here touches
a database: the connections belong to the drainers and each puts its own down.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from declaro_persistum.types import NOTHING, Crew, Fault


def _one_did_not_stop_cleanly(said: list[BaseException]) -> Fault:
    """What the drainers raised, on the way back to whoever is shutting down."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"{len(said)} of the crew's drainers did not stop cleanly: "
            + "; ".join(f"{type(one).__name__}: {one}" for one in said)
        ),
        what_to_do_instead=(
            "Read what each drainer said above. Every other drainer stopped and "
            "put its connection down, and a drainer that raised on its way out "
            "may have left one open."
        ),
    )


def _all_of_them_stopped(_said: list[BaseException]) -> None:
    return NOTHING


_WHETHER_ANY_RAISED: dict[bool, Callable[[list[BaseException]], None | Fault]] = {
    True: _one_did_not_stop_cleanly,
    False: _all_of_them_stopped,
}


async def crew_stop(crew: Crew) -> None | Fault:
    """Tell every drainer to stop, wait for each one, and hold none of them after.

    Comes back with nothing when every drainer finished. A drainer that raised on
    its way out comes back as a fault naming what it said, and every other
    drainer was still waited for.
    """
    crew["stop"].set()
    # EVERY ONE IS WAITED FOR EVEN WHEN ONE RAISES, which is what
    # `return_exceptions` buys: without it the first failure comes back while the
    # rest are still running, and they are still running when the process dies.
    ended = await asyncio.gather(*crew["drainers"], return_exceptions=True)
    crew["drainers"] = []
    said = [one for one in ended if isinstance(one, BaseException)]
    return _WHETHER_ANY_RAISED[said != []](said)
