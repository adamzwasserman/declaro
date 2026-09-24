"""A drainer asleep until there is something to do.

    boundary_out fn crew_wait_for_work : (draining: Draining) -> NOTHING | Fault

WHY IT SLEEPS RATHER THAN LOOKS. A drainer that keeps looking burns the loop a
caller is waiting on. `write_queue.py` recorded what that cost before the rewrite
deleted it: a busy loop starves the worker while a caller waits with no time
limit, and it hung about one run in six.

THREE THINGS WAKE IT, AND ALL THREE ARE ORDINARY. A write arriving is the one it
is there for. The stop is shutdown, and a drainer that slept through it would
still be holding a connection when the process died. The idle limit is a free
turn: `orchestrator fn lifecycle_catch_up` spends one of those syncing, so a
drainer with nothing to write is a drainer with time to push. None of the three
is a failure and none comes back as one.

WORK ALREADY THERE MEANS NO SLEEP AT ALL. The arrivals are read before the bell
is touched, so a write deposited between one turn and the next is picked up on
this turn rather than waited for. A drainer that slept first would hold that
write until the next deposit or the idle limit, whichever came sooner, and the
caller would wait out both.

THE BELL IS CLEARED BEFORE SLEEPING, AND ONLY THEN. `fn crew_deposit` rings it
and nothing else clears it, so a bell left set from a write another drainer
already took would wake every later sleeper at once and the sleeping would be
decorative. Clearing it while work is waiting would lose the ring that brought
that work in.

WHAT IT DOES NOT DO. It takes no write, runs nothing and touches no database.
`fn next_in_order` decides whose write is next and it is pure; this only says
that the waiting is over.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from declaro_persistum.types import NOTHING, Draining, Fault


async def _there_is_work_already(_draining: Draining) -> None | Fault:
    """Nothing to wait for, so the turn starts now."""
    return NOTHING


async def _sleep_until_something_happens(draining: Draining) -> None | Fault:
    """Sleep on the two bells and the clock, and come back on the first of them.

    THE THREE ARE RACED, NOT TRIED IN TURN. Waiting on the arrival first and the
    stop after would sleep through a shutdown until a write happened to arrive,
    which on an idle database is until the process is killed.

    THE LOSERS ARE CANCELLED, because a sleeper left behind is a task the runtime
    holds for the life of the process, one per turn, on every drainer.
    """
    crew = draining["db"]["crew"]
    crew["arrived"].clear()
    waiting = [
        asyncio.ensure_future(crew["arrived"].wait()),
        asyncio.ensure_future(draining["stop"].wait()),
    ]
    try:
        await asyncio.wait(waiting, timeout=draining["idle"], return_when=asyncio.FIRST_COMPLETED)
    finally:
        for sleeper in waiting:
            sleeper.cancel()
    return NOTHING


# Whether there is anything to do before any sleeping is considered. Both rows
# are here, so a crew holding a write is answered by a row rather than by a
# sleep nobody needed.
_WHETHER_THERE_IS_WORK: dict[bool, Callable[[Draining], Awaitable[None | Fault]]] = {
    True: _there_is_work_already,
    False: _sleep_until_something_happens,
}


async def crew_wait_for_work(draining: Draining) -> None | Fault:
    """Sleep until a write arrives, the crew stops, or the idle limit runs out.

    Comes back with nothing, whichever of the three happened, because all three
    are ordinary. A drainer that woke on the idle limit has a free turn rather
    than a problem.
    """
    return await _WHETHER_THERE_IS_WORK[draining["db"]["crew"]["arrivals"] != []](draining)
