"""One drainer's turn, which arrives because that drainer woke.

    boundary_in fn lifecycle_turn : (draining: Draining) -> NOTHING | Fault

NOBODY OUTSIDE ASKED FOR THIS. Every other door in this package is knocked on by
a program: a caller opens a database, asks a question, or the platform sends a
signal. This one arrives because a drainer that was asleep woke up, which is the
runtime handing the library a spare moment. It is the only way a deposited write
ever runs and the only way an opportunistic sync ever happens.

IT IS THE TURN `boundary_out fn crew_start` WAS HANDED. That door starts one
drainer per writer, each running the turn it was given, and never decides what
that turn is. This is what it is given, and the two meet at
`orchestrator fn session_connect`.

A DOOR NEVER RAISES, AND HERE THAT MATTERS MORE THAN ANYWHERE. A drainer runs
this forever. An exception reaching the runtime ends that drainer for good: the
crew goes on holding its connection, every write deposited afterwards waits for
a worker that is not there, and `fn crew_stop` waits at shutdown for a task that
already died. So whatever went wrong comes back as a value and the loop goes
round again.

IT DECIDES NOTHING. `orchestrator fn lifecycle_catch_up` owns the sequence: wait
for work, run what the crew holds, and sync if nobody is writing. This turns a
`Draining` into what that orchestrator takes and hands back what it said.

WHAT IT READS IS THE CREW, WHICH IS WHY IT IS AN INPUT BOUNDARY. The world comes
in here as "there is a spare moment and this drainer has it", and everything
downstream is a function of the `Draining` this was handed.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.lifecycle_catch_up import lifecycle_catch_up
from declaro_persistum.log_write import log_write
from declaro_persistum.types import NOTHING, CatchingUp, Draining, Fault


def _what_went_wrong(draining: Draining, raised: BaseException) -> Fault:
    """Something inside the turn raised, which a drainer must survive."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A turn on {draining['db']['target']} raised rather than answering: "
            f"{type(raised).__name__}: {raised}"
        ),
        what_to_do_instead=(
            "Nothing here. This drainer goes round again; a turn that raises is "
            "a defect in this package rather than something a caller did."
        ),
    )


def _the_turn_that_ran(answered: CatchingUp | Fault) -> None | Fault:
    """A turn that came back with a `CatchingUp` did what it was for."""
    return _WHAT_THE_TURN_SAID[isinstance(answered, dict) and "kind" in answered](answered)


def _it_said_nothing_went_wrong(_answered: CatchingUp | Fault) -> None:
    return NOTHING


def _it_said_what_stopped_it(answered: CatchingUp | Fault) -> Fault:
    return answered  # type: ignore[return-value]


_WHAT_THE_TURN_SAID: dict[bool, Callable[..., None | Fault]] = {
    False: _it_said_nothing_went_wrong,
    True: _it_said_what_stopped_it,
}


async def lifecycle_turn(draining: Draining) -> None | Fault:
    """Spend one drainer's turn, and come back however it went.

    Nothing comes back when the turn ran. A sync the channel refused, or
    anything at all that raised inside, comes back as a `Fault`, because a
    drainer that stops running is a crew that holds writes and drains none.
    """
    try:
        return _the_turn_that_ran(
            await lifecycle_catch_up(
                CatchingUp(
                    db=draining["db"],
                    writer=draining["writer"],
                    statement=NOTHING,
                    writer_waiting=False,
                    attempts=0,
                )
            )
        )
    except Exception as raised:  # noqa: BLE001
        stopped = _what_went_wrong(draining, raised)
        log_write({"event": "turn_raised", "level": "error", "why": stopped["what_went_wrong"]})
        return stopped
