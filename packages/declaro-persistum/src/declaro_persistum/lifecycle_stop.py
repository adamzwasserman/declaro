"""The platform's shutdown signal, arriving with nothing in its hands.

    boundary_in fn lifecycle_stop : () -> NOTHING | Fault

IT TAKES NOTHING, AND THAT IS THE SIGNAL'S DOING RATHER THAN A CHOICE. A
platform sending SIGTERM does not know what this process opened. So the door
takes no arguments and `orchestrator fn lifecycle_shut_down` reads the register
for itself, which is what `fn lifecycle_register` was filling in all along.

WHY THIS DOOR EXISTS AT ALL WHEN THE ORCHESTRATOR COULD BE CALLED DIRECTLY.
`boundary_out fn lifecycle_install_handler` puts a function in the platform's
signal slot, and the platform calls whatever is there. This is that function's
work, written where the declaration can see it: a signal is the world coming in,
which is column one.

A DOOR NEVER RAISES, AND THIS ONE RUNS INSIDE A SIGNAL HANDLER. An exception
here reaches the platform mid-signal. There is no caller to catch it, the
process is already on its way out, and whatever it would have said goes nowhere
while the databases it had not reached are never shut down at all.

WHAT IT DECIDES IS NOTHING. The order, the retrying and the closing belong to
the orchestrator. This starts the clock, because "how long has this taken" is a
fact about the shutdown that only whoever began it can know.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.clock_read import clock_read
from declaro_persistum.lifecycle_shut_down import lifecycle_shut_down
from declaro_persistum.log_write import log_write
from declaro_persistum.types import NOTHING, Fault, ShuttingDown


def _what_went_wrong(raised: BaseException) -> Fault:
    """Something inside shutdown raised, which a signal handler must survive."""
    return Fault(
        kind="cut_short",
        what_went_wrong=(
            f"Shutting down raised rather than answering: {type(raised).__name__}: {raised}"
        ),
        what_to_do_instead=(
            "Nothing here. The process is on its way out, and whatever had not "
            "been pushed when this happened is still on this machine."
        ),
    )


def _it_finished(_answered: ShuttingDown | Fault) -> None:
    return NOTHING


def _it_did_not(answered: ShuttingDown | Fault) -> Fault:
    return answered  # type: ignore[return-value]


_WHAT_SHUTDOWN_SAID: dict[bool, Callable[..., None | Fault]] = {
    False: _it_finished,
    True: _it_did_not,
}


async def lifecycle_stop() -> None | Fault:
    """Work through every database that asked to replicate at exit.

    Nothing comes back when every local commit reached its primary and every
    connection closed. A shutdown that could not finish comes back as a `Fault`,
    in the return, because raising inside a signal handler tells nobody anything
    and leaves the rest of the databases unreached.
    """
    try:
        answered = await lifecycle_shut_down(
            ShuttingDown(databases=[], started_at=clock_read(), attempts=0)
        )
        return _WHAT_SHUTDOWN_SAID[isinstance(answered, dict) and "kind" in answered](answered)
    except Exception as raised:  # noqa: BLE001
        stopped = _what_went_wrong(raised)
        log_write({"event": "shutdown_raised", "level": "error", "why": stopped["what_went_wrong"]})
        return stopped
