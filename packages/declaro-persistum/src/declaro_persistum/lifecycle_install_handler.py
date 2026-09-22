"""One signal handler for the process, chained to whatever was already there.

    boundary_out fn lifecycle_install_handler : () -> NOTHING | Fault
    side_effect writes "signal"

WHY IT EXISTS. `docs/architecture.md` line 314: the usual deployment has disk
that does not survive the process, so anything unreplicated at exit is gone, and
the window between the platform's signal and the kill is the whole of the time
there is to push. Nothing pushes unless something is listening for that signal,
and this is what listens.

NOBODY ASKS FOR IT. There is no door a caller knocks on to start the library.
A door like that would be one more line a program could leave out, and leaving
it out costs the data rather than costing a feature. `orchestrator fn
session_connect` calls this while it opens a database that asked to replicate at
exit, and a caller who never asked never reaches it.

IT GOES IN ONCE, NOT ONCE PER DATABASE. Two opens in one process are two
databases and one operating system, and the handler answers for the process. The
mistake a second install can write is not a second handler -- the platform holds
one function per signal, so the second `signal.signal` puts the same object back.
It is the record of what was there before: read the platform twice and the second
read finds this module's own handler, records it as the host's, and every signal
then runs the library's shutdown, reaches the chain, and runs the library's
shutdown again. `WHAT_WAS_THERE` is written with `setdefault` so the first answer
is the only answer.

IT CHAINS, IT NEVER REPLACES. A host application may already handle SIGTERM, and
a library that takes that slot and keeps it has changed the behaviour of a
program that asked for a database. The handler this module installs runs the
library's door and then runs the handler it displaced.

THE ORDER IS WHY `IN_ORDER` IS DATA. The door has to run first. A host handler
is free to call `sys.exit` or `os._exit`, and one that does would end the process
with the local commits still local. Written as two lines of a body the order is a
detail; written as a tuple it is a statement, and a test can read it.

IT NAMES NO ENGINE AND CARRIES NO SQL. It is handed nothing, so there is nothing
here that could know which engine anything is.

WHAT COMES BACK. Nothing on success. `signal.signal` refuses outside the main
thread and refuses a signal number the platform does not have, and both are
failures a caller has to hear about rather than failures to swallow, because a
process whose handler never went in is a process that loses its writes on exit.
"""

from __future__ import annotations

import signal
from collections.abc import Callable
from types import FrameType
from typing import Any

from declaro_persistum.types import NOTHING, Fault

# WHAT A HOST APPLICATION'S HANDLER LOOKS LIKE. The platform hands a handler the
# signal number and the frame it interrupted, and ignores what it gives back.
HostHandler = Callable[[int, FrameType | None], object]

# WHAT THE PLATFORM SENDS ON THE WAY OUT. SIGTERM is what a host asking for a
# clean stop sends; SIGINT is the same request typed at a terminal.
WHAT_THE_PLATFORM_SENDS: tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGINT)

# SIGNAL NUMBER -> WHAT ANSWERED IT BEFORE THE LIBRARY TOOK IT. Written once per
# signal, on the first install, and read by the chain.
WHAT_WAS_THERE: dict[int, object] = {}

_OPEN_IT_FROM_THE_MAIN_THREAD = (
    "Open the database from the main thread, which is the only thread the "
    "platform lets install a signal handler. A program that must open it from a "
    "worker has to run shutdown replication itself before the process exits, "
    "because nothing will run it for that program."
)


def _the_platform_refused(went_wrong: str) -> Fault:
    """The one failure this function has, said in the words the platform used."""
    return {
        "kind": "refused",
        "what_went_wrong": "The signal handler did not go in, so nothing will "
        "replicate when the process is asked to stop and every local commit the "
        f"primary has not seen is lost at exit. The platform said: {went_wrong}",
        "what_to_do_instead": _OPEN_IT_FROM_THE_MAIN_THREAD,
    }


# --- the chain -------------------------------------------------------------
#
# A displaced handler is a callable, or it is one of the three things that are
# not: `SIG_DFL`, `SIG_IGN`, and `None` for a handler installed from outside
# Python. None of the three is ours to run, and the reading is a table over
# `callable` rather than a test of which sentinel arrived.


def _nothing_of_ours_to_run(_previous: Any, _number: int, _frame: FrameType | None) -> bool:
    """The platform's default, ignore, or a handler set outside Python.

    It answers False rather than falling off the end. A caller reading nothing
    back cannot tell a signal that had no host handler from a function somebody
    left half written, and both halves of a pair have to say which case ran.
    """
    return False


def _run_it(previous: HostHandler, number: int, frame: FrameType | None) -> bool:
    """A handler the host application installed from Python."""
    previous(number, frame)
    return True


# THE KEY IS WHAT SAYS WHICH TYPE ARRIVED, and no type system here can say that.
# `callable(previous)` has already settled it by the time either row is reached,
# so the displaced handler is `Any` in the table and its real type in the row
# that receives it.
RUN_THE_DISPLACED_HANDLER: dict[bool, Callable[[Any, int, FrameType | None], bool]] = {
    True: _run_it,
    False: _nothing_of_ours_to_run,
}


def whatever_was_there(number: int, frame: FrameType | None) -> bool:
    """Run the handler this signal had before the library took it.

    True when a handler the host application installed ran, False when the
    signal had none of ours to run.

    `WHAT_WAS_THERE[number]` has no default on purpose: a signal reaching this
    that the library never took is a bug in the installer, and a missing row
    says so at the lookup rather than by quietly running nothing.
    """
    previous = WHAT_WAS_THERE[number]
    return RUN_THE_DISPLACED_HANDLER[callable(previous)](previous, number, frame)


def the_door(_number: int, _frame: FrameType | None) -> None:
    """Hand the signal to the door that receives it.

    The import is inside the body. `lifecycle_stop` is a sibling module written
    on its own, and the platform calls this function long after this module is
    imported, so reaching for the door at call time is both the earliest it is
    needed and the latest it can be got wrong. The door takes no arguments: the
    signal carries none, and the list of what to push is read by the module the
    door reaches.
    """
    from declaro_persistum.lifecycle_stop import lifecycle_stop

    lifecycle_stop()


# THE DOOR FIRST, THE HOST'S HANDLER SECOND. A host handler that exits would end
# the process, so everything the library owes has to have happened by then.
IN_ORDER: tuple[Callable[[int, FrameType | None], object], ...] = (the_door, whatever_was_there)


def THE_HANDLER(number: int, frame: FrameType | None) -> None:
    """What the platform calls when it asks this process to stop."""
    for step in IN_ORDER:
        step(number, frame)


def lifecycle_install_handler() -> None | Fault:
    """Put the library's handler in front of the signals that end a process.

    Twice changes nothing: the same function object goes back in each signal's
    slot, and what was there before is recorded only the first time.
    """
    try:
        for number in WHAT_THE_PLATFORM_SENDS:
            WHAT_WAS_THERE.setdefault(number, signal.getsignal(number))
            signal.signal(number, THE_HANDLER)
    except (ValueError, OSError) as refused:
        return _the_platform_refused(str(refused))
    return NOTHING
