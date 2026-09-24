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

IT CHAINS, IT NEVER REPLACES, AND THAT IS MEASURED RATHER THAN IMAGINED.
`uvicorn/server.py:321` does this:

    original_handlers = {sig: signal.signal(sig, self.handle_exit) for sig in HANDLED_SIGNALS}

uvicorn takes SIGTERM and SIGINT when the server starts, so it can drain the
requests already in flight. A database opens inside the application's startup,
which runs after that, so this library installs second and displaces uvicorn's
handler. Without chaining, uvicorn never drains: every in-flight request dies
mid-response on every deploy. The handler this module installs runs the
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
# WHAT MARKS A HANDLER AS THIS LIBRARY'S. The platform hands back whatever is in
# the slot, and the one question the installer asks of it is whether this library
# already put it there. An attribute on the function answers that by looking at
# the object the platform returned, which is reading the platform rather than
# reading a record of our own.
OURS = "_declaro_persistum_handler"


def a_handler_that_chains(displaced: object) -> Callable[[int, FrameType | None], None]:
    """Build the function the platform will call, carrying what it displaced.

    THE DISPLACED HANDLER IS IN THE FUNCTION, NOT IN A RECORD BESIDE IT. It used
    to live in a module dict keyed by signal number: one function wrote it and
    another read it, so what the chain did depended on something no caller could
    see, and the dict could hold any number at all. A slop audit on 2026-09-24
    named that dict as the one piece of unbounded state in this package and
    graded the package on it.

    Bound here it is neither. There is one value, it is fixed when the handler
    is made, and the handler is the only thing that can reach it.

    THE DOOR FIRST, THE HOST'S HANDLER SECOND. A host handler that exits would
    end the process, so everything the library owes has to have happened by then.
    """

    def handler(number: int, frame: FrameType | None) -> None:
        the_door(number, frame)
        RUN_THE_DISPLACED_HANDLER[callable(displaced)](displaced, number, frame)

    setattr(handler, OURS, True)
    return handler


def _leave_it_alone(_number: int) -> bool:
    """This library is already in this slot, so a second install changes nothing.

    READING THE PLATFORM TWICE IS THE MISTAKE THIS EXISTS TO STOP. The second
    read finds this library's own handler, a new one built around it would chain
    to it, and every signal would then run the shutdown twice.
    """
    return False


def _put_ours_in_front(number: int) -> bool:
    """Take the slot, carrying whatever the platform had in it."""
    signal.signal(number, a_handler_that_chains(signal.getsignal(number)))
    return True


_WHETHER_THIS_LIBRARY_IS_ALREADY_THERE: dict[bool, Callable[[int], bool]] = {
    True: _leave_it_alone,
    False: _put_ours_in_front,
}


def lifecycle_install_handler() -> None | Fault:
    """Put the library's handler in front of the signals that end a process.

    Twice changes nothing. The platform is asked what is in each slot, and a
    handler this library already put there is left where it is: nothing here
    remembers whether it has run before, because the slot itself is the record.
    """
    try:
        for number in WHAT_THE_PLATFORM_SENDS:
            _WHETHER_THIS_LIBRARY_IS_ALREADY_THERE[getattr(signal.getsignal(number), OURS, False)](
                number
            )
    except (ValueError, OSError) as refused:
        return _the_platform_refused(str(refused))
    return NOTHING
