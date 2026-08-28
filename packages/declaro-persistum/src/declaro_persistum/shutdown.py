"""Trapping the signal that ends the process, so unreplicated writes are not lost.

On Render and anything like it the process is ended by a signal, not by the
application deciding to stop. Replication that only happens in `close(db)`
happens when someone remembers to call it.

Installing a signal handler changes the behaviour of the program persistum is a
guest in, so it happens only when the database was opened with
`shutdown="replicate"`. The host's own handler runs first, then replication:
chaining rather than replacing is the difference between adding a guarantee and
removing one.

The platform can still cut this short — SIGTERM, a grace period, then SIGKILL.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from collections.abc import Callable
from types import FrameType

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.database import (
    Database,
    replicate_on_shutdown,
)
from declaro_persistum.reporting import say

# WHAT THE OS HANDS A SIGNAL HANDLER, AND WHAT WE HAND IT BACK. `_handler` took
# four unannotated arguments: two from the signal module and two bound per
# signal in the loop below. An audit reads that as an undeclared domain and is
# right; `signal.getsignal` returns exactly this union, so the name already
# existed and was simply not written down.
Handler = Callable[[int, FrameType | None], object] | int | None

__all__ = ["trap_shutdown"]

logger = logging.getLogger(__name__)


@helper
def trap_shutdown(db: Database) -> Database:
    """Install SIGTERM/SIGINT handlers that replicate before the process exits.

    RETURNS THE DATABASE, carrying its own undo in `untrap`. It used to hand
    that undo back as a loose callable, and both openers dropped it on the
    floor: the handlers went on at open and nothing ever took them off, so a
    SIGTERM arriving after `close(db)` drove a database that had been
    released. Returning the finished value means there is nothing for a
    caller to remember to store.

    The previous handlers live in the `untrap` closure and nowhere else, so
    two databases trapping shutdown in one process cannot overwrite each
    other's record of what they replaced.

    Does nothing and returns a no-op unless the database was opened with
    `shutdown="replicate"`. Reading the policy off the value means the decision
    lives where the database was opened rather than wherever this was called.
    """
    return _TRAP[db["shutdown"] == "replicate"](db)


@helper
def _trap_nothing(db: Database) -> Database:
    """Not opened asking to replicate, so nothing is listening and nothing undoes.

    The database comes back as it went in, already carrying
    `nothing_was_trapped`, so both halves of this table return the same kind
    of value and the caller reads one thing.
    """
    return db


@boundary
def _trap_signals(db: Database) -> Database:
    """Install the handlers, and put the function that removes them in the value."""
    replaced: list[tuple[int, object]] = []

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous = signal.getsignal(sig)
        except (ValueError, OSError):
            continue

        @boundary
        def _handler(
            signum: int,
            frame: FrameType | None,
            _previous: Handler = previous,
            _db: Database = db,
        ) -> None:
            # Python installs a default handler that is not callable, and
            # calling it would raise. Only a handler somebody set runs.
            [h(signum, frame) for h in [_previous] if callable(h)]
            say(
                __name__,
                "info",
                "Signal %s received; replicating %s before exit.",
                signum,
                _db["path"],
            )
            coro = replicate_on_shutdown(_db, now=time.monotonic)
            try:
                # ASK FOR A RUNNING LOOP, which is the only kind that can take
                # the work. `get_event_loop` answers with whatever loop object
                # is lying around, including a closed one, and then this had to
                # interrogate what it got back. A closed loop reports
                # `is_running() == False`, which read as "stopped, so run it
                # here" and raised instead of replicating.
                #
                # It also CREATES a loop when there is none and nothing closes
                # it, which the suite reports as an unraisable exception from
                # `BaseEventLoop.__del__` attributed to whichever test happened
                # to be running when the collector fired.
                #
                # `get_running_loop` cannot do either. It returns a running
                # loop or it raises, so the question `_ON_THE_LOOP` used to ask
                # has one answer and no table is needed for it.
                loop = asyncio.get_running_loop()
            except RuntimeError:
                asyncio.run(coro)
            else:
                loop.create_task(coro)

        try:
            # TRAPPING SIGTERM IS WHAT THIS MODULE IS FOR, and POSIX offers
            # no declarative equivalent. The deployment has ephemeral disk, so
            # a process that exits without replicating loses whatever it
            # wrote. What the clause warns about is behaviour parked where a
            # reader does not look, and a reader looking for what happens at
            # shutdown looks here.
            # honest-code-allow: L1.21.16 — no declarative equivalent exists
            signal.signal(sig, _handler)
            replaced.append((sig, previous))
        except (ValueError, OSError):
            # Not the main thread. Nothing to trap; close(db) still works.
            pass

    @helper
    def restore() -> int:
        """Put back what was replaced, and say how many."""
        put_back = [sig for sig, previous in replaced if _restored(sig, previous)]
        replaced.clear()
        return len(put_back)

    return {**db, "untrap": restore}


# Whether this database was opened asking to replicate at shutdown.
_TRAP: dict[bool, Callable[[Database], Database]] = {
    True: _trap_signals,
    False: _trap_nothing,
}


@helper
def _restored(sig: int, previous: object) -> bool:
    """Put one handler back. False when the platform refuses it."""
    with contextlib.suppress(ValueError, OSError, TypeError):
        # honest-code-allow: L1.21.16 — putting back a handler this module
        # replaced, which is the other half of the trap above and has the same
        # answer: there is no declarative way to restore a POSIX handler.
        signal.signal(sig, previous)  # type: ignore[arg-type]
        return True
    return False
