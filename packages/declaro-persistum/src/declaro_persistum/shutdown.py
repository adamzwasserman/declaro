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
from typing import Any

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.database import Database, replicate_on_shutdown

# WHAT THE OS HANDS A SIGNAL HANDLER, AND WHAT WE HAND IT BACK. `_handler` took
# four unannotated arguments: two from the signal module and two bound per
# signal in the loop below. An audit reads that as an undeclared domain and is
# right; `signal.getsignal` returns exactly this union, so the name already
# existed and was simply not written down.
Handler = Callable[[int, FrameType | None], object] | int | None

__all__ = ["trap_shutdown"]

logger = logging.getLogger(__name__)


@helper
def _noop() -> int:
    """No handlers restored, because none was trapped.

    `trap_shutdown` hands back the function that undoes it. When no handler
    could be installed -- not the main thread -- there is nothing to undo,
    and the caller still gets something to call. How many it put back is a
    true answer either way, where returning nothing left the two cases
    looking identical.
    """
    return 0


@helper
def trap_shutdown(db: Database) -> Callable[[], int]:
    """Install SIGTERM/SIGINT handlers that replicate before the process exits.

    Returns a callable that puts back whatever handlers were there before.
    The previous handlers live in that closure and nowhere else, so two
    databases trapping shutdown in one process cannot overwrite each other's
    record of what they replaced.

    Does nothing and returns a no-op unless the database was opened with
    `shutdown="replicate"`. Reading the policy off the value means the decision
    lives where the database was opened rather than wherever this was called.
    """
    return _TRAP[db["shutdown"] == "replicate"](db)


@helper
def _trap_nothing(_db: Database) -> Callable[[], int]:
    """This database was not opened asking to replicate, so nothing is trapped."""
    return _noop


@boundary
def _trap_signals(db: Database) -> Callable[[], int]:
    """Install the handlers, and hand back the function that removes them."""
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
            logger.info(
                "Signal %s received; replicating %s before exit.",
                signum, _db["path"],
            )
            coro = replicate_on_shutdown(_db, now=time.monotonic)
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                asyncio.run(coro)
            else:
                # A running loop takes the work as a task; a stopped one runs
                # it to completion here.
                _ON_THE_LOOP[loop.is_running()](loop, coro)

        try:
            signal.signal(sig, _handler)
            replaced.append((sig, previous))
        except (ValueError, OSError):
            # Not the main thread. Nothing to trap; close(db) still works.
            pass

    @boundary
    def restore() -> int:
        """Put back what was replaced, and say how many."""
        put_back = [
            sig
            for sig, previous in replaced
            if _restored(sig, previous)
        ]
        replaced.clear()
        return len(put_back)

    return restore


@helper
def _as_a_task(loop: asyncio.AbstractEventLoop, coro: Any) -> None:
    """The loop is running, so the work joins it."""
    loop.create_task(coro)


@helper
def _to_completion(loop: asyncio.AbstractEventLoop, coro: Any) -> None:
    """The loop is stopped, so the work runs here and finishes before we go."""
    loop.run_until_complete(coro)


# Whether the loop is already running.
_ON_THE_LOOP: dict[bool, Callable[[asyncio.AbstractEventLoop, Any], None]] = {
    True: _as_a_task,
    False: _to_completion,
}

# Whether this database was opened asking to replicate at shutdown.
_TRAP: dict[bool, Callable[[Database], Callable[[], int]]] = {
    True: _trap_signals,
    False: _trap_nothing,
}


@boundary
def _restored(sig: int, previous: object) -> bool:
    """Put one handler back. False when the platform refuses it."""
    with contextlib.suppress(ValueError, OSError, TypeError):
        signal.signal(sig, previous)  # type: ignore[arg-type]
        return True
    return False
