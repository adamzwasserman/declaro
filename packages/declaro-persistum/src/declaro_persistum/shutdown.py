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

from declaro_persistum.database import Database, replicate_on_shutdown

# WHAT THE OS HANDS A SIGNAL HANDLER, AND WHAT WE HAND IT BACK. `_handler` took
# four unannotated arguments: two from the signal module and two bound per
# signal in the loop below. An audit reads that as an undeclared domain and is
# right; `signal.getsignal` returns exactly this union, so the name already
# existed and was simply not written down.
Handler = Callable[[int, FrameType | None], object] | int | None

__all__ = ["trap_shutdown"]

logger = logging.getLogger(__name__)


def _noop() -> None:
    return None


def trap_shutdown(db: Database) -> Callable[[], None]:
    """Install SIGTERM/SIGINT handlers that replicate before the process exits.

    Returns a callable that puts back whatever handlers were there before.
    The previous handlers live in that closure and nowhere else, so two
    databases trapping shutdown in one process cannot overwrite each other's
    record of what they replaced.

    Does nothing and returns a no-op unless the database was opened with
    `shutdown="replicate"`. Reading the policy off the value means the decision
    lives where the database was opened rather than wherever this was called.
    """
    if db["shutdown"] != "replicate":
        return _noop

    replaced: list[tuple[int, object]] = []

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous = signal.getsignal(sig)
        except (ValueError, OSError):
            continue

        def _handler(
            signum: int,
            frame: FrameType | None,
            _previous: Handler = previous,
            _db: Database = db,
        ) -> None:
            if callable(_previous):
                _previous(signum, frame)
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
                if loop.is_running():
                    loop.create_task(coro)
                else:
                    loop.run_until_complete(coro)

        try:
            signal.signal(sig, _handler)
            replaced.append((sig, previous))
        except (ValueError, OSError):
            # Not the main thread. Nothing to trap; close(db) still works.
            pass

    def restore() -> None:
        for sig, previous in replaced:
            with contextlib.suppress(ValueError, OSError, TypeError):
                signal.signal(sig, previous)
        replaced.clear()

    return restore
