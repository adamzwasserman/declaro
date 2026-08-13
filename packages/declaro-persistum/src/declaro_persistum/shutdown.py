"""Trapping the signal that ends the process, so unreplicated writes are not lost.

WHY A TRAP AND NOT JUST `close(db)`. On Render and anything like it the process
is ended by a signal, not by the application deciding to stop. If replication
only happens in `close`, it happens when someone remembers to call it — and
"whoever remembers" is the shape this package has already paid for once, in a
`.env` variable named for a guarantee nothing enforced.

WHY IT IS NEVER AUTOMATIC. Installing a signal handler changes the behaviour of
the program persistum is a guest in. A library that silently replaces SIGTERM
handling breaks the host's own shutdown work with no message. So the caller
states `shutdown="replicate"` when the database is opened, and `trap_shutdown`
does nothing otherwise.

WHY THE HOST'S HANDLER STILL RUNS. The previous handler is called first, then
replication. Chaining rather than replacing is the difference between adding a
guarantee and removing one.

THE PLATFORM CAN STILL CUT THIS SHORT. SIGTERM, a grace period, then SIGKILL.
This makes truncation far less likely, not impossible — which is why
`replicate_on_shutdown` puts the elapsed time on every line it logs rather than
only at the end.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Any

from declaro_persistum.database import Database, replicate_on_shutdown

__all__ = ["trap_shutdown", "restore_shutdown"]

logger = logging.getLogger(__name__)

_TRAPPED: dict[int, Any] = {}


def trap_shutdown(db: Database) -> None:
    """Install SIGTERM/SIGINT handlers that replicate before the process exits.

    Does nothing unless the database was opened with `shutdown="replicate"`.
    That is not a convenience check — it is the whole opt-in, and reading it
    from the value means the decision lives where the database was opened
    rather than wherever someone happened to call this.
    """
    if db["shutdown"] != "replicate":
        return

    loop = asyncio.get_event_loop

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            previous = signal.getsignal(sig)
        except (ValueError, OSError):
            continue

        def _handler(signum, frame, _previous=previous, _db=db):
            # The host's work first. It may be the thing that decides whether
            # there is anything worth replicating.
            if callable(_previous):
                _previous(signum, frame)
            logger.info(
                "Signal %s received; replicating %s before exit.",
                signum, _db["path"],
            )
            coro = replicate_on_shutdown(_db, now=_monotonic)
            try:
                running = loop()
            except RuntimeError:
                asyncio.run(coro)
            else:
                if running.is_running():
                    running.create_task(coro)
                else:
                    running.run_until_complete(coro)

        try:
            signal.signal(sig, _handler)
            _TRAPPED[sig] = previous
        except (ValueError, OSError):
            # Not the main thread. Nothing to trap here; close(db) still works.
            _TRAPPED.pop(sig, None)


def restore_shutdown() -> None:
    """Put back whatever handlers were there before. For tests and for hosts
    that hand control back rather than exiting."""
    for sig, previous in list(_TRAPPED.items()):
        with contextlib.suppress(ValueError, OSError, TypeError):
            signal.signal(sig, previous)
    _TRAPPED.clear()


def _monotonic() -> float:
    """The clock, injected into replicate_on_shutdown rather than called inside it.

    Passing it in is what lets a test assert the elapsed time appears in the
    log without sleeping for it.
    """
    import time

    return time.monotonic()
