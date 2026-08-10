"""A queue is a list of writes that have not happened yet.

That is the whole idea, and everything here follows from it.

The queue is a tuple of ``PendingWrite`` dicts. ``add`` and ``remove`` are
pure functions over that tuple. ``drain`` walks it, calls the write function
you give it, and hands back what is still outstanding. There is no class, no
hidden state, no background task, and no clock.

The caller holds the list::

    pending = add(pending, {"key": "users:1", "sql": ..., "params": ...})
    ...
    pending = await drain(pending, execute, attempts=3)

The write function is an argument, so this module never touches a pool, a
connection or a database. That also means it is tested with plain functions
rather than mocks.

Two consequences worth stating, because the previous implementation of this
idea got both wrong:

* A queue nobody drains is visible in the caller's own code, because the
  caller runs the loop. It cannot sit empty and unnoticed inside an object.
* A ``PendingWrite`` is a dict of JSON-native values, so persisting the queue
  is ``json.dumps(list(pending))``. No function here is needed for it, and
  none is provided.
"""

from collections.abc import Awaitable, Callable
from typing import Any, TypedDict


class PendingWrite(TypedDict):
    """One write that has not happened yet."""

    key: str        # the caller's name for this write; used to remove it
    sql: str
    params: Any


class DrainFailed(Exception):
    """A write did not land after every attempt.

    Carries the writes that are still outstanding. A bare raise would lose
    the queue, and the caller needs it back to try again later. Writes that
    already landed are not in ``pending``, so retrying cannot re-apply them.
    """

    def __init__(
        self,
        pending: tuple[PendingWrite, ...],
        write: PendingWrite,
        attempts: int,
    ) -> None:
        self.pending = pending
        self.write = write
        self.attempts = attempts
        super().__init__(
            f"write {write['key']!r} did not land after {attempts} attempt(s); "
            f"{len(pending)} write(s) still pending"
        )


def add(
    pending: tuple[PendingWrite, ...], write: PendingWrite
) -> tuple[PendingWrite, ...]:
    """Return the list with one more write on the end.

    Two writes to the same row both stay. They both happened, and applying
    only the last would lose one.
    """
    return (*pending, write)


def remove(pending: tuple[PendingWrite, ...], key: str) -> tuple[PendingWrite, ...]:
    """Return the list without the writes under this key."""
    return tuple(w for w in pending if w["key"] != key)


async def drain(
    pending: tuple[PendingWrite, ...],
    execute: Callable[[PendingWrite], Awaitable[Any]],
    *,
    attempts: int,
) -> tuple[PendingWrite, ...]:
    """Execute each write in turn and return what is still outstanding.

    ``execute`` returning is the signal that the write landed and the next
    one can go. Nothing overlaps and nothing polls.

    A write that fails is tried ``attempts`` times, then ``DrainFailed`` is
    raised with the outstanding writes attached. It is not removed, because
    only success removes.
    """
    for write in pending:
        for attempt in range(attempts):
            try:
                await execute(write)
            except Exception as exc:
                if attempt == attempts - 1:
                    raise DrainFailed(pending, write, attempts) from exc
            else:
                pending = remove(pending, write["key"])
                break
    return pending
