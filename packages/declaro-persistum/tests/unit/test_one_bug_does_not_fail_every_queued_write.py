"""A defect in the appender must not be reported to every caller in the room.

`drain` hands each write's failure to that write's ticket, so `collect` raises
it in the caller that deposited it. That is the contract and it is right for a
write the engine refuses: a constraint violation belongs to whoever wrote the
row.

IT IS WRONG FOR A BUG OF OURS, because ours fails every write. Measured
2026-08-17 with five writes queued and a `TypeError` from `execute`:

    drained: attempted=5 orphaned=0
    callers told their write failed: 5 of 5

Five callers each learn that their SQL failed. None of them wrote anything
wrong, and each will look at their own statement.

WHAT `drain` DOES INSTEAD. It tells the caller whose write was in flight,
because that caller is genuinely waiting and must not hang, and then raises so
the appender stops. The writes still in the room stay there, unattempted,
rather than being ground through one at a time against the same broken call.

THE APPENDER IS THE BOUNDARY. `write_queue` starts no task of its own; the
caller runs the loop, and a bug of ours reaching it is exactly the thing that
loop should not swallow.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.retry import NO_RETRY
from declaro_persistum.write_queue import collect, deposit, drain, new_room

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit]


def _five(room) -> list[str]:
    return [
        deposit(room, {"sql": f"INSERT INTO t VALUES ({i})", "params": ()})
        for i in range(5)
    ]


async def test_a_refused_write_still_reaches_only_its_own_caller() -> None:
    """The contract that must survive. One bad row, one told caller."""
    import sqlite3

    room = new_room()
    tickets = _five(room)

    async def refuse_the_third(write):
        if "VALUES (2)" in write["sql"]:
            raise sqlite3.IntegrityError("UNIQUE constraint failed")
        return None

    drained = await drain(room, refuse_the_third, NO_RETRY)
    assert drained["attempted"] == 5

    told = []
    for index, ticket in enumerate(tickets):
        try:
            await collect(room, ticket)
        except sqlite3.IntegrityError:
            told.append(index)

    assert told == [2], (
        f"a refusal of one row reached callers {told}. Failure is per ticket "
        f"or it is not per ticket."
    )


async def test_a_bug_of_ours_stops_the_appender() -> None:
    """The measured misattribution, in one assertion."""
    room = new_room()
    _five(room)

    async def our_bug(_write):
        raise TypeError("execute() got an unexpected keyword argument")

    with pytest.raises(A_BUG_IN_THIS_PACKAGE):
        await drain(room, our_bug, NO_RETRY)


async def test_a_bug_of_ours_reaches_at_most_one_caller() -> None:
    """Five callers were each told their own write failed. Now one is.

    The one in flight is told because it is genuinely waiting and must not
    hang. The rest are left queued, which is visible, rather than each being
    handed the same unrelated exception.
    """
    room = new_room()
    tickets = _five(room)

    async def our_bug(_write):
        raise TypeError("execute() got an unexpected keyword argument")

    with pytest.raises(TypeError):
        await drain(room, our_bug, NO_RETRY)

    told = 0
    for ticket in tickets:
        waiter = room["waiting"].get(ticket)
        if waiter is not None and waiter.done() and waiter.exception() is not None:
            told += 1

    assert told <= 1, (
        f"{told} callers were handed a bug that was not about their write"
    )


async def test_the_writes_not_yet_attempted_stay_in_the_room() -> None:
    """Stopping must not silently discard what it did not try.

    A write left in the room is recoverable. A write popped and dropped is
    gone, and its caller waits for a receipt that will never come.
    """
    room = new_room()
    _five(room)

    async def our_bug(_write):
        raise TypeError("a bug of ours")

    with pytest.raises(TypeError):
        await drain(room, our_bug, NO_RETRY)

    assert len(room["writes"]) >= 3, (
        f"only {len(room['writes'])} of 5 writes are still queued, so the "
        f"appender kept going after a failure that could not succeed"
    )


async def test_the_caller_in_flight_does_not_hang() -> None:
    """Raising must not leave a waiter nobody will ever resolve.

    Bounded, because the failure mode here is a hang rather than an error.
    """
    room = new_room()
    tickets = _five(room)

    async def our_bug(_write):
        raise TypeError("a bug of ours")

    with pytest.raises(TypeError):
        await drain(room, our_bug, NO_RETRY)

    with pytest.raises(TypeError):
        await asyncio.wait_for(collect(room, tickets[0]), timeout=2)


async def test_a_clean_drain_is_unchanged() -> None:
    """The ordinary path, so none of the above is bought with a broken queue."""
    room = new_room()
    tickets = _five(room)

    async def fine(_write):
        return None

    drained = await drain(room, fine, NO_RETRY)

    assert drained["attempted"] == 5
    assert drained["orphaned"] == []
    for ticket in tickets:
        receipt = await collect(room, ticket)
        assert receipt["ok"] is True
