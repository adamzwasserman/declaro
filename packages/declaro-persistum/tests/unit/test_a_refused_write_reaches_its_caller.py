"""A write the engine refuses must reach its depositor as an exception.

`_append_one` used to end every failure the same way::

    except Exception as exc:
        if attempt == retry["attempts"] or not is_contention(exc):
            return {"id": ticket, "ok": False, "error": str(exc)}

Read the `or`. On the left, contention that outlasted the retry budget, which
retry.py rules is an ordinary answer a write can give. On the right, every
other failure — a constraint violation, a typo in the SQL, a full disk — with
its type and traceback replaced by `str(exc)` and posted into the same field.

The caller could not tell the two apart, and the correct response differs
completely: contention says try again later, a constraint violation says the
write will never succeed and the caller's logic is wrong.

These tests hold the two apart. The first says a refusal raises with its own
type. The second says contention exhaustion still returns a receipt, because
that distinction is the entire point and a fix that raised both would be a
different bug.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.retry import NO_RETRY, Retry
from declaro_persistum.write_queue import collect, deposit, drain, new_room

pytestmark = pytest.mark.precommit


class TheRowAlreadyExists(Exception):
    """Stands in for a constraint violation: the engine's answer is "no"."""


class TheEngineIsBusy(Exception):
    """Contention. `is_contention` matches on the word "busy"."""


THREE_ATTEMPTS: Retry = {"attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0}


@pytest.mark.asyncio
async def test_a_refused_write_raises_in_the_caller_that_deposited_it() -> None:
    """The failure keeps its type. That is what the receipt destroyed."""
    room = new_room()

    async def execute(_write):
        raise TheRowAlreadyExists("UNIQUE constraint failed: t.id")

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})
    await drain(room, execute, NO_RETRY)

    with pytest.raises(TheRowAlreadyExists, match="UNIQUE constraint failed"):
        await collect(room, ticket)


@pytest.mark.asyncio
async def test_contention_exhaustion_is_still_a_receipt() -> None:
    """The other half. An engine that stayed busy gives an answer, not a raise.

    retry.py: "Contention exhaustion is an ORDINARY outcome of a write, not an
    exceptional condition, and every consumer surface must carry it as one of
    the answers a write can give."
    """
    room = new_room()
    attempts = 0

    async def execute(_write):
        nonlocal attempts
        attempts += 1
        raise TheEngineIsBusy("database is busy")

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})
    await drain(room, execute, THREE_ATTEMPTS)

    receipt = await collect(room, ticket)
    assert receipt["ok"] is False
    assert attempts == 3, "the whole budget is spent before giving up"
    assert "busy" in receipt["error"]


@pytest.mark.asyncio
async def test_a_refusal_is_not_retried() -> None:
    """"No" fails identically next time, so spending the budget on it is waste."""
    room = new_room()
    attempts = 0

    async def execute(_write):
        nonlocal attempts
        attempts += 1
        raise TheRowAlreadyExists("UNIQUE constraint failed")

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})
    await drain(room, execute, THREE_ATTEMPTS)

    with pytest.raises(TheRowAlreadyExists):
        await collect(room, ticket)
    assert attempts == 1, "a constraint violation is not contention"


@pytest.mark.asyncio
async def test_one_refused_write_does_not_stop_the_queue() -> None:
    """The promise the module docstring makes to the OTHER depositors.

    This is why `drain` routes the exception to one future instead of letting
    it out of the loop. A drainer runs `drain` inside `while not stop.is_set()`
    — an exception escaping would kill that task, close its connection, and
    shrink the crew by one for the life of the process, silently.
    """
    room = new_room()

    async def execute(write):
        if write["params"] == (2,):
            raise TheRowAlreadyExists("row 2 is a duplicate")

    tickets = [
        deposit(room, {"sql": "INSERT INTO t VALUES (?)", "params": (i,)})
        for i in (1, 2, 3)
    ]
    drained = await drain(room, execute, NO_RETRY)

    assert drained["appended"] == 3, (
        "every write was attempted, including the two after the failure"
    )
    assert (await collect(room, tickets[0]))["ok"] is True
    with pytest.raises(TheRowAlreadyExists):
        await collect(room, tickets[1])
    assert (await collect(room, tickets[2]))["ok"] is True


@pytest.mark.asyncio
async def test_a_raised_collect_still_empties_the_room() -> None:
    """A regression in the fix above, found by re-reading it an hour later.

    `collect` was `await` then `pop`, on two lines. Once the future could
    hold an exception the `await` raised and the `pop` never ran, so every
    failed write left its ticket behind for the life of the process. Its own
    docstring promises the opposite: "the ticket is dropped once collected,
    so the room does not grow."

    A leak has no symptom until the process is old, which is why it needs a
    test rather than a reading.
    """
    room = new_room()

    async def execute(_write):
        raise TheRowAlreadyExists("nope")

    for _ in range(5):
        ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})
        await drain(room, execute, NO_RETRY)
        with pytest.raises(TheRowAlreadyExists):
            await collect(room, ticket)

    assert room["waiting"] == {}, "a failed write must not leave its ticket behind"
    assert room["writes"] == []


@pytest.mark.asyncio
async def test_a_failure_with_no_living_caller_comes_back_as_data() -> None:
    """The last hole in the router, and the one that had no owner.

    `drain` can only route a failure where someone is listening. A depositor
    that cancelled its own `collect` has taken its future with it, so the
    branch ended at the `if` and the exception fell off the end.

    Cancelling a collect says "I no longer need the answer". It does not say
    "do not tell anyone the database refused this write". So the failure
    leaves as data and the crew logs it.
    """
    room = new_room()

    async def execute(_write):
        raise TheRowAlreadyExists("nobody is listening for this one")

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})

    # Exactly what a cancelled `collect` leaves behind: the ticket popped by
    # its `finally`, and a future already settled.
    room["waiting"][ticket].cancel()
    room["waiting"].pop(ticket, None)

    drained = await drain(room, execute, NO_RETRY)

    assert drained["appended"] == 1
    assert len(drained["orphaned"]) == 1, "the exception must not fall off the end"
    assert isinstance(drained["orphaned"][0], TheRowAlreadyExists)
    assert "nobody is listening" in str(drained["orphaned"][0])


@pytest.mark.asyncio
async def test_a_write_that_lands_for_nobody_is_not_an_orphan() -> None:
    """Only failures are orphans. A successful write nobody collected is fine."""
    room = new_room()

    async def execute(_write):
        return None

    ticket = deposit(room, {"sql": "INSERT INTO t VALUES (1)", "params": ()})
    room["waiting"][ticket].cancel()
    room["waiting"].pop(ticket, None)

    drained = await drain(room, execute, NO_RETRY)

    assert drained["appended"] == 1
    assert drained["orphaned"] == []


@pytest.mark.asyncio
async def test_the_failure_goes_only_to_the_ticket_that_owns_it() -> None:
    """Concurrent depositors, one bad write. Nobody else sees the exception."""
    room = new_room()

    async def execute(write):
        if write["params"] == (7,):
            raise TheRowAlreadyExists("row 7 is a duplicate")

    tickets = [
        deposit(room, {"sql": "INSERT INTO t VALUES (?)", "params": (i,)})
        for i in range(10)
    ]
    await drain(room, execute, NO_RETRY)

    settled = await asyncio.gather(
        *(collect(room, t) for t in tickets), return_exceptions=True
    )
    failed = [i for i, r in enumerate(settled) if isinstance(r, Exception)]
    assert failed == [7], f"exactly one ticket carries the failure, got {failed}"
