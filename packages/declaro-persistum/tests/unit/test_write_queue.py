"""The queue is a waiting room in front of the WAL.

A WAL is already the queue: a write is durable once it is in the log, and
the engine applies it later. So the only job left is to buffer callers who
arrive at the same instant and hand them to the log in order.

This does not serialise the database. Turso supports concurrent writers
through MVCC and BEGIN CONCURRENT, and the pool still opens a write
connection per concurrent caller.

Nothing is stored here. The room is empty except during the microseconds
when callers overlap.

Deposit returns a ticket immediately. The caller awaits that ticket and gets
back the same ticket with a success or failure code. That is what makes it
asynchronous rather than a lock: a caller can deposit several writes, keep
working, and collect when it actually needs the answer.
"""

import asyncio

import pytest

from declaro_persistum.retry import NO_RETRY
from declaro_persistum.write_queue import collect, deposit, drain, new_room


def _write(n: int):
    return {"sql": f"INSERT INTO t VALUES ({n})", "params": (n,)}


class TestDepositReturnsATicketAtOnce:
    @pytest.mark.asyncio
    async def test_deposit_returns_a_ticket(self):
        room = new_room()
        ticket = deposit(room, _write(1))
        assert isinstance(ticket, str) and ticket

    @pytest.mark.asyncio
    async def test_every_ticket_is_different(self):
        room = new_room()
        tickets = {deposit(room, _write(n)) for n in range(5)}
        assert len(tickets) == 5

    @pytest.mark.asyncio
    async def test_deposit_does_not_wait_for_the_write(self):
        """Nothing has been executed yet -- the appender has not run."""
        room = new_room()
        executed = []

        async def execute(w):
            executed.append(w)

        deposit(room, _write(1))
        assert executed == []

        await drain(room, execute, NO_RETRY)
        assert len(executed) == 1


class TestArrivalOrderIsKept:
    @pytest.mark.asyncio
    async def test_writes_reach_the_log_in_deposit_order(self):
        room = new_room()
        order = []

        async def execute(w):
            order.append(w["params"][0])

        for n in (1, 2, 3):
            deposit(room, _write(n))
        await drain(room, execute, NO_RETRY)

        assert order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_writes_never_overlap(self):
        """The room hands writes over one at a time, in arrival order."""
        room = new_room()
        events = []

        async def execute(w):
            n = w["params"][0]
            events.append(f"start {n}")
            await asyncio.sleep(0)          # give the loop a chance to interleave
            events.append(f"end {n}")

        for n in (1, 2):
            deposit(room, _write(n))
        await drain(room, execute, NO_RETRY)

        assert events == ["start 1", "end 1", "start 2", "end 2"]

    @pytest.mark.asyncio
    async def test_draining_an_empty_room_does_nothing(self):
        async def execute(_w):
            raise AssertionError("nothing to append")

        assert await drain(new_room(), execute, NO_RETRY) == 0


class TestCollect:
    @pytest.mark.asyncio
    async def test_collect_returns_the_same_ticket(self):
        room = new_room()

        async def execute(_w):
            return None

        ticket = deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)
        receipt = await collect(room, ticket)

        assert receipt["id"] == ticket

    @pytest.mark.asyncio
    async def test_a_write_that_landed_reports_success(self):
        room = new_room()

        async def execute(_w):
            return None

        ticket = deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)
        receipt = await collect(room, ticket)

        assert receipt["ok"] is True
        assert receipt["error"] == ""

    @pytest.mark.asyncio
    async def test_the_caller_can_deposit_then_work_then_collect(self):
        """Deposit, do something else, collect. The point of the ticket."""
        room = new_room()

        async def execute(_w):
            return None

        tickets = [deposit(room, _write(n)) for n in range(3)]
        appended = asyncio.create_task(drain(room, execute, NO_RETRY))
        await asyncio.sleep(0)              # the caller is free in the meantime
        await appended

        receipts = [await collect(room, t) for t in tickets]
        assert [r["id"] for r in receipts] == tickets
        assert all(r["ok"] for r in receipts)

    @pytest.mark.asyncio
    async def test_collect_waits_until_the_write_is_appended(self):
        room = new_room()
        ticket = deposit(room, _write(1))

        async def execute(_w):
            return None

        collected = asyncio.create_task(collect(room, ticket))
        await asyncio.sleep(0)
        assert not collected.done(), "collect returned before the write was appended"

        await drain(room, execute, NO_RETRY)
        assert (await collected)["ok"] is True


class TestFailureGoesBackToItsOwnCaller:
    @pytest.mark.asyncio
    async def test_a_failed_write_reports_failure(self):
        room = new_room()

        async def execute(_w):
            raise RuntimeError("UNIQUE constraint failed")

        ticket = deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)
        receipt = await collect(room, ticket)

        assert receipt["ok"] is False
        assert "UNIQUE constraint failed" in receipt["error"]
        assert receipt["id"] == ticket

    @pytest.mark.asyncio
    async def test_one_failure_does_not_affect_another_caller(self):
        room = new_room()

        async def execute(w):
            if w["params"][0] == 2:
                raise RuntimeError("nope")

        first = deposit(room, _write(1))
        bad = deposit(room, _write(2))
        third = deposit(room, _write(3))
        await drain(room, execute, NO_RETRY)

        assert (await collect(room, first))["ok"] is True
        assert (await collect(room, bad))["ok"] is False
        assert (await collect(room, third))["ok"] is True

    @pytest.mark.asyncio
    async def test_a_failed_write_is_not_retried(self):
        """There is no contention to retry. A real error belongs to its caller."""
        room = new_room()
        attempts = []

        async def execute(_w):
            attempts.append(1)
            raise RuntimeError("nope")

        deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)

        assert len(attempts) == 1


class TestTheRoomStaysEmpty:
    @pytest.mark.asyncio
    async def test_nothing_is_left_after_a_drain_and_collect(self):
        room = new_room()

        async def execute(_w):
            return None

        ticket = deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)
        await collect(room, ticket)

        assert room["writes"] == []
        assert room["waiting"] == {}, (
            f"the room kept something after the caller collected: {room['waiting']}"
        )

    @pytest.mark.asyncio
    async def test_a_failed_write_leaves_nothing_behind_either(self):
        room = new_room()

        async def execute(_w):
            raise RuntimeError("nope")

        ticket = deposit(room, _write(1))
        await drain(room, execute, NO_RETRY)
        await collect(room, ticket)

        assert room["writes"] == []
        assert room["waiting"] == {}
