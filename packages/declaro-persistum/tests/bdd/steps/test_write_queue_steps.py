"""Steps for write_queue.feature. A real SQLite database, no doubles.

The queue's contract is about WHO gets told when a write fails, so what the
engine refuses has to be the engine's own answer. A stand-in raising a chosen
exception would be asserting the stand-in.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from pathlib import Path
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.retry import NO_RETRY
from declaro_persistum.write_queue import PendingWrite, collect, deposit, drain, new_room

scenarios("../features/write_queue.feature")

TABLE = "notes"


class Ctx(TypedDict, total=False):
    room: Any
    conn: Any
    run: Any
    tickets: list[str]
    receipts: list[Any]
    raised: Exception | None
    drained: Any
    deposit_returned_before_write: bool


@pytest.fixture(scope="function")
def loop():
    """One loop per scenario, so `deposit` and `collect` share it.

    A ticket is an `asyncio.Future` created by `deposit`, and a Future belongs
    to the loop it was made on. `asyncio.run` per step would make a new loop
    each time, so `collect` would await a Future from a loop that has closed.
    """
    made = asyncio.new_event_loop()
    asyncio.set_event_loop(made)
    yield made
    made.close()


def _in_loop(ctx, fn):
    """Run `fn` inside the scenario's loop.

    `deposit` is synchronous but calls `asyncio.get_running_loop()` to make the
    ticket's Future, so "call from inside the event loop" in its docstring is a
    requirement and not advice. Calling it from a plain step raises
    `RuntimeError: no running event loop`, which is the right failure and an
    easy one to mistake for a broken fixture.
    """

    async def run():
        return fn()

    return ctx["run"](run())


@pytest.fixture
def ctx(loop) -> Ctx:
    return {"tickets": [], "receipts": [], "raised": None, "run": loop.run_until_complete}


def _execute_against(conn):
    """The appender's `execute`: run one pending write on the real connection."""

    async def execute(write: PendingWrite) -> Any:
        cursor = conn.execute(write["sql"], write["params"])
        conn.commit()
        return cursor

    return execute


@given("an empty write queue and a real database")
def _given_room_and_db(ctx, tmp_path: Path):
    conn = sqlite3.connect(tmp_path / "q.db")
    conn.execute(f"CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, body TEXT)")
    conn.commit()
    ctx["conn"] = conn
    ctx["room"] = new_room()


@given("a row that already holds the only allowed primary key")
def _given_taken_key(ctx):
    ctx["conn"].execute(f"INSERT INTO {TABLE} VALUES (1, 'first')")
    ctx["conn"].commit()


@when("a caller deposits a write")
def _when_deposit(ctx):
    """`deposit` is synchronous on purpose: it must not await the write."""
    ticket = _in_loop(
        ctx,
        lambda: deposit(
            ctx["room"],
            {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (10, "a")},
        ),
    )
    ctx["tickets"].append(ticket)

    cursor = ctx["conn"].execute(f"SELECT COUNT(*) FROM {TABLE}")
    ctx["deposit_returned_before_write"] = cursor.fetchone()[0] == 0


@then("it is handed a ticket without waiting for the write")
def _then_ticket_without_waiting(ctx):
    assert ctx["tickets"] and isinstance(ctx["tickets"][0], str), ctx["tickets"]
    assert ctx["deposit_returned_before_write"], (
        "the row was already in the database when deposit returned, so the "
        "caller waited for the write. A queue that blocks the depositor is a "
        "lock wearing a ticket."
    )


@when("a caller deposits a write and the appender drains once")
def _when_deposit_and_drain(ctx):
    ticket = _in_loop(
        ctx,
        lambda: deposit(
            ctx["room"],
            {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (10, "a")},
        ),
    )
    ctx["tickets"].append(ticket)
    ctx["drained"] = ctx["run"](drain(ctx["room"], _execute_against(ctx["conn"]), NO_RETRY))


@when("the caller collects its ticket")
def _when_collect(ctx):
    ctx["receipts"].append(ctx["run"](collect(ctx["room"], ctx["tickets"][0])))


@then("the receipt says the write succeeded")
def _then_receipt_ok(ctx):
    receipt = ctx["receipts"][0]
    assert receipt["ok"] is True, f"the receipt reports failure: {receipt}"
    assert not receipt["error"], (
        f"a successful receipt carries an error: {receipt}. Measured: the "
        f"field is the empty string on success, not None."
    )


@then("the row is in the database")
def _then_row_present(ctx):
    cursor = ctx["conn"].execute(f"SELECT body FROM {TABLE} WHERE id = 10")
    assert cursor.fetchone() == ("a",), (
        "the receipt said the write succeeded and the row is not there"
    )


@when("a caller deposits three writes before collecting any of them")
def _when_deposit_three(ctx):
    for i in (20, 21, 22):
        ctx["tickets"].append(
            _in_loop(
                ctx,
                lambda i=i: deposit(
                    ctx["room"],
                    {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (i, f"n{i}")},
                ),
            )
        )


@when("the appender drains once")
def _when_drain(ctx):
    ctx["drained"] = ctx["run"](drain(ctx["room"], _execute_against(ctx["conn"]), NO_RETRY))


@then("all three receipts say the writes succeeded")
def _then_three_ok(ctx):
    receipts = [ctx["run"](collect(ctx["room"], t)) for t in ctx["tickets"]]

    assert len(receipts) == 3, receipts
    failed = [r for r in receipts if not r["ok"]]
    assert failed == [], f"these deposits failed: {failed}"

    ids = {r["id"] for r in receipts}
    assert ids == set(ctx["tickets"]), (
        f"a receipt came back under a ticket nobody deposited: {ids} against "
        f"{set(ctx['tickets'])}. A ticket belongs to one caller."
    )


@then("all three rows are in the database")
def _then_three_rows(ctx):
    cursor = ctx["conn"].execute(f"SELECT id FROM {TABLE} WHERE id IN (20, 21, 22)")
    assert sorted(r[0] for r in cursor.fetchall()) == [20, 21, 22]


@when("a caller deposits a write that violates the primary key")
def _when_deposit_doomed(ctx):
    ctx["tickets"].append(
        _in_loop(
            ctx,
            lambda: deposit(
                ctx["room"],
                {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (1, "dup")},
            ),
        )
    )


@then("collecting that ticket raises the engine's own error")
def _then_raises_engine_error(ctx):
    with pytest.raises(sqlite3.IntegrityError) as caught:
        ctx["run"](collect(ctx["room"], ctx["tickets"][0]))

    assert "UNIQUE" in str(caught.value) or "PRIMARY KEY" in str(caught.value), (
        f"the raised error is not the constraint violation: {caught.value}. "
        f"A refusal wrapped in our own type takes the engine's answer away."
    )


@when("one caller deposits a doomed write and another deposits a good one")
def _when_one_doomed_one_good(ctx):
    for key, body in ((1, "dup"), (30, "fine")):
        ctx["tickets"].append(
            _in_loop(
                ctx,
                lambda k=key, b=body: deposit(
                    ctx["room"],
                    {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (k, b)},
                ),
            )
        )


@then("the doomed ticket raises and the good ticket reports success")
def _then_isolated(ctx):
    doomed, good = ctx["tickets"]

    with pytest.raises(sqlite3.IntegrityError):
        ctx["run"](collect(ctx["room"], doomed))

    receipt = ctx["run"](collect(ctx["room"], good))
    assert receipt["ok"] is True, (
        f"one caller's constraint violation reached another caller: {receipt}. "
        f"Failure is per ticket or it is not per ticket."
    )


@then("the good row is in the database")
def _then_good_row(ctx):
    cursor = ctx["conn"].execute(f"SELECT body FROM {TABLE} WHERE id = 30")
    assert cursor.fetchone() == ("fine",), "the doomed write took the good one down with it"


@when("a caller deposits a doomed write and then abandons its ticket")
def _when_abandon(ctx):
    """Abandoning means never collecting. The ticket's waiter is gone."""

    async def abandon() -> None:
        ticket = deposit(
            ctx["room"],
            {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (1, "dup")},
        )
        ctx["tickets"].append(ticket)
        task = asyncio.ensure_future(collect(ctx["room"], ticket))
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    ctx["run"](abandon())


@then("the appender is given that failure to log")
def _then_orphaned(ctx):
    orphaned = ctx["drained"]["orphaned"]

    assert orphaned, (
        "the appender was handed nothing. A write whose depositor canceled "
        "has no ticket left to raise on, so if `drain` does not report it the "
        "failure is gone."
    )
    assert any(isinstance(e, sqlite3.IntegrityError) for e in orphaned), (
        f"the orphaned failures are {[type(e).__name__ for e in orphaned]}, "
        f"not the engine's constraint violation"
    )


@then("it is the only place that failure can still be seen")
def _then_nowhere_else(ctx):
    """Collecting an abandoned ticket must not resurrect it as a second copy."""
    cursor = ctx["conn"].execute(f"SELECT COUNT(*) FROM {TABLE} WHERE body = 'dup'")
    assert cursor.fetchone()[0] == 0, "the doomed row was written after all"


@then("the room is empty again")
def _then_room_empty(ctx):
    """Emptied by COLLECT, not by drain, and that distinction is the point.

    I wrote this scenario the other way round first: "the room holds nothing
    once the appender has run". It failed, and the code was right. `drain`
    resolves the ticket's Future and leaves it in `waiting`, because the
    receipt belongs to the caller until the caller takes it. Emptying at drain
    would throw away the answer before anyone read it, which is what a ticket
    exists to prevent.

    THE FAILING PATH IS THE ONE THAT BROKE ONCE. `collect` used to pop the
    ticket on the line after the await, so when the future began carrying
    exceptions the await raised and the pop never ran. Every FAILED write left
    its ticket for the life of the process. The `finally` is what fixes it,
    and the second scenario above is what holds it fixed.
    """
    room = ctx["room"]
    holdings = {k: v for k, v in room.items() if isinstance(v, (list, dict)) and v}

    assert holdings == {}, (
        f"the room still holds {holdings} after every ticket was collected. "
        f"Nothing is persisted here, so anything left grows for the life of "
        f"the process."
    )


@when("a caller deposits an appending write and the appender drains once")
def _when_deposit_counted(ctx):
    """The appender's `execute` counts its calls, which is the only way to see this.

    A write that ran twice with the same primary key fails the second time and
    the failure goes to a ticket nobody collects, so it lands in `orphaned` and
    the row count is still 1. Counting at the seam is what tells "ran once"
    from "ran twice and the second was refused".
    """
    calls: list[PendingWrite] = []
    inner = _execute_against(ctx["conn"])

    async def counting(write: PendingWrite) -> Any:
        calls.append(write)
        return await inner(write)

    ctx["executed"] = calls
    ctx["tickets"].append(
        _in_loop(
            ctx,
            lambda: deposit(
                ctx["room"],
                {"sql": f"INSERT INTO {TABLE} (body) VALUES (?)", "params": ("once",)},
            ),
        )
    )
    ctx["drained"] = ctx["run"](drain(ctx["room"], counting, NO_RETRY))


@then("the engine was asked to run it exactly once")
def _then_executed_once(ctx):
    executed = ctx["executed"]

    assert len(executed) == 1, (
        f"the engine was asked {len(executed)} time(s) for one deposit: "
        f"{executed}. A queue that runs a write twice corrupts data, and a "
        f"row count cannot see it when the second attempt is refused."
    )


@then("the table holds one row")
def _then_one_row(ctx):
    cursor = ctx["conn"].execute(f"SELECT COUNT(*) FROM {TABLE} WHERE body = 'once'")
    assert cursor.fetchone()[0] == 1


@when("a caller deposits a write with no SQL")
def _when_deposit_empty(ctx):
    """`PendingWrite` types `sql` as `str`, and `str` includes "".

    So the type admits a write that is not one. Deposited, it would take a
    ticket, wake a drainer, occupy a connection and come back as a receipt
    reading "no SQL statements to execute" -- a full round trip to learn what
    the caller could have been told at the door.
    """
    try:
        _in_loop(ctx, lambda: deposit(ctx["room"], {"sql": "   ", "params": ()}))
        ctx["raised"] = None
    except ValueError as e:
        ctx["raised"] = e


@then("it is refused before it takes a ticket")
def _then_refused_at_the_door(ctx):
    assert ctx["raised"] is not None, (
        "an empty statement was accepted. The engine would refuse it after a "
        "ticket, a drainer and a round trip, and the caller would read the "
        "answer as a write that failed rather than a call that was wrong."
    )
    assert isinstance(ctx["raised"], ValueError), type(ctx["raised"]).__name__
    assert "needs SQL" in str(ctx["raised"]), str(ctx["raised"])

    assert ctx["room"]["writes"] == [], (
        f"the refused write is queued anyway: {ctx['room']['writes']}. "
        f"Refusing after appending leaves a write nobody holds a ticket for."
    )
    assert ctx["room"]["waiting"] == {}, ctx["room"]["waiting"]


@then("the appender is given nothing to do")
def _then_appender_idle(ctx):
    drained = ctx["run"](drain(ctx["room"], _execute_against(ctx["conn"]), NO_RETRY))

    assert drained["orphaned"] == [], drained
    cursor = ctx["conn"].execute(f"SELECT COUNT(*) FROM {TABLE}")
    assert cursor.fetchone()[0] == 0, "a refused deposit still reached the database"


@when("a caller deposits a good write and then a doomed one")
def _when_good_then_doomed(ctx):
    """ORDER IS THE WHOLE POINT. The doomed-then-good scenario above asks
    whether a failure leaks sideways. This asks whether it reaches BACKWARD,
    to a write that has already reached the log.
    """
    for key, body in ((40, "landed"), (1, "dup")):
        ctx["tickets"].append(
            _in_loop(
                ctx,
                lambda k=key, b=body: deposit(
                    ctx["room"],
                    {"sql": f"INSERT INTO {TABLE} VALUES (?, ?)", "params": (k, b)},
                ),
            )
        )


@then("the earlier row stands even though the later ticket raises")
def _then_earlier_stands(ctx):
    good, doomed = ctx["tickets"]

    receipt = ctx["run"](collect(ctx["room"], good))
    assert receipt["ok"] is True, f"the earlier write did not land at all: {receipt}"

    with pytest.raises(sqlite3.IntegrityError):
        ctx["run"](collect(ctx["room"], doomed))

    cursor = ctx["conn"].execute(f"SELECT body FROM {TABLE} WHERE id = 40")
    assert cursor.fetchone() == ("landed",), (
        "a later failure rolled back a write that had already landed. A deposit "
        "is not a transaction: a caller who needs several statements to land "
        "together holds the connection with `async with writing(db)` instead."
    )
