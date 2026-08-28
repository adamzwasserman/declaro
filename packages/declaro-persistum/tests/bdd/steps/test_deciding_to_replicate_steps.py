"""Steps for replication/deciding_to_replicate.feature: a real Database, no patching.

WHAT THIS REPLACES. Three tests patched `replicate` on the module and counted
the calls. The dispatch table binds that function when the module loads, so
the patch never reached the code under test. Deleting all three changed
nothing, which is how they were found.

THE RECORDERS ARE INPUT, NOT REPLACEMENTS. `replicate_once` and `refresh_once`
are fields of `Database`: this hands them functions, which is calling the code
with its arguments. Nothing of ours is stopped from running, and `replicate`,
`replicate_if_idle` and the tables between them are the real ones.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.database import (
    Database,
    _nothing_to_deliver,
    _refuse_replicating_nowhere,
    _replication_stands,
    is_replicated,
    no_passes,
    nothing_happened,
    nothing_was_trapped,
    replicate,
    replicate_if_idle,
    writers_waiting,
    writing,
)

scenarios("../features/replication/deciding_to_replicate.feature")


class Ctx(TypedDict, total=False):
    db: Any
    order: list[str]
    answer: Any
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"db": None, "order": [], "answer": None, "refusal": None}


def _a_database(ctx: Ctx, *, primary: str | None, waiting: int, closed: bool) -> Any:
    """A real Database record, with the two replication steps recording order."""

    async def _sent(_db: Any) -> bool:
        ctx["order"].append("sent")
        return True

    async def _received(_db: Any) -> int:
        ctx["order"].append("received")
        return 1

    async def _never(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("this scenario opened a door it does not exercise")

    return Database(
        path="x.db",
        dialect="turso",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary=primary,
        token=None,
        connect=_never,
        close_connection=no_passes,
        for_ddl=writing,
        serialise={"waiting": waiting, "lock": None},
        shutdown="exit_immediately",
        write_one=_never,
        replicate_once=_sent,
        refresh_once=_received,
        release=no_passes,
        sleep=_never,
        retry_delay_s=0.0,
        closed=closed,
        untrap=nothing_was_trapped,
    )


@given("a database with a primary")
def _given_replica(ctx):
    ctx["db"] = _a_database(ctx, primary="libsql://example", waiting=0, closed=False)


@given("a database with no primary")
def _given_local(ctx):
    ctx["db"] = _a_database(ctx, primary=None, waiting=0, closed=False)


@given("a database with a primary and one writer waiting")
def _given_busy(ctx):
    ctx["db"] = _a_database(ctx, primary="libsql://example", waiting=1, closed=False)


@given("a database with a primary that has been closed")
def _given_closed(ctx):
    ctx["db"] = _a_database(ctx, primary="libsql://example", waiting=0, closed=True)


@then("is_replicated answers yes")
def _then_replicated(ctx):
    assert is_replicated(ctx["db"]) is True


@then("is_replicated answers no")
def _then_not_replicated(ctx):
    assert is_replicated(ctx["db"]) is False


@then(parsers.parse("writers_waiting counts {expected:d}"))
def _then_waiting(ctx, expected):
    assert writers_waiting(ctx["db"]) == expected


# THE STEPS RUN THE COROUTINE THEMSELVES. pytest-bdd calls a step function
# and does not await what it returns, so an `async def` step leaves a
# coroutine nobody ran and the assertions read an untouched context. It cost
# seven red tests here before the cause was obvious.


@when("nothing_happened is asked to run a pass")
def _when_nothing_happened(ctx):
    ctx["answer"] = asyncio.run(nothing_happened(ctx["db"]))


@when("replicate_if_idle is asked to run a pass")
def _when_if_idle(ctx):
    ctx["answer"] = asyncio.run(replicate_if_idle(ctx["db"]))


@when("replicate runs one pass")
def _when_replicate(ctx):
    ctx["answer"] = asyncio.run(replicate(ctx["db"]))


@when("_nothing_to_deliver is asked to deliver before exit")
def _when_nothing_to_deliver(ctx):
    ctx["answer"] = asyncio.run(_nothing_to_deliver(ctx["db"], lambda: 0.0))


@when(parsers.parse('_replication_stands is asked about "{policy}" from "{opener}"'))
def _when_stands(ctx, policy, opener):
    try:
        ctx["answer"] = _replication_stands(policy, opener)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - the refusal is the answer
        ctx["refusal"] = refused


@when(parsers.parse('_refuse_replicating_nowhere is asked about "{policy}" from "{opener}"'))
def _when_refuse(ctx, policy, opener):
    try:
        ctx["answer"] = _refuse_replicating_nowhere(policy, opener)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - the refusal is the answer
        ctx["refusal"] = refused


@then("it answers that no pass ran")
def _then_no_pass(ctx):
    assert ctx["answer"] is False, ctx["answer"]
    assert ctx["order"] == [], f"a pass was dropped and still ran: {ctx['order']}"


@then("it answers that a pass ran")
def _then_a_pass(ctx):
    assert ctx["answer"] is True, ctx["answer"]


@then("it sent before it received")
def _then_order(ctx):
    assert ctx["order"] == ["sent", "received"], ctx["order"]


@then(parsers.parse("it answers {expected:d} passes"))
def _then_passes(ctx, expected):
    assert ctx["answer"] == expected, ctx["answer"]


@then("it accepts")
def _then_accepts(ctx):
    assert ctx["refusal"] is None, ctx["refusal"]


@then(parsers.parse('it refuses and names "{named}"'))
def _then_refuses(ctx, named):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r} instead"
    assert named in str(ctx["refusal"]), ctx["refusal"]


def _run_the_loop(ctx, *, pending: bool) -> None:
    """Start the real loop, let it settle, then close it down.

    THE RECORDER IS INJECTED, NOT PATCHED. `replicate_once` and `refresh_once`
    are fields the loop's own `replicate` reads, so the pass this counts is a
    real pass through the real dispatch tables.
    """

    async def _drive() -> None:
        from declaro_persistum.database import replication_loop

        db = ctx["db"]
        wanted = asyncio.Event()
        task = asyncio.create_task(replication_loop(db, wanted))
        try:
            _woken = [wanted.set() for _ in [pending] if pending]
            await asyncio.sleep(0.05)
        finally:
            db["closed"] = True
            wanted.set()
            await asyncio.wait_for(task, timeout=2)

    asyncio.run(_drive())


@when("the loop runs with nothing pending")
def _when_loop_idle(ctx):
    _run_the_loop(ctx, pending=False)


@when("the loop runs and a commit says there is work")
def _when_loop_woken(ctx):
    _run_the_loop(ctx, pending=True)


@then("no pass ran")
def _then_loop_quiet(ctx):
    assert ctx["order"] == [], f"the loop replicated with nothing pending: {ctx['order']}"


@then("a pass ran")
def _then_loop_ran(ctx):
    assert ctx["order"] == ["sent", "received"], (
        f"a commit set the event and the loop did not replicate: {ctx['order']}"
    )


@when("flush is awaited")
def _when_flush(ctx):
    from declaro_persistum.database import flush

    asyncio.run(flush(ctx["db"]))


@when("refresh is awaited")
def _when_refresh(ctx):
    from declaro_persistum.database import refresh

    asyncio.run(refresh(ctx["db"]))


@then("nothing was sent or received")
def _then_nothing_moved(ctx):
    assert ctx["order"] == [], f"a database with no primary reached for one anyway: {ctx['order']}"


@then("it received")
def _then_received(ctx):
    assert "received" in ctx["order"], f"refresh brought nothing down: {ctx['order']}"
