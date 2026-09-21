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
    writes_in_flight,
    writing,
)

scenarios("../features/lifecycle/lifecycle_catch_up.feature")


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
        engine="turso",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary=primary,
        token=None,
        connect=_never,
        close_connection=no_passes,
        for_ddl=writing,
        in_flight={"count": waiting},
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


@then(parsers.parse("writes_in_flight counts {expected:d}"))
def _then_waiting(ctx, expected):
    assert writes_in_flight(ctx["db"]) == expected


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


@when("no_passes is asked how many passes ran")
def _when_no_passes(ctx):
    ctx["answer"] = asyncio.run(no_passes(ctx["db"]))


@when("already_delivered is asked whether the primary has it all")
def _when_already_delivered(ctx):
    from declaro_persistum.database import already_delivered

    ctx["answer"] = asyncio.run(already_delivered(ctx["db"]))


@then("it answers yes")
def _then_yes(ctx):
    assert ctx["answer"] is True, (
        f"answered {ctx['answer']!r}; an engine with no primary has delivered "
        f"everything vacuously, and saying otherwise makes flush loop forever "
        f"waiting for a delivery nothing will report"
    )


@given("a signal handler this module installed")
def _given_a_handler(ctx):
    import signal

    ctx["signal"] = signal.SIGTERM
    ctx["previous"] = signal.getsignal(signal.SIGTERM)


@when("it is put back")
def _when_put_back(ctx):
    from declaro_persistum.shutdown import _restored

    ctx["answer"] = _restored(ctx["signal"], ctx["previous"])


@when("it is put back somewhere the platform refuses")
def _when_put_back_off_thread(ctx):
    """OFF THE MAIN THREAD, WHICH IS A REAL REFUSAL, NOT AN INVENTED ONE.

    `signal.signal` raises `ValueError: signal only works in main thread of
    the main interpreter`. That is the case the suppression exists for.
    """
    import threading

    from declaro_persistum.shutdown import _restored

    answers: list[bool] = []

    def _off_the_main_thread() -> None:
        answers.append(_restored(ctx["signal"], ctx["previous"]))

    thread = threading.Thread(target=_off_the_main_thread)
    thread.start()
    thread.join()
    ctx["answer"] = answers[0]


@then("it reports one handler restored")
def _then_restored(ctx):
    assert ctx["answer"] is True, ctx["answer"]


@then("it reports no handler restored")
def _then_not_restored(ctx):
    assert ctx["answer"] is False, (
        "the platform refused to put the handler back and this reported "
        "success, so a caller counting them is told one came off that did not"
    )


@given("a database that trapped the shutdown signal")
def _given_trapped(ctx):
    import signal

    from declaro_persistum.shutdown import trap_shutdown

    ctx["previous"] = signal.getsignal(signal.SIGTERM)
    ctx["db"] = trap_shutdown(
        {**_a_database(ctx, primary="libsql://x", waiting=0, closed=False), "shutdown": "replicate"}
    )


@when("the handlers are put back somewhere the platform refuses")
def _when_untrap_off_thread(ctx):
    """Off the main thread, where `signal.signal` really does refuse."""
    import signal
    import threading

    outcome: list[Any] = []

    def _off_the_main_thread() -> None:
        try:
            outcome.append(("returned", ctx["db"]["untrap"]()))
        except Exception as refused:  # noqa: BLE001 - the refusal is the answer
            outcome.append(("refused", refused))

    thread = threading.Thread(target=_off_the_main_thread)
    thread.start()
    thread.join()
    ctx["outcome"] = outcome[0]
    signal.signal(signal.SIGTERM, ctx["previous"])


@then("it refuses, naming the handler still installed")
def _then_refuses_naming(ctx):
    kind, value = ctx["outcome"]
    assert kind == "refused", (
        f"it returned {value!r}. A handler left installed holds the database "
        f"that was just released, and a SIGTERM arriving later drives it."
    )
    assert "SIGTERM" in str(value) or "handler" in str(value).lower(), value
