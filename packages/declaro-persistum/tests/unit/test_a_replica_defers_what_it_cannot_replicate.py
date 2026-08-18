"""What a replica defers, and when it replicates. Two untested functions.

`partition_replica_operations` decides which migration operations an embedded
replica may apply locally. A reconstruction cannot travel through the
replication engine -- it drops and recreates a table -- so those are deferred
to `declaro migrate-remote` and REPORTED, because a caller who is not told
what was skipped mistakes a partial migration for a whole one.

`replication_loop` decides when a replica pushes. Nothing is on a clock: a
timer wakes whether or not anything is pending, takes the connection to find
out, and still leaves a fresh write sitting for a whole interval. It waits on
an event a commit sets.

THE OPERATIONS COME FROM THE DIFFER, not from tuples I write, because what
counts as a reconstruction is decided by `requires_reconstruction` and the
differ is what produces the shapes it reads.
"""

from __future__ import annotations

import asyncio

import pytest

from declaro_persistum.differ import diff
from declaro_persistum.migrations import partition_replica_operations

pytestmark = pytest.mark.precommit


def _plan(current: dict, target: dict):
    plan = diff(current, target, dialect="turso")
    return plan["operations"], plan["execution_order"]


# ---------------------------------------------------------------------------
# What a replica may apply
# ---------------------------------------------------------------------------


def test_creating_a_table_is_safe_on_a_replica() -> None:
    operations, order = _plan({}, {"t": {"columns": {"c": {"type": "text"}}}})

    safe, skipped = partition_replica_operations(operations, order)

    assert safe == order
    assert skipped == []


def test_a_reconstruction_is_deferred_rather_than_attempted() -> None:
    """Changing a column type rebuilds the table, which cannot replicate."""
    operations, order = _plan(
        {"t": {"columns": {"c": {"type": "text"}}}},
        {"t": {"columns": {"c": {"type": "integer"}}}},
    )

    safe, skipped = partition_replica_operations(operations, order)

    assert safe == []
    assert [s["op"] for s in skipped] == ["alter_column"]


def test_a_deferred_operation_says_which_table_it_was_on() -> None:
    """"Something was skipped" is not actionable. The table name is."""
    operations, order = _plan(
        {"t": {"columns": {"c": {"type": "text"}}}},
        {"t": {"columns": {"c": {"type": "integer"}}}},
    )

    _safe, skipped = partition_replica_operations(operations, order)

    assert skipped[0]["table"] == "t"


def test_the_safe_operations_keep_their_dependency_order() -> None:
    """Filtering must not reorder. A CREATE INDEX before its table fails."""
    operations, order = _plan(
        {},
        {
            "t": {
                "columns": {"c": {"type": "text"}},
                "indexes": {"i": {"columns": ["c"]}},
            }
        },
    )

    safe, _skipped = partition_replica_operations(operations, order)

    assert safe == [i for i in order if i in safe], "the order was rearranged"
    ops = [operations[i]["op"] for i in safe]
    assert ops.index("create_table") < ops.index("add_index")


def test_a_mixed_plan_is_split_and_nothing_is_lost() -> None:
    """Every operation is either applied or reported. None may vanish."""
    operations, order = _plan(
        {"t": {"columns": {"c": {"type": "text"}}}},
        {
            "t": {"columns": {"c": {"type": "integer"}}},
            "fresh": {"columns": {"id": {"type": "text"}}},
        },
    )

    safe, skipped = partition_replica_operations(operations, order)

    assert len(safe) + len(skipped) == len(order), (
        f"{len(order)} operations went in and {len(safe) + len(skipped)} came "
        f"out, so one was neither applied nor reported"
    )


def test_an_empty_plan_defers_nothing() -> None:
    assert partition_replica_operations([], []) == ([], [])


# ---------------------------------------------------------------------------
# When a replica replicates
# ---------------------------------------------------------------------------


# THE RECORD BELOW IS BUILT FROM WHAT THE LOOP READS, not from what I first
# assumed it reads. My first version gave it `stopping` and `replicate_once`,
# and the loop reads `closed`, `primary` and `serialise` and calls
# `replicate`. It raised `KeyError: 'closed'`, which is the seventh time this
# session that a hand-made input tested my idea of a shape.


def _replica(push, *, waiting: int = 0) -> dict:
    """The smallest Database `replication_loop` actually reads."""
    return {
        "closed": False,
        "primary": "libsql://example",
        "serialise": {"waiting": waiting},
        "push": push,
        "pull": push,
        "path": "x.db",
    }


@pytest.mark.asyncio
async def test_the_loop_waits_rather_than_polling(monkeypatch) -> None:
    """Nothing is on a clock. With no event set, nothing should happen.

    A timer would wake anyway, take the connection to find out there was
    nothing to do, and still leave the next write waiting for an interval.
    """
    from declaro_persistum import database
    from declaro_persistum.database import replication_loop

    passes = 0

    async def _replicate(_db):
        nonlocal passes
        passes += 1
        return True

    monkeypatch.setattr(database, "replicate", _replicate)

    db = _replica(None)
    wanted = asyncio.Event()
    task = asyncio.create_task(replication_loop(db, wanted))
    try:
        await asyncio.sleep(0.05)
        assert passes == 0, f"the loop ran {passes} passes with nothing pending"
    finally:
        db["closed"] = True
        wanted.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_a_commit_wakes_the_loop(monkeypatch) -> None:
    """The floor. A loop that never replicates passes the test above."""
    from declaro_persistum import database
    from declaro_persistum.database import replication_loop

    ran = asyncio.Event()

    async def _replicate(_db):
        ran.set()
        return True

    monkeypatch.setattr(database, "replicate", _replicate)

    db = _replica(None)
    wanted = asyncio.Event()
    task = asyncio.create_task(replication_loop(db, wanted))
    try:
        wanted.set()
        await asyncio.wait_for(ran.wait(), timeout=2)
    finally:
        db["closed"] = True
        wanted.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.asyncio
async def test_a_waiting_writer_defers_the_pass(monkeypatch) -> None:
    """Replication holds the connection every write holds.

    A writer blocked on the serialise lock is a request paying for the round
    trip, so the pass yields to it.
    """
    from declaro_persistum import database
    from declaro_persistum.database import replicate_if_idle

    async def _replicate(_db):
        raise AssertionError("replicated while a writer was waiting")

    monkeypatch.setattr(database, "replicate", _replicate)

    assert await replicate_if_idle(_replica(None, waiting=1)) is False


@pytest.mark.asyncio
async def test_a_database_with_no_primary_never_replicates(monkeypatch) -> None:
    """There is nowhere to send anything."""
    from declaro_persistum import database
    from declaro_persistum.database import replicate_if_idle

    async def _replicate(_db):
        raise AssertionError("replicated with no primary")

    monkeypatch.setattr(database, "replicate", _replicate)

    db = _replica(None)
    db["primary"] = None

    assert await replicate_if_idle(db) is False
