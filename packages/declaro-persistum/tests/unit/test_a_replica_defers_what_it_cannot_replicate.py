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
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit


def _plan(current: dict, target: dict):
    plan = diff(current, target, dialect="turso", decisions=NOTHING)
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
async def test_a_commit_wakes_the_loop() -> None:
    """The floor. A loop that never wakes passes the test above.

    IT USED TO SUBSTITUTE `database.replicate` FOR A FAKE and watch the fake
    run. Two things were wrong with that. It swapped a real function for one
    that only records being called, which proves the loop called something
    rather than that it did anything; and on 2026-08-22 it stopped working
    silently, because `_IF_IDLE` holds the real `replicate` from import and
    never reads the module attribute the patch rebinds. The patch was ignored,
    the real function ran against this test's data, and the failure arrived as
    a KeyError from somewhere else entirely.

    The loop clears `wanted` before each pass, on purpose, so that a signal
    arriving mid-pass is not discarded. That makes the cleared event the
    observable: a loop still parked in `wanted.wait()` leaves it set. Nothing
    is substituted, and the real dispatch runs.
    """
    import tempfile

    from declaro_persistum import close, open_turso
    from declaro_persistum.database import replication_loop
    from declaro_persistum.types import NOTHING

    with tempfile.TemporaryDirectory() as folder:
        # No primary and no token: this is a local file, which is what a
        # database with nothing to replicate to looks like.
        db = await open_turso(
            f"{folder}/wake.db",
            primary=NOTHING,
            token=NOTHING,
            shutdown="exit_immediately",
        )
        wanted = asyncio.Event()
        task = asyncio.create_task(replication_loop(db, wanted))
        try:
            wanted.set()
            woke = [
                None
                for _ in range(200)
                if not wanted.is_set() or await asyncio.sleep(0.005)
            ]
            assert not wanted.is_set(), (
                f"the loop never woke after {len(woke)} turns: `wanted` is "
                f"still set, so nothing consumed the signal a commit sends"
            )
        finally:
            db["closed"] = True
            wanted.set()
            await asyncio.wait_for(task, timeout=2)
            await close(db)




