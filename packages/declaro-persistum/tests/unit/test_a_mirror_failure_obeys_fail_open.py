"""What a replica failure means is a decision, and neither answer was run.

MEASURED 2026-08-18 with `coverage json` over the whole suite. `mirror.py`
sits at 82 percent, and the four statements missing from `compare` and the
five missing from `parallel_write` are the same four lines twice:

    except Exception:          <- never raised
        if not m["fail_open"]: <- never evaluated
            raise              <- never taken
        ...record and carry on <- never taken

So the entire replica-failure path was unrun, in both directions, in the two
functions that exist to survive one. `fail_open` decides whether a cutover
takes production down when the replica is sick, and nothing had ever made a
replica sick to find out.

`begin_cutover` defaults that parameter to True, which selects the swallowing
branch. The default sat on the untested side.

HOW THE REPLICA IS BROKEN HERE. Its table is dropped, so the read and the
write both fail at the engine, for the reason a real replica fails: it does
not have what the primary has. Nothing is patched and nothing is mocked. A
`MagicMock` raising on call would exercise the same four lines while proving
nothing about what a database does.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite, writing
from declaro_persistum.mirror import compare, mirror, parallel_write

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

CREATE = 'CREATE TABLE "widgets" (id INTEGER PRIMARY KEY, name TEXT)'
INSERT = 'INSERT INTO "widgets" (id, name) VALUES (?, ?)'
SELECT = 'SELECT id, name FROM "widgets" ORDER BY id'


async def _database(name: str) -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), name),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _run(database: Any, sql: str, params: Any = ()) -> None:
    async with writing(database) as connection:
        await connection.execute(sql, params)
        await connection.commit()


async def _pair(*, replica_has_the_table: bool) -> tuple[Any, Any]:
    """A primary with a row, and a replica that may or may not be usable."""
    primary, replica = await _database("p.db"), await _database("r.db")
    await _run(primary, CREATE)
    await _run(primary, INSERT, (1, "first"))
    if replica_has_the_table:
        await _run(replica, CREATE)
        await _run(replica, INSERT, (1, "first"))
    return primary, replica


def _mirror(primary: Any, replica: Any, *, fail_open: bool) -> Any:
    return mirror(
        primary=primary,
        replica=replica,
        fail_open=fail_open,
        compare_on_read=True,
    )


# ---------------------------------------------------------------------------
# compare: reading through a sick replica
# ---------------------------------------------------------------------------


async def test_a_read_raises_when_the_mirror_must_not_fail_open() -> None:
    """fail_open=False means the caller asked to hear about it."""
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        with pytest.raises(Exception) as caught:
            await compare(_mirror(primary, replica, fail_open=False), SELECT, ())

        assert "widgets" in str(caught.value), caught.value
    finally:
        await close(primary)
        await close(replica)


async def test_a_read_still_answers_when_the_mirror_may_fail_open() -> None:
    """fail_open=True means the cutover must not take production down."""
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        rows, after = await compare(_mirror(primary, replica, fail_open=True), SELECT, ())

        assert rows == [(1, "first")], rows
        assert after["divergences"] == [], after["divergences"]
    finally:
        await close(primary)
        await close(replica)


async def test_a_disagreement_is_recorded_rather_than_raised() -> None:
    """A replica that answers and answers differently is not a failure.

    It is the finding the cutover exists to produce, so it goes in the list
    and the primary's answer is still returned.
    """
    primary, replica = await _pair(replica_has_the_table=True)
    await _run(replica, 'UPDATE "widgets" SET name = ? WHERE id = ?', ("other", 1))
    try:
        rows, after = await compare(_mirror(primary, replica, fail_open=False), SELECT, ())

        assert rows == [(1, "first")], rows
        assert len(after["divergences"]) == 1, after["divergences"]
        assert after["divergences"][0]["sql"] == SELECT
    finally:
        await close(primary)
        await close(replica)


async def test_agreement_records_nothing() -> None:
    """The control. A mirror that agrees must stay quiet."""
    primary, replica = await _pair(replica_has_the_table=True)
    try:
        rows, after = await compare(_mirror(primary, replica, fail_open=False), SELECT, ())

        assert rows == [(1, "first")] and after["divergences"] == []
    finally:
        await close(primary)
        await close(replica)


async def test_the_replica_is_not_read_when_comparison_is_off() -> None:
    """compare_on_read=False must not touch the replica at all.

    The replica has no table here, so any read of it raises. Returning the
    primary's rows is the proof it was never asked.
    """
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        rows, after = await compare(
            mirror(
                primary=primary,
                replica=replica,
                fail_open=False,
                compare_on_read=False,
            ),
            SELECT,
            (),
        )

        assert rows == [(1, "first")], rows
        assert after["divergences"] == []
    finally:
        await close(primary)
        await close(replica)


# ---------------------------------------------------------------------------
# parallel_write: writing through a sick replica
# ---------------------------------------------------------------------------


async def test_a_write_raises_when_the_mirror_must_not_fail_open() -> None:
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        with pytest.raises(Exception) as caught:
            await parallel_write(_mirror(primary, replica, fail_open=False), INSERT, (2, "second"))

        assert "widgets" in str(caught.value), caught.value
    finally:
        await close(primary)
        await close(replica)


async def test_a_failed_replica_write_is_recorded_when_failing_open() -> None:
    """The primary took it. The replica did not. Both facts are kept."""
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        after = await parallel_write(
            _mirror(primary, replica, fail_open=True), INSERT, (2, "second")
        )

        assert len(after["divergences"]) == 1, after["divergences"]
        assert after["divergences"][0] == {
            "sql": INSERT,
            "primary": "ok",
            "replica": "failed",
        }
    finally:
        await close(primary)
        await close(replica)


async def test_the_primary_keeps_the_write_the_replica_refused() -> None:
    """The floor under failing open.

    Swallowing the replica's failure is only acceptable if the primary really
    has the row. Recording a divergence while losing the write would be the
    worst of both.
    """
    primary, replica = await _pair(replica_has_the_table=False)
    try:
        await parallel_write(_mirror(primary, replica, fail_open=True), INSERT, (2, "second"))

        async with writing(primary) as connection:
            cursor = await connection.execute(SELECT)
            rows = await cursor.fetchall()

        assert rows == [(1, "first"), (2, "second")], rows
    finally:
        await close(primary)
        await close(replica)


async def test_a_healthy_pair_records_nothing_and_writes_both() -> None:
    """The control. Both databases must actually receive the write."""
    primary, replica = await _pair(replica_has_the_table=True)
    try:
        after = await parallel_write(
            _mirror(primary, replica, fail_open=True), INSERT, (2, "second")
        )

        assert after["divergences"] == []
        for database in (primary, replica):
            async with writing(database) as connection:
                cursor = await connection.execute(SELECT)
                assert await cursor.fetchall() == [(1, "first"), (2, "second")]
    finally:
        await close(primary)
        await close(replica)
