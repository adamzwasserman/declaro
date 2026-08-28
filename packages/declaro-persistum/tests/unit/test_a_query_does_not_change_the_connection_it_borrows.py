"""A connection this module is handed goes back exactly as it came.

WHAT WAS WRONG. Fetching rows as dicts meant setting `connection.row_factory`,
and the code saved the old value and restored it in a `finally`. That is a
read, an await, and then a write to state the caller owns, which is a window
two callers interleave in. The second query saves what the first has already
set, the first restores the driver default, and the second restores the dict
factory. The connection is then changed for good.

MEASURED 2026-08-25. Six concurrent queries on one connection left
`row_factory` set to `_dict_factory` permanently, so every later raw
`conn.execute(...)` the caller made returned dicts where it expected tuples.

The comment that stood in that code described exactly this as a past defect:
on a fresh connection a row is a tuple and `row[0]` works, and after any other
query `row[0]` is a lookup for the key `0`. Restoring in a `finally` fixed the
one-query case and left the concurrent one, because a save and a restore is
not a guard when anything can run between them.

A CURSOR IS NOT SHARED. Setting the factory there is the same operation with
nothing to race over, and the connection is never written to at all.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from declaro_persistum.query.executor import execute, execute_one
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

_ROWS = {"sql": "SELECT * FROM t", "params": []}
_ONE = {"sql": "SELECT * FROM t LIMIT 1", "params": []}


async def _a_connection_with_rows():
    import aiosqlite

    path = str(Path(tempfile.mkdtemp()) / "t.db")
    connection = await aiosqlite.connect(path)
    await connection.execute("CREATE TABLE t (id INTEGER, name TEXT)")
    await connection.executemany("INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(20)])
    await connection.commit()
    return connection


async def test_concurrent_queries_leave_the_row_factory_alone() -> None:
    """Six at once, which is what made the restore restore the wrong value."""
    connection = await _a_connection_with_rows()
    try:
        before = connection.row_factory

        await asyncio.gather(
            *[execute(_ROWS, connection, row_factory=NOTHING) for _ in range(6)],
            *[execute_one(_ONE, connection, row_factory=NOTHING) for _ in range(6)],
        )

        assert connection.row_factory is before, (
            f"the connection came back changed: {connection.row_factory}. "
            f"Every raw query the caller makes now returns dicts."
        )
    finally:
        await connection.close()


async def test_the_caller_still_gets_what_the_driver_gives() -> None:
    """The consequence, stated as the caller sees it."""
    connection = await _a_connection_with_rows()
    try:
        await asyncio.gather(*[execute(_ROWS, connection, row_factory=NOTHING) for _ in range(4)])

        cursor = await connection.execute("SELECT id FROM t LIMIT 1")
        row = await cursor.fetchone()

        assert isinstance(row, tuple), (
            f"a raw query on the caller's own connection returned {type(row)}, "
            f"so `row[0]` is a key lookup rather than the first column"
        )
    finally:
        await connection.close()


async def test_the_rows_are_still_dicts() -> None:
    """The thing the factory was for, which must survive the fix."""
    connection = await _a_connection_with_rows()
    try:
        rows = await execute(_ROWS, connection, row_factory=NOTHING)

        assert rows and isinstance(rows[0], dict), rows[:1]
        assert set(rows[0]) == {"id", "name"}, rows[0]
    finally:
        await connection.close()
