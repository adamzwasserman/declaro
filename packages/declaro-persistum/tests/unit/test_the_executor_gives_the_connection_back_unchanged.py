"""`execute_one` changed the caller's connection and never changed it back.

MEASURED 2026-08-20 on aiosqlite. After one call, every raw query the caller
makes on their OWN connection returns dicts instead of tuples, for the life of
that connection:

    _execute_fetch        row_factory None -> None
    _execute_fetch_one    row_factory None -> _dict_factory
    _execute_fetch_scalar row_factory None -> None
    _execute_update       row_factory None -> None

    after _execute_fetch_one, `SELECT id, n FROM t` returns {'id': 1, 'n': 'a'}

The connection belongs to the caller. It is passed in, not opened here, so
anything this module leaves on it is a change to somebody else's object made
without telling them.

THIS IS THE SYMPTOM `_first_value` WAS WRITTEN TO SURVIVE. Its docstring
records `row[0]` raising `KeyError: 0` and explains that a row "arrives as a
mapping from these drivers". It arrives as a mapping because of this line. On
a fresh connection a row is a tuple and `row[0]` works; run one `execute_one`
first and it is a dict and `row[0]` is a lookup for the key `0`. The behaviour
depended on what had run before it, which is why it read as a driver quirk.

THE FIX WAS ALREADY WRITTEN, THIRTY LINES ABOVE. `_fetch_aiosqlite` borrows
the factory and gives it back in a `finally`, under a comment that spells out
this exact consequence. `_fetch_one_aiosqlite` is the same handling for the
same driver and did not get it. That is the fifth time a fix has landed on one
copy of a pair in this package, so the tests below run over every executor
entry point rather than over the one that was broken.
"""

import os
import tempfile

import aiosqlite
import pytest

from declaro_persistum.query.executor import (
    _execute_fetch,
    _execute_fetch_one,
    _execute_fetch_scalar,
    _execute_update,
)

_ENTRY_POINTS = {
    "fetch": lambda conn: _execute_fetch(conn, "SELECT id FROM t", {}),
    "fetch_one": lambda conn: _execute_fetch_one(conn, "SELECT id FROM t", {}),
    "fetch_scalar": lambda conn: _execute_fetch_scalar(
        conn, "SELECT COUNT(*) AS c FROM t", {}
    ),
    "update": lambda conn: _execute_update(conn, "UPDATE t SET n='b'", {}),
}


def test_every_executor_entry_point_is_covered_here() -> None:
    """The floor. A fifth `_execute_*` must not join without this invariant.

    The defect being fixed was two functions doing one job and disagreeing, so
    a hand-written list of the ones that exist today is the same failure
    waiting. The names are read from the module, and asserted rather than only
    differenced: a scan that matched nothing would report no gap and read
    exactly like a scan that found everything.
    """
    import declaro_persistum.query.executor as executor

    found = {
        name
        for name in dir(executor)
        if name.startswith("_execute_") and callable(getattr(executor, name))
    }

    assert found == {
        "_execute_fetch",
        "_execute_fetch_one",
        "_execute_fetch_scalar",
        "_execute_update",
    }, f"the scan found {sorted(found)}; if it matched nothing it proves nothing"
    assert {f"_execute_{name}" for name in _ENTRY_POINTS} == found, (
        f"{sorted(found - {f'_execute_{n}' for n in _ENTRY_POINTS})} takes the "
        f"caller's connection with no check here that it hands it back"
    )


async def _with_table(conn: aiosqlite.Connection) -> None:
    await conn.execute("CREATE TABLE t (id INTEGER, n TEXT)")
    await conn.execute("INSERT INTO t VALUES (1,'a')")
    await conn.commit()


@pytest.mark.sqlite
@pytest.mark.parametrize("name", sorted(_ENTRY_POINTS), ids=sorted(_ENTRY_POINTS))
async def test_the_callers_row_factory_survives(name: str) -> None:
    """Whatever the caller had set is what the caller still has afterwards.

    Every entry point is covered rather than the one that was broken, because
    the defect was that two functions doing the same job disagreed. A test
    naming only `fetch_one` would pass again the next time the pair drifts the
    other way.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await _with_table(conn)
        before = conn.row_factory

        await _ENTRY_POINTS[name](conn)

        assert conn.row_factory is before
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.sqlite
@pytest.mark.parametrize("name", sorted(_ENTRY_POINTS), ids=sorted(_ENTRY_POINTS))
async def test_the_callers_own_query_still_returns_a_tuple(name: str) -> None:
    """The consequence, asserted where the caller would meet it.

    The check above reads an attribute; this one runs the query the caller
    would run next and looks at what comes back. They are not the same
    assertion: a future fix that swapped the factory for something else with
    the same identity would pass the first and fail this.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await _with_table(conn)

        await _ENTRY_POINTS[name](conn)
        cursor = await conn.execute("SELECT id, n FROM t")
        row = await cursor.fetchone()

        assert isinstance(row, tuple), (
            f"after {name}, the caller's own query returns {type(row).__name__}; "
            f"row[0] is now a lookup for the key 0"
        )
        assert row[0] == 1
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.sqlite
@pytest.mark.parametrize("name", sorted(_ENTRY_POINTS), ids=sorted(_ENTRY_POINTS))
async def test_a_factory_the_caller_chose_is_given_back(name: str) -> None:
    """Restoring must mean restoring, not resetting to None.

    A fix that assigned `None` in the `finally` would pass both tests above on
    a fresh connection, because a fresh connection's factory IS None. A caller
    who had set their own would silently lose it.
    """

    def caller_factory(cursor, row):  # noqa: ANN001, ANN202
        return {"chosen": row}

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await _with_table(conn)
        conn.row_factory = caller_factory

        await _ENTRY_POINTS[name](conn)

        assert conn.row_factory is caller_factory
    finally:
        await conn.close()
        os.unlink(path)
