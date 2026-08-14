"""Orphan recovery may only touch tables persistum created.

Reconstruction can die between DROP and RENAME, leaving a temp table and no
original. Recovery finds those and puts them back. It identifies them by name,
which is safe only while the name is one persistum owns: `_declaro_tmp_<table>_<8hex>`.

A user's own table is not persistum's to move or destroy.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

pytestmark = pytest.mark.precommit


class _Cursor:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def fetchall(self) -> list[Any]:
        return self._rows

    async def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


class _Conn:
    """A real sqlite3 database behind the async shape migrations expects."""

    def __init__(self, path: str) -> None:
        self._c = sqlite3.connect(path)

    async def execute(self, sql: str, params: tuple = ()) -> _Cursor:
        return _Cursor(list(self._c.execute(sql, params).fetchall()))

    async def commit(self) -> None:
        self._c.commit()

    def tables(self) -> set[str]:
        return {
            r[0]
            for r in self._c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }


async def _write_one(conn, sql, params):
    await conn.execute(sql, params)
    if hasattr(conn, "commit"):
        await conn.commit()


def _database(conn: _Conn):
    """A Database whose connect hands back the real sqlite3 connection.

    Recovery takes a Database now, not the deleted object. Building one here
    rather than a fake of the old shape is what keeps this test honest about
    the interface it is exercising.
    """
    from declaro_persistum.database import new_database

    async def connect(db):
        return conn

    async def close_connection(c):
        return None

    async def nothing(db):
        return True

    async def nothing_down(db):
        return None

    async def release(db):
        return None

    async def sleep(_s):
        return None

    return new_database(
        path=":memory:",
        dialect="sqlite",
        journal_mode="wal",
        busy_timeout_s=5.0,
        primary=None,
        token=None,
        connect=connect,
        close_connection=close_connection,
        serialise=None,
        shutdown="exit_immediately",
        write_one=_write_one,
        replicate_once=nothing,
        refresh_once=nothing_down,
        release=release,
        sleep=sleep,
        retry_delay_s=0.001,
    )


async def _recover(conn: _Conn) -> int:
    from declaro_persistum.migrations import _recover_orphaned_tmp_tables

    return await _recover_orphaned_tmp_tables(_database(conn))


@pytest.mark.asyncio
async def test_a_user_table_ending_in_new_is_not_renamed(tmp_path):
    """`customers_new` with no `customers` must stay where it is."""
    conn = _Conn(str(tmp_path / "a.db"))
    await conn.execute("CREATE TABLE customers_new (id INTEGER PRIMARY KEY)")
    await conn.commit()

    await _recover(conn)

    assert "customers_new" in conn.tables(), (
        "a user's table was renamed because its name ends in _new; persistum "
        "never created it and has no claim on that name"
    )
    assert "customers" not in conn.tables()


@pytest.mark.asyncio
async def test_a_user_table_ending_in_new_is_not_dropped(tmp_path):
    """The destructive case: both tables present.

    Staging a new version of a table beside the old one is an ordinary thing to
    do. Recovery treated the pair as leftover junk and dropped the staged one,
    with a warning log and nothing else.
    """
    conn = _Conn(str(tmp_path / "b.db"))
    await conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
    await conn.execute("CREATE TABLE customers_new (id INTEGER PRIMARY KEY, v TEXT)")
    await conn.execute("INSERT INTO customers_new (id, v) VALUES (1, 'keep me')")
    await conn.commit()

    await _recover(conn)

    assert "customers_new" in conn.tables(), (
        "a user's staged table was DROPPED because its name ends in _new"
    )
    rows = await conn.execute("SELECT v FROM customers_new")
    assert await rows.fetchall() == [("keep me",)], "the rows are gone"


@pytest.mark.asyncio
async def test_our_own_orphan_is_still_recovered(tmp_path):
    """The behaviour that must survive: a real orphan goes back."""
    conn = _Conn(str(tmp_path / "c.db"))
    await conn.execute(
        "CREATE TABLE _declaro_tmp_orders_a1b2c3d4 (id INTEGER PRIMARY KEY)"
    )
    await conn.commit()

    recovered = await _recover(conn)

    assert recovered == 1, "a genuine orphan was not recovered"
    assert "orders" in conn.tables()
    assert "_declaro_tmp_orders_a1b2c3d4" not in conn.tables()
