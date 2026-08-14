"""A caller can open every backend the package supports.

The appliers, inspectors and WRITERS table all cover postgresql, sqlite and
turso. Only `open_turso` existed, so two of the three could not be obtained at
all — `usage.md` promised "the same API spans PostgreSQL, SQLite and Turso"
while the route to two of them had been deleted with the classes.

Neither of these holds a connection. `reading` and `writing` open one for the
span of the block and close it, which is what Rule 12 asks for. The held
connection is a property of a Turso replica, not of a local database.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit


def test_there_is_an_opener_for_every_dialect_in_the_writers_table() -> None:
    import declaro_persistum as pkg
    from declaro_persistum.writers import WRITERS

    openers = {n for n in pkg.__all__ if n.startswith("open_")}
    assert openers == {f"open_{d}" for d in WRITERS}, (
        f"openers {sorted(openers)} do not cover {sorted(WRITERS)}; a dialect "
        f"with a writer, an applier and an inspector but no opener cannot be "
        f"reached by any caller"
    )


@pytest.mark.asyncio
async def test_a_sqlite_database_reads_back_what_it_wrote(tmp_path) -> None:
    from declaro_persistum import open_sqlite
    from declaro_persistum.database import close, reading, writing

    db = await open_sqlite(str(tmp_path / "app.db"), shutdown="exit_immediately", busy_timeout_s=5.0)
    assert db["dialect"] == "sqlite"

    async with writing(db) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        await conn.execute("INSERT INTO t (id, v) VALUES (1, 'kept')")
        await conn.commit()

    async with reading(db) as conn:
        cur = await conn.execute("SELECT v FROM t WHERE id = 1")
        assert (await cur.fetchone())[0] == "kept"

    await close(db)


@pytest.mark.asyncio
async def test_a_sqlite_database_holds_no_connection_between_blocks(
    tmp_path,
) -> None:
    """Rule 12. The resource lives in the block, not on the value."""
    from declaro_persistum import open_sqlite

    db = await open_sqlite(str(tmp_path / "app.db"), shutdown="exit_immediately", busy_timeout_s=5.0)

    for key, value in db.items():
        assert not hasattr(value, "execute"), (
            f"the database carries a live connection in {key!r}; a local "
            f"database opens one per block and closes it"
        )


def test_the_openers_take_no_dialect_argument() -> None:
    """The opener knows its own dialect; a caller cannot contradict it."""
    from declaro_persistum import open_sqlite

    params = inspect.signature(open_sqlite).parameters
    assert "dialect" not in params, (
        "open_sqlite takes a dialect, so a caller can ask for a sqlite "
        "database that claims to be something else"
    )
    assert params["shutdown"].default is inspect.Parameter.empty, (
        "shutdown has a default"
    )
