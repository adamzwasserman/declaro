"""A drainer must not know which engine it is writing to.

`drainer` takes a `Database`, a type that exists to hold engine differences as
data — connect, close_connection, replicate_once, refresh_once, release, sleep
are all injected for exactly that reason. Then it reached past the injection and
wrote three engine assumptions inline:

    await conn.execute("BEGIN CONCURRENT")            Turso's statement
    await conn.execute(write["sql"], write["params"]) DB-API's params-as-tuple
    await conn.commit()                               DB-API's commit

Measured 2026-08-13, a crew of 1 against each engine:

    sqlite crew:    near "CONCURRENT": syntax error
    postgres crew:  syntax error at or near "CONCURRENT"

So the crew was Turso-only while its docstring said "local databases only" and
its signature said `Database`. The name, the type and the prose all claimed
general; one line decided otherwise.

THE CHOICE IS RESOLVED IN A DISPATCH TABLE AND CARRIED ON THE VALUE. The table
says how each engine writes; the Database says which engine it is. Resolved once
at open, not per write — the write path runs on every request and a lookup there
would be a branch for a value that cannot change while the database is open.

An application may hold a Postgres database and a Turso replica at the same
time, so this can never be module-level state.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from declaro_persistum.database import Database, new_database

pytestmark = pytest.mark.precommit


def test_the_database_carries_how_to_write() -> None:
    assert "write_one" in Database.__annotations__, (
        "a Database cannot say how its engine writes, so the crew has to guess"
    )
    params = inspect.signature(new_database).parameters
    assert params["write_one"].default is inspect.Parameter.empty, (
        "write_one has a default, so an engine nobody chose would be applied "
        "to a database nobody checked"
    )


def test_there_is_a_dispatch_table_for_every_supported_engine() -> None:
    from declaro_persistum.writers import WRITERS

    assert set(WRITERS) == {"turso", "sqlite", "postgresql"}, (
        f"the table covers {sorted(WRITERS)}; the package ships appliers and "
        f"inspectors for turso, sqlite and postgresql"
    )


def test_the_drainer_names_no_engine() -> None:
    """The whole point. No SQL, no commit, no parameter convention."""
    from declaro_persistum import crew

    body = inspect.getsource(crew.drainer)
    # `execute` alone is the callback drain() calls, not engine knowledge.
    # What matters is whether the drainer speaks to the connection itself.
    for tell in ("BEGIN CONCURRENT", "BEGIN IMMEDIATE", ".commit()",
                 "conn.execute(", "transaction()"):
        assert tell not in body, (
            f"drainer still contains {tell!r}; it is deciding for one engine "
            f"while accepting a Database that could be any of them"
        )


@pytest.mark.asyncio
async def test_a_drainer_runs_whatever_writer_it_was_given() -> None:
    """Two databases, two writers, one drainer implementation."""
    from declaro_persistum.crew import start_crew, stop_crew
    from declaro_persistum.write_queue import collect, deposit, new_room

    seen: list[str] = []

    def _recording(label: str):
        async def write_one(conn, sql, params):
            seen.append(f"{label}:{sql}")

        return write_one

    async def connect(db):
        return object()

    async def close_connection(conn):
        return None

    async def noop(db):
        return True

    async def noop_none(db):
        return None

    def _db(label: str) -> Database:
        return new_database(
            path=f"/tmp/{label}.db",
            dialect="sqlite",
            journal_mode="wal",
            busy_timeout_s=5.0,
            primary=None,
            token=None,
            connect=connect,
            close_connection=close_connection,
            serialise=None,
            shutdown="exit_immediately",
            write_one=_recording(label),
            replicate_once=noop,
            refresh_once=noop_none,
            release=noop_none,
            sleep=asyncio.sleep,
            retry_delay_s=0.001,
        )

    retry = {"attempts": 1, "base_delay_s": 0.0, "max_delay_s": 0.0}
    for label in ("pg", "turso"):
        room = new_room()
        crew = await start_crew(room, _db(label), size=1, retry=retry, idle_s=0.05)
        ticket = deposit(room, {"sql": f"INSERT {label}", "params": ()})
        receipt = await collect(room, ticket)
        await stop_crew(crew)
        assert not receipt.get("error"), receipt.get("error")

    assert seen == ["pg:INSERT pg", "turso:INSERT turso"], (
        f"the drainer did not use the writer each database carried: {seen}"
    )
