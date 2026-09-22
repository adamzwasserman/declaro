"""`query_run_statement`: one statement, on the connection the database holds.

THE FIRST FUNCTION HERE THAT TOUCHES THE OUTSIDE. Everything else in this
package is a function over data: hand it the same arguments and it writes the
same answer on a machine with no database on it. This one reaches a driver,
which is its whole job, and `boundary_out fn query_run_statement` is the card
that says so.

IT NEVER PUTS A VALUE INTO THE TEXT, AND THIS IS THE LAST PLACE THAT COULD.
A `Statement` is text carrying placeholders and the values that go in them,
kept apart. Both go to the driver and the driver binds them. `fn
compile_statement` names the values `p1`, `p2` and so on in the order it bound
them, because asyncpg reads `$1` and aiosqlite and pyturso read `?`, and all
three of those are positional. So the values travel as `list(parameters
.values())` and the names are for a reader.

WHY IT ANSWERS TWO SHAPES. A question hands back rows and a change hands back
how many rows it touched, and which one a caller gets follows from the
statement rather than from a flag passed in here. The driver is what knows:
aiosqlite and pyturso give a cursor whose `description` holds the columns a
select returns and nothing at all otherwise, and asyncpg's prepared statement
answers the same question with `get_attributes`.

WHY THERE IS A ROW PER ENGINE AND NO BRANCH. Three drivers are three rows. Each
one is asked in its own words, catches its own errors, and commits if its own
driver needs it; asyncpg outside a transaction does not. An engine with no row
comes back as a fault naming what arrived, never as a fallback answering for a
driver nobody wrote a row for.

THE CONNECTION IS A HANDLE. It is passed to the driver and never asked what
engine it belongs to. `docs/architecture.md` refuses to work an engine out from
a connection, and the `Database` carries the engine already, decided once when
it was opened.

WHERE THE STATEMENT RUNS, AND WHY IT MATTERS. On `db["connection"]` and never on
a writer from `db["writers"]`. `docs/architecture.md` keeps DDL off the crew,
because a table created on one MVCC connection is invisible to every other one.

A FAILURE COMES BACK IN THE RETURN. A statement the engine refuses is a `Fault`
of kind `refused` carrying the engine's own words, so a reader sees what the
engine objected to rather than what this module guessed it meant. Each row
catches its own driver's error type and nothing wider: a bug in this package
raises, where a driver's refusal is an answer.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any, get_args

from declaro_persistum.types import (
    Connection,
    Database,
    Engine,
    Fault,
    Rows,
    Statement,
    Value,
)

_THE_THREE_ENGINES = get_args(Engine)


def _values(statement: Statement) -> list[Value]:
    """The values, in the order the compiler bound them.

    POSITIONAL, WHATEVER THE DICT SUGGESTS. All three drivers count their
    placeholders, so the names are a reader's handle on the order and the order
    is what goes to the driver.
    """
    return list(statement["parameters"].values())


def _rows_from(description: Any, fetched: Any) -> Rows:
    """What a driver handed back, as column names to values."""
    names = [column[0] for column in description]
    return [dict(zip(names, row, strict=True)) for row in fetched]


def _refused(engine_said: BaseException) -> Fault:
    """What the engine said, on its way back to the caller as a value."""
    return {
        "kind": "refused",
        "what_went_wrong": f"The engine refused the statement: {engine_said}",
        "what_to_do_instead": (
            "Read what the engine said above and change the question or the "
            "models it is asked against. A statement is refused by the engine "
            "rather than by this library, so nothing here can make it run."
        ),
    }


# --- one row per engine ------------------------------------------------------
#
# EACH ROW IS THE DRIVER'S OWN MANUAL AND NOTHING ELSE. They look alike from a
# distance and they are not one function: aiosqlite is awaited, pyturso is not,
# and asyncpg answers a read and a change through two different calls. Folding
# them together would make the next difference between two drivers an edit to
# the shape of this table rather than an edit to one row.


async def _on_sqlite(connection: Connection, statement: Statement) -> Rows | int | Fault:
    """aiosqlite. The cursor holds the columns of a read and none of a change."""
    try:
        cursor = await connection.execute(statement["text"], _values(statement))
        answer = await _WHAT_AN_AWAITED_CURSOR_HOLDS[cursor.description is not None](cursor)
        # SQLITE OPENS A TRANSACTION FOR A CHANGE AND THIS IS THE ONLY DOOR IT
        # WENT THROUGH, so the commit belongs to whoever ran it.
        await connection.commit()
        return answer
    except sqlite3.Error as engine_said:
        return _refused(engine_said)


async def _awaited_rows(cursor: Any) -> Rows:
    return _rows_from(cursor.description, await cursor.fetchall())


async def _awaited_count(cursor: Any) -> int:
    """How many rows it touched. A driver says -1 where the question does not
    apply, and a statement that touched no rows touched none."""
    return max(int(cursor.rowcount), 0)


_WHAT_AN_AWAITED_CURSOR_HOLDS: dict[bool, Callable[..., Awaitable[Rows | int]]] = {
    True: _awaited_rows,
    False: _awaited_count,
}


async def _on_postgresql(connection: Connection, statement: Statement) -> Rows | int | Fault:
    """asyncpg. Preparing the text is what asks the server which it is.

    `get_attributes` is empty for everything that returns no columns, which is
    every change and every schema statement. Preparing does not run it.
    """
    import asyncpg  # type: ignore[import-untyped]

    try:
        prepared = await connection.prepare(statement["text"])
        return await _WHAT_POSTGRESQL_PREPARED[prepared.get_attributes() != ()](
            connection, prepared, statement
        )
    except asyncpg.PostgresError as engine_said:
        return _refused(engine_said)


async def _postgresql_rows(_connection: Any, prepared: Any, statement: Statement) -> Rows:
    return [dict(record) for record in await prepared.fetch(*_values(statement))]


async def _postgresql_count(connection: Any, _prepared: Any, statement: Statement) -> int:
    """PostgreSQL answers a change with a tag: `INSERT 0 3`, `UPDATE 2`,
    `CREATE TABLE`. The number is the last word where there is one."""
    tag = await connection.execute(statement["text"], *_values(statement))
    counted = tag.rpartition(" ")[2]
    return int({True: counted, False: "0"}[counted.isdigit()])


_WHAT_POSTGRESQL_PREPARED: dict[bool, Callable[..., Awaitable[Rows | int]]] = {
    True: _postgresql_rows,
    False: _postgresql_count,
}


async def _on_turso(connection: Connection, statement: Statement) -> Rows | int | Fault:
    """pyturso, on a local database. Its cursor answers now rather than later.

    THE REPLICATED SHAPE IS NOT THIS SHAPE. `turso.aio.sync` hands back a
    connection whose `execute` and `fetchall` are awaitable, and this row does
    not await them. `boundary_out fn session_open_connection` is handed a target
    and a count and no primary, so the connection it can open is the local one.
    """
    import turso

    try:
        cursor = connection.execute(statement["text"], _values(statement))
        answer = _WHAT_A_CURSOR_HOLDS[cursor.description is not None](cursor)
        connection.commit()
        return answer
    except turso.Error as engine_said:
        return _refused(engine_said)


def _turso_rows(cursor: Any) -> Rows:
    return _rows_from(cursor.description, cursor.fetchall())


def _turso_count(cursor: Any) -> int:
    return max(int(cursor.rowcount), 0)


_WHAT_A_CURSOR_HOLDS: dict[bool, Callable[..., Rows | int]] = {
    True: _turso_rows,
    False: _turso_count,
}


_RUN_ON: dict[Engine, Callable[..., Awaitable[Rows | int | Fault]]] = {
    "sqlite": _on_sqlite,
    "postgresql": _on_postgresql,
    "turso": _on_turso,
}


# --- the one question this function asks before it runs anything -------------


async def _on_the_databases_own_connection(
    db: Database, statement: Statement
) -> Rows | int | Fault:
    """The connection the database holds, and never a writer from the crew."""
    return await _RUN_ON[db["engine"]](db["connection"], statement)


async def _no_such_engine(db: Database, _statement: Statement) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"No engine here is called {db['engine']!r}, so there is no driver "
            "to put this statement to."
        ),
        "what_to_do_instead": (
            "Open the database with a target naming one of "
            + ", ".join(sorted(_THE_THREE_ENGINES))
            + "; the engine on the database is the one that open decided."
        ),
    }


_WHEN_THE_ENGINE_IS_DECLARED: dict[bool, Callable[..., Awaitable[Rows | int | Fault]]] = {
    True: _on_the_databases_own_connection,
    False: _no_such_engine,
}


async def query_run_statement(db: Database, statement: Statement) -> Rows | int | Fault:
    """Run one statement on this database's connection, and answer what it gave.

    Rows for a question, a count for a change, and a fault for a statement the
    engine refused. It writes no SQL: the text arrives already spelled for this
    engine, and the values arrive beside it and go to the driver to be bound.
    """
    return await _WHEN_THE_ENGINE_IS_DECLARED[db["engine"] in _RUN_ON](db, statement)
