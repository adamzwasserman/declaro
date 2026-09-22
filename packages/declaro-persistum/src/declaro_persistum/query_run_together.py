"""`query_run_together`: several writes that land together, or not at all.

WHAT A CALLER ASKED FOR WHEN THEY HANDED OVER A LIST. Several rows in one call
means all of them or none, so they run inside one transaction and come back as
one number: how many rows the whole group touched. A statement the engine
refuses comes back as a `Fault`, and nothing the group carried is in the
database when it does.

WHY THIS IS NOT `boundary_out fn query_run_statement`. That door runs one
statement and commits it. Two deposits are two independent writes, and the
first stays landed when the second fails, because the first is already in the
log; `docs/architecture.md` says so where it describes the room. Holding one
connection across the whole group is the only rollback there is, which is why
this write does not take a ticket and never goes through the room.

WHO DECIDED IT WAS A GROUP. `fn travels_together` reads one declaration and
answers whether its rows have to land together. It has already answered by the
time anything gets here, so this function is handed the statements and runs
them.

NOTHING HERE WRITES SQL. The text arrives already spelled for this engine and
the values arrive beside it, in `parameters`, and go to the driver to be bound.
`fn compile_statement` names them `p1`, `p2` in the order it bound them, and
all three drivers count their placeholders rather than reading those names, so
the values travel as `list(parameters.values())`.

BEGIN AND COMMIT ARE NOT STATEMENTS THIS FUNCTION COMPILED. They are how
aiosqlite and pyturso are told that a transaction is open, the way asyncpg is
told by `connection.transaction()`. No caller's declaration reaches them.

WHY THERE IS A ROW PER ENGINE AND NO BRANCH. Three drivers are three rows, and
they are not one function wearing three hats. asyncpg hands out a transaction
object and a pool to acquire a connection from; the other two take BEGIN and
COMMIT as statements and count rows off a cursor. An engine with no row comes
back as a fault naming what arrived, never as a fallback answering for a driver
nobody wrote a row for.

A POOL IS NOT A CONNECTION. `boundary_out fn session_open_connection` opens
asyncpg's own pool for PostgreSQL, so a connection is acquired from it before a
transaction can start on it. A pool has no `transaction`, and that exact
mismatch cost `boundary_out fn query_run_statement` every PostgreSQL statement
it ever ran until 2026-09-22.

NOTHING IN pyturso IS NATIVELY SYNCHRONOUS. `turso.aio` is the driver and
`turso.connect` is a shim over it, so `execute`, `commit` and `rollback` are all
awaited. Calling one without awaiting raises nothing and does nothing, because
a coroutine is truthy, and the answer then comes back built from a cursor that
never ran.

THE CONNECTION IS A HANDLE. It is passed to the driver and never asked what
engine it belongs to. The `Database` carries the engine, decided once when it
was opened.

A FAILURE COMES BACK IN THE RETURN, carrying what the engine said, so a reader
sees what the engine objected to rather than what this module guessed it meant.
Each row catches its own driver's error type and nothing wider: a bug in this
package raises, where a driver's refusal is an answer. A rollback that itself
fails is the driver breaking rather than the engine refusing a statement, and it
raises for the same reason.
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
    Statement,
    Value,
)

_THE_THREE_ENGINES = get_args(Engine)

# What a driver that takes its transaction as statements is told.
_BEGIN = "BEGIN"


def _values(statement: Statement) -> list[Value]:
    """The values, in the order the compiler bound them.

    POSITIONAL, WHATEVER THE DICT SUGGESTS. All three drivers count their
    placeholders, so the names are a reader's handle on the order and the order
    is what goes to the driver.
    """
    return list(statement["parameters"].values())


def _touched(rowcount: Any) -> int:
    """How many rows one statement touched. A driver says -1 where the question
    does not apply, and a statement that touched no rows touched none."""
    return max(int(rowcount), 0)


def _refused(statements: list[Statement], engine_said: BaseException) -> Fault:
    """What the engine said, on its way back to the caller as a value."""
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"The engine refused one of the {len(statements)} statements this "
            f"group asked to land together, so none of them landed: {engine_said}"
        ),
        "what_to_do_instead": (
            "Read what the engine said above and change the statement it names, "
            "or the models it is asked against. Every statement in a group runs "
            "inside one transaction, so the group is refused whole and the "
            "database is as it was before the group ran."
        ),
    }


# --- one row per engine ------------------------------------------------------
#
# EACH ROW IS THE DRIVER'S OWN MANUAL AND NOTHING ELSE. asyncpg opens a
# transaction as an object and acquires the connection it runs on from a pool;
# aiosqlite and pyturso are told BEGIN and COMMIT as statements and count rows
# off a cursor. Folding them together would make the next difference between two
# drivers an edit to the shape of this table rather than an edit to one row.


async def _on_sqlite(connection: Connection, statements: list[Statement]) -> int | Fault:
    """aiosqlite. BEGIN, every statement, COMMIT, and ROLLBACK on a refusal."""
    try:
        await connection.execute(_BEGIN)
        counted = [await _one_on_sqlite(connection, statement) for statement in statements]
        await connection.commit()
        return sum(counted)
    except sqlite3.Error as engine_said:
        await connection.rollback()
        return _refused(statements, engine_said)


async def _one_on_sqlite(connection: Connection, statement: Statement) -> int:
    cursor = await connection.execute(statement["text"], _values(statement))
    return _touched(cursor.rowcount)


async def _on_postgresql(connection: Connection, statements: list[Statement]) -> int | Fault:
    """asyncpg. The pool hands over a connection and the connection a transaction.

    `transaction()` commits when the block ends and rolls back when anything
    leaves it raising, which is the whole of what this function needs from it.
    A change answers with a tag -- `INSERT 0 3`, `UPDATE 2` -- and the number is
    the last word where there is one.
    """
    import asyncpg  # type: ignore[import-untyped]

    try:
        async with connection.acquire() as held, held.transaction():
            tags = [
                await held.execute(statement["text"], *_values(statement))
                for statement in statements
            ]
        return sum(_counted(tag) for tag in tags)
    except asyncpg.PostgresError as engine_said:
        return _refused(statements, engine_said)


def _counted(tag: str) -> int:
    """The row count PostgreSQL wrote at the end of its tag, or none."""
    counted = tag.rpartition(" ")[2]
    return int({True: counted, False: "0"}[counted.isdigit()])


async def _on_turso(connection: Connection, statements: list[Statement]) -> int | Fault:
    """pyturso, awaited throughout, on one cursor for the whole group."""
    import turso

    try:
        cursor = connection.cursor()
        await cursor.execute(_BEGIN)
        counted = [await _one_on_turso(cursor, statement) for statement in statements]
        await connection.commit()
        return sum(counted)
    except turso.Error as engine_said:
        await connection.rollback()
        return _refused(statements, engine_said)


async def _one_on_turso(cursor: Any, statement: Statement) -> int:
    await cursor.execute(statement["text"], _values(statement))
    return _touched(cursor.rowcount)


_RUN_ON: dict[Engine, Callable[..., Awaitable[int | Fault]]] = {
    "sqlite": _on_sqlite,
    "postgresql": _on_postgresql,
    "turso": _on_turso,
}


# --- the one question this function asks before it runs anything -------------


async def _on_the_databases_own_connection(
    db: Database, statements: list[Statement]
) -> int | Fault:
    """The connection the database holds, which is what makes the rollback real.

    A drainer's connection would not do: a group is one caller's transaction,
    and a drainer is shared by everybody who deposited.
    """
    return await _RUN_ON[db["engine"]](db["connection"], statements)


async def _no_such_engine(db: Database, statements: list[Statement]) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"No engine here is called {db['engine']!r}, so there is no driver "
            f"to put these {len(statements)} statements to, and no transaction "
            "to run them in."
        ),
        "what_to_do_instead": (
            "Open the database with a target naming one of "
            + ", ".join(sorted(_THE_THREE_ENGINES))
            + "; the engine on the database is the one that open decided."
        ),
    }


_WHEN_THE_ENGINE_IS_DECLARED: dict[bool, Callable[..., Awaitable[int | Fault]]] = {
    True: _on_the_databases_own_connection,
    False: _no_such_engine,
}


async def query_run_together(db: Database, statements: list[Statement]) -> int | Fault:
    """Run these statements as one transaction, and answer how many rows landed.

    One number for the whole group, or a fault for a group the engine refused,
    in which case none of the group is in the database. It writes no SQL: the
    text arrives already spelled for this engine, and the values arrive beside
    it and go to the driver to be bound.
    """
    return await _WHEN_THE_ENGINE_IS_DECLARED[db["engine"] in _RUN_ON](db, statements)
