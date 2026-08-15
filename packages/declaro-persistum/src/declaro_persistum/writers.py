"""How each engine runs one write inside a transaction.

The engines disagree about three things at once: the statement that opens a
transaction, how parameters are passed, and whether there is a commit to call.
Those differences live here, in the table, and nowhere else.

`WRITERS[db["dialect"]]` is read ONCE, by the opener, and the result is carried
on the `Database`. Not per write: the write path runs on every request, and the
engine cannot change while a database is open.

An application may hold a Postgres database and a Turso replica at the same
time, so this is never module-level state about "the" engine. The table says how
each engine writes; the value says which engine it is.

TWO CONNECTION SHAPES, AND THEY DO NOT OVERLAP. A DB-API connection runs
`execute` then `commit`. An asyncpg connection runs `execute` inside a
`transaction()` block and has no commit at all. Each is declared below and each
writer takes the one it actually uses, so the calls in the body are the calls
the type admits.

There used to be three `# type: ignore[attr-defined]` here instead, one on every
line that touched a method the single `Connection` Protocol did not declare. The
escapes were what kept the Protocol wrong: it described a connection none of the
three writers had.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Protocol

__all__ = [
    "WRITERS",
    "TRANSACTIONS",
    "DbApiConnection",
    "AsyncpgConnection",
    "WriteOne",
    "turso_write_one",
    "sqlite_write_one",
    "postgres_write_one",
    "Transaction",
]

# "Scope a write block on this engine." Same shape as WriteOne and for the
# same reason: the connection type and the dialect vary together.
Transaction = Callable[[Any], "AbstractAsyncContextManager[None]"]


class DbApiConnection(Protocol):
    """Turso and SQLite. Parameters as a sequence, an explicit commit."""

    async def execute(self, sql: str, parameters: Sequence[object] = ..., /) -> object:
        ...

    async def commit(self) -> None:
        ...

    async def rollback(self) -> None:
        # DECLARED BECAUSE `dbapi_transaction` DEPENDS ON IT. The Protocol
        # named `execute` and `commit` only, so the rollback half of the
        # transaction scope rested on a method no contract required. mypy
        # caught it the moment the dialect vocabulary made these signatures
        # precise enough to check.
        ...


class AsyncpgConnection(Protocol):
    """PostgreSQL. Parameters positionally, the transaction block commits."""

    async def execute(self, sql: str, /, *args: object) -> object:
        ...

    def transaction(self) -> AbstractAsyncContextManager[object]:
        ...


class WriteOne(Protocol):
    """Run one write in a transaction on this engine.

    `conn` is `Any` and that is the one place looseness is unavoidable, not an
    oversight. The dialect key and the connection shape are correlated — the
    Turso entry is only ever called with a `DbApiConnection`, the Postgres entry
    only ever with an `AsyncpgConnection` — and Python's type system cannot say
    "these two vary together". Narrowing it to either Protocol would reject the
    other writer at the table.

    What makes the correlation hold is not the type. It is that one opener binds
    `dialect` and `write_one` from the same `DIALECT` constant, so the pair
    cannot disagree; `test_every_backend_can_be_opened` is what checks it.
    """

    async def __call__(
        self, conn: Any, sql: str, params: Sequence[object]
    ) -> None:
        ...


async def turso_write_one(
    conn: DbApiConnection, sql: str, params: Sequence[object]
) -> None:
    """BEGIN CONCURRENT is what lets a crew write in parallel on Turso.

    A write-write conflict here means "not now" rather than "no", which is why
    `drain` takes a retry policy.
    """
    await conn.execute("BEGIN CONCURRENT")
    await conn.execute(sql, params)
    await conn.commit()


async def sqlite_write_one(
    conn: DbApiConnection, sql: str, params: Sequence[object]
) -> None:
    """BEGIN IMMEDIATE takes the write lock up front.

    SQLite has no BEGIN CONCURRENT. A deferred transaction takes the write lock
    at the first write, so a busy database fails part-way through; IMMEDIATE
    fails at the start or not at all.
    """
    await conn.execute("BEGIN IMMEDIATE")
    await conn.execute(sql, params)
    await conn.commit()


async def postgres_write_one(
    conn: AsyncpgConnection, sql: str, params: Sequence[object]
) -> None:
    """asyncpg owns the transaction and takes positional parameters.

    There is no `commit()` to call: the context manager commits on exit and
    rolls back on an exception.
    """
    async with conn.transaction():
        await conn.execute(sql, *params)


WRITERS: dict[str, WriteOne] = {
    "turso": turso_write_one,
    "sqlite": sqlite_write_one,
    "postgresql": postgres_write_one,
}


# ---------------------------------------------------------------------------
# Scoping a `writing` block. The same engine difference, one step later.
# ---------------------------------------------------------------------------
#
# `writing(db)` yielded a connection and neither committed nor rolled back, so
# a block that forgot `await conn.commit()` lost its write and said nothing.
# Measured 2026-08-14, two writes through two blocks, the second without a
# commit:
#
#     sqlite: rows after a block that forgot to commit: 1 of 2
#     turso : rows after a block that forgot to commit: 1 of 2
#
# ONE ENTRY, NOT TWO. This was first written as separate COMMIT and ROLLBACK
# tables, and that shape let one half be right while the other was wrong: the
# PostgreSQL entries were no-ops, on the reasoning that `postgres_write_one`
# runs inside `conn.transaction()` and commits itself. True of the crew path
# and false of this one — `writing` hands the raw asyncpg connection to the
# caller, who executes outside any transaction, so asyncpg autocommits each
# statement. Measured, a block that raises:
#
#     sqlite     rows after the block: 0
#     turso      rows after the block: 0
#     postgresql rows after the block: 1     <- committed anyway
#
# Same call, opposite outcome, which is the one thing priority 2 forbids.
# Commit and rollback are two halves of one concept, so they are one entry.
#
# A SECOND COMMIT IS HARMLESS on the DB-API engines, measured, so a caller who
# still writes `await conn.commit()` inside the block is not broken by this.


@asynccontextmanager
async def dbapi_transaction(conn: DbApiConnection) -> AsyncIterator[None]:
    """Turso and SQLite: commit on a clean exit, roll back on an exception."""
    try:
        yield
    except BaseException:
        await conn.rollback()
        raise
    else:
        await conn.commit()


def postgres_transaction(conn: AsyncpgConnection) -> Any:
    """PostgreSQL: asyncpg's own transaction, which already has these
    semantics.

    Returned rather than wrapped. `conn.transaction()` is an async context
    manager that commits on exit and rolls back on an exception, so writing a
    second one around it would add a layer that can only disagree.
    """
    return conn.transaction()


TRANSACTIONS: dict[str, Transaction] = {
    "turso": dbapi_transaction,
    "sqlite": dbapi_transaction,
    "postgresql": postgres_transaction,
}
