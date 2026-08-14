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

from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

__all__ = [
    "WRITERS",
    "DbApiConnection",
    "AsyncpgConnection",
    "WriteOne",
    "turso_write_one",
    "sqlite_write_one",
    "postgres_write_one",
]


class DbApiConnection(Protocol):
    """Turso and SQLite. Parameters as a sequence, an explicit commit."""

    async def execute(self, sql: str, parameters: Sequence[object] = ..., /) -> object:
        ...

    async def commit(self) -> None:
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
