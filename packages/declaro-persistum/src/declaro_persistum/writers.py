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
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

__all__ = [
    "WRITERS",
    "Connection",
    "WriteOne",
    "turso_write_one",
    "sqlite_write_one",
    "postgres_write_one",
]


class Connection(Protocol):
    """What a writer needs of a connection. Nothing more is assumed."""

    async def execute(self, sql: str, parameters: Sequence[object] = ..., /) -> object:
        ...


class WriteOne(Protocol):
    """Run one write in a transaction on this engine."""

    async def __call__(
        self, conn: Connection, sql: str, params: Sequence[object]
    ) -> None:
        ...


async def turso_write_one(
    conn: Connection, sql: str, params: Sequence[object]
) -> None:
    """BEGIN CONCURRENT is what lets a crew write in parallel on Turso.

    A write-write conflict here means "not now" rather than "no", which is why
    `drain` takes a retry policy.
    """
    await conn.execute("BEGIN CONCURRENT")
    await conn.execute(sql, params)
    await conn.commit()  # type: ignore[attr-defined]


async def sqlite_write_one(
    conn: Connection, sql: str, params: Sequence[object]
) -> None:
    """BEGIN IMMEDIATE takes the write lock up front.

    SQLite has no BEGIN CONCURRENT. A deferred transaction takes the write lock
    at the first write, so a busy database fails part-way through; IMMEDIATE
    fails at the start or not at all.
    """
    await conn.execute("BEGIN IMMEDIATE")
    await conn.execute(sql, params)
    await conn.commit()  # type: ignore[attr-defined]


async def postgres_write_one(
    conn: Connection, sql: str, params: Sequence[object]
) -> None:
    """asyncpg owns the transaction and takes positional parameters.

    There is no `commit()` to call: the context manager commits on exit and
    rolls back on an exception.
    """
    async with conn.transaction():  # type: ignore[attr-defined]
        await conn.execute(sql, *params)


WRITERS: dict[str, WriteOne] = {
    "turso": turso_write_one,
    "sqlite": sqlite_write_one,
    "postgresql": postgres_write_one,
}
