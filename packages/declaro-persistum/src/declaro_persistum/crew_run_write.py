"""One deposited write, run on the drainer's own connection.

    boundary_out fn crew_run_write : (writer: Connection, engine: Engine, statement: Statement, transacting: Transacting) -> int | Fault

IT TAKES A CONNECTION AND NOT A DATABASE, WHICH IS THE WHOLE POINT. A drainer
writes and never reads, on the one connection it holds for its life, and
`docs/architecture.md` measured what reuse alone is worth: 250 writes a second
against 1,505 when a connection is opened per write. Handing this a `Database`
would let a write land on the connection reads run on, and a write there blocks
every reader behind it.

THE WORDS COME OFF THE RECORD, NOT OUT OF THIS MODULE. `fn how_to_write` reads
one row per engine and hands back what that engine needs around a write: the
statement that opens it, how the values go to the driver, and whether there is a
commit to call. All three are read here. An engine added to the table is an
engine this runs, without a line changing here.

WHY THERE IS STILL A ROW PER ENGINE. Which driver call carries those words is
not something a record can hold: asyncpg opens a transaction as an object and
the other two are told BEGIN as a statement, and each raises its own type. So
the words are data and the calls are three rows, and the difference between two
drivers is an edit to one row.

A CONTENTION FAILURE IS NOT A REFUSAL, AND TELLING THEM APART IS WHY THIS
FUNCTION RETURNS A `Fault` WITH A KIND. `fn worth_retrying` reads that kind and
nothing else: a conflict lost a race and the next run is a different race, and a
refusal is a statement about the write itself that will not change. Getting it
backwards costs a caller its write or spends a budget spinning on a column the
engine will never accept.

HOW EACH ENGINE SAYS WHICH IT IS, MEASURED 2026-09-24 RATHER THAN ASSUMED.
SQLite carries the engine's own code on the exception: `sqlite_errorname` is
`SQLITE_BUSY` for a conflict and `SQLITE_CONSTRAINT_PRIMARYKEY` for a duplicate
key, so the code decides and the message is never read. asyncpg gives a class
per SQLSTATE, so `SerializationError` and `DeadlockDetectedError` are conflicts
and everything else is a refusal.

TURSO IS THE ONE PLACE THIS PACKAGE READS A DRIVER'S PROSE, AND IT IS A
DELIBERATE EXCEPTION. pyturso 0.7.2 raises `turso.Error` for a write-write
conflict carrying the sentence "Write-write conflict", with no code on the
exception and no class of its own; a missing table raises the same bare
`turso.DatabaseError`. Turso's own guidance does the same thing: the retry
wrapper in <https://turso.tech/blog/concurrent-writes-in-practice> matches
`err.message.includes("SQLITE_BUSY") || err.message.includes("conflict")`.
Treating every bare error as a conflict would retry a missing table until the
budget ran out, and refusing to classify would throw away the concurrency a crew
exists for. So the sentence is matched, here and nowhere else, and this comment
is the record of why. It comes out the day pyturso puts a code on the exception.

BEGIN CONCURRENT NEEDS MVCC, AND THE DRAINER'S CONNECTION HAS IT.
`boundary_out fn session_open_writer` sets `journal_mode = 'mvcc'` and fetches
the answer, because an unfetched pragma is a silent no-op. Measured 2026-09-24:
four held connections to one local file each take `BEGIN CONCURRENT` and write.
Without the fetch every one of them is refused, which is the same trap
`fn session_open_connection` documents beside WAL.

A FAILURE COMES BACK IN THE RETURN, carrying what the engine said. Each row
catches its own driver's error type and nothing wider: a bug in this package
raises, where a driver's refusal is an answer.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any, get_args

from declaro_persistum.types import (
    Connection,
    Engine,
    Fault,
    FaultKind,
    Statement,
    Transacting,
    Value,
)

_THE_THREE_ENGINES = get_args(Engine)


def _values(statement: Statement) -> list[Value]:
    """The values, in the order the compiler bound them.

    POSITIONAL ON ALL THREE, which is what `fn how_to_write` says and what
    `fn compile_statement` writes: `?` on SQLite and Turso, `$1` on PostgreSQL.
    The names are a reader's handle on the order.
    """
    return list(statement["parameters"].values())


def _touched(rowcount: Any) -> int:
    """How many rows the write touched. A driver says -1 where the question does
    not apply, and a write that touched no rows touched none."""
    return max(int(rowcount), 0)


def _as(kind: FaultKind, statement: Statement, engine_said: BaseException) -> Fault:
    """What the engine said, on its way back as a value with a kind on it."""
    return Fault(
        kind=kind,
        what_went_wrong=(
            f"The engine {_WHAT_THE_KIND_MEANS[kind]} the write "
            f"{statement['text']!r}: {engine_said}"
        ),
        what_to_do_instead=_WHAT_TO_DO[kind],
    )


_WHAT_THE_KIND_MEANS: dict[FaultKind, str] = {
    "conflict": "lost a race at",
    "refused": "refused",
    "budget_spent": "ran out of attempts on",
    "cut_short": "was stopped part way through",
}

_WHAT_TO_DO: dict[FaultKind, str] = {
    "conflict": (
        "Nothing. Another writer touched the same rows first, so this one is "
        "worth running again and the crew will run it again."
    ),
    "refused": (
        "Read what the engine said above and change the write or the models it "
        "is asked against. The engine refused it rather than this library, so "
        "running it again would be refused the same way."
    ),
    "budget_spent": "Nothing here decides this.",
    "cut_short": "Nothing here decides this.",
}


# --- which failures each engine calls a conflict -----------------------------
#
# ASKED OF THE CODE OR THE CLASS, NEVER OF THE MESSAGE, except on Turso, where
# the driver gives neither and the module docstring says why.

# SQLite's own names for a write that lost a race. The extended codes start with
# these, so `SQLITE_BUSY_SNAPSHOT` reads as busy without a row of its own.
_SQLITE_LOST_A_RACE = ("SQLITE_BUSY", "SQLITE_LOCKED")

# What pyturso 0.7.2 writes into a conflict's message, and what Turso's own
# retry wrapper matches on. Read the module docstring before adding to this.
_TURSO_LOST_A_RACE = ("Write-write conflict", "SQLITE_BUSY")


def _sqlite_kind(engine_said: BaseException) -> FaultKind:
    named = str(getattr(engine_said, "sqlite_errorname", ""))
    lost = [start for start in _SQLITE_LOST_A_RACE if named.startswith(start)]
    return {True: "conflict", False: "refused"}[lost != []]  # type: ignore[return-value]


def _turso_kind(engine_said: BaseException) -> FaultKind:
    said = str(engine_said)
    lost = [words for words in _TURSO_LOST_A_RACE if words in said]
    return {True: "conflict", False: "refused"}[lost != []]  # type: ignore[return-value]


def _postgresql_kind(engine_said: BaseException) -> FaultKind:
    import asyncpg  # type: ignore[import-untyped]

    lost = isinstance(engine_said, asyncpg.SerializationError | asyncpg.DeadlockDetectedError)
    return {True: "conflict", False: "refused"}[lost]  # type: ignore[return-value]


# --- one row per engine ------------------------------------------------------


async def _on_sqlite(
    writer: Connection, statement: Statement, transacting: Transacting
) -> int | Fault:
    """aiosqlite. The opening statement, the write, and a commit if there is one."""
    try:
        await writer.execute(transacting["begin"])
        cursor = await writer.execute(statement["text"], _values(statement))
        return await _WHETHER_THERE_IS_A_COMMIT[transacting["commits"]](
            writer, _touched(cursor.rowcount)
        )
    except sqlite3.Error as engine_said:
        await writer.rollback()
        return _as(_sqlite_kind(engine_said), statement, engine_said)


async def _on_turso(
    writer: Connection, statement: Statement, transacting: Transacting
) -> int | Fault:
    """pyturso, awaited throughout, on one cursor for the whole write.

    NOTHING IN pyturso IS NATIVELY SYNCHRONOUS. `turso.aio` is the driver and
    `turso.connect` is a shim over it, so `execute`, `commit` and `rollback` are
    all awaited. Calling one without awaiting raises nothing and does nothing,
    because a coroutine is truthy.
    """
    import turso

    try:
        cursor = writer.cursor()
        await cursor.execute(transacting["begin"])
        await cursor.execute(statement["text"], _values(statement))
        return await _WHETHER_THERE_IS_A_COMMIT[transacting["commits"]](
            writer, _touched(cursor.rowcount)
        )
    except turso.Error as engine_said:
        await writer.rollback()
        return _as(_turso_kind(engine_said), statement, engine_said)


async def _on_postgresql(
    writer: Connection, statement: Statement, _transacting: Transacting
) -> int | Fault:
    """asyncpg. The connection hands out a transaction and owns the commit.

    `transaction()` commits when the block ends and rolls back when anything
    leaves it raising, which is the whole of what this needs from it, and is why
    this engine's row says there is no commit to call. A change answers with a
    tag -- `INSERT 0 3`, `UPDATE 2` -- and the number is the last word where
    there is one.
    """
    import asyncpg  # type: ignore[import-untyped]

    try:
        async with writer.transaction():
            tag = await writer.execute(statement["text"], *_values(statement))
        return _counted(tag)
    except asyncpg.PostgresError as engine_said:
        return _as(_postgresql_kind(engine_said), statement, engine_said)


def _counted(tag: str) -> int:
    """The row count PostgreSQL wrote at the end of its tag, or none."""
    counted = tag.rpartition(" ")[2]
    return int({True: counted, False: "0"}[counted.isdigit()])


# BOTH HALVES HAND BACK THE ANSWER, so neither is a call made for its effect.
# The half that commits answers with what the write touched once the commit is
# through; the half that does not answers with the same number. A pair whose
# quiet half returned nothing would read the same as a half nobody had written.
async def _and_commit(writer: Connection, touched: int) -> int:
    await writer.commit()
    return touched


async def _the_driver_owns_it(_writer: Connection, touched: int) -> int:
    """This driver commits on its own, so a call here would find nothing."""
    return touched


_WHETHER_THERE_IS_A_COMMIT: dict[bool, Callable[[Connection, int], Awaitable[int]]] = {
    True: _and_commit,
    False: _the_driver_owns_it,
}


_RUN_ON: dict[Engine, Callable[..., Awaitable[int | Fault]]] = {
    "sqlite": _on_sqlite,
    "postgresql": _on_postgresql,
    "turso": _on_turso,
}


# --- the one question this function asks before it runs anything -------------


async def _on_the_writer_it_was_handed(
    writer: Connection, engine: Engine, statement: Statement, transacting: Transacting
) -> int | Fault:
    return await _RUN_ON[engine](writer, statement, transacting)


async def _no_such_engine(
    _writer: Connection, engine: Engine, _statement: Statement, _transacting: Transacting
) -> Fault:
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"No engine here is called {engine!r}, so there is no driver to put this write to."
        ),
        what_to_do_instead=(
            "Open the database with a target naming one of "
            + ", ".join(sorted(_THE_THREE_ENGINES))
            + "; the engine on the database is the one that open decided."
        ),
    )


_WHEN_THE_ENGINE_IS_DECLARED: dict[bool, Callable[..., Awaitable[int | Fault]]] = {
    True: _on_the_writer_it_was_handed,
    False: _no_such_engine,
}


async def crew_run_write(
    writer: Connection,
    engine: Engine,
    statement: Statement,
    transacting: Transacting,
) -> int | Fault:
    """Run one write on this drainer's connection, and say how many rows it touched.

    A number for a write that landed, and a `Fault` for one that did not. The
    fault's kind says whether it is worth running again: a conflict lost a race
    and a refusal will not change. It writes no SQL, and the words a transaction
    opens and closes with come off the record it is handed.
    """
    return await _WHEN_THE_ENGINE_IS_DECLARED[engine in _RUN_ON](
        writer, engine, statement, transacting
    )
