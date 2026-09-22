"""One drainer's connection, opened once and held for its life.

A target in, one driver connection out, or a fault. It is the door a crew is
built out of: several drainers empty the waiting room at once, each holding a
connection of its own. `docs/architecture.md` measured what the holding is
worth: every connection is an operating system thread, so opening one per write
pays for that thread every time, and reuse alone is a 6x difference. The arm
that opened one per write ran 250 writes a second against 1,505.

IT OPENS ONE, AND COUNTING IS SOMEBODY ELSE'S JOB. `crew_size` says how many
drainers a deployment runs, and `orchestrator fn session_connect` calls this
that many times. Nothing here reads the environment, counts anything, or knows
that a crew exists.

IT IS NOT THE CONNECTION READS RUN ON. `boundary_out fn session_open_connection`
opens that one and takes a count with it; this one takes only a target, because
a writer is one connection. A drainer writes and never reads, and on Turso it
writes in a journal mode of its own, which is the second reason the two doors
are not one.

NOTHING ASKS THE CONNECTION WHAT ENGINE IT IS. The engine comes from the target,
through `engine_of`, before anything is opened. `docs/architecture.md` refuses
the other direction in its own words: a driver connection does not say which
engine it belongs to, and the closest you can get is where its class happens to
live, which would make the SQL this package generates depend on somebody else's
file layout.

A DATABASE THAT CANNOT BE REACHED COMES BACK IN THE RETURN. A drainer is opened
while a process is starting up, where an unreachable target is the commonest
thing that happens, and the caller is an orchestrator holding a `Fault` arm
already. So the driver's exception is turned into a refusal that names the
target and quotes what the driver said.

THE DRIVERS ARE IMPORTED INSIDE THEIR OWN OPENERS. pyturso is an optional extra,
so importing it here would make this module unimportable on an install that
never asked for Turso. An import that fails is one more thing that stops this
target being openable, and it comes back the same way, naming
`ModuleNotFoundError` and the driver in the refusal.

WHAT IS SET ON THE CONNECTION, AND WHAT IS NOT. Turso opens in MVCC, from the
measured recipe in `docs/architecture.md`, and the PRAGMA is fetched because an
unfetched one is a silent no-op. SQLite and PostgreSQL are opened and nothing
else is said to them, because nothing is measured for them here. A journal mode
for SQLite's crew is a decision nobody has taken yet, and taking it quietly in
this file would be the wrong place for it.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any

from declaro_persistum.types import Connection, Engine, Fault

# --- the path each driver wants, read out of the target --------------------
#
# WHICH SLASHES BELONG TO THE URL AND WHICH TO THE PATH is a table rather than a
# rule, because a bare path and a `sqlite://` target are two readings of the same
# string. `str.partition` hands back the separator it found, which is `""` when
# there was none, so the separator is the key.

_A_SQLITE_PATH: dict[str, Callable[[str, str], str]] = {
    # `app.db` names no scheme and is already the path.
    "": lambda target, _rest: target,
    # `sqlite:///app.db` is the relative file `app.db`, and `sqlite:////tmp/app.db`
    # is the absolute `/tmp/app.db`. One slash belongs to the empty authority.
    "://": lambda _target, rest: rest.removeprefix("/"),
}


def _a_sqlite_file(target: str) -> str:
    _head, separator, rest = target.partition("://")
    return _A_SQLITE_PATH[separator](target, rest)


def _a_turso_file(target: str) -> str:
    """A Turso target always carries its scheme, because `libsql` is the only
    thing in a target that names Turso, so what is left of it is the file."""
    return target.partition("://")[2]


def _the_target_as_written(target: str) -> str:
    """PostgreSQL is reached by the whole target: asyncpg reads the host, the
    database and the credentials out of it."""
    return target


_THE_DRIVER_PATH: dict[Engine, Callable[[str], str]] = {
    "sqlite": _a_sqlite_file,
    "postgresql": _the_target_as_written,
    "turso": _a_turso_file,
}


# --- the three drivers ------------------------------------------------------


async def _a_sqlite_writer(path: str) -> Connection:
    import aiosqlite

    return await aiosqlite.connect(path)


async def _a_postgresql_writer(target: str) -> Connection:
    # asyncpg ships no type information, and mypy runs strict here. aiosqlite
    # and pyturso both carry theirs, so this line is the only one that needs it.
    import asyncpg  # type: ignore[import-untyped]

    return await asyncpg.connect(target)


async def _a_turso_writer(path: str) -> Connection:
    import turso.aio

    connection = await turso.aio.connect(path)
    cursor = await connection.execute("PRAGMA journal_mode = 'mvcc'")
    # AN UNFETCHED PRAGMA IS A SILENT NO-OP, so the answer is read and the mode
    # is really set rather than merely asked for.
    await cursor.fetchall()
    return connection


_OPEN_A_WRITER: dict[Engine, Callable[[str], Coroutine[Any, Any, Connection]]] = {
    "sqlite": _a_sqlite_writer,
    "postgresql": _a_postgresql_writer,
    "turso": _a_turso_writer,
}


# --- what comes back --------------------------------------------------------


async def _a_writer_on_that_engine(engine: Engine, target: str) -> Connection | Fault:
    """Open one connection, or say what stopped it.

    The driver raises many different things — a missing folder, a host that does
    not resolve, a refused password, an absent driver — and every one of them
    means the same thing to a caller: this target did not open. So the type and
    the message are quoted rather than sorted into kinds this package would then
    have to keep in step with three drivers.
    """
    try:
        return await _OPEN_A_WRITER[engine](_THE_DRIVER_PATH[engine](target))
    except Exception as stopped_it:
        return Fault(
            kind="refused",
            what_went_wrong=(
                f"No drainer could be opened on the {engine} target {target}: "
                f"{type(stopped_it).__name__}: {stopped_it}"
            ),
            what_to_do_instead=(
                "Reach that database from this machine first, then open it again. "
                "A drainer holds its connection for as long as it lives, so this "
                "runs once at open and not once per write."
            ),
        )


# A TARGET NAMES AN ENGINE OR IT NAMES A REFUSAL, and `engine_of` answers with
# one of the two. An engine is one of three words and a refusal is a `Fault`, so
# which arrived is a lookup on what came back rather than a question asked twice.


async def session_open_writer(target: str, engine: Engine) -> Connection | Fault:
    """Open one drainer's connection, or refuse and say what stopped it.

    THE ENGINE IS HANDED OVER, NOT WORKED OUT AGAIN. Until 2026-09-22 this called
    `fn engine_of` itself and carried an arm for the refusal that call could
    return. `orchestrator fn session_connect` asks that question once, before it
    opens anything, and is holding the answer when it calls here. So the question
    is answered upstream and this is handed the word.

    The connection is a handle: it is held, passed onward, and never opened up
    and read.
    """
    return await _a_writer_on_that_engine(engine, target)
