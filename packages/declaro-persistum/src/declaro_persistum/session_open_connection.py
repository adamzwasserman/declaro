"""`session_open_connection`: ask the driver for the connection reads run on.

`boundary_out fn session_open_connection` is the first function in this package
that reaches outside the process. Everything else here is a function over data.
This one opens a database, which is its whole job, and the only rule that
changes for it is that one: a failure still comes back in the return, nothing is
logged, nothing is raised past the caller, and no decision is a branch.

IT IS CALLED AT OPEN, NOT PER QUESTION. Every connection is an operating system
thread. `docs/architecture.md` measured the difference reuse alone makes: the
arm that opened one connection per write ran 250 writes a second against 1,505.
So this is called once, the handle is held for the life of the database, and
`fn session_close_connection` closes it at shutdown.

NOTHING HERE READS THE CONNECTION. `docs/architecture.md` refuses working an
engine out from one in its own words: a driver connection does not say which
engine it belongs to, and the closest you can get is where its class happens to
live, which would make the SQL this package generates depend on somebody else's
file layout. The engine arrives as an argument, settled from the target by
`fn engine_of` before anything was opened, and the handle that comes back is
passed onward unopened.

WHAT `connections` IS FOR, ENGINE BY ENGINE. It is the number
`fn permitted_connections` settled, and it is the size of the read side.
PostgreSQL's driver has an object for exactly that, so the handle is
`asyncpg`'s own pool bounded by the number, and five callers reading at once
run at once. SQLite and Turso have no such object in their drivers, so there
the number bounds nothing and the handle is one connection; five callers are in
flight together and the engine runs them one at a time.

WHY A REPLICATED TURSO CANNOT BE OPENED HERE, and it is a hole in the
declaration rather than in the driver. `turso.aio.sync.connect` takes a local
file to replicate into, the primary's address and an auth token. The address is
the target. The other two are not arguments of this function and there is
nowhere honest to get them: a local file this function chose would be a
deployment decision nobody made, and a token read from the environment would be
hidden state. Measured 2026-09-22 against the staging primary: with its token a
replica opens in 1.85s and answers, and without it the primary answers 401. So
the turso row refuses, naming the target and saying what is missing, until the
declaration hands this door the two values.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable, Callable

from declaro_persistum.types import Connection, Engine, Fault


def _could_not_be_reached(target: str, refused_by_the_driver: BaseException) -> Fault:
    """A database that is named and not reachable, in the driver's own words."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The database at {target} could not be opened. The driver said: "
            f"{type(refused_by_the_driver).__name__}: {refused_by_the_driver}"
        ),
        what_to_do_instead=(
            f"Check that {target} names a database that is running and that this "
            "process can reach it, then open it again."
        ),
    )


# =============================================================================
# sqlite
# =============================================================================
#
# HOW A PATH IS READ OUT OF A TARGET, the same way `engine_of` reads a scheme:
# `str.partition` hands back the separator it found, which is `""` when there
# was none, so the two readings are a table keyed by that separator. One leading
# slash belongs to the `://`, so `sqlite:///app.db` is `app.db` and
# `sqlite:////var/app.db` is `/var/app.db`.
_THE_PATH_IN: dict[str, Callable[[str, str], str]] = {
    "": lambda target, _rest: target,
    "://": lambda _target, rest: rest.removeprefix("/"),
}


async def _wal_was_refused(connection: Connection, target: str) -> Connection | Fault:
    """WAL did not take, so a reader here would wait behind every writer.

    A PRAGMA that does not take is silent: the connection works, and the only
    thing that changes is that a read blocks until the write ahead of it
    commits. That is the one property this connection exists to have, so it
    comes back as a refusal rather than as a slower database nobody can see.
    """
    await connection.close()
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The SQLite database at {target} would not take WAL journal mode, so "
            "a read on this connection would wait for whichever write holds the "
            "file."
        ),
        what_to_do_instead=(
            "Put the database on a filesystem that supports WAL, or open a "
            "database somewhere else. A network filesystem is the usual reason."
        ),
    )


async def _the_connection_itself(connection: Connection, _target: str) -> Connection | Fault:
    """WAL took, so this is the handle."""
    return connection


_WHETHER_THE_FILE_TOOK_WAL: dict[
    bool, Callable[[Connection, str], Awaitable[Connection | Fault]]
] = {
    True: _the_connection_itself,
    False: _wal_was_refused,
}


async def _open_sqlite(target: str, _connections: int) -> Connection | Fault:
    """One aiosqlite connection, in WAL, autocommitting.

    WAL IS WHAT LETS A READ NOT WAIT FOR A WRITER. In the default journal mode a
    writer holding the file blocks every reader until it commits, so the read a
    caller is waiting on finishes after a write it has nothing to do with.

    AUTOCOMMIT, BECAUSE THIS CONNECTION HOLDS NO TRANSACTION. An open read
    transaction pins the WAL and stops it being checkpointed, and reads are what
    this connection is for.
    """
    import aiosqlite

    head, separator, rest = target.partition("://")
    path = _THE_PATH_IN[separator](target, rest)
    try:
        connection = await aiosqlite.connect(path, isolation_level=None)
    except (sqlite3.Error, OSError) as unreachable:
        return _could_not_be_reached(target, unreachable)

    # AN UNFETCHED PRAGMA IS A SILENT NO-OP, which `docs/architecture.md` states
    # beside the drainer's own `journal_mode`, so the answer is read rather than
    # assumed.
    cursor = await connection.execute("PRAGMA journal_mode = WAL")
    answered = await cursor.fetchall()
    took = str(answered[0][0]).lower() == "wal"
    return await _WHETHER_THE_FILE_TOOK_WAL[took](connection, target)


# =============================================================================
# postgresql
# =============================================================================


async def _open_postgresql(target: str, connections: int) -> Connection | Fault:
    """asyncpg's own pool, bounded by the number this database may hold.

    A POOL IS THE DRIVER'S OWN OBJECT AND STILL ONE HANDLE. It answers `execute`
    and `fetch` the way a connection does, it is opened once and closed once,
    and nothing in this package opens it up. What it buys is the thing a single
    asyncpg connection cannot do: two reads at the same time on one connection
    raise `another operation is in progress`, and five callers reading at once
    is what this database is bounded to allow.
    """
    import asyncpg

    try:
        return await asyncpg.create_pool(target, min_size=1, max_size=connections)
    except (asyncpg.PostgresError, OSError) as unreachable:
        return _could_not_be_reached(target, unreachable)


# =============================================================================
# turso
# =============================================================================


async def _open_turso(target: str, _connections: int) -> Connection | Fault:
    """A replica needs three things and this door is handed one of them."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"{target} names a Turso primary, and opening a replica of it also "
            "takes a local file to replicate into and an auth token. This "
            "function is handed a target, an engine and a number of connections, "
            "so it has the address and neither of the other two."
        ),
        what_to_do_instead=(
            "Open a SQLite or PostgreSQL target here. A replicated Turso database "
            "needs the local file and the token to arrive with the target, and "
            "the declaration does not carry them yet."
        ),
    )


# WHICH DRIVER OPENS WHICH ENGINE. The key is the engine this door was handed,
# which is one of the three words `Engine` holds, so every key has a row and the
# read needs no fallback.
_OPEN: dict[Engine, Callable[[str, int], Awaitable[Connection | Fault]]] = {
    "sqlite": _open_sqlite,
    "postgresql": _open_postgresql,
    "turso": _open_turso,
}


# =============================================================================
# what the target named, and whether the bound permits a connection at all
# =============================================================================


async def _no_connection_is_permitted(
    _named: Engine, target: str, connections: int
) -> Connection | Fault:
    """A bound under one permits nothing, so nothing is opened."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"This database may hold {connections} connections, so there is none "
            f"left to read {target} on."
        ),
        what_to_do_instead=(
            "State a bound of at least one where the database is opened. The "
            "right number is a property of the machine, so nothing here can "
            "invent it."
        ),
    )


async def _ask_that_engines_driver(
    named: Engine, target: str, connections: int
) -> Connection | Fault:
    """The engine the target named opens it."""
    return await _OPEN[named](target, connections)


_WHETHER_THE_BOUND_PERMITS_ONE: dict[
    bool, Callable[[Engine, str, int], Awaitable[Connection | Fault]]
] = {
    True: _ask_that_engines_driver,
    False: _no_connection_is_permitted,
}


async def _open_what_the_target_named(
    named: Engine, target: str, connections: int
) -> Connection | Fault:
    return await _WHETHER_THE_BOUND_PERMITS_ONE[connections >= 1](named, target, connections)


async def session_open_connection(
    target: str, engine: Engine, connections: int
) -> Connection | Fault:
    """Open the one connection this database reads on, or say why it could not.

    `target` says where the database is. `engine` says which driver to ask, and
    `connections` is how many this database may hold, already settled by
    `fn permitted_connections`.

    THE ENGINE IS HANDED OVER, NOT WORKED OUT AGAIN. Until 2026-09-22 this called
    `fn engine_of` itself and carried an arm for the refusal that call could
    return. `orchestrator fn session_connect` asks that question once, before it
    opens anything, and is holding the answer when it calls here. A local Turso
    database is also a bare path, and a bare path reads as sqlite, so a target
    alone could not have named one.

    What comes back is the driver's own object, held and passed onward and never
    opened up. A database that cannot be reached comes back as a `Fault` naming
    it, in the return value, never raised and never written anywhere.
    """
    return await _open_what_the_target_named(engine, target, connections)
