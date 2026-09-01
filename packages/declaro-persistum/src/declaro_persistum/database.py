"""A database is a value. Functions open it, read it, write it, close it.

A local database file, sometimes with a primary in the cloud. So it is called a
`Database`, it is a TypedDict, and the operations are functions that take it.

    db = await open_turso(path, shutdown=..., primary=NOTHING, token=NOTHING)                    local only
    db = await open_turso(path, primary=..., token=..., shutdown=...)

    async with reading(db) as conn: ...
    async with writing(db) as conn: ...

    await flush(db)      block until local writes reach the primary
    await close(db)      final replication, then release everything

THE CONNECTION IS NEVER STORED ON ANYTHING. `reading` and `writing` are context
managers, so a connection exists for the span of the block and not one
statement longer (Rule 12). What `Database` carries is configuration and the
injected callables — never a live handle a caller could reach around and use.

CALLABLES ARE INJECTED. `connect` and `close_connection` come in as arguments,
so the same functions drive SQLite, Turso and PostgreSQL, and a test drives
them against a real database rather than a fake.

READ THESE BEFORE CHANGING ANYTHING HERE. NOT OPTIONAL.

    https://turso.tech/blog/concurrent-writes-on-turso-cloud
    https://turso.tech/blog/turso-0.7.0
    https://turso.tech/blog/concurrent-writes-in-practice

A SYNC CONNECTION AND A WRITE TO THE JOURNAL ARE TWO DIFFERENT THINGS, and
every wrong answer this project has produced about concurrency came from
treating them as one:

                    a write to the journal      a sync connection
    what it is      one transaction appending   the channel between this
                    to the local file           copy and the primary
    how many        MANY, under MVCC with       EXACTLY ONE
                    BEGIN CONCURRENT
    limited by      row conflicts, at commit    the engine: a second one is
                                                "database tape error:
                                                 database is busy"
    when            whenever a caller writes    on push and pull
    bounds writers  no                          NO

THE ONE SYNC CONNECTION IS NOT A WRITER AND DOES NOT MAKE WRITERS WAIT. It
carries a log to and from the primary. Local writes go to the journal,
concurrently, and the sync connection ships them later. A measurement that a
second sync connection fails is a fact about the channel and says NOTHING
about how many writes can be in flight.

So "a replica permits one connection" is false, and "a replicated database
serialises writers" is false. If a sentence concludes something about
concurrency from a count of connections, it is wrong.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Literal, TypedDict

from declaro_persistum.boundary import helper
from declaro_persistum.exceptions import DatabaseClosedError
from declaro_persistum.reporting import say
from declaro_persistum.types import Engine
from declaro_persistum.writers import TRANSACTIONS, WriteOne

logger = logging.getLogger(__name__)

__all__ = [
    "Database",
    "ShutdownPolicy",
    "check_shutdown",
    "WritesInFlight",
    "nothing_was_trapped",
    "no_writes_in_flight",
    "writes_in_flight",
    "reading",
    "writing",
    "is_replicated",
    "replicate",
    "replicate_if_idle",
    "replication_loop",
    "replicate_on_shutdown",
    "refresh",
    "flush",
    "close",
]


# WHAT HAPPENS TO UNREPLICATED WRITES WHEN THE PROCESS IS SIGNALLED.
# No default, and not for symmetry with Rule 14 for its own sake. persistum's
# usual home has ephemeral disk, where anything not replicated when the process
# dies is gone — so a default would silently pick the losing side of the exact
# failure this exists to prevent, for every caller who never read the changelog.
#
#   "replicate"         trap SIGTERM/SIGINT, replicate to completion, then exit
#   "exit_immediately"  install no handler; exit without replicating
#
# "exit_immediately" is a real answer — a read-only copy, a test, a host that
# already calls close(). Stating it is the difference between a decision and an
# omission, and a default cannot tell those apart.
ShutdownPolicy = Literal["replicate", "exit_immediately"]

# The two answers, written once. `ShutdownPolicy` is a `Literal`, which Python
# does not enforce at runtime, so the vocabulary needs a value to check against.
POLICIES: tuple[str, ...] = ("replicate", "exit_immediately")


@helper
def _policy_stands(_policy: str, _opener: str) -> int:
    """The policy names one of the two answers, so nothing is refused."""
    return 0


@helper
def _refuse_unknown_policy(policy: str, opener: str) -> int:
    """A misspelt policy reads as 'do not replicate', so it is refused by name."""
    raise ValueError(
        f"{opener}: shutdown={policy!r} is not a policy. It takes "
        f"'exit_immediately', or 'replicate' where there is a primary to "
        f"replicate to. A value that is neither reaches the database "
        f"record and reads as 'do not replicate', so the writes are lost "
        f"at the next deploy without anything being said."
    )


_KNOWN_POLICY: dict[bool, Callable[[str, str], int]] = {
    True: _policy_stands,
    False: _refuse_unknown_policy,
}


@helper
def _replication_stands(_policy: str, _opener: str) -> int:
    """Either there is a primary, or the policy does not ask for one."""
    return 0


@helper
def _refuse_replicating_nowhere(_policy: str, opener: str) -> int:
    """`replicate` with no primary would fail at SIGTERM, so it fails at open."""
    raise ValueError(
        f"{opener}: shutdown='replicate' needs a primary to replicate to, "
        f"and this database has none. Pass 'exit_immediately', or open a "
        f"replica with a primary and a token."
    )


_CAN_HONOUR_POLICY: dict[bool, Callable[[str, str], int]] = {
    True: _replication_stands,
    False: _refuse_replicating_nowhere,
}


@helper
def check_shutdown(policy: str, *, can_replicate: bool, opener: str) -> None:
    """Refuse a shutdown policy this database cannot honour.

    `shutdown` is required on every opener and has no default, because the
    usual deployment has ephemeral disk and durability rests on replicating at
    SIGTERM. It was also unchecked, and `ShutdownPolicy` is a `Literal`, which
    Python does not enforce -- so a plain string reached the record.

    THE TYPO WAS THE EXPENSIVE CASE. `trap_shutdown` reads

        if db["shutdown"] != "replicate": return

    so anything misspelt means "do not replicate". Someone who meant to
    replicate and typed it wrong installed no handler, was told nothing, and
    lost the writes on the next deploy. The argument that exists to guarantee
    durability switched it off in silence.

    `replicate` with nowhere to replicate to is the other half: the failure
    would otherwise arrive at SIGTERM, which is the worst moment to find it.
    """
    _KNOWN_POLICY[policy in POLICIES](policy, opener)
    _CAN_HONOUR_POLICY[can_replicate or policy != "replicate"](policy, opener)


# THE INJECTED BOUNDARY, NAMED. Every one of these was `Callable[..., Any]`,
# which declares that a value can be called and nothing else — not the arity,
# not the argument, not the return. A caller could hand `replicate_once` a
# function taking two arguments and nothing would say so until a shutdown ran
# it, which is the worst moment to find out.
#
# The connection itself stays `Any`, and that one is a real boundary rather than
# a gap: three engines return three unrelated objects, and `reading` yields the
# object straight to caller code that may call anything on it. What IS declared
# is the shape of the call — one `Database` in, an awaitable out.
Connect = Callable[["Database"], Awaitable[Any]]
# The door DDL goes through, which is not always the door a write goes
# through. See `Database.for_ddl`.
ForDDL = Callable[["Database"], "AbstractAsyncContextManager[Any]"]
# How many connections it closed: one, or none for an engine that holds
# its connection open past every write.
CloseConnection = Callable[[Any], Awaitable[int]]
ReplicateOnce = Callable[["Database"], Awaitable[bool]]
# HOW MANY PASSES RAN, NOT NOTHING. Both were `Awaitable[None]`, so the
# implementation for an engine with nothing to do was a function with an empty
# body, which reads to a scanner as code nobody wrote and to a reader as a
# line that might have been deleted. A count is a true answer either way, and
# `flush` could report whether it sent anything.
RefreshOnce = Callable[["Database"], Awaitable[int]]
Release = Callable[["Database"], Awaitable[int]]
Sleep = Callable[[float], Awaitable[None]]

# HOW A DATABASE STOPS LISTENING FOR THE SIGNAL THAT ENDS THE PROCESS. It
# answers how many handlers it put back, so "nothing was trapped" and "the
# handlers came off" are different answers rather than the same silence.
Untrap = Callable[[], int]


class WritesInFlight(TypedDict):
    """How many writes are running right now.

    IT WAS A LOCK UNTIL 2026-09-01, carrying its own count of writers queued
    behind it. The lock is gone, because a replica holds one connection and one
    connection runs many writers. The COUNT is not gone, and deleting it with
    the lock disabled opportunistic replication for an afternoon.

    IT DESCRIBES THE CONTENDED RESOURCE. Replication goes out on the same
    connection every write uses, so a write in flight is a request paying for
    the round trip that replication would otherwise take. Counting anything
    else — CPU, active readers, a queue length — is a cheaper signal standing
    in for the real noun, which is the substitution behind every replication
    incident this package has had.

    A reader is not counted. `reading` contends with nothing and must never
    defer replication.
    """

    count: int


@helper
def no_writes_in_flight() -> WritesInFlight:
    return {"count": 0}


@helper
def writes_in_flight(db: Database) -> int:
    """How many writes are running on this database right now."""
    return db["in_flight"]["count"]


class Database(TypedDict):
    """A database, as data.

    `path` is the local file. `primary` is the cloud database it replicates
    to, or None when there is none — and that single field decides everything
    else, which is why it is a field and not an argument anyone can pass.

    `connect` and `close_connection` are the injected boundary. `serialise` is
    the lock a replicated database needs and a local one does not; it is None
    for local, which is the whole difference expressed as data rather than as
    a branch someone has to remember.

    `for_ddl` is the door a SCHEMA CHANGE goes through, and it is not always
    the door a write goes through. On a local Turso database DDL must run on
    WAL: a table created on an MVCC connection is invisible to any other
    connection that has already read, which is silent and total. Measured
    2026-08-14, pyturso 0.7.2, the other connection open in every case:

        A writes, then DDL                  the other connection is fine
        the other connection READ first     "Parse error: no such table: t"
        ALTER instead of CREATE             fine
        the other connection WROTE first    fine

    One row fails and it fails every run, so the trigger is a prior READ.

    On a replicated database the same substitution would be wrong: DDL has to
    go through the held sync connection or it never reaches the primary. So
    the right door differs by topology, is known at open, and is a field here
    rather than a branch at the call site.
    """

    path: str
    # `Engine`, not `str`. This field IS the one place the engine is decided,
    # now that no public function takes it as an argument, so a loose type here
    # would put every string back in play at the one place that matters.
    engine: Engine
    journal_mode: str
    busy_timeout_s: float
    primary: str | None
    token: str | None
    connect: Connect
    close_connection: CloseConnection
    for_ddl: ForDDL
    in_flight: WritesInFlight
    shutdown: ShutdownPolicy
    write_one: WriteOne
    closed: bool
    untrap: Untrap
    replicate_once: ReplicateOnce
    refresh_once: RefreshOnce
    release: Release
    sleep: Sleep
    retry_delay_s: float


@helper
def is_replicated(db: Database) -> bool:
    """True when this database has a primary.

    The one predicate the rest of the library branches on: whether there is
    somewhere to replicate to, and so whether shutdown has anything to push.

    IT DOES NOT DECIDE WHETHER WRITERS SERIALISE. It used to, and that was
    wrong. A replica takes one connection, and one connection runs many
    writers under `BEGIN CONCURRENT`, so a count of connections says nothing
    about how many writes can be in flight. Measured 2026-09-01 against a real
    primary: 16 concurrent writers with no lock, nothing blocked, 17 of 17
    durable locally and 17 of 17 on the primary. See `turso_database.py`.

    It is derived from data rather than stored as a second flag that could
    disagree with `primary`.
    """
    return db["primary"] is not None


@helper
def _still_open(_db: Database) -> int:
    """The database is open, so the caller may go on."""
    return 0


@helper
def _refuse_closed(_db: Database) -> int:
    """`close` released everything, so there is nothing left to work on."""
    raise DatabaseClosedError("Database has been closed")


_IS_CLOSED: dict[bool, Callable[[Database], int]] = {
    True: _refuse_closed,
    False: _still_open,
}


@helper
def _check_open(db: Database) -> None:
    _IS_CLOSED[db["closed"]](db)


@helper
@asynccontextmanager
async def reading(db: Database) -> AsyncIterator[Any]:
    """A connection for reading, for the span of the block.

    Reads never touch the primary — the local file is a full copy — so a
    reader takes no lock and waits for nothing.
    """
    _check_open(db)
    conn = await db["connect"](db)
    try:
        yield conn
    finally:
        await db["close_connection"](conn)


@helper
@asynccontextmanager
async def _write_connection(db: Database) -> AsyncIterator[Any]:
    """One connection, one transaction, one place a write block can end.

    A TRANSACTION SCOPE, NOT JUST A CONNECTION. `writing` yielded and did
    nothing else, so a block that forgot `await conn.commit()` lost its write
    and reported nothing. Measured 2026-08-14: two writes through two blocks,
    the second without a commit, left one row.

    Committing cannot be a call to `conn.commit()`, because asyncpg has none;
    see `writers.COMMIT`. A second commit is harmless on the engines that have
    one, so a caller who still writes it explicitly is unaffected.

    THIS IS A SEPARATE FUNCTION BECAUSE `writing` HAS TWO PATHS AND I FIXED
    ONE. A local database takes an early return with no lock; a replicated one
    takes the serialised path. The commit went into the second only, so every
    local write still vanished, and the probe that caught it read 0 rows where
    it had read 1 before the "fix". Two paths that must agree are one function
    or they drift.
    """
    # COUNTED AROUND THE WRITE, not anywhere convenient. A counter that is
    # initialised and never incremented satisfies every test that sets it by
    # hand, which is how machinery that does nothing passes for healthy. Around
    # the write it means exactly: writes holding the connection replication
    # would take.
    db["in_flight"]["count"] += 1
    conn = await db["connect"](db)
    try:
        async with TRANSACTIONS[db["engine"]](conn):
            yield conn
    finally:
        await db["close_connection"](conn)
        db["in_flight"]["count"] -= 1


@helper
# honest-code-allow: L1.21.1 - three answers, not one. no_passes says zero passes ran, already_delivered says the primary has everything, nothing_happened says no pass ran: different facts, different types, and each the do-nothing half of a DIFFERENT dispatch pair injected into a different Database field. One function over a table would return int or bool by row and could not be typed
async def no_passes(_db: Database) -> int:
    """Zero passes ran, which is a real answer and not an absence.

    Every `Database` carries `refresh_once`, `release`, `refresh` and `flush`
    as callables, so an engine with nothing to do still has to answer, and the
    answer is how many passes it ran.

    NINE IDENTICAL BODIES WITH NINE DIFFERENT DOCSTRINGS stood here and in the
    three engine modules. The answer is the same in all of them; only the
    reason differs, so the reason moved to the injection site, where a reader
    is when they ask why this engine does nothing here.
    """
    return 0


@helper
def nothing_was_trapped() -> int:
    """No signal handler was installed, so none comes off. Zero is the answer.

    NOT AN EMPTY BODY. `close` reads this count, and "there was nothing to
    remove" has to look different from "this was never written". A database
    that has only just been built has trapped nothing, which is a fact about
    the value rather than a default absorbing anybody's omission -- the same
    kind of fact as `closed: False` beside it.
    """
    return 0


@helper
async def already_delivered(_db: Database) -> bool:
    """Yes, everything is where it belongs, because there is nowhere to send it.

    `replicate_once` answers "has the primary got it all". An engine with no
    primary has, vacuously, and saying so is what keeps `flush` from looping
    forever waiting for a delivery that will never be reported.
    """
    return True


@helper
async def nothing_happened(_db: Database) -> bool:
    """No, and that is an answer rather than a failure.

    The same shape as `no_passes` for the calls that report whether a pass
    happened rather than how many. Two functions and not one because the two
    return types are different things: a count of passes and a yes or no.
    """
    return False


@helper
@asynccontextmanager
async def writing(db: Database) -> AsyncIterator[Any]:
    """A connection for writing, for the span of the block.

    NO DATABASE SERIALISES ITS WRITERS, on any engine. There is one write path
    and it takes no lock.

    IT SAID THE OPPOSITE UNTIL 2026-09-01: that a replicated database
    serialises its writers because the engine allows one, citing a run where
    1 of 8 concurrent writes landed and 7 errored. That run opened eight
    connections. A replica holds ONE and hands it to every write, so the run
    described a situation this code never creates, and `BEGIN CONCURRENT`
    carries many writers on one connection. Re-measured against a real
    primary: 16 concurrent writers with no lock, nothing blocked, 17 of 17
    durable locally and on the primary.

    The block commits on a clean exit and rolls back on an exception. See
    `_write_connection`.
    """
    _check_open(db)
    async with _write_connection(db) as conn:
        yield conn


# ---------------------------------------------------------------------------
# Replication. Two copies of one database exist, and these functions bring them
# into conformity. "Sync" means synchronous and nothing else.
# ---------------------------------------------------------------------------


@helper
async def replicate(db: Database) -> bool:
    """Bring this copy and its primary into conformity. BOTH DIRECTIONS, one pass.

    UP FIRST, THEN DOWN, AND THE ORDER IS LOAD-BEARING. Bringing the primary's
    changes down first could overwrite a local commit that has been waiting
    since the last shutdown. Sending up first makes the local copy the
    primary's problem before the primary becomes the local copy's.

    THIS USED TO SEND ONLY. `replicate` pushed, `refresh` pulled, and the
    comment above them claimed the pair "bring the two copies into conformity"
    — which neither did alone. A caller that only ever called `replicate` had a
    local copy that learned nothing about its primary for the life of the
    process, and nothing said so.

    It also returned True for a local-only database, and its own docstring said
    the value meant nothing: "true by vacuity rather than by success". A caller
    cannot tell that apart from a replication that worked, so it now raises.
    """
    _HAS_A_PRIMARY[is_replicated(db)](db)
    _check_open(db)
    return await _AFTER_SENDING[await db["replicate_once"](db)](db)


@helper
def _primary_stands(_db: Database) -> int:
    """There is a primary, so there is something to bring into conformity."""
    return 0


@helper
def _refuse_local_only(db: Database) -> int:
    """A local-only database has no second copy, so the request has no meaning."""
    raise ValueError(
        f"cannot replicate {db['path']}: it has no primary. A local-only "
        f"database has nothing to bring into conformity — open it with a "
        f"primary, or do not ask."
    )


_HAS_A_PRIMARY: dict[bool, Callable[[Database], int]] = {
    True: _primary_stands,
    False: _refuse_local_only,
}


@helper
async def _bring_the_primary_down(db: Database) -> bool:
    """The writes went up, so now the primary's changes come down."""
    await db["refresh_once"](db)
    return True


_AFTER_SENDING: dict[bool, Callable[[Database], Awaitable[bool]]] = {
    True: _bring_the_primary_down,
    # The primary did not take the writes, so the local copy still holds
    # them.
    False: nothing_happened,
}


@helper
async def replicate_if_idle(db: Database) -> bool:
    """Replicate only when no writer is waiting. Returns whether a pass ran.

    OPPORTUNISTIC MEANS YIELDING TO REAL WORK. Replication holds the same
    connection every write holds, so a write in flight is a request paying for
    the round trip.

    A DEFERRED PASS IS DROPPED, NOT QUEUED. Ten passes skipped under load
    produce one pass when the load clears, not ten — otherwise recovering load
    releases a backlog onto a connection at the moment it can least afford it.
    """
    idle = not db["closed"] and is_replicated(db) and writes_in_flight(db) == 0
    return await _IF_IDLE[idle](db)


_IF_IDLE: dict[bool, Callable[[Database], Awaitable[bool]]] = {
    True: replicate,
    # Closed, local-only, or a writer is waiting: the pass is dropped
    # rather than queued.
    False: nothing_happened,
}


@helper
async def replication_loop(db: Database, wanted: asyncio.Event) -> None:
    """Replicate when work arrives and no writer waits. NOTHING IS ON A CLOCK.

    `wanted` is an event a commit sets, and the caller's idleness sets. A timer
    would wake whether or not anything was pending, take the connection to find
    out, and still leave a fresh write sitting for a whole interval.
    """
    while not db["closed"]:
        await wanted.wait()
        # Cleared BEFORE the pass, never after. Clearing afterwards discards a
        # signal that arrived while the pass was running, and that write then
        # waits for an unrelated trigger — a lost wakeup, which is how an
        # eventual-consistency loop quietly stops being eventual.
        wanted.clear()
        # Closed while we waited: nothing runs, and the loop's own condition
        # ends it on the next turn.
        await _AFTER_WAKING[db["closed"]](db)


_AFTER_WAKING: dict[bool, Callable[[Database], Awaitable[bool]]] = {
    # Closed, so the wake-up finds nothing to do.
    True: nothing_happened,
    False: replicate_if_idle,
}


@helper
async def replicate_on_shutdown(db: Database, *, now: Callable[[], float]) -> None:
    """Block until the primary has everything, and SAY SO WHILE IT HAPPENS.

    THE ONE PLACE BLOCKING IS CORRECT, AND THE ONE PLACE POLITENESS IS WRONG.
    On ephemeral disk anything not replicated when the process dies is gone,
    and sustained load is exactly when the most unreplicated data is sitting
    there. So this ignores `writes_in_flight` entirely.

    IT LOGS BECAUSE A PROCESS THAT WILL NOT EXIT IS INDISTINGUISHABLE FROM A
    HUNG ONE. Every line carries the path and the elapsed time, so an operator
    can tell work from a stall, and can tell WHICH database is holding up the
    exit when several are open.

    `now` is injected rather than called here, which is what lets a test assert
    the elapsed time appears without sleeping for it.
    """
    await _ON_SHUTDOWN[is_replicated(db)](db, now)


@helper
async def _nothing_to_deliver(_db: Database, _now: Callable[[], float]) -> int:
    """A local-only database has no primary, so shutdown waits for nothing."""
    return 0


@helper
async def _deliver_before_exit(db: Database, now: Callable[[], float]) -> int:
    """Retry until the primary has everything, saying so on every attempt."""
    started = now()
    say(
        __name__,
        "info",
        "Shutdown: replicating %s to its primary before exit. The process will "
        "not exit until this completes.",
        db["path"],
    )
    attempt = 0
    while not await db["replicate_once"](db):
        attempt += 1
        say(
            __name__,
            "warning",
            "Shutdown: replicating %s — attempt %d failed after %.1fs. "
            "Not yet delivered to the primary.",
            db["path"],
            attempt,
            now() - started,
        )
        await db["sleep"](db["retry_delay_s"])
    await db["refresh_once"](db)
    say(
        __name__,
        "info",
        "Shutdown: %s is in conformity with its primary after %.1fs (%d retries).",
        db["path"],
        now() - started,
        attempt,
    )
    return 1


_ON_SHUTDOWN: dict[bool, Callable[[Database, Callable[[], float]], Awaitable[int]]] = {
    True: _deliver_before_exit,
    False: _nothing_to_deliver,
}


@helper
async def _refresh_from_primary(db: Database) -> int:
    """Take whatever the primary has that this copy does not."""
    _check_open(db)
    await db["refresh_once"](db)
    return 1


_REFRESH: dict[bool, Callable[[Database], Awaitable[int]]] = {
    True: _refresh_from_primary,
    # No primary, so there is nothing to bring down.
    False: no_passes,
}


@helper
async def _flush_to_primary(db: Database) -> int:
    """Keep sending until the primary takes it, then take back what it has."""
    # IGNORES LOAD. A caller that awaited this has said it is willing to wait;
    # politeness belongs to the background pass, not here.
    while not await db["replicate_once"](db):
        await db["sleep"](db["retry_delay_s"])
    await db["refresh_once"](db)
    return 1


_FLUSH: dict[bool, Callable[[Database], Awaitable[int]]] = {
    True: _flush_to_primary,
    # No primary, so every write is already where it is going.
    False: no_passes,
}


@helper
async def close(db: Database) -> Database:
    """Replicate whatever is left, then release everything.

    Retries the final replication until it succeeds, because the local file
    may be on disk that does not survive the process. Returns the closed
    value rather than mutating in place, so a caller cannot hold a `Database`
    that silently became unusable.

    `flush` on a local-only database returns at once, so it is called
    unconditionally here rather than guarded a second time.
    """
    await flush(db)
    await db["release"](db)
    # THE HANDLERS THIS DATABASE INSTALLED COME OFF WITH IT. `trap_shutdown`
    # used to hand the undo back as a loose callable and both openers dropped
    # it, so a SIGTERM arriving after this line drove a released database.
    # The value returned says nothing is trapped any more, so a second close
    # finds nothing left to remove rather than removing somebody else's.
    _put_back = db["untrap"]()
    return {**db, "closed": True, "untrap": nothing_was_trapped}


# THE TWO DIRECTIONS A CALLER ASKS FOR BY NAME, one row each. Both were one
# line -- look up whether this database has a primary, and call -- so the table
# and the words were the only things that differed.
#
# ON A LOCAL FILE BOTH RETURN AT ONCE, and that is an answer rather than an
# omission: there is nowhere to send and nothing to fetch, and a caller that
# awaited either has been told so.
_DIRECTIONS: dict[str, tuple[dict[bool, Any], str]] = {
    "flush": (
        _FLUSH,
        "Block until every local write has reached the primary.\n\n"
        "    The deliberate durability call. Writes reach the primary on a\n"
        "    background schedule; a caller that needs them there NOW says so\n"
        "    here. On ephemeral disk, anything not replicated when the process\n"
        "    dies is gone.\n    ",
    ),
    "refresh": (
        _REFRESH,
        "Bring the primary's changes down into the local copy.\n\n"
        "    The other direction. Named for what it does to the local file\n"
        '    rather than "pull", which describes the wire and not the effect.\n'
        "    ",
    ),
}


def _direction(name: str) -> Callable[[Database], Awaitable[None]]:
    """Build the function a caller names for one direction of replication."""

    table, purpose = _DIRECTIONS[name]

    @helper
    async def move(db: Database) -> None:
        await table[is_replicated(db)](db)

    move.__name__ = name
    move.__qualname__ = name
    move.__doc__ = purpose
    return move


flush = _direction("flush")
refresh = _direction("refresh")
