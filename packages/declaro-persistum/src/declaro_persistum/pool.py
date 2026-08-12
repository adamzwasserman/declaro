"""
Unified connection pool for declaro_persistum.

Provides a consistent async context manager API for:
- PostgreSQL (asyncpg with native pooling)
- SQLite (aiosqlite with semaphore-based limiting)
- Turso (pyturso with semaphore-based limiting + optional replication)

Example:
    pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")
    async with pool.acquire() as conn:
        results = await users.select().execute(conn)
    await pool.close()

READ THIS BEFORE RAISING PUSH OR REPLICATION ABOUT THE WRITE PATH
-----------------------------------------------------------------

From the owner of this repository, verbatim and repeatedly:

    I DO NOT GIVE A FUCK ABOUT PUSH/SYNC
    I DO NOT GIVE A FUCK ABOUT PUSH/SYNC
    I DO NOT GIVE A FUCK ABOUT PUSH/SYNC
    I DO NOT GIVE A FUCK ABOUT PUSH/SYNC
    I DO NOT GIVE A FUCK ABOUT PUSH/SYNC

It had to be said that many times because it was ignored that many times.
Push and cloud delivery were raised as a blocker on the write path in
three consecutive exchanges after the first time it was made clear they
are not one.

Operationally: cloud delivery is eventual and is the push loop's problem,
on writer zero. It is NOT a constraint on the shape of the write path, it
is NOT a reason to hold connections, and the unresolved stranding defect
(declaro-p39) is NOT an argument for or against anything here. Do not
gate write-path work on it. Do not re-open it in a docstring. Do not put
it in front of the person who wrote the five lines above.

Direction of travel: stateless writes for Turso embedded, by default
--------------------------------------------------------------------

Pooling a write connection is machinery that a stateless write does not
need. A connection that opens, writes, commits and closes holds nothing,
so it has no holding cost to pay and no state to leak, pin or poison.
Everything the pool below does for writers exists to manage state that a
stateless write never creates.

What an open costs, measured 2026-08-11 on this machine, raw pyturso,
no declaro code in the path. Python 3.14.0 free-threaded, macOS arm64:

    Local file                                      p50        p99
      turso.connect() open (blocking)            0.026ms    0.064ms
      blocking close()                           0.006ms    0.032ms
      INSERT + commit, existing connection       0.064ms    1.513ms
      turso.aio.connect() open                   0.289ms    4.497ms
      turso.aio close()                          0.463ms    3.205ms
      INSERT + commit, aio, existing connection  0.349ms    2.978ms

    Cloud replica (aws-us-west-2, turso.aio.sync)   p50        p99
      COLD open, empty dir, bootstrap pull      440.299ms 1030.403ms
      WARM reopen, all four files present         0.985ms    5.034ms
      EXTRA conn to already-open replica          1.162ms    2.210ms
      close()                                     0.593ms    1.850ms
      INSERT + commit on open replica             0.501ms    2.749ms

So an open-per-write on a live cloud replica costs 1.16ms + 0.59ms of
setup and teardown around a 0.50ms write. That is the entire price of
statelessness, and it is a fixed price that does not grow with uptime.

The 440ms cold open is the bootstrap pull. It is paid once, when the
replica directory does not exist yet. It is not a per-connection cost
and must not be quoted as one.

An open is a thread. The database open itself is 0.026ms — under a tenth
of the async open. The rest is `threading.Thread.start()`, because
pyturso's async API is a worker thread per connection (see
`turso/lib_aio.py`), not native async I/O. The Turso MVCC engine has no
async I/O yet either. `await` frees the event loop; it does not make the
engine concurrent. Concurrency comes from separate connections, which
Turso documents directly:
https://docs.turso.tech/tursodb/concurrent-writes

DEPRECATED — POISONOUS PRACTICE — the section below
---------------------------------------------------

Everything from here to the end of this docstring is poisonous practice
and is retained only as a record. It argues at length about which write
strategy should be the DEFAULT on a consumer-facing surface. That the
argument was had at all is the defect: the pool decision must never reach
the consumer or the common syntax.

The consumer chooses async (default) or sync. Nothing else. Whether a pool
exists behind that choice, whether a write reuses a connection, and
whether the engine runs MVCC or WAL are internal, owned by exactly one
writer, and invisible above that boundary.

`pooled_writes` is the widest this leak has ever been, and it was added
2026-08-11 by the same reasoning the section below sets out.

Binding constraint: docs/design/state-ownership-and-the-pool-boundary.md

Why the pooled write path is NOT the default  [RETAINED AS A RECORD]
--------------------------------------------

It is still here, behind an explicit opt-in, and this section exists so
that nobody promotes it back to the default by re-deriving the argument
for it. That argument has been made and it is wrong. Read this before
writing a free list, a writer semaphore or a shared write holder.

1. A pool is machinery for managing state. A stateless write creates no
   state, so the machinery has nothing to manage. Asking "how should we
   pool write connections" skips the question of whether anything needs
   pooling.

2. The cost of statelessness is one fixed, measured number: 1.16ms open
   plus 0.59ms close, per write, on a live cloud replica. It does not
   grow with uptime or concurrency.

3. The cost of the pool is a set of quantities that DO grow with uptime
   and concurrency — resident threads, pinned row versions, checkout
   overhead, poisoned connections — and not one of them is measured
   anywhere in this repo. See `_write_connection` for the itemised ledger.

4. The comparison was originally made by pricing (2) and giving (3) a
   pass, on the unexamined assumption that a pool is simply how this is
   done. That is the reasoning error to not repeat. If the pool is ever
   argued back to the default, the argument must price the holding cost
   first, with measurements.

5. "Per-write connections caused push failures because each connection
   tracked its own replication state" is the specific claim that will surface
   in favour of pooling. It is UNVERIFIED — an explanation, never a
   measurement. See `acquire_write`.

What the opt-in is for: a caller who wants writers serialised behind one
held connection, deliberately, with the cost accepted. It is not a
fallback for "stateless felt risky".

THE TWO LEVERS. Measured 2026-08-12, Mac, crew 16, 2000 writes, 3 retries,
journal mode asserted on every connection. DO NOT RE-DERIVE THIS.

    WAL  + one-and-done     250 writes/s   1629/2000 landed   371 LOST
    WAL  + persistent      1505 writes/s   1812/2000 landed   216 LOST
    MVCC + one-and-done     426 writes/s   2000/2000 landed     0 lost
    MVCC + persistent      4721 writes/s   2000/2000 landed     0 lost

    reuse alone (WAL)          6.01x
    concurrency alone (MVCC)   1.70x
    both together             18.87x   <- they COMPOUND, they do not add

Connection REUSE removes the per-write OS thread; that is the larger lever.
MVCC plus BEGIN CONCURRENT is what lets a crew write in PARALLEL; that is what
makes the crew CORRECT. Neither alone gets there. The 13,826 writes/sec figure
on pro_ultra requires BOTH.

WAL LOSES WRITES at crew 16 even after three retries. MVCC loses none. So
"WAL plus persistent connections" is not a cheaper safe option, it is a lossy
one. WAL's safe crew is 1, or writers serialised behind a lock.

A REPLICA TAKES ONE REPLICA CONNECTION. That is the constraint, and it
is NOT about MVCC. Measured 2026-08-12 against a real replica, pyturso 0.7.2:

    MVCC on a replica          journal_mode = 'mvcc', 4 of 4 runs
    20 writes, sequential, 1 conn     20 local -> 20 ON PRIMARY, no checkpoint
    8 writes over 8 connections       5 local -> 0 ON PRIMARY, no convergence
    opening a 2nd replica connection     "database tape error: database is busy"
                                      3 of 4 runs failed outright, one with
                                      12 retries over 30s on an IDLE database

So MVCC plus replication is fine for sequential writes. What breaks is more
than one replica connection against one replica, which is what persistum's
one-connection-per-write does the moment nothing serialises it. MVCC is
incidental: it is merely the mode in which `_write_serialisation` stops
taking the lock, and that lock is what has been masking this on WAL.

THIS PARAGRAPH PREVIOUSLY SAID "MVCC IS LOCAL ONLY ... it creates local-only
internal tables the replication engine cannot reconcile." Both halves were wrong.
MVCC runs on a replica, measured repeatedly, and the internal-table
mechanism was asserted from one correlational observation and never proven.
The engine has never refused this combination; persistum's policy did.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

# The write connection belonging to the transaction the current task is
# inside, if any. A ContextVar rather than pool state because a transaction
# belongs to one task: two requests running concurrently against one pool
# must not join each other's transaction, and asyncio gives each task its
# own copy of this automatically.
_active_transaction: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "declaro_active_transaction", default=None
)

# How many times a stateless write retries a contended boundary before it
# raises. Three attempts, then the caller hears about it.
#
# The retry covers the boundaries and NOT the caller's statements. A context
# manager cannot re-run the block it yielded into, so opening the connection
# and BEGIN CONCURRENT are retried (nothing is staged yet) and the commit is
# retried (the statements are staged, so re-committing lands the same set).
# A statement that fails mid-transaction is raised, not replayed. Replaying a
# write the pool never saw as data is what the write queue is for; see
# docs/design/concurrent-writes-and-the-write-queue.md.

from declaro_persistum.exceptions import (
    PoolClosedError,
    PoolConnectionError,
    PoolExhaustedError,
)
# Re-exported so the import paths that existed before pool.py was split
# (declaro-tvx) keep working. ruff strips these as unused without the
# noqa — they are a deliberate compatibility surface, not dead imports.
from declaro_persistum.turso_driver import (  # noqa: F401
    TursoAsyncConnection,
    TursoAsyncCursor,
    _TursoConnectionHolder,
)

if TYPE_CHECKING:
    import aiosqlite
    import asyncpg

















# Re-exported so `from declaro_persistum.pool import MirrorPool` keeps
# working for consumers written before the split (declaro-tvx). The real
# home is declaro_persistum.mirror. Imported at the END of the module: it
# depends on BasePool, which now lives in pool_base, so there is no cycle.
from declaro_persistum.cloud_manager import TursoCloudManager  # noqa: E402
from declaro_persistum.mirror import (  # noqa: E402
    MirrorConnection,
    MirrorCursor,
    MirrorPool,
)
from declaro_persistum.sync_pool import (  # noqa: E402
    SyncConnectionPool,
)

__all__ = [
    "BasePool",
    "TursoAsyncConnection",
    "TursoAsyncCursor",
    "ConnectionPool",
    "MirrorConnection",
    "MirrorCursor",
    "MirrorPool",
    "PostgreSQLPool",
    "SQLitePool",
    "SyncConnectionPool",
    "TursoCloudManager",
    "TursoPool",
]
