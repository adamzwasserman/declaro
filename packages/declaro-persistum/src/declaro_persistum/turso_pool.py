"""The Turso pool: stateless writes, MVCC concurrency, background push.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). The measured costs, the reason writes are
stateless by default, and the argument NOT to re-derive in favour of
pooling all live in pool.py's module docstring. Read it before changing
how a write acquires its connection.

The replication half of this pool — the push loop, its failure
accounting, and the initial pull — lives in replication.py as functions
taking the pool. They are I/O against a remote and have nothing to do
with handing out connections.

DEPRECATED / POISONOUS PRACTICE — `pooled_writes`, and the argument for
stateless writes in pool.py's module docstring, are both poisonous. They
put a pool decision on a consumer-facing surface. The consumer chooses
async or sync and nothing else; whether a pool exists behind that is
internal, single-owner, and invisible above the boundary. See
docs/design/state-ownership-and-the-pool-boundary.md.

This class currently has four writers of its holder state — the acquire
path, the push loop, the migration refresh, and close. That is the defect
those documents were arguing around instead of removing.

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
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from declaro_persistum import replication
from declaro_persistum.exceptions import (
    PoolClosedError,
    PoolConnectionError,
)
from declaro_persistum.turso_driver import (
    TursoAsyncConnection,
    _TursoConnectionHolder,
)

logger = logging.getLogger(__name__)

# The write connection belonging to the transaction the current task is
# inside, if any. A ContextVar rather than pool state because a transaction
# belongs to one task: two requests running concurrently against one pool
# must not join each other's transaction, and asyncio gives each task its
# own copy of this automatically.
import contextvars

_active_transaction: contextvars.ContextVar[Any | None] = contextvars.ContextVar(
    "declaro_active_transaction", default=None
)


