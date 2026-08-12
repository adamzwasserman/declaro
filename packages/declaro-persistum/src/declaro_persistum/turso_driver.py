"""pyturso driver adapters: a holder, a connection, and a cursor.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). These are DB-API shims over pyturso, not pooling
— the pools use them, nothing here knows a pool exists.

`_TursoConnectionHolder` owns the pyturso connection and supports both
modes: async via turso.aio / turso.aio.sync, and the blocking API on a
dedicated executor thread for the sync pools.

Worth knowing while reading this: pyturso's async API is a worker THREAD
per connection wrapping the blocking driver (see turso/lib_aio.py), not
native async I/O. `await` frees the event loop; it does not make the
engine concurrent. Concurrency comes from separate connections. The cost
numbers are in pool.py's module docstring.

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
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

logger = logging.getLogger(__name__)






