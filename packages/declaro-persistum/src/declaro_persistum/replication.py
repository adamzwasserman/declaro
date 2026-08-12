"""Replication: pushing local frames to the Turso primary.

Lifted out of TursoPool, which was 1044 lines inside a 2689-line pool.py
— a Slop Audit L1.17 god-file (declaro-tvx). Shipping WAL frames over the
network is not handing out connections, and it was thirteen of the pool's
thirty-seven methods.

Functions, not methods, taking the pool explicitly. TursoPool keeps thin
delegates so `pool.flush()` and `pool.push_healthy` still work for
consumers.

The load-bearing property: NOTHING on a consumer request path waits on
this. A write commits to the local replica and returns; the push loop
delivers to cloud afterwards. `flush()` and `close()` are the deliberate
exceptions, called by someone who has decided to wait.

Still open here: writes are stranded under MVCC with concurrent write
connections, cause NOT established — 17 written, 4 on the primary, no
convergence in 348s (measured 2026-08-10, declaro-p39). Making the push
cover every holder did not fix it. Do not assume this module is correct.

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

A SYNCED REPLICA TAKES ONE SYNC CONNECTION. That is the constraint, and it
is NOT about MVCC. Measured 2026-08-12 against a real replica, pyturso 0.7.2:

    MVCC on a synced replica          journal_mode = 'mvcc', 4 of 4 runs
    20 writes, sequential, 1 conn     20 local -> 20 ON PRIMARY, no checkpoint
    8 writes over 8 connections       5 local -> 0 ON PRIMARY, no convergence
    opening a 2nd sync connection     "database tape error: database is busy"
                                      3 of 4 runs failed outright, one with
                                      12 retries over 30s on an IDLE database

So MVCC plus cloud sync is fine for sequential writes. What breaks is more
than one sync connection against one replica, which is what persistum's
one-connection-per-write does the moment nothing serialises it. MVCC is
incidental: it is merely the mode in which `_write_serialisation` stops
taking the lock, and that lock is what has been masking this on WAL.

THIS PARAGRAPH PREVIOUSLY SAID "MVCC IS LOCAL ONLY ... it creates local-only
internal tables the sync engine cannot reconcile." Both halves were wrong.
MVCC runs on a synced replica, measured repeatedly, and the internal-table
mechanism was asserted from one correlational observation and never proven.
The engine has never refused this combination; persistum's policy did.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def pause_push(pool) -> None:
    """Pause the background push loop (e.g. during migrations)."""
    pool._push_paused = True


def resume_push(pool) -> None:
    """Resume the background push loop."""
    pool._push_paused = False


def set_push_failure_callback(pool, callback: Any, *, threshold: int = 1) -> None:
    """Register a callback invoked when the push loop crosses ``threshold``
    consecutive failures.

    The callback is called as ``callback(error, consecutive_failures)`` once
    per failure episode (re-armed after the next successful push). Use this
    to surface non-durable writes — a committed write whose push keeps
    failing is otherwise only visible as a WARNING log line.
    """
    pool._push_failure_callback = callback
    pool._push_failure_threshold = threshold


def last_push_error(pool) -> Exception | None:
    """The most recent push failure, or None if the last push succeeded."""
    return pool._last_push_error


def push_healthy(pool) -> bool:
    """True when the last push attempt succeeded (or none has failed)."""
    return pool._last_push_error is None


def record_push_failure(pool, error: Exception) -> None:
    pool._consecutive_push_failures += 1
    pool._last_push_error = error
    logger.warning("Push to cloud failed: %s", error)
    if (
        pool._push_failure_callback is not None
        and pool._push_failure_threshold
        and pool._consecutive_push_failures >= pool._push_failure_threshold
        and not pool._push_failure_notified
    ):
        pool._push_failure_notified = True
        try:
            pool._push_failure_callback(error, pool._consecutive_push_failures)
        except Exception:
            logger.exception("push failure callback raised")


def record_push_success(pool) -> None:
    if pool._consecutive_push_failures > 0:
        logger.info(
            "Push to cloud recovered after %d failures",
            pool._consecutive_push_failures,
        )
    pool._consecutive_push_failures = 0
    pool._last_push_error = None
    pool._push_failure_notified = False


def local_replica_has_data(pool) -> bool:
    """True when a non-empty local replica file already exists.

    Decides whether the initial sync can be backgrounded: with data on
    disk the pool can serve reads immediately, without it the pool would
    otherwise hand out an empty database.

    An unreadable or missing path answers False — the conservative
    direction, since a False answer only costs a blocking sync while a
    wrong True serves empty results.
    """
    try:
        return os.path.getsize(pool._database_path) > 0
    except OSError:
        return False


async def initial_sync(pool) -> None:
    """Deliver un-pushed local writes, then pull cloud state.

    The push must precede the pull: a prior process may have committed
    locally and died before pushing, and pull() would overwrite those
    frames with cloud state (W3). That ordering holds whether this runs
    inline or as a background task.

    Never raises. When backgrounded there is no caller to catch it, and a
    failed refresh must not kill the pool — the replica stays readable at
    its current revision and the push loop keeps retrying. The error is
    recorded for initial_pull_complete() to re-raise at a call site that
    did ask to wait.
    """
    try:
        await pool._push_once()
        if pool._write_holder:
            await pool._write_holder.pull()
    except Exception as e:
        pool._initial_sync_error = e
        logger.warning(
            "Initial sync failed for %s; serving the local replica at its "
            "current revision and retrying via the push loop: %s",
            pool._database_path,
            e,
        )
    finally:
        if pool._initial_sync_event:
            pool._initial_sync_event.set()


async def initial_pull_complete(pool) -> None:
    """Wait until the pool's initial cloud sync has finished.

    Await this before any operation that must not observe a stale
    replica — schema introspection above all, where a stale read makes
    the differ compute against a schema that is not the primary's and
    emit operations that correct code then faithfully applies.

    Returns immediately for local-only pools, and for pools whose sync
    already ran inline. Re-raises the initial sync's failure, so a caller
    that asked for a consistent view is told it did not get one rather
    than proceeding on stale data.
    """
    if pool._initial_sync_event is not None:
        await pool._initial_sync_event.wait()
    if pool._initial_sync_error is not None:
        raise pool._initial_sync_error


async def push_once(pool) -> bool:
    """Ship pending frames to cloud on the write connection, in turn.

    The push used to hold a sync connection of its own so a write never
    waited for a cloud round trip. It no longer does: nothing is waiting
    on the push, so it can queue behind writes like anything else, and
    one fewer connection is one fewer thing writing to the replica.

    This does NOT rest on any claim that the sync engine takes a single
    writer. Turso supports concurrent writers through MVCC and
    BEGIN CONCURRENT, and `Error::Busy` at commit is a documented,
    retryable conflict signal rather than evidence of a broken shape.
    See docs/turso-cloud-sync.md.

    There is deliberately no per-push delivery check. One was written and
    removed: it compared the replica's sync revision either side of each
    push, and fired on essentially every push that had pending writes.
    Two downstream runs measured it -- 1002 warnings in a capacity test,
    and 32 in a 66-second soak where an independent oracle confirmed all
    2541 writes were delivered. It never once indicated real loss. Push
    failures are surfaced by _record_push_failure, last_push_error and
    the push-failure callback, which report what happened rather than
    inferring it.
    """
    if not pool._remote_url:
        return True

    if pool._write_holder is None or pool._write_holder.conn is None:
        return False

    # The push contends with writers on the replica, and it is the one
    # operation this pool can safely retry: it ships frames, so there
    # are no caller statements to replay. Contention is absorbed here
    # rather than serialised away, so no writer waits on a round trip.
    # Pushes writer zero only. Frames committed on the other write
    # connections are stranded: measured against a real replica, 17
    # writes reported ok, 4 reached the primary, and it never converged
    # in 348s. Making the push cover every holder in _write_holders did
    # NOT fix it -- the frames were still stranded -- so the cause is
    # not simply which connection is pushed. Open, cause unknown.
    try:
        await pool._retry_while_busy(pool._write_holder.push, "push")
    except Exception as e:
        pool._record_push_failure(e)
        return False

    pool._record_push_success()
    return True


async def push_loop(pool) -> None:
    """Guaranteed eventual consistency loop.

    Retries indefinitely with exponential backoff (capped at 30s).
    Acquires _conn_lock for push, then releases — reads and writes
    can proceed between push attempts without waiting for cloud I/O.
    Failure/recovery state is tracked on the pool (see _record_push_*).
    """
    max_backoff = 30.0

    while not pool._closed:
        if getattr(pool, "_push_paused", False):
            await asyncio.sleep(pool._push_interval_s)
            continue

        success = await pool._push_once()

        if success:
            await asyncio.sleep(pool._push_interval_s)
        else:
            delay = min(
                pool._push_retry_base_s * (2 ** pool._consecutive_push_failures),
                max_backoff,
            )
            logger.warning(
                "Push to cloud: %d consecutive failures, retrying in %.1fs",
                pool._consecutive_push_failures, delay,
            )
            await asyncio.sleep(delay)


async def flush(pool) -> None:
    """Block until all pending local writes have been pushed to cloud.

    Retries indefinitely with exponential backoff.  Does NOT close
    the pool — the connection remains usable after flush returns.
    """
    if pool._write_holder and pool._remote_url:
        attempt = 0
        while not await pool._push_once():
            attempt += 1
            delay = min(pool._push_retry_base_s * (2 ** attempt), 30.0)
            logger.warning("Flush attempt %d failed, retrying in %.1fs", attempt, delay)
            await asyncio.sleep(delay)
