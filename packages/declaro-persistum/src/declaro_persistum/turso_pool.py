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

MVCC IS LOCAL ONLY. Never on a synced replica - it creates local-only internal
tables the sync engine cannot reconcile, which is the declaro-p39 stranding.
A synced target therefore gets NO write concurrency at all.
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
from declaro_persistum.pool_base import BasePool
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


class TursoPool(BasePool):
    """
    Turso connection pool using pyturso with optional cloud sync.

    Two modes:
    - **Local-only** (no remote_url): turso.aio.connect() — embedded
      SQLite-compatible DB with MVCC for concurrent writers.
    - **Cloud sync** (remote_url provided): turso.aio.sync.connect() —
      local writes commit sub-ms, background push loop sends changes
      to Turso Cloud every ``push_interval_s`` seconds.

    All connections are natively async (turso.aio) — no ThreadPoolExecutor.
    MVCC with ``BEGIN CONCURRENT`` enables concurrent writers.
    """

    def __init__(
        self,
        database_path: str,
        *,
        remote_url: str | None = None,
        auth_token: str | None = None,
        max_size: int = 5,
        acquire_timeout: float = 30.0,
        push_interval_s: float = 1.0,
        push_retry_base_s: float = 0.1,
        background_pull: bool = True,
        busy_retry_budget_s: float = 5.0,
    ) -> None:
        self._database_path = database_path
        self._remote_url = remote_url
        self._push_retry_base_s = push_retry_base_s
        self._auth_token = auth_token
        self._max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._push_interval_s = push_interval_s
        self._background_pull = background_pull
        # Whether to ask the engine for MVCC. On by default: concurrent writes
        # are a Turso feature and the pool should not decline them. Set False
        # only to force WAL deliberately.
        # THE ENGINE CHOICE IS PERSISTUM'S, NEVER THE CALLER'S.
        #
        #   remote_url set -> synced -> MVCC OFF
        #   no remote_url  -> local  -> MVCC ON
        #
        # MVCC cannot run on a synced replica: it creates local-only internal
        # tables the sync engine cannot reconcile, and writes report success
        # without reaching the primary (declaro-p39; 0.1.29 was yanked for it).
        # This was a caller parameter defaulting to True, so omitting it on a
        # synced pool selected the losing configuration. There is now no way
        # to ask for that.
        self._mvcc_requested = remote_url is None
        # How long to keep absorbing "database is busy" at a transaction
        # boundary before giving up and telling the caller. A busy database
        # means "not now", not "no".
        self._busy_retry_budget_s = busy_retry_budget_s
        # Opt-in: hold write connections in a free list and serialise writers,
        # instead of opening one per write. NOT the default, deliberately.
        # The module docstring says why, and says it in the form of an
        # argument not to re-derive. Read it before flipping this.
        self._replica_write_lock = asyncio.Lock()
        # Read connections. Reads do not sync, so each gets its own plain
        # local connection and they run in parallel. See acquire().
        self._read_holders: list[_TursoConnectionHolder] = []
        self._free_readers: asyncio.LifoQueue[_TursoConnectionHolder] | None = None
        # Write connections. Each concurrent writer gets its own sync
        # connection, so BEGIN CONCURRENT over MVCC can actually do what it
        # is for. See acquire_write.
        # NO WRITE FREE LIST. A write opens its own connection and closes it
        # (`_write_connection`). `_write_holders` / `_free_writers` /
        # `_get_writer` / `_release_writer` were the pooled write path; they
        # became unreachable the moment `pooled_writes` was removed, and dead
        # machinery is how a removed feature comes back. Readers keep a free
        # list — a read holds no unpushed state and pins no row versions.
        # Connections marked stale by a migration. Disposed of on release
        # rather than reused, so no caller waits for a refresh.
        self._stale_holders: set[_TursoConnectionHolder] = set()
        # Kept only so an older caller reading it does not crash. The push
        # has no connection of its own; it uses the write connection and
        # takes its turn. See _push_once.
        self._push_holder: _TursoConnectionHolder | None = None
        # Initial-sync state. The event is created in _initialize, on the
        # running loop, rather than here — a pool may be constructed outside
        # the loop that later runs it.
        self._initial_sync_event: asyncio.Event | None = None
        self._initial_sync_task: asyncio.Task[None] | None = None
        self._initial_sync_error: Exception | None = None
        # No semaphore and no lock. A consumer must never wait on this pool's
        # bookkeeping: not on a lock, not behind a concurrency cap, and never
        # refused because the pool is busy. Reads, writes and the push each
        # have their own connections, so there is nothing shared to guard.
        # max_size bounds retained idle connections only — see _release.
        self._closed = False
        self._active_connections = 0
        self._push_task: asyncio.Task[None] | None = None
        self._write_holder: _TursoConnectionHolder | None = None
        # Push-durability state (W2): a persistently failing push must be
        # observable by the application, not merely logged.
        self._consecutive_push_failures = 0
        self._last_push_error: Exception | None = None
        self._push_failure_callback: Any = None
        self._push_failure_threshold = 0
        self._push_failure_notified = False

    async def _initialize(self) -> None:
        """Bootstrap from cloud (if remote_url) and configure journal mode."""
        self._write_holder = _TursoConnectionHolder(
            self._database_path, self._remote_url, self._auth_token
        )
        # Whether a usable local replica already exists is observable, so
        # observe it rather than assuming. Checked BEFORE connect_async,
        # which bootstraps the file into existence when it is missing and
        # would make the check trivially true afterwards.
        had_local_data = self._local_replica_has_data()

        await self._write_holder.connect_async()
        if self._remote_url:
            self._initial_sync_event = asyncio.Event()
            if self._background_pull and had_local_data:
                # There is a populated replica on disk, so reads can be served
                # the instant connect_async returns. The initial sync is then
                # a freshness step, not a correctness precondition, and
                # blocking the open on it would push network latency onto
                # every caller for no benefit.
                #
                # Callers needing a primary-consistent view await
                # initial_pull_complete(). apply_migrations_async does exactly
                # that before introspecting, so a backgrounded sync can never
                # feed the differ a stale schema.
                self._initial_sync_task = asyncio.create_task(self._initial_sync())
            else:
                # No local data to serve (first creation, or a wiped ephemeral
                # disk). Returning here without syncing would hand out a pool
                # that reads an empty database and reports success — silently
                # wrong. Blocking is the only correct option in this case, and
                # it is a once-per-replica cost, not a per-open one.
                await self._initial_sync()
        # MVCC is requested on every pool, cloud-backed or not. Turso supports
        # concurrent writes through BEGIN CONCURRENT over MVCC, and
        # acquire_write only issues BEGIN CONCURRENT when self._mvcc is true.
        # This block previously ran only when there was no remote_url, so
        # every cloud pool serialized its writes while the engine below it
        # supported concurrent ones — the configuration that needs the
        # throughput least likely to get it.
        #
        # The engine has the last word. If it does not return 'mvcc' the pool
        # records that and continues on WAL; nothing here fails the open.
        if self._mvcc_requested:
            try:
                cur = await self._write_holder.conn.execute("PRAGMA journal_mode = 'mvcc'")
                rows = await cur.fetchall()
                mode = rows[0][0] if rows else "unknown"
                if mode == "mvcc":
                    self._mvcc = True
                    # Logged on grant as well as refusal. Only the refusal was
                    # logged before, so an operator could not tell which of
                    # the two had happened on a given host — which is exactly
                    # what you need when comparing write latency between two
                    # boxes where one may have been granted MVCC and the other
                    # refused it.
                    logger.info(
                        "MVCC granted for %s (remote_url=%s) — acquire_write will "
                        "use BEGIN CONCURRENT",
                        self._database_path,
                        bool(self._remote_url),
                    )
                else:
                    self._mvcc = False
                    logger.info(
                        "MVCC not available for %s (got %s), using WAL",
                        self._database_path,
                        mode,
                    )
            except Exception as e:
                self._mvcc = False
                logger.info("MVCC request failed for %s (%s), using WAL",
                            self._database_path, e)
        else:
            self._mvcc = False

        if not self._remote_url:
            try:
                await self._write_holder.conn.execute("PRAGMA cache_size = -256")
            except Exception:
                logger.debug("PRAGMA cache_size not supported — using default")
            await self._write_holder.conn.commit()
        else:
            # Cloud-sync replicas must enforce the same FK constraints as the
            # primary. The FK *definition* bootstraps down with the schema, but
            # pyturso leaves enforcement OFF per connection by default — so a
            # write that violates a primary FK commits locally, fails to push
            # ("FOREIGN KEY constraint failed"), and is silently lost on the
            # next re-sync. Enabling enforcement makes that write fail fast at
            # commit instead. Must run outside any transaction (fresh conn here).
            await self._enable_replica_fk_enforcement()
        if self._remote_url:
            # The push connection is NOT opened here. Opening a sync
            # connection costs a cloud handshake — measured at ~790ms — and
            # opening two of them made pool open cost two handshakes when
            # only one is needed before the pool can serve.
            #
            # _push_once opens it on the push loop's first iteration, which
            # runs in a background task. The handshake still happens
            # immediately, but off the path a caller is waiting on.
            self._push_task = asyncio.create_task(self._push_loop())

    async def _enable_replica_fk_enforcement(self) -> None:
        """Turn on FK enforcement for the replica write-holder connection."""
        if not self._write_holder or self._write_holder.conn is None:
            return
        try:
            await self._write_holder.conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            logger.warning(
                "PRAGMA foreign_keys = ON not accepted on replica connection — "
                "FK-violating writes may commit locally and fail to push"
            )

    async def refresh_connections(self) -> None:
        """Reopen every connection so they see a migrated schema.

        Call after DDL migrations. A connection's cached state does not see
        tables created on another connection, even after pull().

        Every connection is refreshed, not just the first write connection.
        A pool now holds several write connections and several read
        connections; refreshing one of them would leave the rest reading and
        writing against a schema that no longer exists. That is the same
        stale-input class as the defects this pool has already shipped.

        Writers are quiesced first. Closing a connection while a writer holds
        it is a use-after-close, so this takes every write slot before it
        touches anything. That is a real pause on writes, and it is the one
        place a pause is correct: a schema is changing underneath them.

        Read connections are discarded rather than reopened. They are opened
        on demand, so the next reader gets a fresh one.
        """
        # Nothing is quiesced and no caller waits. Connections currently held
        # by a writer or reader are marked stale and disposed of when they
        # are released; idle ones are closed here. The next caller opens a
        # fresh connection against the migrated schema.
        #
        # Waiting for writers to drain would have been simpler, but a
        # migration would then stall every write until it finished, which is
        # the cost this pool exists to keep away from callers.
        # Only readers are tracked. Writer zero is refreshed below, and every
        # other write connection is already gone — a stateless write closes
        # its own connection before it returns, so none can outlive a
        # migration and serve the pre-migration schema.
        self._stale_holders.update(self._read_holders)

        if self._write_holder:
            if self._write_holder.conn is not None:
                await self._write_holder.conn.close()
                self._write_holder.conn = None
            await self._write_holder.connect_async()
            if self._remote_url:
                # W3: push un-pushed local frames before pull() (see
                # _initialize).
                await self._push_once()
                try:
                    await self._write_holder.pull()
                except Exception as exc:
                    # Not fatal: the local replica keeps the state it has
                    # and the next pull retries. But a refresh that never
                    # pulled has connections that cannot see remote DDL,
                    # which is the exact confusion this refresh exists to
                    # resolve, so it must not be invisible.
                    logger.warning(
                        "Pull from remote failed during connection refresh: "
                        "%s. The local replica keeps its current state and "
                        "may not yet see remote schema changes.",
                        exc,
                    )
                await self._enable_replica_fk_enforcement()

        # Close every idle connection now. Any connection currently in
        # use is in _stale_holders and is closed when its caller releases
        # it, so no caller is interrupted mid-operation.
        await self._close_idle(self._free_readers)
        self._free_readers = None
        self._read_holders.clear()

        logger.info(
            "Refreshed the write connection and marked %d other "
            "connection(s) stale after migration; none will be reused",
            len(self._stale_holders),
        )

    def pause_push(self) -> None:
        replication.pause_push(self)

    def resume_push(self) -> None:
        replication.resume_push(self)

    def set_push_failure_callback(self, callback: Any, *, threshold: int = 1) -> None:
        replication.set_push_failure_callback(self, callback, threshold=threshold)

    @property
    def last_push_error(self) -> Exception | None:
        return replication.last_push_error(self)

    @property
    def push_healthy(self) -> bool:
        return replication.push_healthy(self)

    def _record_push_failure(self, error: Exception) -> None:
        replication.record_push_failure(self, error)

    def _record_push_success(self) -> None:
        replication.record_push_success(self)

    def _local_replica_has_data(self) -> bool:
        return replication.local_replica_has_data(self)

    async def _initial_sync(self) -> None:
        await replication.initial_sync(self)

    async def initial_pull_complete(self) -> None:
        await replication.initial_pull_complete(self)

    async def _release_reader(self, holder: "_TursoConnectionHolder") -> None:
        """Return a read connection, or close it if the pool is already full.

        max_size bounds how many idle connections are RETAINED, not how many
        callers may proceed. Concurrency above max_size is allowed and costs
        a connection that is opened, used, and closed again — never a wait.
        """
        await self._release(holder, self._free_readers, self._read_holders)

    async def _release(
        self,
        holder: "_TursoConnectionHolder",
        free: "asyncio.LifoQueue[_TursoConnectionHolder] | None",
        tracked: list["_TursoConnectionHolder"],
    ) -> None:
        if free is None:
            return
        if holder in self._stale_holders:
            self._stale_holders.discard(holder)
            if holder.conn is not None:
                try:
                    await holder.conn.close()
                except Exception:
                    logger.debug("Stale connection already closed")
                holder.conn = None
            if holder in tracked:
                tracked.remove(holder)
            return
        if free.qsize() < self._max_size:
            free.put_nowait(holder)
            return

        # Above the retention limit. Close it rather than growing the pool
        # without bound, and never hand the cost of that back to a caller as
        # a wait. The write holder is never disposed of: migration and
        # shutdown hold a reference to it.
        if holder is self._write_holder:
            free.put_nowait(holder)
            return
        if holder.conn is not None:
            try:
                await holder.conn.close()
            except Exception:
                logger.debug("Connection already closed on release")
            holder.conn = None
        if holder in tracked:
            tracked.remove(holder)

    async def _close_idle(
        self, free: "asyncio.LifoQueue[_TursoConnectionHolder] | None"
    ) -> None:
        """Close every connection sitting idle in a free list."""
        if free is None:
            return
        while not free.empty():
            holder = free.get_nowait()
            if holder is self._write_holder:
                continue
            if holder.conn is not None:
                try:
                    await holder.conn.close()
                except Exception:
                    logger.debug("Idle connection already closed")
                holder.conn = None

    def _write_serialisation(self) -> Any:
        """Serialise writers only when MVCC is not active.

        Measured against a real Turso Cloud replica, concurrent writers to
        one replica, distinct rows so there is no logical conflict:

                        K=2    K=5    K=10   K=20
            MVCC on     2/2    4/5    9/10   20/20    (with this lock removed)
            MVCC off    1/2    3/5    3/10   6/20

        With MVCC the lock changes nothing: twenty concurrent writers land
        twenty writes without it. Without MVCC, single-writer is Turso's
        documented default and the second writer is rejected — so the lock
        is what keeps those writes from being lost.

        0.1.22 added this lock unconditionally, on a measurement taken in
        WAL mode. That measurement is the MVCC-off row above: real, but a
        measurement of the engine running without the feature rather than a
        limit of the engine.

        WHICH ARM A POOL LANDS IN IS NOT A CHOICE ANY CALLER MAKES. MVCC is
        local only, so a pool with a `remote_url` is ALWAYS in the MVCC-off
        arm and its writers are ALWAYS serialised here. That is deliberate:
        without this lock a WAL replica loses writes outright (6 of 20 land
        at K=20 above), so serialised-and-correct beats concurrent-and-lossy.

        The consequence is worth stating plainly for anyone measuring write
        throughput against a synced replica: concurrent writers do not
        increase it, because they queue. A write-concurrency ceiling on a
        cloud-synced pool is this lock doing its job, not a limit to tune
        away. Raising it means not being synced on that path, not a setting.

        This paragraph previously said "`mvcc=True` is the default and the
        engine has the last word". That parameter was removed in 0.2.0 and
        the sentence was left behind by the change.

        See https://docs.turso.tech/tursodb/concurrent-writes
        """
        if self._remote_url and not getattr(self, "_mvcc", False):
            return self._replica_write_lock
        return contextlib.nullcontext()

    @staticmethod
    def _is_busy(error: Exception) -> bool:
        """True when an error means "not now" rather than "no".

        The documented retryable set is Error::Busy, Error::BusySnapshot and
        anything reporting a conflict — a commit-time row conflict under
        BEGIN CONCURRENT. The sync engine also wraps contention as
        "database tape error: database is busy".
        """
        text = str(error).lower()
        return "busy" in text or "sqlite_busy" in text or "conflict" in text

    async def _retry_while_busy(self, operation: Any, what: str) -> Any:
        """Run a transaction-boundary operation, absorbing contention.

        Only used where the safety argument is clear: starting a
        transaction, where nothing has been staged, and committing one,
        where the statements are already staged and re-committing lands the
        same set. A statement failing mid-transaction is not retried — the
        pool cannot replay the caller's statements, and a half-applied
        transaction is not something to guess about.

        Bounded in wall-clock time rather than attempts, so a database that
        is busy for a long time still returns to the caller rather than
        retrying forever.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._busy_retry_budget_s
        delay = 0.01
        attempts = 0
        while True:
            try:
                return await operation()
            except Exception as e:
                if not self._is_busy(e) or loop.time() >= deadline:
                    if attempts:
                        logger.warning(
                            "Gave up after %d busy retries on %s: %s", attempts, what, e
                        )
                    raise
                attempts += 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.25)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator["TursoAsyncConnection"]:
        """Run several writes as one transaction.

        Every ORM write inside the block shares one connection and one
        commit, so they land together or not at all. Without this, each ORM
        write took its own acquire_write and its own commit, and batching
        was only possible by dropping to raw SQL — which defeats the point
        of having an ORM.

        The cost of not having it is not tidiness. A caller updating a row
        and a derived index had to do them separately, so a failure between
        the two left the index disagreeing with the row it describes.

            async with pool.transaction():
                await cards.update_one(where=..., data=...)
                await index.update_one(where=..., data=...)

        The transaction belongs to the calling TASK, not to the pool. Two
        requests running concurrently against one pool each get their own;
        neither joins the other's. Writes outside a transaction are
        unaffected and still commit individually.

        The replica write lock is held for the whole block, which is what
        makes the writes atomic against other writers. Keep transactions
        short for the same reason you would anywhere else.

        A transaction holds its connection for the span of the block,
        because that is what a transaction IS. Statelessness is per
        transaction, not per statement: the connection is opened when the
        block is entered and closed when it leaves. Nothing survives it.
        """
        token = None
        async with (
            self._write_connection() as holder,
            self._write_serialisation(),
        ):
            conn = TursoAsyncConnection(holder)
            if getattr(self, "_mvcc", False):
                await self._retry_while_busy(
                    lambda: conn.execute("BEGIN CONCURRENT"), "BEGIN CONCURRENT"
                )
            token = _active_transaction.set({"pool": self, "conn": conn})
            try:
                yield conn
                await self._retry_while_busy(conn.commit, "commit")
            except Exception:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
            finally:
                if token is not None:
                    _active_transaction.reset(token)

    async def _configure_write_connection(
        self, holder: "_TursoConnectionHolder"
    ) -> None:
        """Give a new write connection the same settings as writer zero.

        Journal mode and foreign-key enforcement are per connection, so a
        writer opened later must be configured or it would silently behave
        differently from the first one — no MVCC, and no FK enforcement,
        which is the setting that stops a violating write committing locally
        and being lost on the next re-sync.
        """
        if self._mvcc_requested:
            try:
                await holder.conn.execute("PRAGMA journal_mode = 'mvcc'")
            except Exception:
                logger.debug("MVCC not available on this write connection")
        if self._remote_url:
            try:
                await holder.conn.execute("PRAGMA foreign_keys = ON")
            except Exception:
                logger.debug("Could not enable FK enforcement on write connection")

    async def _open_write_connection(self) -> _TursoConnectionHolder:
        """Open one connection for one write. The whole of the stateless path.

        Measured cost on a live cloud replica: 1.16ms to open, 0.59ms to
        close, around a 0.50ms write (module docstring). Nothing is held,
        so nothing is reused, pinned or poisoned.
        """
        holder = _TursoConnectionHolder(
            self._database_path, self._remote_url, self._auth_token
        )
        await holder.connect_async()
        await self._configure_write_connection(holder)
        return holder

    @asynccontextmanager
    async def _write_connection(self) -> AsyncIterator[_TursoConnectionHolder]:
        """Yield a connection to write on, and dispose of it correctly.

        Open, yield, close. There is one write path and this is it. There is
        no free list, no checkout, and no way for a caller to ask for one —
        the pooled opt-in and its `_get_writer` free list were removed with
        `pooled_writes`.

        Why the pooled write path is not coming back — the ledger
        --------------------------------------------------------------------

        This list lived on `_get_writer` and moved here when that method was
        deleted, because a cost list that dies with the code it argues
        against leaves nothing to re-read the next time someone proposes it.

        The open cost of a connection is measured and small (module
        docstring). The holding cost is what makes a pool a pool, and NONE
        of it is measured in this repo. It is listed so the omission is
        visible in the place that causes it, not so the list can be quoted
        as evidence.

        Sourced from Turso's own documentation of the MVCC preview
        (https://turso.tech/blog/beyond-the-single-writer-limitation-with-tursos-concurrent-writes):

        - Each row version stores a full 1KB copy, not a delta.
        - Row version management uses locks, not wait-free structures.
        - A held connection with an old read snapshot keeps versions from
          being reclaimed. A connection that opens, writes and closes
          cannot pin anything, because it is gone before the next write.

        Reasoned, NOT measured:

        - One connection is one OS worker thread for the whole life of the
          process, writing or idle (pyturso `lib_aio.py`). Pool size is
          thread count, exactly.
        - Checkout and checkin are per-write overhead on the pooled path.
          They may be a real fraction of the 1.16ms an open costs. Nobody
          has compared the two.
        - A pooled connection that breaks is handed to the next caller. A
          stateless one is clean by construction.

        The asymmetry is the point: the stateless path's cost is a single
        fixed number, paid per write and known. The pool's cost is a set of
        quantities that grow with uptime and concurrency, and that no
        measurement here bounds.

        CONNECTION REUSE IS STILL THE LARGER THROUGHPUT LEVER (6.01x on its
        own; 18.87x combined with MVCC — see `retry.py`). That is measured
        and it is not disputed here. Reuse belongs to a CREW that owns its
        connections for the length of a drain, not to a pool that hands one
        connection to unrelated callers across a process lifetime. This
        docstring rejects the second shape, not the lever.
        """
        holder = await self._retry_while_busy(
            self._open_write_connection, "open write connection"
        )
        try:
            yield holder
        finally:
            if holder.conn is not None:
                with contextlib.suppress(Exception):
                    await holder.conn.close()
                holder.conn = None

    async def _push_once(self) -> bool:
        return await replication.push_once(self)

    async def _push_loop(self) -> None:
        await replication.push_loop(self)

    async def _get_reader(self) -> "_TursoConnectionHolder":
        """Take a read connection from the free list, opening one if needed.

        Read connections are plain local connections to the same replica
        file. They never push and never pull, so they hold no sync state and
        cannot diverge from the write connection's view of the cloud. That is
        what lets them run outside _conn_lock.

        At most max_size are ever opened, because the semaphore admits at
        most max_size callers before any of them reach this point.
        """
        assert self._free_readers is not None
        if not self._free_readers.empty():
            return self._free_readers.get_nowait()

        holder = _TursoConnectionHolder(self._database_path)
        await holder.connect_async()
        self._read_holders.append(holder)
        return holder

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a read connection.

        Each caller gets its own connection, so reads run in parallel up to
        max_size. Previously every read was served from the single write
        connection under _conn_lock, and the lock was held for as long as the
        caller held the connection — so max_size bounded how many callers
        could queue, not how many could proceed. Five readers doing 100ms of
        work each took 500ms rather than 100ms.

        Reads see the write connection's committed data because both open the
        same local replica file; a commit is visible to every connection on
        that file. Reads never reach the network.
        """
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        if self._write_holder is None or self._write_holder.conn is None:
            raise PoolConnectionError("Write holder not initialized")

        if self._free_readers is None:
            self._free_readers = asyncio.LifoQueue()

        # No semaphore. A reader never queues behind a cap and is never
        # refused because the pool is busy: it takes an idle connection if
        # there is one, and opens its own if there is not.
        holder = await self._get_reader()
        self._active_connections += 1
        try:
            yield TursoAsyncConnection(holder)
        finally:
            self._active_connections -= 1
            await self._release_reader(holder)

    @asynccontextmanager
    async def acquire_write(self, *, concurrent: bool = True) -> AsyncIterator[TursoAsyncConnection]:
        """Acquire a write connection.

        SUPERSEDED. This docstring used to read "All writes go through
        _write_holder ... Creating separate connections per write caused
        push failures because each connection tracked its own sync state
        independently." The code below it no longer does that: a write opens
        its OWN connection, uses it, and closes it. The docstring described
        the pool as it was before that change and was left behind by it.

        Provenance of the superseded claim: UNVERIFIED. "Each connection
        tracked its own sync state independently" is an explanation, not a
        measurement, and no probe in this repo establishes it. Do not cite
        it as a reason against per-write connections. Do not treat it as
        disproved either — it has not been tested.

        What IS measured: per-write connections cost 1.16ms to open and
        0.59ms to close on a live cloud replica, around a 0.50ms write
        (2026-08-11, module docstring above). That is the whole price.

        Push and cloud delivery are not a consideration on this path. See
        the directive at the top of this module before raising them.

        Args:
            concurrent: Use BEGIN CONCURRENT when MVCC is available (default True).
                        Pass False for DDL operations which require exclusive transactions.
        """
        if self._closed:
            raise PoolClosedError("Pool has been closed")

        if self._write_holder is None or self._write_holder.conn is None:
            raise PoolConnectionError("Write holder not initialized")

        # No semaphore. A writer never queues behind a cap and is never
        # refused because the pool is busy.
        # Already inside a transaction on this task: reuse its connection so
        # every ORM write in the block lands in one commit. The transaction
        # owns the connection, the replica lock and the commit; this just
        # hands the connection over.
        active = _active_transaction.get()
        if active is not None and active["pool"] is self:
            yield active["conn"]
            return

        # Stateless by default: this write gets its own connection and gives
        # it back to the operating system when it is done. Concurrency is
        # what separate connections are for
        # (https://docs.turso.tech/tursodb/concurrent-writes).
        #
        # _write_serialisation applies on BOTH paths. It is keyed on MVCC,
        # not on pooling: with MVCC off, one of two concurrent writers loses
        # its write (measured, see _write_serialisation), and separate
        # connections do not change that. Statelessness removes the free
        # list, not the engine's need for a single writer in WAL mode.
        async with (
            self._write_connection() as holder,
            self._write_serialisation(),
        ):
            async_conn = TursoAsyncConnection(holder)
            if concurrent and getattr(self, "_mvcc", False):
                # Nothing staged yet, so retrying is free.
                await self._retry_while_busy(
                    lambda: async_conn.execute("BEGIN CONCURRENT"),
                    "BEGIN CONCURRENT",
                )
            try:
                yield async_conn
                # The statements are staged, so re-committing lands the
                # same set. The statements are never replayed here.
                await self._retry_while_busy(async_conn.commit, "commit")
            except Exception:
                with contextlib.suppress(Exception):
                    await async_conn.rollback()
                raise
        # No push here — the push loop handles cloud delivery on writer zero.

    async def flush(self) -> None:
        await replication.flush(self)

    async def close(self) -> None:
        """Flush all pending writes to cloud, then close.

        Retries push indefinitely until cloud confirms receipt.
        After this method returns, all local writes are guaranteed
        to be on cloud.  It is safe to delete local DB files after
        close() completes.

        Call this on SIGTERM / application shutdown before exiting.
        """
        self._closed = True
        # Final push — retry indefinitely.  No persistent disk means
        # data not pushed is lost permanently.
        if self._write_holder and self._remote_url:
            attempt = 0
            while not await self._push_once():
                attempt += 1
                delay = min(self._push_retry_base_s * (2 ** attempt), 30.0)
                logger.warning("Final push attempt %d failed, retrying in %.1fs", attempt, delay)
                await asyncio.sleep(delay)
        # Read connections hold no unpushed state, so they close after the
        # final push rather than before it.
        for reader in self._read_holders:
            if reader.conn is not None:
                try:
                    await reader.conn.close()
                except Exception:
                    logger.debug("Read connection already closed")
                reader.conn = None
        self._read_holders.clear()

        # There are no additional write connections to close. Writer zero is
        # the only long-lived one, and it is closed just below.
        if self._write_holder:
            if self._write_holder.conn is not None:
                await self._write_holder.conn.close()
                self._write_holder.conn = None
        if self._push_task and not self._push_task.done():
            self._push_task.cancel()
            try:
                await self._push_task
            except asyncio.CancelledError:
                pass

    @property
    def dialect(self) -> str:
        return "turso"

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        return self._closed

    @property
    def size(self) -> int:
        """Maximum number of concurrent connections."""
        return self._max_size

    @property
    def available(self) -> int:
        """Number of available connection slots."""
        return self._max_size - self._active_connections
