# Changelog

All notable changes to `declaro-persistum` are recorded here.

## 0.1.21 — 2026-08-06

### Bugfixes

- **A busy database no longer reaches the caller as an error.** Concurrent writes to one cloud replica could fail with `sync engine operation failed: database tape error: database is busy`. Measured downstream: 30 concurrent writes across 8 replicas produced 5 failures, surfaced to users as HTTP 500. A single writer never saw it; it appeared only when two writes landed on one replica at once.

  That is I/O contention reaching the consumer, which is the one thing this pool exists to prevent. A busy database means "not now", not "no". The pool now absorbs it and retries.

  **Retries happen at transaction boundaries only**, where the safety argument is clear. A failed `BEGIN` staged nothing, so retrying applies nothing twice. A failed commit did not land, so the statements are still staged and re-committing lands the same set. A statement failing *mid*-transaction is deliberately not retried: the pool cannot replay the caller's statements, and a half-applied transaction is not something to guess about.

  Only contention is retried. A constraint violation is an answer, not a "not now", and propagates immediately rather than being retried into a stall.

  The budget is wall-clock, not attempts — `busy_retry_budget_s`, default 5 seconds — so a database that stays busy still returns to the caller instead of retrying forever.

  **This contention was introduced by 0.1.17.** Before it, every writer shared one connection under one lock, so two writes could never reach the replica at once. Removing the lock was right — it was costing every writer a cloud round trip — but it exposed a constraint in the sync engine that the lock had been hiding. Absorbing that constraint belongs here, not in the caller.

## 0.1.20 — 2026-08-06

### Performance

- **Opening a cloud pool pays one cloud handshake instead of two.** `_initialize` opened the write connection *and* the dedicated push connection. A sync connection costs a handshake — measured downstream at ~790ms against a real remote — so pool open paid it twice when only the write connection is needed before the pool can serve a caller.

  The push connection now opens on the push loop's first iteration, which runs in a background task. The handshake still happens immediately; it is simply no longer on the path a caller awaits.

  This does not shrink the handshake itself, which belongs to the engine and the network. It removes the second one. Downstream measurement put pool open at ~1077ms against a raw `turso.aio.sync` connect of ~788ms; the difference was the connection this release defers.

## 0.1.19 — 2026-08-06

### Concurrency

No consumer operation waits on this pool's own bookkeeping any more. Not on a lock, not behind a concurrency cap, and never refused because the pool is busy. Three things violated that and all three are gone.

- **The last lock is removed.** `_conn_lock` was still taken when a push fell back to the write connection, which held that connection across a cloud round trip and stalled every write for its duration. The push now retries opening its own connection instead. A push deferred to the next cycle costs latency to the cloud; a push on the write connection costs the caller. `TursoPool` no longer has a lock at all.

- **`max_size` no longer caps concurrency.** It capped both readers and writers, so caller number `max_size + 1` queued, and a writer could be refused outright with `PoolExhaustedError` simply because the pool was busy. Concurrency is now unbounded and **`max_size` bounds how many idle connections are retained**. A caller above that limit opens a connection, uses it, and closes it on release rather than waiting for someone else's. Retention stays bounded; nobody waits.

- **A migration no longer stalls writes.** `refresh_connections` acquired every write slot before touching anything, so a DDL migration blocked every write until it finished. It now marks in-use connections stale and disposes of them when their caller releases them, closing idle ones immediately. An in-flight write runs to completion and is never interrupted; the next caller gets a fresh connection against the migrated schema.

Reads, writes and the push each hold their own connections, so there is nothing shared left to guard. A test asserts the absence of `_conn_lock`, `_semaphore` and `_write_semaphore`, so reintroducing any of them fails the build.

## 0.1.18 — 2026-08-06

### Removed

- **The push delivery tripwire is removed.** It was a false positive generator, not a safety net.

  It compared the replica's sync revision either side of each push and warned if the revision had not moved while writes were pending. The revision is read from the push connection, and pushing another connection's frames does not advance that connection's own revision — so it fired on essentially every push that had pending writes.

  Two downstream runs measured it: 1002 warnings during a capacity test, and 32 during a 66-second soak in which an independent oracle confirmed all 2541 writes were delivered. It never once indicated real loss. No data was ever lost; the alarm was wrong.

  A warning that fires on nearly every operation is worse than no warning, because it teaches everyone to ignore warnings. The underlying error was mine and it is worth stating plainly: the signal was chosen without verifying what it measures, and shipped with a comment admitting its semantics were not pinned down.

  The only signal found so far that provably tracks delivery is reading the rows back from the primary on a fresh connection, which is far too expensive to run per push. Until such a signal exists, push failures are surfaced by `last_push_error`, `push_healthy` and the push-failure callback, which report what actually happened rather than inferring it. The reasoning is recorded in `_push_once` so the same guard is not rebuilt the same way.

## 0.1.17 — 2026-08-06

### Concurrency

- **Concurrent writers now actually run concurrently.** Every writer shared one connection under one lock, so callers were serialized before they reached it. `BEGIN CONCURRENT` was issued into a queue of one, and MVCC — enabled by default in 0.1.13 — could not do the thing MVCC is for. Each writer now takes its own sync connection, so `max_size` bounds how many writers proceed rather than how many queue. Measured: five writers each holding a connection for 100ms went from 0.505s to parallel.

  Write connections are opened on demand up to `max_size`, returned to a free list, and reused. Each new one is configured like the first — journal mode and foreign-key enforcement are per connection, so a writer opened later would otherwise silently run without MVCC and without the FK enforcement that stops a violating write committing locally and being lost on the next re-sync.

- **`refresh_connections` now refreshes every connection, and quiesces writers first.** It reopened only the pool's original write connection. With several write connections and several read connections, that left the rest reading and writing against a schema that no longer existed after a migration — the same stale-input class as the defects fixed in 0.1.10 and 0.1.15. It also closed that connection while writers might hold it, which is a use-after-close.

  It now takes every write slot before touching anything, reopens all write connections, and discards read connections so the next reader opens a fresh one. Writes do pause for the duration, and that is the one place a pause is correct: the schema is changing underneath them.

## 0.1.16 — 2026-08-06

### Bugfixes

- **The push delivery tripwire could never fire.** It exists to catch one failure: a push that reports success while delivering none of the write connection's frames. That is silent data loss, and it is the risk taken on in 0.1.14 by moving the push to its own connection.

  On the async connection `stats()` is a coroutine function. The pool called it without awaiting, so reading `.revision` off the returned coroutine gave `None` every time. The tripwire treats `None` as "cannot tell" and returns early, so it returned early on every push and the alarm was decorative.

  The cause was checking the wrong class: the signature was read from `turso.sync.ConnectionSync`, where `stats()` returns directly, while the pool uses `turso.aio.sync`, where it does not. The revision read is now awaited when awaitable, so it works on both.

  This was never live data loss. It was a hole in the safety net, found by a downstream soak that noticed Python reporting `coroutine 'ConnectionSync.stats' was never awaited`. An independent delivery oracle confirmed all 7863 writes in that run reached the cloud.

  Tests now assert the tripwire reads a **moving** revision, stays silent while it advances, warns when it is static with writes pending, and leaves no coroutine un-awaited. A tripwire that always reads `None` either never fires or fires always, and neither is an alarm — nothing had asserted which it was.

## 0.1.15 — 2026-08-06

### Bugfixes

- **Two services sharing one database no longer fight over the skip-if-clean stamp.** The stamp key was built from the schema file's *name* alone, so two services migrating the same database wrote the same row in `_declaro_meta`. They disagree whenever their computed hash differs — two services each with their own `models.py`, or two services on different library versions, since the version is mixed into the hash deliberately. Each read the other's stamp, saw a mismatch, re-introspected and re-stamped. Neither was wrong and neither could win, so the pair stayed permanently unclean with operations re-proposed on every boot.

  That is also the symptom of genuine schema drift, and of the foreign-key defect fixed in 0.1.10, so it was easy to misdiagnose. Do not attribute never-clearing operations to a differ defect without first confirming that only one service, on one version, migrates that database.

  The hash is now part of the key rather than only the value, so each distinct (schema, version) records its own row and no service can evict another's. Two services applying genuinely the same schema on the same version still share one row, which is correct — they are recording the same fact.

  Rows for superseded hashes are left behind rather than pruned. Pruning by schema name is what caused the collision, since it would delete the other service's stamp. The cost is a few rows in a metadata table.

  Reported by a consumer whose stage service pointed its central pool at the production central database.

### Internal

- `scripts/publish.sh` builds, tests, uploads and then **verifies the release is installable from PyPI**. `uv publish` exits 0 whether or not an upload lands, so its exit code carries no information and a release can appear to succeed while nothing reached the index. The script polls the index — which lags the upload — and fails loudly if the version is not installable. It also refuses to publish a version that already exists.

## 0.1.14 — 2026-08-06

### Concurrency

- **The cloud push now runs on its own connection, so a write never waits for a round trip.** The push previously shared the write connection and held `_conn_lock` across the whole cloud round trip, so a write arriving mid-push waited for the network — measured at 0.280s against a 0.300s push. The push now has a dedicated sync connection and takes no lock.

  That a push on a separate connection still delivers the write connection's frames was verified, not assumed. Under free-threaded CPython with the GIL confirmed off: 1353 rows written on one connection, 40 pushes issued on another, and a fresh third connection pulled all 1353 rows back from cloud. Full delivery, no crash. The concern this test existed to rule out was a push that reports success and silently delivers nothing, which would be worse than the stall it removes.

- **A delivery tripwire guards against that failure returning.** The pool counts committed writes and reads the replica's sync revision either side of each push. If a push reports success while writes were pending and the revision does not move, it logs a warning naming the count and the revision. It logs rather than raises, because the engine's revision semantics are not pinned down here and a healthy push should not be failed on a guess. If it ever fires in the field, investigate before silencing it.

- If the dedicated push connection cannot be opened, the pool falls back to pushing on the write connection and says so once. Delivery continues at the old cost rather than stopping. The connection is opened once during initialization, so an unreachable remote costs one failed connect at startup rather than one per push cycle.

With this, no consumer operation on a Turso pool waits on the network: reads take their own connections, writes take the write connection, and the push takes its own. `tests/unit/test_push_lock_contention.py` has no `xfail` left in it.

## 0.1.13 — 2026-08-06

### Concurrency

- **MVCC is now requested on every Turso pool, not only local ones.** Turso supports concurrent writes through `BEGIN CONCURRENT` over MVCC, and `acquire_write` issues `BEGIN CONCURRENT` only when the pool holds MVCC. The pool previously asked for MVCC only when there was no `remote_url`, so every cloud-backed pool ran serialized writes while the engine below it supported concurrent ones — the configuration that needs the throughput was the one least likely to get it. The engine still has the last word: if it does not grant MVCC the pool falls back to WAL and logs that. Pass `mvcc=False` to force WAL deliberately.

- **Reads no longer share the write connection.** Each read now takes its own plain local connection to the replica file, so reads run in parallel up to `max_size`. Previously every read was served from the single write connection under one lock, and the lock was held for as long as the caller held the connection — so `max_size` bounded how many callers could queue, not how many could proceed. Measured: five readers doing 100ms of work each took 0.505s, an effective concurrency of 1. They now overlap.

  A read arriving while a cloud push is in flight also no longer waits for the round trip. Read connections never push and never pull, so they hold no sync state and cannot diverge from the write connection's view of the cloud.

- **Still open at this release, fixed in 0.1.14:** a write arriving while a push is in flight waits for the round trip, because writes and the push still share the write connection. That test was `xfail(strict=True)` in `tests/unit/test_push_lock_contention.py`. Fixing it meant giving the push its own connection, which was gated on proving that a push on one sync connection is safe concurrently with a write on another.

### Dependencies

- **`pyturso` floor raised from `>=0.5.1` to `>=0.7.0`**, for a correctness fix rather than features. Turso 0.7.0 carries upstream PR #7813, "sync: fix race between concurrent opens of the same synced replica" (merged 2026-07-11). Opening several `turso.sync.connect()` handles to the same already-hydrated replica concurrently from threads failed intermittently — at K=8 nearly every run — with `meta must be initialized before open` or `deserialization error: trailing characters`. Two defects compounded: the metadata file was rewritten on *every* open (the configuration comparison reported a change whenever a `remote_url` was present rather than when it differed), and the Python binding's metadata write was not atomic despite its docstring, so a concurrent opener could read an empty or torn file.

  Verified in the published wheels rather than inferred: pyturso 0.5.1 writes the metadata file in place with `open(path, "wb")`; 0.7.2 writes a temporary file and `os.replace()`s it. The full test suite passes identically on 0.5.1, 0.7.0 and 0.7.2, so 0.7.0 is a tested floor and not merely the version where the fix landed.

### How this defect was found

The contention was measured rather than argued, in `tests/unit/test_push_lock_contention.py`. Against a 0.30s push, a read arriving mid-push waited 0.280s, and five concurrent reads serialised completely behind it. Separately, five concurrent readers each doing 100ms of work took 0.505s at `max_size=5`.

Those measurements were committed as `xfail(strict=True)` before the fix. When the read path was fixed they turned into XPASS and failed the suite, which forced this entry to be written rather than letting the tests quietly start passing. The write-versus-push case stayed `xfail(strict=True)` at this release and was fixed the same way in 0.1.14.

Reported downstream by a consumer running a live write workload.

## 0.1.12 — 2026-08-06

### Performance

- **Opening a cloud-backed Turso pool no longer waits on the network.** `_initialize` awaited `push()` then `pull()` inline on every open, so every open paid a full round trip — measured downstream at ~1.35s against an existing 20-card replica, which is the round trip rather than data transfer, and which dominated request latency once pools were re-opened after idle eviction. Whether a usable local replica exists is observable, so it is now observed rather than assumed (checked before `connect_async`, which would otherwise bootstrap the file into existence and make the check trivially true). With a populated replica on disk the initial sync runs as a background task and the pool is usable as soon as the connection opens.
- **When no local replica exists the sync is still awaited inline.** There is nothing on disk to serve, so returning early would hand out a pool that reads an empty database and reports success. That is a once-per-replica cost, not a per-open one.
- **`initial_pull_complete()`** is the barrier for callers that must not observe a stale replica. `apply_migrations_async` awaits it before the skip-if-clean probe and before introspection: a diff computed against a replica that has not caught up produces operations that correct code then faithfully applies — the same stale-input class as the foreign-key defect fixed in 0.1.10. The guarantee sits at the one call site that needs it rather than being charged to every open.
- `_push_once()` still strictly precedes `pull()` on both the inline and background paths, so frames a prior process committed locally but never pushed are delivered before `pull()` can overwrite them.
- A background sync that fails does not raise on open — there is no caller to catch it, and a failed refresh must not kill a pool whose replica is still readable; the push loop keeps retrying. The error is recorded and re-raised by `initial_pull_complete()`, so a caller that did ask for a consistent view is told it did not get one.
- `background_pull=False` restores fully inline syncing.

No consumer request path waits on remote Turso. `acquire()` and `acquire_write()` contain no remote I/O at all — writes commit locally and the background push loop delivers them. The remaining blocking remote calls are all off the request path: the initial sync when no local replica exists (once per replica), the post-migration connection refresh, and `sync()` / `flush()` / `close()`, which a consumer invokes deliberately.

Durability on ephemeral disks (Render and similar), where the local file is wiped on restart, is handled at those deliberate points rather than by blocking writes: `close()` retries the final push indefinitely, `flush()` blocks until pending writes are pushed, and `configure_write_queue(persistence_path=...)` persists a queue across restarts.

## 0.1.11 — 2026-08-05

### Features

- **`TursoCloudManager.create_database(..., seed=...)`** — optional pass-through to the Turso create-database API's `seed` object, so a new database can be created as a copy of an existing one instead of empty. Provisioning a tenant from a pre-built template becomes a single copy rather than create-empty, then migrate, then insert rows, which matters under concurrent onboarding. Passed through as an opaque dict rather than modelled as named parameters, so seed variants the API grows are usable without a release here — and so the `timestamp` field, which selects an ISO 8601 recovery point rather than current state, is available for free. Requested downstream; thank you.

## 0.1.10 — 2026-08-05

### Bugfixes

- **Foreign keys were silently dropped from introspected Turso schemas.** The `sqlite_master` fallback for `PRAGMA foreign_key_list` returned the wrong referencing column, so foreign keys vanished from the introspected schema with no error raised. The inline-FK pattern left its column-name group unanchored and let an unbounded `.*?` span the optional constraints, so matching began at the top of the DDL and captured `CREATE` as the referencing column — `CREATE TABLE o (id TEXT PRIMARY KEY, user_id TEXT REFERENCES u(id))` yielded `from_col="CREATE"`. The inspector found no column by that name and dropped the foreign key entirely. It only ever produced correct results on DDL that put each column on its own line, because `.` does not cross a newline without `re.DOTALL`. The column name is now anchored to the start of a column definition and the constraint gap is bounded so it cannot run into an adjacent column.
- **Scope:** Turso only, and only on engines without native `PRAGMA foreign_key_list` — notably pyturso 0.5.1, the current pinned floor, where the PRAGMA raises "Not a valid pragma name". Current Turso supports it natively (verified against pyturso 0.7.2, where the native path returns correct rows and the emulation is never reached). No PostgreSQL or SQLite code path is involved.
- **Because the failure was silent, there is no local signal that it occurred** — introspection simply reported a schema with no foreign keys on it. If you use Turso on pyturso 0.5.x, re-run a migration after upgrading and confirm your foreign keys are present.
- Regression tests added in `tests/unit/test_pragma_compat_fk_emulation.py`, covering the referencing column across nine DDL shapes (first column, later column, multiple inline FKs, quoted identifiers, sized and multi-word types, a default preceding `REFERENCES`, multi-line DDL, and no FKs at all).

### Test suite

- Cloud mode in the Turso integration tests is now opt-in via `TEST_TURSO_CLOUD` rather than engaging whenever credentials happen to be set. Because `tests/conftest.py` calls `load_dotenv()`, credentials present in a `.env` file previously pointed an ordinary test run at a real remote database, where the fixture drops every table matching `test_%`. The fixture also read its auth token and never passed it to `ConnectionPool.turso`, so every cloud run failed with `401 ... empty JWT token` — which read as absent credentials rather than a dropped argument.
- The BDD connection factory passed `TEST_TURSO_URL`, a remote `libsql://` URL, to `turso.connect()`, which takes a local filesystem path; it tried to open a file by that literal name and failed with `IoError: open: NotFound`. BDD runs now use a local temp database, matching the SQLite factory.
- The "Rollback on reconstruction failure" scenario forced its failure by pre-creating a table named `{table}_new`. Reconstruction now names its temp table `_declaro_tmp_{table}_{uuid8}` and drops any leftover first, so that collision became impossible and nothing failed — the scenario went on asserting a rollback that was never triggered. It is now failed genuinely, in the data-copy phase it names, by copying a NULL row into a NOT NULL column. Its two follow-up assertions verified nothing (one re-checked the error flag, the other was a bare `pass`), so a reconstruction that failed *after* mutating the table would have satisfied both; they now compare schema and rows against a snapshot taken before the failure.

## 0.1.9 — 2026-08-05

### Action required if you are on 0.1.7 or 0.1.8

`__version__` was frozen at `"0.1.6"` through both of those releases. It is mixed into the schema hash (see 0.1.4) specifically so that a version bump invalidates the skip-if-clean cache and forces one fresh introspection on upgrade — that is the mechanism by which loader, differ and inspector fixes reach an existing deployment.

With the constant frozen, that mechanism did not run. For any consumer on 0.1.7 or 0.1.8 **whose schema file has not changed**, there is no symptom: the stored hash matches, `apply_migrations_async` returns `{'success': True, 'skipped': True}`, and the old introspection path keeps running indefinitely with no local signal that anything is stale. Only consumers who happened to edit their model file ever picked up the newer code.

This is a correction to something that was quietly not happening, not an optional improvement. Upgrading invalidates the stale hash and performs exactly one full re-introspection on first startup. That re-introspection is the remedy; it is expected, and it is what makes the fixes below take effect.

### Bugfixes

- **`drop_index` crashed on PostgreSQL.** `PostgreSQLApplier._drop_index_sql` was declared `(self, details)` while the dispatch in `generate_operation_sql` calls every generator as `generator(table, details)`, so any migration emitting a `drop_index` died with `TypeError: _drop_index_sql() takes 2 positional arguments but 3 were given`. It was the only generator in that dispatch table with the wrong arity; SQLite and Turso route `drop_index` through `applier/shared.py` and were unaffected.
- **A UNIQUE constraint's backing index was scheduled for dropping.** PostgreSQL introspection filtered indexes on `NOT indisprimary` alone, so the index PostgreSQL builds to implement a UNIQUE constraint was reported as an ordinary index. A model declares that as `unique: True` on the column and never as an index entry, so the differ saw it in `current - target` and emitted a `drop_index` for a constraint the model still declares. Introspection now excludes indexes owned by a PRIMARY KEY, UNIQUE or EXCLUDE constraint via `pg_constraint.conindid`. Standalone `CREATE [UNIQUE] INDEX` definitions are still reported and still diffed, so an index you genuinely remove from your models is still dropped. PostgreSQL refuses to drop a constraint-backed index, so this surfaced as a hard migration failure rather than a silent loss of the uniqueness guarantee. Any schema with a `unique: True` column was affected. Reported via downstream bug report; thank you.
- **`__version__` reported a stale value.** It is now derived from installed package metadata rather than written as a literal, so it cannot drift from `pyproject.toml` again. See the note above for why the drift mattered.
- **Boolean defaults churned an `alter_column` on every migration.** The loader emits `"FALSE"` for a Python `False` default while PostgreSQL introspects the same default back as `"false"`, and the two were compared verbatim — so every PostgreSQL model with a boolean field re-emitted an `alter_column` that changed nothing, forever. Boolean literals are now folded to a canonical spelling before comparison. Only boolean literals are folded; every other default is an opaque SQL expression where case can carry meaning, and those are still compared verbatim.
- Regression tests added in `tests/unit/test_postgresql_unique_index_regression.py` and `tests/integration/test_postgresql.py`.

### Internal

- `pydantic` added to the `dev` extra. The loader duck-types Pydantic model modules rather than importing pydantic, so it is deliberately not a runtime dependency and your install profile is unchanged — but the test suite execs Pydantic model fixtures and could not run from a clean checkout without it.

## 0.1.8 — 2026-07-05

### Bugfixes

- Embedded-replica FK-push durability (defense-in-depth) for Turso embedded replicas.

Note: because `__version__` was frozen at `"0.1.6"` in this release, the changes above did not reach consumers whose schema file was unchanged. See the 0.1.9 entry.

## 0.1.7 — 2026-06-22

### Features

- Opt-in `use_tursodb` support in `TursoCloudManager`.

Note: because `__version__` was frozen at `"0.1.6"` in this release, the changes above did not reach consumers whose schema file was unchanged. See the 0.1.9 entry.

## 0.1.6 — 2026-05-13

### Bugfixes
- **`update_many(..., increment=...)` crashed on Turso / MVCC pools** with
  `TypeError: object of type 'int' has no len()`. Root cause was in the
  executor: every write op on a pool with ``acquire_write`` was routed
  through ``_execute_update`` (cursor rowcount path), regardless of
  whether the SQL had a ``RETURNING`` clause. Reported via downstream
  bug report; thank you.
- **`update_one` / `create` / `delete` silently returned `int` instead of
  the documented `dict | None`** on Turso / MVCC pools, for the same
  reason. The bug surfaced loudly only in `update_many`'s `len()` call,
  but the others were silently corrupting return types — any consumer
  that dereferenced the result on Turso would have hit
  `TypeError: 'int' object is not subscriptable`.
- **Fix:** the executor now consults `has_returning_clause(sql)` for
  write ops on `acquire_write` pools. SQL with `RETURNING` is routed
  through the fetch path (rows) on the write connection; SQL without
  `RETURNING` keeps the count path (int rowcount). One dispatch
  decision, two correct behaviors. Same fix resolves all four reported
  symptoms.

### Honest Code refactors (no behavior change for honest callers)
- `_compute_schema_hash(schema_path, version)` — version is now passed as
  a parameter rather than read from a module-level constant inside the
  function. Tests no longer monkeypatch `declaro_persistum.__version__`
  to verify version-mixing; they call the pure function with explicit
  version arguments. (Honest Code Rule 11: Configuration as Parameters.)
- `_dialect_needs_orphan_recovery(dialect) -> bool` — the dispatch
  decision that gates the SQLite-specific orphaned-tmp-table recovery
  scan is now a pure helper. Tests assert it directly instead of
  monkeypatching `_recover_orphaned_tmp_tables` and using a fake pool
  with a sentinel exception to short-circuit `apply_migrations_async`.
- `compose_update_values(data, increment)` — moved from method on
  `PrismaQueryBuilder` to module-level pure function. The method form
  read nothing from `self` and was masquerading as instance-tied.
  (Honest Code Rule 3: Pure Functions Over Methods.)
- `has_returning_clause(sql)` — new pure helper in
  `declaro_persistum.instrumentation`, used by the executor to route
  write ops. Tested with whole-word matching to prevent false positives
  on column / table names containing the substring `returning`.

### Test cleanups
- `test_dirty_when_hash_matches_but_no_user_tables` previously declared
  a Pydantic model with `class Meta: table_name = 'users'`, which
  declaro's loader does not recognize. The loader returned an empty
  schema, the empty-schema branch of `_schema_is_clean` fired, and the
  test silently asserted on the wrong code path. Now uses
  `__tablename__` (the actual convention) so the test exercises what
  its docstring claims.
- `test_migrations_dialect_dispatch.py` and `test_schema_hash_version.py`
  rewritten as pure-function assertions against the new helpers
  (`_dialect_needs_orphan_recovery`, `_compute_schema_hash(schema, version)`)
  — no monkeypatching, no fake pools, no sentinel exceptions.

## 0.1.5 — 2026-05-13

### Features
- **Atomic `increment={"col": delta}`** on Prisma-style `update_one` /
  `TableProxy.update_one`. Emits `SET col = col + :inc_col` so the read and
  the write happen inside a single statement — no application-side RMW
  round trip, no race window between concurrent writers, no need to fetch
  the old value first. Negative deltas are supported (the signed value
  binds to the parameter; SQL stays `col + :param`). `data=` and
  `increment=` compose in the same UPDATE.
- **`update_many(where=, data=, increment=) -> int`** on Prisma-style API
  and `TableProxy`. Applies a uniform update to every matching row in one
  statement and returns the count of rows updated. Replaces the
  `1 + N`-round-trip pattern (one batched read + N per-row updates) with
  a single UPDATE for the uniform-delta case. Counter maintenance against
  large `IN` lists is the motivating use case (e.g. tag card-counts).
- **`increment(delta)` factory** exported at the top level
  (`from declaro_persistum import increment`). Pass as a value in the
  native `UpdateQuery` API for fluent atomic increments:
  `items.update(card_count=increment(1)).where(...).execute()`. Same
  semantics whether you use the native, Prisma, or `TableProxy` surface.

### Tests
- `tests/unit/test_increment_and_update_many.py` covers SQL emission,
  composition with `data=`, negative deltas, integration against real
  SQLite for atomicity, `update_many` row-count semantics, error cases
  (missing data/increment, column-in-both, unknown column), and hook
  integration (post-hook row count flows through to the returned count).

## 0.1.4 — 2026-04-28

### Bugfixes
- **Skip-if-clean cache hid loader/applier fixes from upgrades.** The
  schema-hash optimization stored a hash representing "the result of
  running this version of declaro against this file." After a buggy
  version stamped a "clean" hash, upgrading to a fixed version did
  nothing on next startup — the hash still matched the unchanged source
  file, so the runner skipped re-introspection and the corrupted schema
  silently persisted until the user edited their model file or passed
  `force=True`. The 0.1.3 PEP-563 fix was visible to consumers only after
  manual cache invalidation.
- **Fix:** `_compute_schema_hash` now mixes `declaro_persistum.__version__`
  into the hash input (with a NUL delimiter so file content cannot collide
  with the version string). Any version bump invalidates the cache,
  triggering exactly one re-introspection pass on first startup after an
  upgrade. Cost is milliseconds for typical schemas; the alternative is
  silent persistence of bugs across upgrades.

### Operational note
- After upgrading from 0.1.3 (or earlier) to 0.1.4, your app will perform
  one introspection pass on first startup even if your schema file is
  unchanged. This is intentional — it ensures any fixes shipped in 0.1.4
  (or future versions) take effect against your existing database. No
  action required.

### Internal
- `__version__` moved to the top of `declaro_persistum/__init__.py`
  (above submodule imports) so submodules can read it without circular
  imports.
- Regression tests added in `tests/unit/test_schema_hash_version.py`.

## 0.1.3 — 2026-04-28

### Bugfixes
- **`bool` columns silently became `text`; `T | None` columns silently became
  `NOT NULL`** — for any model file that used `from __future__ import
  annotations` (PEP 563) or any string forward reference. `pydantic_loader`
  read `cls.__annotations__` directly, which under PEP 563 returns *strings*
  ("bool", "datetime | None") rather than types. Strings missed every
  type-keyed lookup in `python_type_to_sql` (falling through to the `text`
  default) and `is_optional_type` couldn't introspect them (returning False
  for every union). The result: silent schema corruption — wrong column
  types and NOT NULL where the user wrote `T | None`. The loader now uses
  `typing.get_type_hints(model_cls)`, which resolves string annotations
  against the model's module globals regardless of PEP 563. Falls back to
  `__annotations__` only if `get_type_hints` raises (unresolvable forward
  ref). Reported via downstream bug report; thank you.
- Regression tests added in `tests/unit/test_pydantic_loader_pep563.py`
  cover `bool`, `T | None`, and a byte-identity check between PEP-563 and
  non-PEP-563 model files.

## 0.1.2 — 2026-04-28

### Bugfixes
- **Crash on Postgres at lifespan startup.** `apply_migrations_async` called
  `_recover_orphaned_tmp_tables(pool)` as a pre-flight unconditionally, which
  queries `sqlite_master`. On Postgres this raised
  `asyncpg.exceptions.UndefinedTableError: relation "sqlite_master" does not
  exist` before any actual migration logic ran, breaking every Postgres-backed
  app at startup. The recovery scan now dispatches by dialect and only runs
  for `sqlite` and `turso`. Postgres reconstruction does not produce these
  temp tables, so the scan is meaningless there. Reported via downstream
  bug report; thank you.
- Regression tests added in `tests/unit/test_migrations_dialect_dispatch.py`
  cover all three supported dialects.

## 0.1.1 — 2026-04-19

### Documentation
- README: new "Query Hooks (pre / post)" section with the function-passing
  design explained and an end-to-end RLS / audit example.
- `docs/hooks.md`: expanded "Design: hooks are passed in, not registered"
  section explaining why hook functions are passed as arguments rather than
  registered via decorators — no module-level registry, no import-time
  side effects, every hook traceable to a call site.

### Internal
- `query/prisma_style.py::PrismaQueryBuilder._where_to_conditions` returns
  `list[Condition | ConditionGroup]` rather than `list[Condition]` with a
  `type: ignore[arg-type]` override. Same runtime behavior, honest types.

## 0.1.0 — 2026-04-19

Initial public release on PyPI.

### Core
- Schema-first migration toolkit: Pydantic models → `types.py` TypedDict
  schemas → protocol-based inspector/differ/applier per dialect.
- Unified `ConnectionPool` API across PostgreSQL (asyncpg), SQLite
  (aiosqlite), and Turso (pyturso, with optional cloud sync).
- Fluent query builder with native, Django-style, Prisma-style, and
  SQLAlchemy-compat surfaces — all schema-validated at build time.
- Enum abstraction: `Literal[...]` types auto-generate FK-constrained
  lookup tables.

### Query hooks
- `table_factory(schema, pool, *, pre=None, post=None)` — binds pre-hook
  and post-hook functions to every query built from the returned factory.
- `PreHook = (query) -> query` and `PostHook = (rows, QueryMeta) -> rows`.
- Pre-hooks can structurally rewrite queries (e.g. DELETE → UPDATE for
  soft delete) by returning a different query type.
- `.execute(pre=..., post=..., without_hooks=...)` override or bypass at
  call time.
