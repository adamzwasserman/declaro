# Bug: FK-violating write commits locally, silently fails to push, and is lost on re-sync

## Severity: CRITICAL — Committed writes silently discarded; no error returned to the caller

## Summary

On a Turso embedded replica (`ConnectionPool.turso(path, remote_url=..., auth_token=...)`),
a row that violates a **foreign key present on the cloud primary but absent on the
local replica** commits successfully locally, then fails to push to the cloud with
`FOREIGN KEY constraint failed`. The failure is only logged as a `WARNING` and retried
forever; `commit()` returns success to the caller. The row lives only in the local
replica until the replica is re-synced from cloud (on the next pool init / bootstrap),
at which point it is **silently and permanently discarded**.

The local↔cloud FK divergence is self-inflicted by persistum itself: `apply_migrations_async`
**skips `add_foreign_key` on embedded replicas** (`Skipping add_foreign_key on <table> —
reconstruction is unsafe on embedded replicas. Run 'declaro migrate-remote'`), so after
`migrate-remote` applies the FK to the cloud primary, the two schemas permanently disagree
on FK enforcement.

Net effect: a class of writes that the application believes succeeded are never durable,
and vanish on the next restart with no error anywhere the application can observe.

## Reproduction (minimal, self-contained)

Cloud primary schema (FK exists on the cloud — e.g. applied via `declaro migrate-remote`):

```sql
CREATE TABLE parent (id TEXT PRIMARY KEY);
CREATE TABLE child  (id TEXT PRIMARY KEY,
                     parent_id TEXT,
                     FOREIGN KEY (parent_id) REFERENCES parent(id));
```

The embedded replica's `child` has **no** FK, because persistum skipped `add_foreign_key`
on the replica.

```python
import asyncio
from declaro_persistum import ConnectionPool

URL   = "libsql://<db>.turso.io"
TOKEN = "<auth-token>"

async def main():
    pool = await ConnectionPool.turso("/tmp/replica.db", remote_url=URL, auth_token=TOKEN)

    # Insert a child that references a NON-EXISTENT parent (violates the cloud FK).
    async with pool.acquire_write() as c:
        await c.execute("INSERT INTO child (id, parent_id) VALUES ('c1', 'no-such-parent')")
        await c.commit()          # <-- returns success; replica has no FK to enforce

    async with pool.acquire() as c:
        cur = await c.execute("SELECT count(*) FROM child WHERE id='c1'")
        print("local replica has row:", (await cur.fetchall())[0][0])   # -> 1

    await asyncio.sleep(5)         # background push loop runs (~push_interval_s)
    # logs, repeatedly:
    #   WARNING ... Push to cloud failed: sync engine operation failed: database sync
    #   engine error: failed to execute sql: Error { message: "SQLite error: FOREIGN KEY
    #   constraint failed", code: "SQLITE_CONSTRAINT" }
    await pool.close()

    # A FRESH replica bootstraps from cloud -> sees only what actually reached the primary:
    verify = await ConnectionPool.turso("/tmp/verify.db", remote_url=URL, auth_token=TOKEN)
    async with verify.acquire() as c:
        cur = await c.execute("SELECT count(*) FROM child WHERE id='c1'")
        print("cloud has row:", (await cur.fetchall())[0][0])           # -> 0  (LOST)
    await verify.close()

asyncio.run(main())
```

### Expected

One of:
- `commit()` fails at write time (the replica enforces the same FK as the primary), so the
  application learns immediately that the write is invalid; **or**
- a persistent push failure is surfaced to the application (raised, or via a health/error
  callback), so it can tell the write did not become durable; **or**
- on re-sync/bootstrap, committed-but-un-pushed local writes are not silently discarded.

### Actual

- `commit()` returns success.
- `local replica has row: 1`
- Background push loops forever on `FOREIGN KEY constraint failed` (WARNING only).
- `cloud has row: 0` — the write never reached the primary.
- After the local replica is re-synced/re-bootstrapped from cloud, the row is gone from
  the replica too. **Silent permanent data loss, no error observable by the app.**

## Root cause

1. `apply_migrations_async` **skips `add_foreign_key` on embedded replicas** (by design —
   "reconstruction is unsafe on embedded replicas"), and directs the operator to
   `declaro migrate-remote`. This leaves the local replica **without** an FK the cloud
   primary **has**.
2. The replica therefore accepts writes the primary will reject. `commit()` succeeds locally.
3. The write is applied to the cloud primary only via the background push (`push()` on the
   sync connection). There it hits the FK and fails: `FOREIGN KEY constraint failed`.
4. The push failure path only logs a `WARNING` ("Push to cloud failed: …") and retries; it
   never propagates back to the `commit()` that produced the write.
5. Because embedded replicas re-sync from cloud on init/bootstrap, the un-pushed local write
   is discarded the next time the replica is (re)opened. No error is ever raised.

## Compounding factor — poisoned push queue

Push appears to apply the accumulated local write-log as a batch. A single un-pushable row
at the head of the log makes **every** subsequent push retry the same failing batch, so *all*
later writes for that database stop syncing to cloud — even perfectly valid ones — until the
offending write is removed (e.g. by wiping the local replica, which also loses everything
un-pushed). The observable symptom is a database whose cloud copy silently stops advancing
while the local replica keeps accepting writes.

## Impact

Any write that satisfies the (FK-stripped) replica schema but violates the primary's FK is
silently lost. In practice this includes:
- inserting a child before its parent has been pushed (ordering), and
- inserting a child whose parent write itself failed to push (cascading loss).

The application has no way to detect this: `commit()` succeeds, reads from the replica show
the row, and the only signal is a WARNING log line that is easily lost and carries no
identity of the affected row.

## Environment

- `declaro-persistum` 0.1.6
- `pyturso` 0.5.1
- Python 3.13.0
- Turso Cloud embedded replica (`ConnectionPool.turso`, `turso.aio.sync.connect`), cloud
  primary schema created/altered via `declaro migrate-remote`.

## Suggested fix (feature request)

In priority order:

1. **Fail fast, not silently.** Enforce the same FK constraints on the embedded replica that
   the primary enforces (apply `add_foreign_key` to the replica, or validate at write time),
   so the invalid `commit()` errors immediately instead of producing an un-pushable write.
2. **Surface persistent push failures to the caller.** A push that keeps failing for the same
   batch/row should be raised (or delivered via a health/error callback / awaitable "durable"
   mode), not only logged at WARNING. The application must be able to learn that a committed
   write did not become durable.
3. **Never discard un-pushed local writes on re-sync.** On init/bootstrap, if the local
   replica holds committed-but-un-pushed frames, do not silently overwrite them with cloud
   state — detect the divergence and report it (or push-before-pull).
4. **Quarantine the poison.** Isolate a single un-pushable write rather than retrying the whole
   batch forever, so one bad row cannot block all subsequent writes from syncing.

Item 1 alone eliminates the silent-loss class: if the replica rejected the write, there would
be nothing un-pushable to lose. Items 2–4 are defense-in-depth for any residual divergence.
