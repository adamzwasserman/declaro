# Replication, redesigned: split the directions

**Status: RETRACTED the same day it was written, 2026-08-11. Do not build this.**

The central premise below — "the push ships WAL frames, replace it with statements" — is **false**. It was checked against Turso's documentation only after Adam asked where the documentation said it. It does not say it, and it says the opposite.

**persistum already uses Turso Sync, which is already logical change-data-capture.** `turso.aio.sync.connect(path, remote_url)` with `push()` / `pull()` is that product's API, and the `turso_cdc` tables measured below are its capture. Turso's own docs:

> "Turso keeps track of everything that changed in the database. This allows us to send just the *logical changes* to the remote server"
> — [Turso Sync: a much, much, much better way to sync](https://turso.tech/blog/sync-benchmark)

> "Sync is built on the Turso Database engine and uses logical change-data-capture, providing local-first writes, explicit `push()` / `pull()` … significantly less bandwidth and lower latency than the page-level replication Embedded Replicas use."
> — [Embedded Replicas](https://docs.turso.tech/features/embedded-replicas/introduction)

Page-level frame replication is **Embedded Replicas**, the older thing persistum does *not* use. So the outbox-over-HTTP design below reimplements, worse and by hand, what the library already does.

**What the documentation DOES say, and it is the answer to the defect** — Turso's `COMPAT.md`:

> "In experimental MVCC mode there is an additional known gap: all statements on a connection share one MVCC transaction, so a write statement that finishes while a sibling statement is still active defers its commit until the last sibling finishes."

> "a write that reported success is not durable while sibling statements remain active, and it is silently rolled back if the transaction then ends abnormally."

That is a documented, exact description of the failure this document called unexplained: 8 writes reported ok, 6 durable locally, **no exception raised**. Concurrent writers are sibling statements. The write reports success and is silently rolled back. It is a known MVCC gap, not a sync defect.

MVCC is also **beta, not GA** — Turso 0.5 moved it from tech preview to beta with "use caution with production data".

**What remains true and measured below:** the A/B numbers on two machines, the `__turso_internal_mvcc_meta` local-only table, and the 1.5–1.7s cloud round trip.

**What remains my inference and is NOT in any Turso document:** that the `__turso_internal_mvcc_meta` divergence is what produces multicardz's "Database schema changed". No Turso documentation states MVCC is incompatible with sync. I measured a divergence and attached a mechanism to it. The mechanism is unsourced.

**The actual conclusion:** don't run MVCC on a replicated database — it is beta and has a documented silent-rollback gap that matches what we measured. That is a one-line change, not a redesign.

The rest of this document is kept as a record of a design argued from a mechanism nobody had checked — the same error its own closing section warns about.

---

Every measurement in this document carries the date it was taken and the box it was taken on. Everything else is marked as a proposal.

## The defect this exists to remove

`declaro-p39`: writes report success and never reach the primary. Two independent machines, two different presentations, one variable.

| | this machine, 3.14t (2026-08-11) | multicardz, 3.13 (2026-08-11) |
|---|---|---|
| MVCC on | 8 ok / 6 local / 3 primary | push fails forever, strands 6/20, pool hangs on close |
| MVCC off | 8 ok / 8 local / 8 primary | 16 ok / 16 local / 16 primary |

Measured cause, 2026-08-11: **MVCC creates a local table the primary does not have.**

```
MVCC ON   local_only_tables: ["__turso_internal_mvcc_meta"]
MVCC OFF  local_only_tables: []
```

Under WAL the local and primary schemas are identical. Under MVCC they are not, and the replication engine — which relates local state to the remote — cannot place the difference. multicardz observed the consequence directly: `replication engine operation failed: database error: Database schema changed`, retrying forever.

## The shape underneath the defect

Every failure we have is on the **push** side, and they share one cause: **the push ships WAL frames.**

- Frames cannot carry DDL. `migrate-remote` exists only to work around that.
- Frames require CDC capture. CDC is what MVCC collides with.
- MVCC adds local-only state the frame stream cannot carry.
- `flush()` returns ok without arrival, so the durability signal is not tied to the truth.

The **pull** side has never failed. It is what an embedded replica is good at.

So the redesign is not a better push. It is: **stop making one mechanism carry both directions.**

| Direction | Mechanism | Why |
|---|---|---|
| Pull — others' writes reach me | embedded replica, **pull-only** | already works, unchanged |
| Push — my writes reach the primary | **logical outbox over HTTP** | statements survive what frames cannot |

## The unlock, measured 2026-08-11

Turso Cloud accepts statements over plain HTTP at `/v2/pipeline`. Verified against a real database: `CREATE TABLE`, a parameterised `INSERT`, and a `SELECT` all returned `ok`, with no replication engine and no pyturso in the path.

**Including DDL** — the thing frame replication has never been able to do.

## The constraint that decides the design

**A write to cloud is never under a second. That is why the local replica exists.**

Corroborated here, 2026-08-11: `flush()` — which is one push to the primary — returned in 1.7s and 1.5s on the two arms. A cloud round trip is three orders of magnitude off a local commit, which is 0.5ms on the same replica.

So the replica is not an optimisation to be traded away. It is the reason the library is usable:

- a write commits **locally, sub-millisecond**
- and is **immediately readable**, because the reader and the writer are the same store

**An earlier draft of this document got this wrong.** It moved local writes to a separate database and served reads from a pull-only replica, so a write became readable only after a cloud round trip plus a pull — over a second, to read back something you just wrote. That design is dead. It is recorded here because it is the exact mistake to not make twice: *the cloud must never be between a write and reading it back.*

## What actually changes

Reads and writes both stay on the local replica, exactly as today. **Only the push mechanism changes.**

| | today | proposed |
|---|---|---|
| local write | replica, sub-ms | **unchanged** |
| read | replica, sub-ms | **unchanged** |
| pull (others' writes) | replication engine | **unchanged** |
| **push (my writes → primary)** | **WAL frames** | **statements over `/v2/pipeline`** |

That is the whole redesign. Everything that works is untouched; the one mechanism that fails is replaced.

## The write path

The write and its shipping record commit in **one local transaction** on the replica:

```
_declaro_outbox
  seq    INTEGER PRIMARY KEY   -- ordering, explicit rather than implied by frames
  sql    TEXT
  params BLOB
```

A background shipper drains it to `/v2/pipeline` in seq order and deletes a row only after the remote acknowledges. DDL goes down the same pipe, so `migrate-remote` stops being a special case.

The outbox is a table in the replica, so it is atomic with the write it describes. It never reaches the primary, because push is now our own code emitting statements and it simply never emits one for the outbox. **To verify before building:** that a pull cannot disturb a local-only table.

## MVCC stays off on cloud-backed pools

Not as a guard bolted onto a broken mechanism — as a consequence of the shape. `__turso_internal_mvcc_meta` breaks the engine's ability to relate local state to remote, and the pull depends on that relation just as the push did. Concurrency comes from stateless connections and the writer lock instead; multicardz measured 16/16/16 that way on 2026-08-11, with 4 honest contention failures out of 20 and no silent loss.

## Read-your-writes is not a problem here

It is free: the write is applied locally and the reader reads the same store. This is what the earlier draft threw away.

The ticket still has a job, but a narrower and more honest one — **durability**, not visibility. `collect` can mean "acknowledged by the primary" for a caller who needs to know the write survived this machine. A caller who only needs to read it back never waits for anything, because there is nothing to wait for.

That keeps the standing 0.1.12 requirement intact: no request path waits on remote unless the caller explicitly asks it to.

## What this deletes

- the push loop's frame semantics
- the CDC dependency, and with it the MVCC/CDC conflict
- `migrate-remote` as a special case
- `configure_write_queue(persistence_path=...)` — the outbox is durable by being a table, which is what Render's ephemeral disk needs
- `mvcc=` as a durability hazard: MVCC becomes safe locally, because nothing ships frames

## Open questions, not yet answered

1. **Idempotency on replay.** A shipper that dies between remote-apply and outbox-delete will resend. Needs either a seq-keyed applied-table on the primary or naturally idempotent statements. Not designed yet.
2. **Does a pull disturb a local-only table?** The outbox lives in the replica and must survive every pull. Unverified, and the design rests on it.
3. **Ordering across instances.** seq is per-instance. Two instances writing the same row still need a conflict rule, exactly as they do today.
4. **HTTP cost per shipment.** The `/v2/pipeline` round trip was not timed. It is off the request path so it does not gate a write, but it sets how far behind the primary runs. Batching is the obvious answer and is unmeasured.
5. **Does the local write still land when the replication engine is not pushing?** Today the replica's writes are captured by CDC for the frame push. With frames abandoned, whether CDC can be left off — and whether the engine still pulls correctly with it off — is unestablished.

## Beliefs to not re-derive

- **"MVCC is skipped for cloud replicas because CDC is incompatible."** Wrong in the old form and wrong in the new: MVCC *activates* on a cloud replica and CDC *does* run — `turso_cdc` held 12 rows under MVCC on 2026-08-11. What breaks is the schema divergence, not capture.
- **"The push loop is failing to run."** No. `flush()` returned ok in 1.7s while three of six rows were absent from the primary. It ran and reported success.
- **"An unfetched PRAGMA takes effect."** It does not. `PRAGMA journal_mode = 'mvcc'` issued without fetching the cursor is a no-op in pyturso. A probe that did this measured WAL and reported it as MVCC.

## Related

- `declaro-p39` — the stranding defect this removes
- `docs/design/concurrent-writes-and-the-write-queue.md` — the ticket design this builds on
- `docs/turso-cloud-sync.md` — the embedded-replica limitations that motivated the workarounds being deleted
