# Concurrent local writes, the write queue, and opportunistic push


> ## ⚠️ DEPRECATED IN PART — POISONOUS PRACTICE
>
> **Everything below about `pooled_writes` is poisonous practice.** It put a pool decision on a consumer-facing surface, which is forbidden.
>
> The consumer chooses **async (default) or sync**, and nothing else. Whether a pool exists, whether a write reuses a connection, and whether the engine runs MVCC or WAL are internal, owned by exactly one writer, and invisible above that boundary.
>
> Binding constraint: the pool is never a surface. That constraint document was deleted in `07fc023` with ten other superseded planning documents, once the pool was gone from the code.
>
> The measurements in this document remain valid. The API shape does not.

**Status: implemented 2026-08-11, except the open defect below. Written 2026-08-10.**


> ### THE TWO LEVERS — measured 2026-08-12, do not re-derive
>
> | arm | writes/s | landed | |
> |---|---|---|---|
> | WAL + one-and-done | 250 | 1629 / 2000 | **371 LOST** |
> | WAL + persistent | 1,505 | 1812 / 2000 | **216 LOST** |
> | MVCC + one-and-done | 426 | 2000 / 2000 | 0 |
> | **MVCC + persistent** | **4,721** | **2000 / 2000** | 0 |
>
> **reuse alone 6.01× · MVCC concurrency alone 1.70× · both 18.87× — they compound.**
>
> Connection **reuse** removes the per-write OS thread and is the larger lever. **MVCC + `BEGIN CONCURRENT`** is what lets a crew write in parallel and is what makes it *correct*. Neither alone gets there; the 13,826/sec figure needs both.
>
> **WAL loses writes at crew 16 even after three retries. MVCC loses none.** WAL's safe crew is 1, or writers serialised behind a lock.
>
> **A replica takes ONE replica connection.** Measured 2026-08-12, pyturso 0.7.2, real replica: MVCC *does* run on a replica (`journal_mode = 'mvcc'`, 4 of 4 runs) and 20 sequential writes under it reached the primary intact. What fails is a *second* replica connection — `database tape error: database is busy`, 3 of 4 runs outright, one after 12 retries over 30s on an idle database. In the run where eight opened: 5 writes local, **0 on the primary**. MVCC is incidental; it is the mode in which the pool stops serialising writers, and that serialisation is what keeps one replica connection alive at a time. **The earlier claim "MVCC is local only — it creates local-only internal tables the replication engine cannot reconcile" was wrong in both halves and is retracted.**

Landed: stateless writes are the default for the Turso backend (`acquire_write` and `transaction()` open their own connection and close it), the pooled/serialised path survives behind `pooled_writes=True`, and `drain` takes a required `Retry` policy so contention is retried and constraint violations are not. The rationale for stateless-by-default — and the reasoning error that first argued against it — is recorded in `pool.py`'s module docstring so it is not re-derived.

Still open: the stranding defect in "The open defect" below. It was measured on the pooled code and is not caused by, or fixed by, statelessness.

This is the next piece of work on declaro-persistum. The goal is performance: let writers run concurrently against the local replica, absorb contention with retries instead of a lock, and deliver to cloud opportunistically for eventual consistency.

Everything below that states an engine behaviour carries the date it was measured. Nothing here is inferred. That distinction matters more than usual on this file — see "Beliefs to not re-derive" at the end.

## The goal

1. **Concurrent local writes.** Turso supports them through MVCC and `BEGIN CONCURRENT`. Use that rather than serialising writers behind a lock.
2. **A minimal queue.** The waiting room described below — deposit returns a ticket, the caller collects when it needs the answer.
3. **Retries on busy.** A write-write conflict is the documented price of MVCC concurrency, and the documented handling is to roll back and retry. persistum must be able to retry, which means holding the statement.
4. **Opportunistic push.** The local write is durable the moment it commits. Delivery to cloud is eventual, and nothing on a request path waits for it.

## The Honest Queue

A queue is a list of writes that have not happened yet. The WAL already provides durability and deferred application, so the only job left is to buffer callers who arrive at the same instant.

```python
ticket = deposit(room, write)        # returns at once
...                                  # the caller is free
receipt = await collect(room, ticket)  # {"id": ..., "ok": True, "error": ""}
```

`deposit` hands back a ticket immediately. `collect` awaits that ticket and returns the same ticket with a success or failure code. **That is what makes it asynchronous rather than a lock** — a lock makes you wait at the moment of writing.

The current implementation is in `src/declaro_persistum/write_queue.py`: `new_room`, `deposit`, `collect`, `drain`, over a `Room` TypedDict holding `writes` (arrival order) and `waiting` (ticket to future). Nothing is stored; the room is empty except while callers overlap. The caller runs `drain`; the module starts no task of its own.

### The ticket is how a caller designs its own atomicity

This is the part that is easy to miss and is the reason the design is shaped this way.

A transaction is a boundary the library imposes: everything inside it, all or nothing, decided by whoever wrote the wrapper. Tickets put that choice with the caller, who draws the boundary by choosing when to collect:

- deposit three, collect all three — those three are a unit
- collect the first before depositing the one that depends on it — an ordering constraint, expressed directly
- deposit three, collect only the one the caller actually needs
- deposit, do an HTTP call, collect later — a window no transaction could hold open

Failure is per ticket too, so a caller can retry one write and carry on with the rest instead of losing work that was independently fine.

A real example: a signup writes a user row, claims a pooled database, and writes a route. The route has a foreign key onto the user, the claim must be atomic, and the route is read back on the next line. Three writes, three different consistency requirements, one request. No single transaction shape fits that; three tickets do.

### Retry belongs here

`acquire_write` cannot retry a failed write, because it cannot replay a caller's statements — the pool never sees them as data. **A deposited write holds its own SQL and parameters, so the drain loop can retry it.** That is the difference between the queue and `acquire_write`, and it is what makes MVCC concurrency usable: conflicts become retries rather than lost writes.

Current `drain` does not retry. Adding it is part of this work. The retryable set is `Error::Busy`, `Error::BusySnapshot`, and anything reporting a conflict — `TursoPool._is_busy` already recognises all three.

## Measured facts

### Concurrent writers, one cloud replica (2026-08-10)

Distinct rows, so no logical conflict:

| | K=2 | K=5 | K=10 | K=20 |
|---|---|---|---|---|
| MVCC on, **no lock at all** | 2/2 | 4/5 | 9/10 | 20/20 |
| MVCC off (WAL) | 1/2 | 3/5 | 3/10 | 6/20 |

Single-writer is Turso's documented **default**; MVCC lifts it. `_write_serialisation()` now returns the lock only when MVCC is off.

### MVCC activation (2026-08-10)

- Requested by default. `PRAGMA journal_mode` returns `mvcc` on a cloud replica.
- Survives CDC capture coming up, and survives reopening the replica.
- **Refused on an existing wal+CDC replica**: `cannot change journal_mode (from wal to mvcc) while CDC capture is active`. A replica must be created in MVCC mode.
- persistum never enables CDC; the replication engine does, because capture is how it tracks frames to push.

### Write latency (2026-08-09)

Consumer-visible, cloud replica: p50 0.3–4.3ms, p99 mostly under 15ms, 2 of 2397 writes over 50ms. Local commit is sub-millisecond because the remote is not on the write path. PostgreSQL loopback: 0 of 1200 over 50ms, one round trip 0.107ms.

## The open defect

**Writes are stranded under MVCC with concurrent write connections. Cause NOT established.**

Measured 2026-08-10, real cloud replica: 17 writes reported ok, 15 locally durable, 4 reached the primary. It plateaued at 4 after one minute and **did not converge in 348 seconds**. Making the push cover every holder in `_write_holders` did not change it — that change was reverted.

Separately measured the same day: concurrent writers to one table under MVCC raise `turso.Error: Write-write conflict` **even for distinct rows**. `acquire_write` does not retry it.

**Whether the conflicts and the stranding are the same failure is unknown.** A conflicted write never lands, so there would be nothing to push and nothing to converge — plausible, unproven. Do not treat it as established.

Unmeasured lead: whether MVCC writes produce WAL frames at all. A startup error once reported `CheckpointResult { wal_max_frame: 0, ... }`. The replication engine ships WAL frames; if MVCC rows live only in the in-memory version index, there would be nothing to ship. A probe for this crashed before measuring.

## How to measure this engine

`TURSO_KEY` in `~/dev/.env` is the platform API token. Create a throwaway database, mint a token, run, delete it.

**Measure convergence, not a snapshot.** Poll the primary until it matches or a generous budget expires. Reading the primary three seconds after a write and calling the gap "data loss" produced hours of wrong conclusions on 2026-08-09.

**Use fresh row ids every run.** Three separate probes were contaminated by reusing ids, so a rerun's duplicate-key failures were counted as losses.

**Do not filter the logs you are measuring.** `Push to cloud: N consecutive failures` is emitted from the push loop's failure branch; it was grepped out of every probe for a whole session.

## Beliefs to not re-derive

Each of these was stated confidently in this codebase and each was wrong. They are recorded so they are not reconstructed from first principles.

- **"There is a 2–3 concurrent writer ceiling."** No. That was measured in WAL mode and published as an engine limit. See the table above.
- **"The replication engine is a single-appender log."** Invented. No Turso documentation says this.
- **"MVCC is skipped for cloud replicas because CDC is incompatible."** No. MVCC activates and survives CDC.
- **"The write queue was never wired up."** It was, until commit `15f72b6` (2026-03-11) detached it as a side effect of a driver migration. Use `git log -S` to ask what was ever called; the working tree cannot answer that.
- **"Writers off writer-zero don't get their frames pushed."** Plausible, and disproved — pushing every holder did not fix the stranding.

## Related

- `write_queue.py` — the waiting room implementation
- `pool.py` `_write_serialisation`, `_is_busy`, `_push_once` — carry their measurements in comments
- `docs/turso-cloud-sync.md` — replica sidecars, `migrate-remote`, measured concurrency
- 0.1.25 and 0.1.29 are yanked from PyPI. 0.1.29 is where the stranding appears.
