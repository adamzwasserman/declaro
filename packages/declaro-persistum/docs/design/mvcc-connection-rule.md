# The MVCC connection rule

**Status: binding rule. 2026-08-12.**


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
> **A synced replica takes ONE sync connection.** Measured 2026-08-12, pyturso 0.7.2, real replica: MVCC *does* run on a synced replica (`journal_mode = 'mvcc'`, 4 of 4 runs) and 20 sequential writes under it reached the primary intact. What fails is a *second* sync connection — `database tape error: database is busy`, 3 of 4 runs outright, one after 12 retries over 30s on an idle database. In the run where eight opened: 5 writes local, **0 on the primary**. MVCC is incidental; it is the mode in which the pool stops serialising writers, and that serialisation is what keeps one sync connection alive at a time. **The earlier claim "MVCC is local only — it creates local-only internal tables the sync engine cannot reconcile" was wrong in both halves and is retracted.**

## The rule

**Never hold a partially-read cursor open across a write on the same MVCC connection.**

In practice, for persistum: **reads and writes do not share a connection.** A write connection writes and nothing else.

## Why — two failure modes, one cause

Both need the same thing: a statement that has been *stepped* and not yet finished, alive on the connection when a write commits.

**1. The write is silently discarded.** Turso's `COMPAT.md`, under *"Same-connection write statements"*:

> "all statements on a connection share one MVCC transaction, so a write statement that finishes while a sibling statement is still active defers its commit until the last sibling finishes … a write that reported success is not durable while sibling statements remain active, and it is silently rolled back if the transaction then ends abnormally"

**2. The engine panics.** Measured here, pyturso 0.7.2, reproducible 3 of 3:

```
thread panicked at core/mvcc/database/mod.rs:5424:
  transaction should exist in txs map

  advance_cursor_and_get_row_id_for_table   core/mvcc/database/mod.rs:5424
    <- advance_mvcc_iterator                core/mvcc/cursor.rs:814
```

The write's `COMMIT` ends the shared transaction and removes it from the `txs` map; the reader's cursor still holds its id; advancing it looks up a transaction that is gone and `.expect()` panics. This kills the process — no exception, no recovery.

**MVCC-specific.** The identical case under WAL drains 4,999 rows with the write durable.

## What exactly makes a statement a "sibling"

Not stated in COMPAT.md. Found in the engine source (verified against `main`, 2026-08-12):

The counter is `n_active_root_statements`. A writer commits only when it reads exactly 1. A statement joins the count **at its first step, not at prepare** — `core/statement.rs`, and the comment there calls it *"the SQLite-compatible notion of 'another SQL statement in progress'"*.

For pyturso specifically:

| what the caller does | is it a sibling? |
|---|---|
| `cur = await conn.execute("SELECT …")`, never fetched | **No.** `execute()` does not step a row-returning statement. |
| one `fetchone()` that returns a row | **Yes**, and it stays one |
| drained, finalized, or closed | no longer counted |

**So the hazard is a PARTIALLY-read cursor, not an unread one.** That is sharper and more useful than COMPAT.md's own advice, which is *"finish or reset sibling statements promptly after writing"* — and finishing the sibling after the write is precisely the sequence that panics.

## Measured, and one thing that did not reproduce

pyturso 0.7.2, local, MVCC, one connection:

| case | result |
|---|---|
| write alone | durable |
| reader stepped once → write → **drop reader** | **durable — the documented loss did NOT occur** |
| reader stepped once → write → **drain reader** | **PANIC**, 3/3 |
| reader fully drained → write | durable |

The documented loss case did not reproduce on 0.7.2. The engine source that explains the mechanism was verified against `main`. **Whether 0.7.2 and `main` behave identically here is not established**, and that gap is the most likely explanation. Do not conclude the durability gap is absent in 0.7.2 — conclude that we did not trigger it.

## Why we are not fixing this upstream or in a fork

**Upstream knows and has pinned the current behaviour with tests.** A `FIXME` in `core/vdbe/execute.rs` on `main` describes the gap, and two tests assert the loss rather than a fix: `test_mvcc_completed_writer_changes_lost_when_last_reader_abandoned` and `test_mvcc_completed_writer_changes_lost_when_joining_writer_errors`. Still open in `v0.8.0-pre.4`.

The real fix is the one their own FIXME names — commit at the writer's own halt and downgrade remaining statements to read-only, as SQLite's `btreeEndTransaction` does. That is an engine change, it would require flipping their tests, building wheels for every platform we deploy on, hosting them, repinning `pyturso>=0.7.0` away from PyPI, and rebasing on a file they are actively changing.

**We remove the cause instead.** It costs nothing, it closes both failure modes, and it does not depend on anyone else's release.

## The framework already forbids the shape

`honest-persist` has required this from the beginning, for unrelated reasons:

- §7.4 — `execute(query, conn) -> [Row]`, rows are plain dicts, **no lazy loading**
- §10 — no lazy loading; a query returns what it says it returns

A conformant implementation materialises the result before `execute` returns, so the cursor is drained inside the call. **A partially-read cursor therefore cannot be alive when the next operation runs, and the object both failure modes require does not exist.** The case table above confirms it from the other side: "reader fully drained → write" is durable, and full drainage is the only read shape the rule permits.

That rule was adopted for predictable queries and no ORM magic. It happens to eliminate a process abort and a silent write loss in an engine that did not exist when it was written.

**The change that would reintroduce the hazard is streaming a large result set** — a reasonable-sounding relaxation of exactly this rule. Anyone proposing it should read this document first.

## Prose that describes steps invites the hazard between them

Both engine hazards found here share a shape: **an implementer who follows the written advice literally gets the failure.**

- *"Check whether the key is claimed, then claim it"* → a guard that protects nothing, because the hazard lives between the two steps.
- *"Finish or reset sibling statements promptly after writing"* → a process abort, because the hazard lives between the write and the finish.

A rule phrased as an **absence** has no gap to fall into: *no partially-consumed cursor outliving the call*, *no read followed by a write on one connection*, *one atomic operation, not two*. State what must not exist rather than what to do in what order.

## Status of MVCC itself

Experimental in every source that states a status: COMPAT.md, `docs/manual.md` (*"not production ready so do not use it for critical data right now"*), `docs/agent-guides/mvcc.md`, the pragmas docs, and the launch blog. Only `README.md` omits it from its experimental list.

**Trap:** the `--experimental-mvcc` flag was removed in 0.4.0 and the mode renamed in 0.5.0. MVCC is now one `PRAGMA journal_mode = 'mvcc'` away with no gate. "There is no experimental flag, so it must be stable" is wrong.

## Related

- `concurrency-and-throughput-measured.md` — the persistent-connection result this rule makes safe
- `refactoring-plan.md` — MVCC Turso is the local, unsynced, non-pooled target
