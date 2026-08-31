# The key-claim function

> ## READ THESE FIRST. NOT OPTIONAL.
>
> Nothing in this document may be reasoned about, quoted or acted on until you have read all three, word for word:
>
> 1. [Concurrent writes on Turso Cloud](https://turso.tech/blog/concurrent-writes-on-turso-cloud)
> 2. [Turso 0.7.0](https://turso.tech/blog/turso-0.7.0)
> 3. [Concurrent writes in practice](https://turso.tech/blog/concurrent-writes-in-practice)
>
> ### A SYNC CONNECTION AND A WRITE TO THE JOURNAL ARE TWO DIFFERENT THINGS
>
> Every wrong answer this project has produced about concurrency came from treating them as one thing. Read this table before you write a sentence containing the word "concurrency".
>
> | | **a write to the journal** | **a sync connection** |
> |---|---|---|
> | what it is | one transaction appending to the local database file | the channel between this copy and the primary |
> | how many at once | many, under MVCC with `BEGIN CONCURRENT` | exactly one |
> | what limits it | row-level write conflicts, at commit | the engine: a second one answers `database tape error: database is busy` |
> | when it happens | whenever a caller writes | on push and pull |
> | does it bound writers | no | **NO** |
>
> **The one sync connection is not a writer and does not make writers wait.** It carries a log to and from the primary. Local writes go to the journal, concurrently, and the sync connection ships them later. A measurement that a second sync connection fails is a fact about the channel and says NOTHING about how many writes can be in flight.
>
> So: "a replica permits one connection" is false. "A replicated database serialises writers" is false. Turso's sync engine syncs MVCC databases, so replicated and concurrent are not opposites. If a sentence concludes something about concurrency from a count of connections, it is wrong.


**Status: designed 2026-08-11, measured, not yet implemented.** Design by Adam. The race and its fix are measured below.

## What it is

One function, one piece of memory it alone owns, two inputs and two outputs.

```
claim(table, key, PUSH)  ->  "success" | "wait"
claim(table, key, POP)   ->  "success"
```

`key` is the **primary key value**. It is already in the write, so there is nothing to parse and nothing to derive.

**It never blocks.** It answers. A semaphore makes the caller wait inside the library; this returns `"wait"` and hands the decision back, so nothing sleeps in code the caller cannot see. `"wait"` folds into the retry that already exists — it is the same "not now" as a busy database, absorbed by the same bounded policy.

## The whole behaviour, four cases

| input | key present? | returns | memory after |
|---|---|---|---|
| `(table, key, PUSH)` | no | `success` | key added |
| `(table, key, PUSH)` | yes | `wait` | unchanged |
| `(table, key, POP)` | yes | `success` | key removed |
| `(table, key, POP)` | no | `success` | unchanged |

Two operations, two states. The input space is bounded and every member can be run, which is the Honest Test property — a semaphore dict scattered through the write path does not have it.

## PUSH must be ONE operation, not two

**This is the finding. Do not write the obvious version.**

```python
# WRONG — two operations with a gap between them
if key in memory:
    return "wait"
memory[key] = True
return "success"
```

```python
# RIGHT — one atomic test-and-set
token = object()                     # UNIQUE to this caller
prior = memory.setdefault(key, token)
return "success" if prior is token else "wait"
```

### Measured, 2026-08-11, Python 3.14.0 free-threaded, `GIL enabled: False`

32 concurrent claimers racing for one key. A conflict is more than one `"success"` with no intervening POP — the gate silently protecting nothing while appearing to work.

| variant | conflicting trials |
|---|---|
| **async, any `await` between check and set** | **400/400** |
| async, no await between (one event loop) | 0/400 |
| sync function, real threads | **3/2000**, worst case 3 claimers |
| **`setdefault` + unique token** | **0/2000** |

**The threads case is rare and real: 3 in 2000, about 0.15%.** It would be effectively impossible to reproduce from a bug report.

**The async case is certain.** If `claim` is `async` and anything at all awaits between the check and the set — a log line, a metric, any I/O — it races **every single time**. That version looks completely reasonable and provides no protection whatsoever.

### The token must be unique per caller

A shared sentinel is not a fix. The first caller inserts it, every later caller reads it back, and every caller matches it, so **all** of them report `"success"`. Measured at 400/400 conflicts before the bug was found. The value stored and the value compared against must be the same object *for that caller only*.

## The caller's contract

`write()`'s caller — the execute step in the MVCC target, which is our own code and one call site — PUSHes before the write and POPs after the return, **whatever the return is**: success, conflict, constraint violation, or exception.

**A missed POP is a permanent stall.** That key returns `"wait"` to every later writer until the process restarts. A transient error becomes a stuck row. Honest Code Rule 12 names "manual open/close lifecycle" as the violation and a context manager as the fix, which writes the `finally` once instead of at every exit path; whether to use one is a call for whoever implements it, but the guarantee is not optional.

## What it does not do

- **It is in-process only.** A second process has its own memory and its own claims. Two services writing the same row collide exactly as they do today.
- **It does not create throughput.** It converts conflicts into `"wait"`. At 20 writers on one row you currently get 14 landing and 6 refused quickly; with the gate you get more landing and more waiting. Whether that is better is the caller's judgement, and the ticket already lets them make it.
- **It may not be needed below five writers per row.** Three retries already lands everything up to four concurrent writers per primary key, measured across five runs. The gate earns its place above that or not at all.

## Related

- `refactoring-plan.md` — the contention curve and the retry bound
- `retry.py` — `"wait"` is absorbed by the same bounded policy as a busy database
- `state-ownership-and-the-pool-boundary.md` — this holds mutable state, so it has exactly one owner and one writer, inside the MVCC target, invisible above it
