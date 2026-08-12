# Concurrency and throughput, measured

**Status: measurement record. 2026-08-11 / 2026-08-12.** Every number here carries the machine it came from. Where a number is wrong or contaminated, it says so rather than being deleted.

## The question

multicardz targets 10,000 concurrent writers. By the nature of that application no two writers ever write the same `(table, key)`, so conflicts are not the constraint. What is?

## The answers, up front

1. **A write costs a thread, not a connection.** pyturso's async API starts one OS thread per connection. Opening a local database costs 0.026ms; starting the thread costs 0.347ms and 8 MB of stack.
2. **More concurrency stops helping very early and then actively hurts.** Peak throughput sits at a crew of 32–64. At 512 workers throughput is *worse than at 16*.
3. **The ceiling is throughput per instance, not concurrency.** ~641 writes/sec on Render Pro, ~1,175/sec on pro_ultra, on tmpfs.
4. **10,000 concurrent database writes is not a thing worth wanting.** 10,000 concurrent *callers* is fine — they deposit, take a promise, and leave. The writes drain through a small crew.
5. **Sustained 10,000 writes/sec is not reachable by tuning.** It needs roughly nine pro_ultra instances, or different storage.

---

## Hardware, stated properly

| | Mac | Render Pro | Render pro_ultra |
|---|---|---|---|
| model | Mac mini M2 (Mac14,3) | container | container |
| CPU | 8 cores (4 perf + 4 eff) | not captured | **8** (`cpu.max 800000/100000`) |
| memory | 24 GB | **4 GB** (`memory.max`) | **32 GB** (`memory.max`) |
| thread cap | **6,144 / process** (`kern.num_taskthreads`) | none (`ulimit -u` unlimited) | none |
| pids limit | — | 75,478 | 151,347 |
| Python | 3.14.0 free-threaded, GIL off | 3.14.7 free-threaded, GIL off | 3.14.7 free-threaded, GIL off |

**Correction.** An earlier report described the Render Pro box as "16 cores, 61.4 GB". Those came from `nproc` and `/proc/meminfo`, which inside a container report **the host, not the allocation**. The container had 4 GB. A conclusion of "no ceiling in sight" was built on host numbers and was wrong.

---

## 1. What a connection costs

Local file, Mac, n=200–500 per row.

| operation | p50 | p99 |
|---|---|---|
| `turso.connect()` open (blocking) | **0.026ms** | 0.064ms |
| blocking `close()` | 0.006ms | 0.032ms |
| `turso.aio.connect()` open | 0.289ms | 4.497ms |
| `turso.aio` `close()` | 0.463ms | 3.205ms |
| thread `start()` | 0.347ms | **10.375ms** |
| thread `join()` | 0.012ms | 0.576ms |
| INSERT + commit, blocking | 0.064ms | 1.513ms |
| INSERT + commit, aio | 0.349ms | 2.978ms |

**The database open is not the cost. The thread is.** The blocking open is under a tenth of the async open; the difference is `threading.Thread.start()`.

Cloud replica, raw `turso.aio.sync`, aws-us-west-2:

| operation | p50 | p99 |
|---|---|---|
| COLD open (empty dir, bootstrap pull) | **440ms** | 1,030ms |
| WARM reopen (all four replica files) | 0.985ms | 5.034ms |
| additional connection to an open replica | 1.162ms | 2.210ms |
| `close()` | 0.593ms | 1.850ms |
| INSERT + commit (local, on the replica) | 0.501ms | 2.749ms |

The 440ms cold open is a bootstrap paid once per replica directory. It is not a per-connection cost and must not be quoted as one.

---

## 2. One thread per connection, confirmed

Peak thread count tracks concurrent writes one-for-one at every level measured, on both platforms:

```
concurrent    20    100    500   1000   2000   5000
threads       23    113    514   1014   2014   5014
```

This is a property of `turso.aio` being a worker-thread wrapper around the blocking driver (`turso/lib_aio.py`), not of Turso or of MVCC.

---

## 3. The macOS ceiling is real and is a macOS artifact

`kern.num_taskthreads = 6144` — a hard per-process cap on Darwin. 5,014 threads was already 82% of it, so **10,000 concurrent one-and-done writes cannot run on macOS in this shape at all.** Not slow: impossible.

Linux has no equivalent. `threads-max` 503,188, `ulimit -u` unlimited, cgroup pids limits of 75k–151k.

---

## 4. The first scaling curve, and why it was the wrong experiment

Mac, distinct keys, **N writes at N concurrency**:

| concurrent | wall | writes/sec |
|---|---|---|
| 20 | 38ms | 522 |
| 50 | 109ms | 458 |
| 100 | 205ms | 488 |
| 250 | 527ms | 475 |
| 500 | 1.07s | 468 |
| 1,000 | 3.61s | 277 |
| 2,000 | 10.8s | 185 |
| 3,000 | 24.2s | 124 |
| 5,000 | 37.6s | 133 |

Zero failures at every level. The shape — flat then collapsing — is right, and it is the first evidence that concurrency past a few hundred is counterproductive.

**But the experiment is flawed: total work grows with concurrency, so it measures batch size as much as throughput.** It also ran with a load average of 21 on 8 cores, so the absolute rates are contaminated. Use it for shape only.

---

## 5. The crew curve — the correct experiment

**Fixed total work (2,000 writes), varying the number of drainers.** Distinct key per write, tmpfs storage, zero failures at every level on both boxes.

### Render Pro (4 GB)

| crew | wall | **writes/sec** | threads |
|---|---|---|---|
| 1 | 11.38s | 176 | 4 |
| 2 | 11.26s | 178 | 6 |
| 4 | 4.34s | 461 | 10 |
| 8 | 3.26s | 614 | 17 |
| 16 | 3.15s | 636 | 29 |
| **32** | **3.12s** | **641** | 52 |
| 64 | 3.38s | 592 | 86 |
| 128 | 3.51s | 571 | 150 |
| 256 | 4.25s | 470 | 278 |
| 512 | 5.00s | 400 | 534 |

### Render pro_ultra (8 CPU, 32 GB)

| crew | wall | **writes/sec** | threads |
|---|---|---|---|
| 1 | 14.60s | 137 | 4 |
| 2 | 9.26s | 216 | 6 |
| 4 | 10.10s | 198 | 9 |
| 8 | 6.27s | 319 | 16 |
| 16 | 2.46s | 812 | 27 |
| 32 | 1.83s | 1,094 | 48 |
| **64** | **1.70s** | **1,175** | 96 |
| 128 | 1.74s | 1,149 | 162 |
| 256 | 1.85s | 1,083 | 290 |
| 512 | 4.51s | 444 | 546 |
| 1,024 | 7.74s | 259 | 1,058 |

### What the two curves say together

| | Pro | pro_ultra |
|---|---|---|
| knee | **32** | **64** |
| peak | **641/sec** | **1,175/sec** |
| cliff | 512 | 512 |

- **Roughly double the box, roughly double the throughput, and the knee moves one step right.**
- On pro_ultra the knee is **8× the CPU count**. That may be the rule; it is one data point, because Pro's `cpu.max` was never captured.
- **The cliff is at 512 on both, and it is steep.** 512 workers deliver less than 16 do.
- The useful band is wide: crews of 8–256 on pro_ultra are all within 30% of peak, and 16–256 within 10%. Precision is not required; order of magnitude is.

---

## 5b. Reuse and MVCC are separate levers, and they compound

Written because the 14x reuse result was once asserted to carry over to WAL. **It does not.** WAL permits one writer at a time, so reuse removes the per-write thread but the writes still serialise.

Four arms, one variable each. Mac, `TOTAL=2000`, crew 16, distinct keys, 3 retries so every arm can complete, journal mode read back and asserted on every connection. GIL on (importing `turso` disables free-threading locally).

| arm | writes/s | **landed** | failed |
|---|---|---|---|
| WAL + one-and-done | 250 | **1629 / 2000** | 371 |
| WAL + persistent | 1,505 | **1812 / 2000** | 216 |
| MVCC + one-and-done | 426 | 2000 / 2000 | 0 |
| **MVCC + persistent** | **4,721** | **2000 / 2000** | 0 |

```
reuse alone (WAL)          6.01x
concurrency alone (MVCC)   1.70x
both together             18.87x     <- they compound, not add
```

**Reuse is the larger single lever. MVCC is what makes the crew correct.**

**WAL LOSES WRITES AT CREW 16, even with three retries** — 371 and 216 of 2,000, to `database is locked`. MVCC loses none in either arm. So "WAL plus persistent connections" is not a cheaper safe option; it is a lossy one. **WAL's safe crew size is 1, or writers serialised behind a lock** — which is what a pooled WAL path with a write lock already does, and why a replicated target gets no write concurrency.

**Do not re-derive:** the 13,826/sec figure requires reuse AND MVCC together. Neither alone approaches it, and reuse on WAL cannot even complete the work at crew 16.

## 6. Storage dominates everything at these rates

Same code, same box, same concurrency (1,000), different filesystem:

| storage | per write |
|---|---|
| container root disk, 84% full | 60ms at crew 500; **process killed at 1,000** |
| `/dev/shm` (tmpfs) | **4.28ms**, then 9.8ms on a later run |

The run that died between 500 and 1,000 died on the disk, not on threads. **Every crew number in this document is tmpfs — the most favourable case there is.** Real storage will lower the plateau.

---

## 7. What this means for the design

**10,000 concurrent callers is fine. 10,000 concurrent writes is slower than 32.**

The shape that reaches the target: the caller deposits a write and receives a promise immediately. A crew of tens of drainers empties the queue, each write getting its own fresh connection — one write per connection, `BEGIN CONCURRENT`, commit, close. Threads are reused; connections are not. The cheap thing is per write, the expensive thing is shared.

10,000 writes through a crew of 64 on pro_ultra: **about 8.5 seconds**. Through a crew of 32 on Pro: about 16 seconds.

**Sustained 10,000 writes/sec needs about nine pro_ultra instances**, or storage faster than we measured, or fewer writes.

---

## 8. Not measured

- Any contention level between 20 disjoint keys and 20 on one key, on these boxes. The contention curve was Mac-only.
- Whether the knee-at-8×-CPU rule holds, since Pro's CPU allocation was never captured.
- Anything on real disk rather than tmpfs.
- Crews below 16 with any confidence. Pro's 1 and 2 are identical at ~176/sec and pro_ultra's crew 4 came out *slower* than crew 2. That is noise, and it needs repeats.
- More than one instance, or any cross-instance behaviour.
- 10,000 concurrent in a single process anywhere. It was never reached: macOS caps at 6,144, and the Render container was killed above ~1,000 before the crew design removed the need.

## 9. Method errors made while producing this

Recorded because the plan now requires it, and because each produced a wrong conclusion that survived until someone challenged it.

- **Host numbers reported as container numbers.** `nproc` and `/proc/meminfo` inside a container describe the host. A "no ceiling in sight" conclusion rested on 61.4 GB that was never available.
- **An experiment that conflated two variables.** N writes at N concurrency measures batch size and throughput together. The fix — fix the work, vary the crew — changed the answer from a vague "somewhere under 500" to a specific knee.
- **A crew size of 32 asserted before any measurement.** It happened to land on Pro's peak. That was luck, and on pro_ultra the peak is 64.
- **Measuring MVCC without `BEGIN CONCURRENT`**, three times, and concluding the engine could not do concurrent writes.
- **An unfetched PRAGMA**, which is a silent no-op in pyturso, causing a run to measure WAL and report it as MVCC.
- **Timing on a machine at load average 21** on 8 cores, without saying so.

## Related

- `refactoring-plan.md` — the contention curve, the retry bound, the claims discipline
- `key-claim-function.md` — conflict prevention, unnecessary when keys never collide
- `retry.py` — the bounded retry these numbers sit beside
