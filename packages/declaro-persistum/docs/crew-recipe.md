# The crew recipe


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

**For testing on a free-threaded build. 2026-08-12.**

## Who this is for

**Local-only stores.** The crew depends on MVCC, and MVCC must never run on a synced replica — it creates local-only internal tables the sync engine cannot reconcile.

**It does not apply to a cloud-synced write surface.** multicardz established this on 2026-08-12: their entire write path (central, ~2000 shelf databases, project pools) is synced for durability, so the crew, `BEGIN CONCURRENT`, and persistent MVCC writer connections are all the wrong tool for them. Their target is the other one — WAL, pooled, **no write concurrency at all** — and their lever is reducing write *volume*, not raising concurrency.

If your durability model is cloud sync, stop here. Nothing below applies, and the numbers are unreachable by construction.

Everything below uses the public API of declaro-persistum 0.1.31. There is nothing to add to the library — `deposit`, `collect`, `drain`, the retry policy, and the query builder are all exported already, and N concurrent `drain()` coroutines over one room work as-is.

## The shape

The caller deposits a write and gets a ticket back immediately. A crew of drainers empties the queue. Each drainer holds **one persistent connection** and uses it for writes only.

```python
import asyncio
import turso.aio
from declaro_persistum import new_room, deposit, collect, drain
from declaro_persistum.retry import ON_CONTENTION
from declaro_persistum.query.builder import insert

# --- the two engine primitives -------------------------------------------

async def open_mvcc_writer(path):
    """One per drainer, opened once. Writes only — never reads."""
    c = await turso.aio.connect(path)
    cur = await c.execute("PRAGMA journal_mode = 'mvcc'")
    await cur.fetchall()          # an unfetched PRAGMA is a silent no-op
    return c

async def run_write(conn, write):
    """One deposited write. BEGIN CONCURRENT is mandatory."""
    await conn.execute("BEGIN CONCURRENT")   # without it: ordinary locking
    await conn.execute(write["sql"], write["params"])
    await conn.commit()

# --- the crew -------------------------------------------------------------

async def drainer(room, path, stop):
    conn = await open_mvcc_writer(path)
    try:
        while not stop.is_set():
            await drain(room, lambda w: run_write(conn, w), ON_CONTENTION)
            await asyncio.sleep(0)
    finally:
        await conn.close()

async def start_crew(room, path, size):
    stop = asyncio.Event()
    tasks = [asyncio.create_task(drainer(room, path, stop)) for _ in range(size)]
    return stop, tasks

# --- the caller -----------------------------------------------------------

room = new_room()
ticket = deposit(room, insert("cards", {"id": 1, "title": "x"}))   # returns at once
...                                                                # free to work
receipt = await collect(room, ticket)     # {"id": ..., "ok": True, "error": ""}
```

A builder `Query` is already a `PendingWrite` — `{sql, params}`, and pyturso accepts the named `:param` style the builder emits. No conversion layer.

## Rules that are not optional

**1. Never hold a partially-read cursor open across a write on the same connection.** Writes get their own connections; reads get theirs. Breaking this either discards the write silently or panics the process. See `docs/design/mvcc-connection-rule.md`.

**2. `BEGIN CONCURRENT` per write.** `journal_mode = 'mvcc'` alone changes nothing — without `BEGIN CONCURRENT` you get ordinary locking and `database is locked` under any concurrency.

**3. Fetch the PRAGMA cursor.** An unfetched PRAGMA does not take effect, silently, and you will measure WAL while believing you are on MVCC.

**4. One sync connection per replica.** MVCC on a synced replica is fine for sequential writes (measured: 20/20 reached the primary). What must never happen is a second concurrent sync connection to one replica — that is what strands writes. The old wording here blamed MVCC and named an unproven mechanism; retracted 2026-08-12.

## Sizing the crew

**This workload is CPU-bound.** Measured on Render: CPU pinned near the container limit while memory stayed flat.

| box | CPUs | knee | peak |
|---|---|---|---|
| Render Pro | not captured | 32 | 641 writes/sec |
| Render pro_ultra | 8 | 64 | 1,175 writes/sec |

Those are with a fresh connection per write. **With persistent per-worker connections the numbers are 11–14× higher and the knee moves left** — 13,826 writes/sec at a crew of **16** on pro_ultra.

**Size the crew to the CPUs you actually have.** A `starter` instance is **0.5 CPU** — a crew of 16 there will be worse than a crew of 1 or 2. None of the numbers above are reachable on starter.

Beyond the knee throughput *falls*. At 512 workers it is worse than at 16.

## Free-threaded specifics

**pyturso ships no wheel for free-threaded 3.14**, so `uv` compiles it from Rust source on every cold environment. That build is memory-hungry and has been killed on a container mid-compile. **Warm the cache in the foreground first**, then run:

```bash
uv run --no-project --python 3.14t --with pyturso python -c "import turso; print('warm')"
```

A second invocation should return instantly. If a detached process rebuilds it, its `HOME`/`UV_CACHE_DIR` differ from the shell that warmed it — pin them explicitly.

## What to measure, and what to check

Throughput is the easy half. The half that matters:

**Run a read-modify-write test and compare the counter to the success count.** `UPDATE t SET v = v + 1` across N concurrent writers, then assert `SUM(v)` equals the number of writes that reported success. Anything less is a silently discarded write. Every run here has lost nothing, on both one-and-done and persistent connections — but that was with writes only, and your workload is not ours.

**Report first-attempt success separately from eventual success.** It is the leading indicator: it falls while the eventual rate still reads perfect, so by the time anything fails you have been deep into the retry budget for a while.
