# Architecture

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


One rule shapes all of this: you declare, the library decides. Every layer below moves a declaration one step closer to a statement, and soaks up an engine difference on the way instead of passing it up to you.

## The one path

A Pydantic model is the only description of the database. Everything downstream reads it. Nothing asks the database what it holds to figure out what belongs there.

**Why the model and not the database.** Introspection tells you what *is*. It can't tell you what *should be*. If the code asked the database which columns exist to decide which ones belong, a column someone added by hand in production becomes part of the spec and never gets dropped. The model is the desired state. The live schema is just the current state, to be corrected. The differ compares them to work out a change, not to pick a winner.

You get a bonus from that. Because the model is the truth, asking for a column it doesn't declare fails in your own process, before anything builds a statement. Ask the database instead and that check costs a round trip, so it can't be a pure function and it can't happen early.

```
models  →  schema  →  difference  →  operations  →  statements  →  engine
```

**Models to schema.** `pydantic_loader` reads `__tablename__` off `@table` and the columns off `model_fields`. `field` says what a column *means*, never how it's spelled. Those belong to different owners: you know the column holds money that can't go below zero, and only the applier knows whether that's `numeric(10,2)` or `REAL`. Put an engine word in the model and it stops being portable, quietly, on the day someone switches engines.

**Schema to difference.** `differ/core.py` compares the declared schema against the live one and hands back operations, the order to run them, and anything ambiguous it couldn't settle on its own. Pure function: same input, same output, no I/O. That's what lets you test a migration without a database, and it's why comparing is split from applying. Deciding what to change and doing it fail in different ways, and one function that does both can't be checked without risking the second.

Ambiguity comes back instead of getting guessed. A column gone and another one showing up is either a rename or a drop plus an add, and the difference is your data. The differ says it can't tell rather than picking, because picking wrong drops a column.

**Difference to statements.** `applier/` turns operations into SQL for one engine. `differ/toposort.py` sorts them, so a table lands before the foreign key pointing at it. That order comes from the declared references, not from the order you happened to write your models in, because that order is arbitrary and the dependency isn't.

**Statements to engine.** One connection, opened and closed by the library. You never hold it. A connection you hold is a resource somebody else has to close, and on the path where that somebody is a crash, nobody does.

Reads and writes take the same path from the same models. One path, not two: a second route to the database is a second place a column name gets written down.

## Engines are data, not branches

Three engines, and what differs between them lives in tables instead of branches, as per Honest Code Principles. Each engine is a row. The table is read by subscript, so an engine nobody declared blows up at the lookup instead of getting handled as something else.

| table | what its rows decide |
| --- | --- |
| `INSPECTORS` | how to read a live schema |
| `APPLIERS` | how to run an operation |
| `SQL_GENERATORS` | how to write one |
| `WRITERS` | how one row is written |
| `TRANSACTIONS` | what a transaction scope is |
| `FACTS` | whether the engine takes all operations at once or one at a time |
| `OPERATORS` | how each of the fifteen conditions renders |

Adding an engine means adding a row to each. The lookup never carries a default: `table[key]`, never `table.get(key, something)`. A default files an input nobody wrote a rule for under an answer written for a different one.

**Why tables and not branches.** `if postgresql ... elif sqlite` is an order-dependent decision copied into every file that has to make it, and adding an engine means editing all of them and hoping you didn't miss one. A table declares what exists, once. It also closes the input: the keys *are* the set of engines, so a test can walk them, and a missing row is a `KeyError` right at the lookup instead of a wrong answer three layers down.

Same reason for the subscript. A `.get` with a fallback re-opens the space the table just closed, and the code still reads like it's closed.

## Capabilities across engines

Engines don't offer the same things. PostgreSQL has a materialized view. SQLite doesn't, so we build one out of a table, its triggers, and a registry row. That's what the compatibility layer is: you declare a capability once, and it reaches every engine. Native where the engine has it, built where it doesn't.

**One table, one source of truth.** Splitting truth across a table per operation and native versus built ends up encoded in function names, where you can't read it. Then "does this engine do ranges" gets answered by running an experiment, and a capability nobody built looks exactly like one nobody wrote down.

**The shape.** One row per capability, one column per engine, each cell saying native or naming what builds it. Three things decide what a cell holds, and none of them changes as capabilities get built:

A cell can't be a single function. Create and drop are different statements, and `DROP VIEW` against an emulated materialized view leaves the object sitting there with the schema still reporting a view. A cell is a small record.

A cell has to cover reads too. An emulated materialized view is a table, so the *inspector* has to recognize it as a view. Otherwise the next diff proposes dropping it. Emulation spans the applier and the inspector, and a table covering only the applier covers half a feature.

And native isn't the same as working. Turso is aiming at SQLite compatibility and isn't there yet, so a cell that says native is a claim about the engine, and only a test against that engine backs it up.

## Where the outside world gets touched

The middle is pure. Diffing, ordering, ambiguity detection, validation, SQL rendering: all functions over data, and none of them opens anything.

I/O happens at named edges, and each one carries a `@boundary` marker so you can count the edges instead of arguing about them. A `@helper` does no I/O, and a test enforces that rather than trusting it.

**Why a marker and not a convention.** Business logic that reaches the network can't be tested without the network behind it, can't be reused where that system is missing, and gives you nowhere single to change how the program talks outward. Those costs show up quietly, one function at a time. A marker turns the edge into something countable, so "how many places touch the outside" has an answer instead of an opinion, and a function that claims to be pure and isn't gets caught by a test rather than by a reader.

Opening a database gives you a `Database`: data carrying its own injected callables and its engine, decided once. Nothing downstream figures out what engine it's talking to, because the answer came with the value.

**Never work the engine out from a connection.** A driver connection doesn't tell you which engine it belongs to. The closest you can get is where its class happens to live, which means substring-matching module names, which makes the SQL you generate depend on somebody else's file layout. The driver reorganizes its own package and your placeholders change, silently, and nothing in your code moved.

## Every string is checked or bound, never interpolated

The surface takes strings. Column names are strings, and so are values. What makes the surface declarative is not their absence but their fate.

**A string is one of exactly two things.** A declared name, looked up in `model_fields` and refused if the model does not declare it. Or a value, bound as a parameter by the driver. There is no third case, and nothing is pasted into a statement.

That is the whole test for a new function, and it needs nobody to recognise SQL. Follow each argument to what it becomes: if it reaches string concatenation or an f-string on the way to a statement, it is a hole, whatever it happens to contain at the time.

**Recognising SQL was the obvious approach and it does not work.** Every statement begins with one of about twenty verbs, so a word scan finds statements. It finds `created_at` and `update_count` too, and it misses every fragment, because `total >= 0` and `name ASC` contain no verb and parse as nothing. Those fragments are exactly the escape hatches. A gate that reads what a string looks like passes the ones that matter.

Fate is also what separates the two forms that look identical. `where({"name": "bob"})` and `where("active = :x")` are both a string reaching a `where`, and one is a declared key with a bound value while the other has nowhere to go but into the statement.

## Failures and absence

A failure comes back as a value, never as a log line. A function that logs an error and hands you something you can't tell from success has two outputs, and only one of them is in the signature. You can't see it, can't test for it without capturing output, and can't turn it down.

And it isn't a pure function any more. Writing a log line is I/O. A function in the middle that logs has reached outside, so it fails the rule the whole middle rests on, and it does it without changing its signature or looking any different at the call site.

**Whether to say anything is the orchestrator's call, not the function's.** A function that finds something returns that fact. It doesn't know whether it's inside a retry that's about to succeed, a test, a dry run, or a production incident at 3am, so it's the wrong place to decide anyone needs to hear about it. The orchestrator knows all of that, and it's where the decision belongs. A function that logs on its own has taken that decision away from the only code with the context to make it.

Absence is a named value, `NOTHING`. Not `None`, not an empty result. A row that isn't there is an answer and you have to look at it. `None` is the wrong shape because `None` is also what a nullable column holds, so "no such row" and "a row whose name is null" stop being distinguishable exactly where it matters.

Nothing takes a default. `expand_enums=NOTHING` isn't ceremony: a default can't tell a caller who chose from a caller who forgot, and the path nobody chose is the path nobody tested. It's catch-and-swallow pointed at the inputs, and it manufactures an untested region of the function by construction. `shutdown` shows what that costs when it matters. The usual deployment has disk that doesn't survive the process, so a default here would decide whether your data gets replicated on exit, somewhere no reader looks.

## Writes: the queue and the crew

**A crew staggers writes.** That is the whole of it, and it is how you get concurrency without running anything at the same time.

A local write takes single or double digit milliseconds, and most of that is the engine working while your code has nothing to do. Run them one after another and the process sits idle through every one of those gaps. Stagger them and they interleave: the next write starts during the wait of the one before it, and the engine stops going idle in between.

That's where the throughput comes from, and it's why the crew is asynchronous. You're overlapping waiting, not executing in parallel, so the useful crew is far larger than the hardware allows to run at once.

**Effective concurrency, not strict concurrency.** Strict concurrency is what the box can literally do at the same instant: on an 8-CPU machine, eight. Effective concurrency is how many callers are in flight, and staggering raises it well past the strict limit. Measured on that 8-CPU box, in `docs/design/concurrency-and-throughput-measured.md`: a crew of 1 does 137 writes/sec, and a crew of 64 does 1,175. Eight times the CPU count, and 8.6 times the throughput of running writes one after another.

That's also why ten thousand concurrent callers is a reasonable thing to want and ten thousand concurrent writes is not. The callers deposit, take a ticket and leave. The writes drain through a crew of a few dozen.

A read is a question with an answer. A write is a request to change the database. Different jobs, different machinery, so writes get their own.

**You deposit and you're free.** `deposit(room, write)` hands back a ticket immediately and runs nothing. Go do whatever you want, then ask for the answer with `collect(room, ticket)`. That's the difference between this and a lock: a lock makes you wait at the moment you write.

**The room is a waiting room, not a store.** A `Room` holds the arrival order and the callers waiting on tickets. Both are empty whenever nobody's mid-flight. No persistence, no pending list to survive a crash, because every write has a caller holding its ticket. The write-ahead log is already the queue; this just buffers callers who show up at the same instant.

**A crew is N drainers over one room.** `drain` appends everything waiting, one write at a time, in arrival order. A crew runs several drainers at once, and each holds one connection for its whole life instead of opening one per write.

**A crew does not need MVCC.** Staggering is the async part, and that works on any engine. Callers stop blocking, the queue keeps the writer fed, and the writer stops going idle between writes. On WAL you get a crew of one that is never idle, which is already the point.

What MVCC buys is the size of the crew. It is what lets several drainers overlap inside the engine instead of queueing behind one writer, so it raises the ceiling rather than making the thing work. The measurements below say where each mode tops out.

**Connection reuse multiplies it.** Every connection is an OS thread. Open one per write and you pay for that thread every time, which is dead cost in exactly the gap the staggering is trying to fill. Reuse alone is a 6x difference, and it compounds with the crew size MVCC allows.

**A deposit is not a transaction.** Two deposits are two independent writes. The second failing doesn't undo the first, because the first is already in the log. If you need several statements to land together, hold the connection: `writing(db)` is the only thing here that gives you rollback.

**A failure belongs to whoever wrote it.** A ticket belongs to exactly one caller, so a constraint violation reaches that caller and nobody else. `collect` raises the engine's own exception for anything refused outright. A receipt saying `ok: False` means only that the engine stayed busy through the whole retry budget. Mix those two up and you'll retry the one that can never succeed.

**The crew is for local databases only.** A replicated database holds one connection by construction, so N drainers there are N coroutines queued behind one lock. The recipe below says why, with the numbers.

**DDL does not go through the crew.** A table created on an MVCC connection is invisible to every other connection, so migration takes its own connection and has to finish before a crew starts.

The rest of this section is the crew recipe as measured. **It is not to be changed without Adam's permission.**

---

> ### THE TWO LEVERS — measured 2026-08-12, do not re-derive
>
> Full runs and method in `docs/design/concurrency-and-throughput-measured.md`.
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
> **A replica takes ONE replica connection.** Measured 2026-08-12, pyturso 0.7.2, real replica, recorded in `docs/design/mvcc-connection-rule.md`: MVCC *does* run on a replica (`journal_mode = 'mvcc'`, 4 of 4 runs) and 20 sequential writes under it reached the primary intact. What fails is a *second* replica connection — `database tape error: database is busy`, 3 of 4 runs outright, one after 12 retries over 30s on an idle database. In the run where eight opened: 5 writes local, **0 on the primary**. This bounds CONNECTIONS and nothing else. Concurrent writers come from `BEGIN CONCURRENT` on one connection, so a replica running MVCC can write concurrently while still permitting only one sync connection.

### Who this is for

**Local-only stores.** A crew opens one connection per drainer, and a replica permits one sync connection: measured 2026-08-12 in `docs/design/mvcc-connection-rule.md`, once anything has been written a second one answers `database tape error: database is busy`, persistent across 15 retries. That is why a crew is local-only. It is not a statement about concurrency, which comes from `BEGIN CONCURRENT` rather than from the number of connections.

**Concurrency is a property of the transaction, not of the connection count.** This is the thing to get right, because reasoning about it by counting connections has produced a wrong answer here every time.

Under MVCC, `BEGIN CONCURRENT` lets multiple writers proceed in parallel instead of serializing behind one write lock, and Turso detects conflicts at the row rather than the page, so unrelated rows in the same page do not collide. One connection running concurrent transactions gives you concurrent writers. Opening a second connection is a different question with a different answer.

Retries do not disappear, they move. A conflict fires at commit time, on a row two transactions both wrote, rather than at transaction start on a locked database. So a writer still needs backoff with jitter, and two writers hammering one hot row still conflict by design.

A replicated database can run MVCC. Turso's sync engine syncs MVCC databases: after an initial page bootstrap it pulls the MVCC logical log directly. So "replicated" does not imply "serialised", and a measurement about how many sync connections a replica permits says nothing about how many writers it can run.

Sources, because none of this should be re-derived: [concurrent writes on Turso Cloud](https://turso.tech/blog/concurrent-writes-on-turso-cloud), [Turso 0.7.0](https://turso.tech/blog/turso-0.7.0), [concurrent writes in practice](https://turso.tech/blog/concurrent-writes-in-practice).

**Do not read that as "replication means serialised".** It does not. A replicated database can run MVCC, its writes go to the journal concurrently, and the one sync connection ships them to the primary afterwards. A drainer's connection is a local connection to the file, not a sync connection, and the engine permits many of those. Whether a crew works over a replicated file has not been measured. Until it has, the numbers below are unverified there rather than unreachable there.

This is built out of the public API, and that API is the whole vocabulary rather than a convenient subset of it. `deposit`, `collect`, `drain`, the retry policy and the query builder are what you may declare. N concurrent `drain()` coroutines over one room work as they are. If something you need is not here, that is a gap in the package and belongs in the tracker, not a licence to reach past it.

### The shape

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
ticket = deposit(room, insert("cards", {"id": 1, "title": "x"}, returning=NOTHING, on_conflict=NOTHING))   # returns at once
...                                                                # free to work
receipt = await collect(room, ticket)     # {"id": ..., "ok": True, "error": ""}
```

A builder `Query` is already a `PendingWrite` — `{sql, params}`, and pyturso accepts the named `:param` style the builder emits. No conversion layer.

### Rules that are not optional

**1. Never hold a partially-read cursor open across a write on the same connection.** Writes get their own connections; reads get theirs. Breaking this either discards the write silently or panics the process. See `docs/design/mvcc-connection-rule.md`.

**2. `BEGIN CONCURRENT` per write.** `journal_mode = 'mvcc'` alone changes nothing — without `BEGIN CONCURRENT` you get ordinary locking and `database is locked` under any concurrency.

**3. Fetch the PRAGMA cursor.** An unfetched PRAGMA does not take effect, silently, and you will measure WAL while believing you are on MVCC.

**4. One replica connection per replica.** MVCC on a replica is fine for sequential writes: 20 of 20 reached the primary, in `docs/design/mvcc-connection-rule.md`. What must never happen is a second concurrent replica connection to one replica — that is what strands writes.

### Sizing the crew

**This workload is CPU-bound.** Measured on Render, in `docs/design/concurrency-and-throughput-measured.md`: CPU pinned near the container limit while memory stayed flat.

| box | CPUs | knee | peak |
|---|---|---|---|
| Render Pro | not captured | 32 | 641 writes/sec |
| Render pro_ultra | 8 | 64 | 1,175 writes/sec |

Those are with a fresh connection per write. **With persistent per-worker connections the numbers are 11–14× higher and the knee moves left** — 13,826 writes/sec at a crew of **16** on pro_ultra.

**Size the crew to the CPUs you actually have.** A `starter` instance is **0.5 CPU** — a crew of 16 there will be worse than a crew of 1 or 2. None of the numbers above are reachable on starter.

Beyond the knee throughput *falls*. At 512 workers it is worse than at 16.

### Free-threaded specifics

**pyturso ships no wheel for free-threaded 3.14**, so `uv` compiles it from Rust source on every cold environment. That build is memory-hungry and has been killed on a container mid-compile. **Warm the cache in the foreground first**, then run:

```bash
uv run --no-project --python 3.14t --with pyturso python -c "import turso; print('warm')"
```

A second invocation should return instantly. If a detached process rebuilds it, its `HOME`/`UV_CACHE_DIR` differ from the shell that warmed it — pin them explicitly.

### What to measure, and what to check

Throughput is the easy half. The half that matters:

**Run a read-modify-write test and compare the counter to the success count.** `UPDATE t SET v = v + 1` across N concurrent writers, then assert `SUM(v)` equals the number of writes that reported success. Anything less is a silently discarded write. Every run here has lost nothing, on both one-and-done and persistent connections — but that was with writes only, and your workload is not ours.

**Report first-attempt success separately from eventual success.** It is the leading indicator: it falls while the eventual rate still reads perfect, so by the time anything fails you have been deep into the retry budget for a while.

---

## Replication

A Turso database opened with a primary holds **one** sync connection for its life. The engine permits one: a second answers `database tape error: database is busy`.

That connection object is doing two separate jobs, and confusing them is the mistake the banner at the top of this document exists to stop.

**As a channel**, it pushes the local log to the primary and pulls the primary's back. That happens on `flush`, `refresh` and at shutdown. This is the job that is limited to one.

**As a handle**, writes are executed on it, because it is the connection object this package holds. A write executed on it is a write to the local journal. It is not a sync operation, it does not travel to the primary at that moment, and it is not rationed by the fact that the channel is single. What bounds concurrent writes is the journal mode and row-level conflicts, exactly as it is for a local database.

So "one sync connection" is a fact about talking to the primary. It is not a write limit, and no sentence should derive one from it.

**Sync is opportunistic and never on the critical path.** No caller waits for the primary. A write returns when it is in the local journal, and the log reaches the primary later, on a pass that runs only when nothing else wants the connection. `replicate_if_idle` checks whether a writer is waiting and drops the pass if one is. A dropped pass is dropped, not queued: ten skipped under load produce one when the load clears, not ten, because releasing a backlog onto the connection at the moment it recovers is the worst possible time.

There are exactly two exceptions, and both are at the edges of the process rather than inside a request.

**A cold open blocks.** A database with no schema is unusable rather than merely stale, so the first open copies the whole thing before returning. A warm open does not: the schema is already on disk and only data can be behind, which is the eventual consistency you asked for by replicating.

**Shutdown blocks**, until the primary has everything, retrying instead of giving up. The usual deployment has disk that does not survive the process, so anything unreplicated at exit is gone. `shutdown` is a required argument at open for the same reason: there is no safe default for whether your data survives.
