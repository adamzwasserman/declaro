# Refactoring plan

**Status: proposed 2026-08-11. Not started. Needs approval before any phase begins.**

## What we are aiming at

The consumer picks one thing: **async or sync**.

- **Sync** does the write and hands back a success code.
- **Async** hands back a promise at once. The consumer collects it when it wants the answer, or never.

The consumer never receives a connection. Behind that line, whether a pool exists is ours. Where a pool exists it has **one owner and one writer**.

Binding constraint: [state-ownership-and-the-pool-boundary.md](state-ownership-and-the-pool-boundary.md)

## The bar

| | target | today | moves by |
|---|---|---|---|
| L1.18 mutable state | under 15% of functions | 44.7% | removing state, not testing |
| L1.19 decision coverage | over 90% | 60.3% | tests |
| L1.20 determinism | 5/5 | 5/5 | holding it |

## The principles this plan executes

This refactor is not a preference. It discharges named rules from the Honest Code skill and the Honest Test principle, several of which describe the current code almost word for word.

### Honest Code — the rules the connection layer breaks today

| Rule | What it says | What we have | Phase |
|---|---|---|---|
| **12. Context Managers Over Instance State** | Violation: *"Persistent connection/resource stored on `self`. Manual open/close lifecycle."* Fix: `async with create_connection(config) as conn`. | `self._write_holder`, `self._read_holders`, `self._free_readers`, `self._free_writers`, `self._stale_holders` — persistent resources on `self`, opened in `_initialize`, closed in `close`. **This rule names our exact defect.** | 3 |
| **11. Configuration as Parameters** | Violation: *"`self._config` set in `__init__`."* Fix: pass config as an argument to each function that needs it. | `self._mvcc_requested`, `self._push_interval_s`, `self._busy_retry_budget_s`, `self._max_size`, `self._acquire_timeout`, `self._pooled_writes` — all set in `__init__` and read later to make decisions. | 3 |
| **14. No Implicit Defaults** | Violation: *"`def f(x, timeout=30)`"* — a default *"silently absorbs the caller's omission"* and *"manufactures an unexercised input region by construction."* | `mvcc: bool = True`, `max_size: int = 5`, `push_interval_s: float = 1.0`, `busy_retry_budget_s: float = 5.0`, `pooled_writes: bool = False`. Every one is an untested input region, and `mvcc=True` is the one that cost us a day. | 0, 1 |
| **8. Typed Exceptions at the Boundary** | Violation: *"Retry logic inline in functions."* Fix: *"Retry logic belongs in infrastructure."* | `_retry_while_busy` is retry logic inline in the pool. `retry.py` was the right instinct; it has not reached the connection layer. | 3 |
| **3. Pure Functions Over Methods** | Violation: methods reading `self` only for data they could receive. | Most of `TursoPool`'s 37 methods. `replication.py` moved 13 of them to functions taking the pool — the direction is right, the parameter is still an object with unowned state. | 3 |
| **2. TypedDicts Over Classes** | Classes acceptable **only** when wrapping a stateful external resource. | A connection wrapper qualifies. A class owning a *fleet* of connections plus configuration plus a push loop plus a dialect decision does not — the exemption covers the resource, not the decisions piled on it. | 3, 4 |
| **1. Dict-Lookup Polymorphism** | Violation: if/elif chains selecting behaviour. | `if self._pooled_writes`, `if is_write_op(op) and hasattr(pool, "acquire_write")`, `_DIALECT_MAP` fallback to `"postgresql"`. | 0, 2 |
| **10. Pure Function Assertions Over Mocks** | *"If you need mocks, the function has hidden dependencies."* | Our connection tests need elaborate fakes, and the tests monkeypatch a module global (`_TursoConnectionHolder`) to install them. That is the tell: **there is no injected seam, so the driver must be swapped out from underneath.** Fakes are the symptom, not the problem. | 3 |

The 2026-02 audit granted pool state an exemption — *"connection pools are inherently stateful resources"*. Rule 12 does not grant it. That document is now marked poisonous.

### The target architecture is already specified — and it names our bug

`honest-persist-architecture.md` §8.1 does not merely permit an honest pool; it describes how to build one, and it describes the failure we spent the day inside:

> "there is nothing hidden to corrupt. This is the bug category the design eliminates, and it is the worst one a pool can have: two requests stepping on a shared cache, **a connection handed out twice, behaviour that depends on invisible history.** When the cache is a value you hold and pass forward, there is no shared, hidden thing for one code path to mutate behind another's back. The state is yours, named, on every line."

The shape it prescribes:

> "**a pure decision in the middle, the state threaded through as a value, and the I/O pushed to one injected seam at the edge.** The pool is just the sharpest example, because it is the component everyone expects to be impossible to do honestly."

And the seam:

> "the one impure act, and the framework never performs it: the adopter supplies a `connect` function for their driver. **The framework imports no database driver at all.**"

That last line is the sharpest test of how far we are. `turso_pool.py` imports its driver, constructs `_TursoConnectionHolder` itself, and holds the result on `self`. Every one of those is the opposite of the spec.

**This replaces "give the state one owner" as Phase 3's target.** One owner is an improvement; state-as-a-value with an injected `connect` is the specified design, and it is strictly stronger — with no hidden state there is no owner to argue about.

### Honest Test — no fakes, ever. Use a real database.

This follows from the architecture rather than being bolted on. `honest-test-architecture.md` §6:

> "honest-test verifies honest-persist's query path against a **real** database, never a mock. … honest-test hands it a real in-memory driver (SQLite `:memory:`) recreated **ephemeral** per test, applies the declared schema, and drives the genuine query builders and `execute` against it. **There are no mock connections and no fakes to drift from how the real database behaves.**"

And §8.1 again: *"There are no mock connections to write and no fakes to drift from how the real thing behaves."*

**Every test in this plan uses a real database.** Never a fake connection, never a recording double, never a monkeypatched module global.

**Turso may be used in place of SQLite** (Adam, 2026-08-11). The spec's example is an in-memory SQLite because it is the cheapest real driver; a real local Turso is equally real and is the engine we actually ship against. That resolves what would otherwise be a gap: the behaviours that matter most here — MVCC's silent rollback, write-write conflict, push convergence — do not exist in SQLite at all, so a SQLite-only rule would have forced a fake for exactly the cases that have bitten us. Use a real local Turso for the fast path, a real Turso replica where sync behaviour is under test, and a real PostgreSQL where the behaviour is PostgreSQL's.

**This condemns most of the connection-layer tests written 2026-08-11.** `_Holder`, `_RecordingConn`, `_FakePool`, `_SlowPushHolder`, `_BusyConn`, `_SharedHolder` are all fakes, installed by monkeypatching `_TursoConnectionHolder` in eleven test files. They pass. They also encode the current shape and cannot catch a drift between the fake and the engine — which is precisely how a documented MVCC silent-rollback went unnoticed by a green suite. They are rewritten against a real database as their code is reached, not left in place.

### Honest Test — what "red first" means here

> "If you can enumerate every valid input, you can run every valid input."

That is the method, not an aspiration, and it is exactly what L1.19 measures — the share of **finitely enumerable** decision points exercised by a test. So Phase 6 is not "write more tests"; it is Honest Test applied.

Every test this plan calls for follows the same discipline:

- **Bounded input space → exhaust it.** Not a sample, not a representative case. Every member. A dialect vocabulary of three runs three times. A retry policy of two named values runs both.
- **Unbounded input space → systematic boundaries, never random.** Read the predicate and generate its edges. No `Hypothesis` fallback where the space is actually finite — that trades a total guarantee for a probabilistic one.
- **Say which is which.** Every test states whether it is Set-based (exhaustive) or predicate-based (boundary-tested). A report that hides the difference is dishonest about its own coverage.
- **Verify the structural properties, not only the behaviour.** Purity (same input, same output — call twice, compare), mutation (input unchanged after the call), idempotency (same result on the second call). These are black-box and domain-independent. *"A function that fails purity is not just buggy, it is architecturally dishonest."*

This bites hardest in Phase 3. Once a field has one owner, the set of states it can hold becomes **enumerable**, and the test can run all of them. While four things write it, that set is unbounded and only samples are possible. **Single ownership is what makes exhaustive testing available at all** — which is why L1.18 must move before L1.19, and why the order is not a preference.

## Every phase is red first

**No change in any phase begins without a failing test that describes the behaviour being asked for.** Write the test, watch it fail for the right reason, then make it pass.

An earlier draft of this plan had no test discipline in phases 0 through 5 and put all testing in phase 6. That was wrong, and it confused two separate things:

- **Bulk coverage tests against the current shape** — correctly deferred. That shape is about to be deleted, and tests written against it get thrown away. This is what phase 6 is.
- **A red test per change** — nothing to do with the old shape. It describes the new behaviour, so it survives the refactor and is the only thing that proves behaviour did not drift while the code moved.

Phase 3 is where this matters most. Moving ownership of mutable state without a failing test per move is how you discover afterwards that something changed.

**Deleting is red first too.** Phase 0 removes `pooled_writes`; the test to write first is the one asserting the parameter no longer exists and that a write still lands.

**A refactor with no behaviour change still starts red** — with a characterisation test that pins the current behaviour, written and passing before the move, and required to still pass after it. That is the one case where the test does not begin failing, and it must be called out as such rather than skipped.

## Order, and why

**State first, bulk tests second.** Testing the current shape harder makes it more expensive to change. Every coverage test written against a pool the consumer holds is a test that has to be rewritten. L1.19 is the last phase for that reason, not the first — but that is about volume, not about discipline.

---

## Phase 0 — remove what was added today that the constraint forbids

Small, and it stops the leak widening.

- Delete `pooled_writes` and the branch it feeds (`_write_connection`, and the `if self._pooled_writes` path).
- Delete the "why the pooled write path is not the default" argument from `pool.py`'s docstring. It is already marked poisonous; the record lives in git.
- Decide one write strategy for the WAL-Turso target and stop having two.

**Keep** from today: the module split, `retry.py`, the CI version matrix, and the 323 new tests. None of those are consumer-surface.

**Red first (Honest Test):** the accepted-parameter set is bounded, so **enumerate it** — assert exactly which parameters the entry point takes, and that `pooled_writes` is not among them. Set-based, exhaustive. Plus a test that a write still lands with it gone.

**Discharges** Rule 14 (no implicit defaults) for `pooled_writes`, and Rule 1 for the `if self._pooled_writes` branch.

**Done when:** no consumer-facing parameter names a pool decision.

## Phase 1 — build the consumer boundary

Two entry points, nothing else.

- Async write returns a promise. This is `deposit` — it already exists in `write_queue.py` and already returns a ticket immediately.
- Sync write returns a success code. This is the receipt `collect` already produces, handed back directly.
- Neither returns a connection.

Additive. The old surface keeps working through this phase so nothing breaks while it is built.

**Open:** what a consumer does when several writes must land together. The ticket design answers it for async — deposit several, collect them together. **It is not worked out for sync, and I will not invent it.** Needs a decision before this phase is written.

**Red first (Honest Test):** the consumer's choice is a bounded vocabulary of exactly two — async and sync — so **run both members**, not one plus an assumption. Async must return a promise: not a connection, and not a completed write. Sync must return a success code. Both fail before the entry points exist.

**Discharges** Rule 14: async-or-sync is an explicit bounded choice, never an implicit default buried in a keyword argument.

**Done when:** a consumer can do every read and write in the test suite without naming a pool or holding a connection.

## Phase 2 — make the common syntax target-shaped

The facade currently asks what it was handed.

- `query/executor.py:420` — `if is_write_op(op) and hasattr(pool, "acquire_write")` must go. The facade must never probe an implementation for capabilities.
- The parameter named `pool` becomes a target. 59 pool-shaped parameters across `query/`.
- Both members required. No `hasattr`, no `getattr(pool, "dialect", None)` fallback.

**Red first (Honest Test):** hand the executor a target with `acquire` and `acquire_write` that is not a pool and answers **no** capability probe. The current executor fails it. That test is the phase.

The dialect set is bounded — enumerate every dialect rather than relying on `detect_dialect`'s `return "postgresql"  # Default`, which is Rule 14's silent fallback and an unexercised region by construction.

**Discharges** Rule 1 (the `hasattr` branch) and Rule 14 (the dialect default).

**Done when:** nothing under `query/` branches on what kind of thing it was given.

## Phase 3 — one owner, one writer

This is the L1.18 work and the largest phase.

Today four things write the holder state: the acquire path, the push loop, the migration refresh, and close. L1.18b named the fields — `_read_holders`, `_free_readers`, `_write_holders`, `_free_writers`, `_stale_holders`, `_write_holder` — as `unresolved, drives a decision`.

Target, from `honest-persist-architecture.md` §8.1 — not invented here:

- **A pure decision in the middle.** Which connection to use is a function of a value, not a read of `self`.
- **State threaded through as a value.** The pool state is passed forward and returned, not stored on an object. *"The state is yours, named, on every line."*
- **I/O at one injected seam.** The adopter supplies `connect`. **`turso_pool.py` must stop importing its driver and stop constructing `_TursoConnectionHolder`.**
- The push loop, refresh and close take the state as a parameter and return the new state. They stop being writers of fields.
- Re-measure L1.18 after each move rather than at the end.

**The seam is what makes the no-fakes rule affordable.** With `connect` injected, a test hands in a real in-memory SQLite and drives the genuine path. Today a test must monkeypatch a module global, which is the same thing said in a way that cannot be verified.

**Red first, per move, not per phase.** Before each field changes owner: a characterisation test pinning that field's behaviour today, passing before the move and required to pass after. Then a test asserting the new owner is its **only** writer — which fails until it is.

**Honest Test, and this is the phase's whole point:** while four things write a field, its state space is unbounded and only sampling is possible. With one owner the space becomes **enumerable**, so the test runs every state rather than a chosen few. Single ownership is what makes exhaustive testing available at all.

Add the structural checks once the owner exists: purity, no mutation of inputs, idempotency. A function failing purity is architecturally dishonest, not merely buggy.

**Watch the fakes.** Rule 10 says mocks signal hidden dependencies. Our connection-layer fakes are elaborate because the state has no owner. **When they get simpler, the refactor is working** — track it.

**Discharges** Rules 12, 11, 8, 3 and 2.

**Done when:** L1.18b reports no unresolved fields in the connection layer, and L1.18 is under 15%.

## Phase 4 — separate the targets

- Postgres, SQLite and WAL-Turso replicas: pooled.
- MVCC Turso: not pooled.
- Different modules. Neither imports the other. The selection happens once, below the consumer's async/sync choice.

**Both blockers are resolved. This phase is no longer blocked.**

1. **MVCC Turso is local only and never syncs.** Sync is the WAL replica's job, and that target is pooled. Every failure measured on 2026-08-11 came from MVCC on a *synced* replica: `__turso_internal_mvcc_meta` exists locally and not on the primary, giving 8 ok / 6 durable / 3 on primary here, and push-fails-forever plus a hang on multicardz's box. MVCC and the sync engine do not go together, so nothing in the MVCC target needs to own a push.

2. **One-and-done does not contend. That result was my measurement error.** Measured 2026-08-11, local Turso, 20 concurrent writers each opening their own connection:

   Mode-verified: every connection reads back its own `journal_mode` and the value is recorded, so the run can name the mode it ran in.

   | arm | modes seen | durable | errors |
   |---|---|---|---|
   | **mvcc + `BEGIN CONCURRENT`** | `mvcc` ×20 | **20/20** | none |
   | wal + `BEGIN CONCURRENT` | `wal` ×20 | **0/20** | 20 × refused |
   | mvcc, no `BEGIN CONCURRENT` | `mvcc` ×20 | 1/20 | 19 × `database is locked` |
   | wal, no `BEGIN CONCURRENT` | `wal` ×20 | 2/20 | 18 × `database is locked` |
   | default + `BEGIN CONCURRENT` | `wal` ×20 | 0/20 | refused (default is wal) |

   An earlier draft cited 1-of-8 and 2-of-8 as a blocker. **Those probes omitted `BEGIN CONCURRENT`, so they measured ordinary locking, not MVCC.** Disregard them.

   **WAL and MVCC are opposites, and `BEGIN CONCURRENT` is MVCC-only.** It does not degrade under WAL, it is refused outright:

       DatabaseError: Transaction error: Concurrent transaction mode is only supported when MVCC is enabled

   The Turso blog describes `BEGIN CONCURRENT` as a SQLite WAL-mode feature and presents it as an alternative to MVCC. **That is SQLite's experimental branch, not pyturso.** In pyturso the two are one mechanism with two required steps. A consequence worth having: because `BEGIN CONCURRENT` cannot run outside MVCC, we are always on Turso's row-level commit-time conflict detection and never on SQLite's page-level detection, so false conflicts from physical row colocation do not apply.

   **Not measured, and not to be claimed:** no conflict occurred in any arm, so the retry policy is unexercised by measurement. 20 writers, one process, one run per arm — not a load test.

**The write sequence for the MVCC target, exactly:**

    connect(path)                    # local turso, no sync engine
    PRAGMA journal_mode = 'mvcc'     # the cursor MUST be fetched or it is a no-op
    BEGIN CONCURRENT                 # mandatory; without it, ordinary locking
    <the deposited sql, params>
    COMMIT                           # conflicts surface here
    close

Retry the whole unit on `Error::Busy`, `Error::BusySnapshot`, or any message containing "conflict". Never retry a constraint violation.

**One-and-done is required here, not merely permitted.** COMPAT.md's silent-rollback gap is about *sibling statements on a connection*. One write per connection means no sibling can exist, so the documented data-loss mode cannot arise by construction.

**Red first:** the sequence above, as a test against a real local Turso — 20 concurrent one-and-done writers, all durable. Then the negative: the same test without `BEGIN CONCURRENT` must fail, so the suite pins *why* the statement is there.

## Phase 5 — close the surface

- `ConnectionPool` stops being exported from `declaro_persistum`.
- Examples, README and `usage.md` rewritten to the consumer boundary. All are currently marked poisonous.

## Phase 6 — L1.19 to over 90%

Last. Roughly four times the volume of tests written on 2026-08-11, against a shape that has stopped moving.

Largest untested blocks today: `turso_pool.py`, `cli/commands.py`'s async commands, `loader.py`, `pydantic_loader.py`, `query/prisma_style.py`.

---

## Not in scope, and staying

- `abstractions/` is the emulation library and is the right shape already.
- `applier/` and `inspector/` are per-dialect behind protocols and are the model the connection layer should follow.
- MVCC on a synced database stays off. Turso documents the gap: all statements on a connection share one MVCC transaction, and a write that reported success "is silently rolled back if the transaction then ends abnormally." MVCC is beta.

## Mistakes from 2026-08-11 not to repeat

Each of these was made with confidence, and each was wrong.

- **Moving the branch to a factory.** The factory is inside the pool's boundary. Still the pool's conditional.
- **Calling one-and-done writes stateless** while writer zero, the reader set and the stale set stayed open. The shape is still incoherent — one-and-done writes inside an object whose remaining purpose is holding connections open — but the "2 of 8" figure once cited here was invalid, because that probe omitted `BEGIN CONCURRENT`.
- **Measuring MVCC without `BEGIN CONCURRENT`.** Three separate times, across a whole day, I concluded from `database is locked` that one-and-done was contended, that MVCC might be unusable in this embedding, and that Phase 4 was blocked. `journal_mode = 'mvcc'` only makes MVCC available; `BEGIN CONCURRENT` is what defers locking to commit. **If a measurement says a documented feature does not work, suspect the measurement before the vendor.**
- **Running a measurement that cannot name its own mode.** The first 20/20 set the pragma and never asserted the value, and there is a documented path — an unfetched pragma — by which it could silently have been WAL. It was caught by a peer, not by me. **A run that cannot state the configuration it ran in cannot support a claim about that configuration.** Every arm now records `journal_mode` per connection.
- **Citing the blog for an embedded-library fact.** `PRAGMA journal_mode = 'mvcc'` is in the docs (docs.turso.tech/tursodb/concurrent-writes); the blog only gives `--experimental-mvcc`, which is the tursodb *server* CLI. Different entry points, different documents. Keep claims attached to the source that actually covers our configuration.
- **Designing from an unchecked mechanism.** The cloud-sync redesign was built on "the push ships WAL frames." Turso Sync is logical change-data-capture and has been all along. Check the vendor's documentation before the design, not after being challenged.
- **Writing a module header by hand** and dropping `from __future__ import annotations`, which shipped a wheel that would not import below Python 3.14.
- **Reading L1.18b `unresolved` as coverage debt.** It means the state has no determinable owner.
