# State ownership: the pool is never a surface

**Status: binding constraint. Written 2026-08-11.**

This document exists because the same error was made four times in one day, each time by a different route, and each route looked reasonable from inside itself. It is written as a constraint rather than a recommendation.

## The constraint

**The consumer chooses one thing: async (the default) or sync. Nothing else.**

Whether a pool exists behind that choice is internal. Whether a write reuses a connection or opens its own is internal. Whether the engine runs MVCC or WAL is internal. None of it reaches the consumer, and none of it reaches the common syntax either.

Behind the choice, a pool may become necessary. When it does:

- its mutable state is **completely encapsulated**
- it has **exactly one owner and one writer**
- nothing outside that owner reads it to make a decision

## Why: promiscuous mutable state is the most dishonest thing code can do

State with more than one writer cannot be reasoned about from any single place. Every reader must model every writer. The bounds of the state are not determinable, so neither is the behaviour of anything that branches on it.

That is not a style preference. It is what makes a system unverifiable — the Slop Audit measures it directly, and it is what L1.18 is for.

## The instrument already said this, and it was misread

`declaro-tr3`, measured 2026-08-11. The L1.18b state-bounds classifier returned:

```
241 neutral / 0 promiscuous / 76 unresolved
```

The unresolved list was almost entirely `pool.py`:

```
self._read_holders    — unresolved, drives a decision
self._free_readers    — unresolved, drives a decision
self._write_holders   — unresolved, drives a decision
self._free_writers    — unresolved, drives a decision
self._stale_holders   — unresolved, drives a decision
self._write_holder    — unresolved, drives a decision
```

**Unresolved is not "not yet examined".** It is the instrument reporting that it cannot establish the bounds of that state — who writes it, when, and what values it can hold. Not promiscuous, because nothing proved it promiscuous; not neutral, because nothing proved it contained.

It was filed as testability debt under a decision-space-coverage heading and left. It was a statement about ownership, and it named the exact fields.

## What the four owners were

Writer zero, the free lists, the stale set and `_mvcc` were reachable and writable from:

1. the acquire path
2. the push loop
3. the migration refresh path
4. the close path

`pooled_writes` then added a fifth reader of that state and called it an option.

**This is why every conditional ended up inside the pool.** With no single owner, there was nowhere else for a branch to live. Moving it to a factory did not help — the factory is still the pool's own boundary. The branch had no outside because the state had no owner.

## What follows from the constraint

- The facade takes a **target**, never a "pool". `acquire()` and `acquire_write()` are both required members; no `hasattr` check, no capability probe, no dialect name in the call.
- `query/executor.py` must not branch on what it was handed. `if is_write_op(op) and hasattr(pool, "acquire_write")` is the leak in its clearest form: the facade asking an implementation what kind of thing it is.
- Pooled and non-pooled targets are **different modules**, selected once, downstream of the consumer's async/sync choice. Neither knows the other exists.
- `ConnectionPool` is not the consumer's entry point and is not exported to them.

## Beliefs to not re-derive

- **"The branch can live in the factory."** No. The factory is inside the pool's boundary. A conditional there is still a conditional the pool owns.
- **"`pooled_writes=` is a reasonable escape hatch."** It is a pool decision on a consumer-facing surface, which is the thing this document forbids. It was added 2026-08-11 and is the widest the leak has ever been.
- **"Stateless writes remove the state."** They do not. Writes stopped reusing connections while writer zero, the reader set and the stale set stayed exactly where they were — one-and-done writes inside an object whose remaining purpose is holding connections open. Measured that day: a one-and-done write alongside a long-lived connection landed 2 of 8.
- **"L1.18b unresolved means low coverage."** It means the state has no determinable owner.

## Related

- `declaro-tr3` — the L1.18 measurement and the unresolved list
- `docs/design/cloud-sync-redesign.md` — RETRACTED, and a worked example of designing from an unchecked mechanism
