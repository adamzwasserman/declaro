# The map

**2026-08-12. A complete inventory of declaro-persistum against the Honest four-column model.** Machine-checkable core in [`persistum.hd`](persistum.hd); this is the whole picture, including the parts `.hd` cannot express.

## Correction, 2026-08-13. The measurements below are one day out of date and badly so.

Everything from "The headline" down was measured on 2026-08-12, before the de-classing. Re-measured today against the same tree, the central numbers have moved far enough that planning from them would point you at work that is already done.

| the map said | measured today |
|---|---|
| 97 classes, 52 of them stateful objects | 66 classes, 64 of them TypedDict / Protocol / Exception |
| `TursoPool` at 28 fields and 35 methods | gone; `Database` is a TypedDict and `pool` is an abolished word |
| Group A: 8 query-builder classes are what runs | 7 of the 8 are deleted; `CaseExpression` is now a TypedDict |
| Group A: the pure builder functions are "called by nothing" | `query/executor.py` imports `Query` from them, and 58 tests exercise all eight |

Only two classes in `src` are not data: `_Inspectors` and `_Appliers`, each a `dict` subclass with one method, holding a dispatch registry.

**Group A is no longer an open decision.** The map calls it "the one still open" and says "nothing else in the query layer moves until it is made". It was made, by deleting the classes. What that leaves is not a decision but a consequence: `tests/unit/test_query_expressions.py` holds 30 tests written against `t.status`, `t.select` and `t.alias`, and `table()` now returns `{"columns": {...}}`. Those 30 cannot be restored and are the last thing in the query layer still waiting.

**Group B is done too.** "Pools and connections, legitimately stateful, wrongly sized" describes objects that no longer exist. `TursoPool`'s 28 fields are a `Database` TypedDict; the replication loop, the retry policy and the serialiser are functions over it, which is exactly what the map proposed.

**The crew is wired.** The map says "`drain` is declared an orchestrator and nothing calls it". `crew.drainer` calls it, and `writers.py` supplies the per-engine write. The order the map sets out — Group B, then wire the crew, then Group A — has been walked in that order.

This note is a correction to the numbers, not to the diagnosis. The diagnosis was right, which is why the work followed it.

## The headline

persistum is **two complete architectures occupying the same package**. One is functional and honest. The other is object-oriented, wraps the first, and is what actually runs.

```
 264 public module-level functions          97 classes
   247 pure                    (94%)          45 TypedDict / Protocol / Exception  (data — legitimate)
     8 input boundary                         52 STATEFUL OBJECTS
     9 output boundary
```

The four-column model maps the left side cleanly. It cannot express the right side at all, because the framework's position is that the right side should not exist.

## Column map — the functional architecture

| column | count | what is there |
|---|---:|---|
| 1 · input boundary | 8 | schema loading from Python modules and files, database introspection, stored-hash reads |
| 2 · orchestrators | 4 | `apply_migrations_async`, `begin_cutover`, `bulk_transfer`, `drain` |
| 3 · pure functions | 247 | diffing, topological sort, ambiguity detection, rename confidence, schema validation, FK ordering, SQL generation and rendering, the write queue, the retry policy, literal-type extraction |
| 4 · output boundary | 9 | DDL application, connection acquisition, push, pull, flush, close |

**94% of the module-level function surface is pure.** The diff engine, the planner and the SQL layer are data in, data out. This part of the package is not the problem and should not be touched.

## The 52 stateful objects — what has no column

```
 28 fields  35 methods   turso_pool.TursoPool
 14 fields  15 methods   query.select.SelectQuery
 11 fields  18 methods   query.django_style.QuerySet
 10 fields  12 methods   cloud_manager.TursoCloudManager
 10 fields   9 methods   query.insert.InsertQuery
 10 fields   9 methods   query.update.UpdateQuery
  8 fields   9 methods   query.delete.DeleteQuery
  7 fields  25 methods   query.table.TableProxy
  6 fields   8 methods   mirror.MirrorPool
  6 fields   8 methods   pool.PostgreSQLPool
  6 fields   7 methods   pool.SQLitePool
  6 fields  17 methods   query.prisma_style.PrismaQueryBuilder
  5 fields  14 methods   mirror.MirrorConnection
  4 fields  11 methods   turso_driver._TursoConnectionHolder
```

They split into two groups with **different diagnoses and different fixes**.

### Group A — query builders. A skin over functions that already exist.

`SelectQuery`, `InsertQuery`, `UpdateQuery`, `DeleteQuery`, `QuerySet`, `PrismaQueryBuilder`, `TableProxy`, `ColumnProxy`, `Condition`, `ConditionGroup`, `CaseExpression`, `SQLFunction`.

`query/builder.py` already contains **pure `select`, `insert`, `update`, `delete`, `raw`, `with_limit`, `with_offset`, `with_params`**. They are exported from `query/__init__.py`. They are called by nothing but their own docstrings.

So the pure query layer was written, exported, and abandoned. The class layer is what runs.

**This is the fourth instance of one pattern.** `declaro_persistum.functions` (deleted today), `query/sqlalchemy.py` (deleted today), `compat/sqlalchemy_shim.py` (deleted today), and now `query/builder.py`. Each time: a correct implementation exists, nothing calls it, an object-shaped rival is what executes. The reachability ratchet added today catches the *unreferenced module* case. It does not catch this one, because `builder.py` is reachable — `query/__init__.py` imports it. It is reachable and unused, which is a weaker signal and needs a different check.

### Group B — pools and connections. Legitimately stateful, wrongly sized.

`TursoPool`, `SQLitePool`, `PostgreSQLPool`, `MirrorPool`, `MirrorConnection`, `_TursoConnectionHolder`, `TursoCloudManager`.

Rule 2 permits a class that wraps a stateful external resource. A connection is exactly that. **The permission covers holding a connection; it does not cover 28 fields and 35 methods.**

`TursoPool` currently owns: two database paths, a remote URL and token, a write connection, a read free list, a replica lock, a push task, a push interval, retry budgets, three initial-replication state fields, stale-holder tracking, instrumentation config, and the MVCC decision. It is the connection holder, the replication engine, the retry policy, the serialiser and the engine-mode decision in one object.

Every defect this week came from it:

- `declaro-p39` — writes stranded, cause misattributed twice
- `declaro-dna` — a connection opened per write, killing a consumer's box
- the `PoolClosedError` `NameError` — a split that left a name behind
- the `_holder.sync` dangling call — a rename the suite could not see

## What the map says to do

**Do not touch column 3.** 247 pure functions are the asset.

**Group A is an API decision, and it is the one still open.** The pure functions exist. Retiring the builder classes means `(a == 1) & (b == 2)` becomes `and_(...)` and `count_("*").as_("n")` becomes `as_(count_("*"), "n")`, at every call site including multicardz's. That decision has been open all day and nothing else in the query layer moves until it is made.

**Group B is not an API decision.** The consumer surface is `ConnectionPool.turso(...)`, `acquire`, `acquire_write`, `transaction`, `flush`, `close`. None of that changes if `TursoPool` becomes a small connection holder plus functions that take it as a parameter. The 28 fields become an explicit config TypedDict resolved at the boundary; the replication loop, the retry policy and the serialiser become functions over that state rather than methods on it.

**The crew is column 2 and is not wired.** `drain` is declared an orchestrator and nothing calls it. The write chain in `persistum.hd` — `deposit → drain → acquire_write → push_once` — is the design. The code runs `acquire_write` alone. On a local database that leaves the reuse lever unclaimed: measured 426 writes/sec against 4,721 for the same engine with held connections.

## The order

1. **Group B first.** It needs no API decision, it removes 28 of the 351 L1.18 offenders in the worst single object, and it is where every recent defect originated.
2. **Wire the crew** on the local/MVCC path. Column 2 gains its real orchestrator; the write path stops being a bare boundary call.
3. **Group A** when the operator question is answered.

## What produced this

Four rounds of writing a correct functional implementation, then reaching for an object when the API had to be pleasant, and leaving both. Nobody deleted the loser. The audit numbers say the same thing from the other side: L1.18 at 39.8% is not 351 scattered mistakes, it is one decision made four times.
