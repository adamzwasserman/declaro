# declaro_persistum Usage Guide


> ## The pool is gone, and this document no longer hands you one
>
> **Updated 2026-08-14.** The banner here said every example below handed a `ConnectionPool` to the consumer and that the document was retained as a record rather than guidance. Both were true when it was written and neither is true now: `ConnectionPool` was erased from code, tests and examples in `bbea6c3`, and no sample in this document names it any more. The sections that did — opening a database, the query styles, the write queue, multi-tenancy, views, materialized views, appliers, inspectors — were rewritten against `open_sqlite` / `open_postgresql` / `open_turso`, and each replacement was executed before being written down. That is not a claim about every line of a 1,900-line document: what is asserted is that no code sample here calls a name the package lacks, which `tests/unit/test_documented_code_can_run.py` checks on every run.
>
> The constraint that banner recorded still holds and is why the shape changed. The pool decision must never reach the consumer. Whether a connection is reused, and whether the engine runs MVCC or WAL, are internal and owned by exactly one writer. A pool exposed as a surface is mutable state with no determinable owner — measured on this codebase 2026-08-11, where L1.18b reported the pool's holder fields as `unresolved, drives a decision` — and it is why every conditional about MVCC kept ending up inside the pool: with no single owner, a branch had no outside to live in.
>
> That banner cited `docs/design/state-ownership-and-the-pool-boundary.md` as the binding constraint. The file was deleted in `07fc023`, along with ten other superseded planning documents, once the pool was gone from the code. The link had been dead ever since; the constraint is restated above so it does not depend on a file that no longer exists.

A Functional Persistence Layer (FPL) for Python. Taking the O out of ORM.

Three architectural priorities shape everything in this guide, in this order. They are stated in full in the [README](../README.md) and the [architecture document](architecture/declaro_persistum_architecture.md); in brief:

1. **Declarative.** You declare the state you want and the library works out how to reach it. No migration files, no revision chains — you edit your models and the diff engine derives the operations.
2. **One surface over every supported database.** The same API spans PostgreSQL, SQLite and Turso, so moving between them is a configuration change rather than a refactor. The three `open_*` functions return the same `Database` value, and the compatibility layer supplies what a given engine lacks; neither is a feature in its own right.
3. **Migrations that survive a team.** No linear revision chain means no migration merge conflicts. Branches merge in the models file, and the difference is computed against the real database at apply time.

Much of what follows reads as a list of features. Each one is in service of one of those three — it is worth knowing which, because that is what tells you how a given piece is meant to be used.

## Installation

```bash
pip install declaro_persistum

# With database drivers
pip install declaro_persistum asyncpg        # PostgreSQL
pip install declaro_persistum aiosqlite      # SQLite
pip install declaro_persistum pyturso        # Turso (embedded)
# Turso Cloud needs no extra package — it is pyturso with a remote_url
```

## Core Concepts

### Schema as Data

Instead of migration files, you declare your desired schema as Pydantic models with the `@table` decorator. The library compares your declared schema against the actual database and generates the necessary DDL.

```
models/
├── users.py        # Pydantic models with @table decorator
├── orders.py
└── snapshot.toml   # Auto-generated, tracks applied state
```

### State Diffing

```
Target Schema (Pydantic) ──┐
                           ├──> Diff ──> Operations ──> Apply
Actual Schema (DB) ────────┘
```

No linear migration chain. Works naturally with git branches.

## Defining Schemas

Create Pydantic models with the `@table` decorator in your models directory.

### Basic Table

```python
# models/users.py
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: UUID = field(primary_key=True)
    email: str = field(unique=True)
    name: str | None = None
    created_at: datetime = field(default="now()")
```

### Foreign Keys

```python
# models/orders.py
from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary_key=True)
    user_id: UUID = field(references="users.id", on_delete="cascade")
    total: Decimal
    status: str = field(default="'pending'")
```

### Composite Primary Keys

```python
# models/order_items.py
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field

@table("order_items")
class OrderItem(BaseModel):
    order_id: UUID = field(references="orders.id")
    product_id: UUID = field(references="products.id")
    quantity: int

    class Meta:
        primary_key = ["order_id", "product_id"]
```

### Indexes

```python
# models/users.py
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: UUID = field(primary_key=True)
    email: str
    status: str | None = None
    deleted_at: datetime | None = None

    class Meta:
        indexes = [
            {"name": "idx_users_email", "columns": ["email"], "unique": True},
            {"name": "idx_users_status", "columns": ["status"]},
            {"name": "idx_users_active", "columns": ["status"], "where": "deleted_at IS NULL"},
        ]
```

### Views

**There is no `@view` decorator.** This section described one and nothing reads it: the loader has no view handling at all, so a decorated class would be ignored. A view is declared as data, matching the `View` type:

```python
from declaro_persistum.types import View

active_users: View = {
    "name": "active_users",
    "query": "SELECT id, email, name FROM users WHERE status = 'active'",
    "materialized": False,
}
```

Views work end to end from a `View` value as of 2026-08-14, and did not before it. Every piece existed: `introspect(conn, dialect, include_views=True)` returned views, `diff_views` emitted `create_view` and `drop_view`, and the appliers executed both. Nothing connected them, so a view declared in a models file never reached the database. The loader now reads `View` values and `apply_migrations_async` diffs them. Only the decorator is missing, and it needs no further loader support to exist.

#### Materialized Views

Materialized views are supported on all databases:

**PostgreSQL** - Uses native `CREATE MATERIALIZED VIEW`:

```python
from declaro_persistum.types import View

user_stats: View = {
    "name": "user_stats",
    "query": "SELECT COUNT(*) as total, status FROM users GROUP BY status",
    "materialized": True,
    "refresh": "on_demand",  # or "on_commit" (not yet implemented)
}
```

**SQLite / Turso / LibSQL** - Uses table-based emulation:

```python
from declaro_persistum.types import View

monthly_stats: View = {
    "name": "monthly_stats",
    "query": "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
    "materialized": True,
    "refresh": "manual",  # or "trigger" or "hybrid"
    "depends_on": ["orders"],
    "trigger_sources": ["orders"],  # Only for trigger/hybrid refresh
}
```

The emulation creates a regular table plus metadata in `_dp_materialized_views`.

#### View Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | View name (from TOML key) |
| `query` | Yes | SELECT query for the view |
| `materialized` | No | Create as materialized view |
| `refresh` | No | Refresh strategy (see below) |
| `depends_on` | No | List of table/view names this view references |
| `trigger_sources` | No | Tables to watch for auto-refresh (SQLite/Turso only) |

**Refresh Strategies:**

| Strategy | PostgreSQL | SQLite/Turso | Description |
|----------|------------|--------------|-------------|
| `on_demand` | ✓ | Maps to `manual` | Refresh manually via `REFRESH MATERIALIZED VIEW` |
| `on_commit` | ✓ (planned) | Maps to `manual` | Refresh on transaction commit |
| `manual` | Maps to `on_demand` | ✓ | Manual refresh via DELETE + INSERT |
| `trigger` | Maps to `on_demand` | ✓ | Auto-refresh via triggers on source tables |
| `hybrid` | Maps to `on_demand` | ✓ | Combination of trigger + manual refresh |

#### View Dependencies

Views are automatically ordered in migrations based on their dependencies. Use `depends_on` to declare what tables or views a view references:

```python
# models/views.py
user_stats: View = {
    "name": "user_stats",
    "query": "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id",
    "materialized": True,
    "depends_on": ["orders"]  # View depends on orders table,
}
```

For views that reference other views:

```python
# models/views.py
top_users: View = {
    "name": "top_users",
    "query": "SELECT * FROM user_stats WHERE order_count > 10",
    "depends_on": ["user_stats"]  # Depends on another view,
}
```

The differ ensures views are created after their dependencies and dropped before them.

#### Concurrent Refresh (PostgreSQL)

PostgreSQL's `REFRESH MATERIALIZED VIEW CONCURRENTLY` allows refreshing without blocking reads, but requires a unique index on the materialized view:

```sql
-- First create a unique index
CREATE UNIQUE INDEX ON user_stats (user_id);

-- Then you can refresh concurrently
REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats;
```

**The library does not help with this on PostgreSQL.** This section described `validate_concurrent_refresh` and `generate_refresh_materialized_view`; neither is defined anywhere in the package. Issue the `REFRESH` yourself:

```python
from declaro_persistum.database import writing

async with writing(db) as conn:
    await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats")
    await conn.commit()
```

What does exist is the SQLite and Turso emulation below, which is a real table plus a metadata row rather than a native materialized view.

#### Materialized View Emulation (SQLite/Turso)

Since SQLite and Turso/LibSQL don't support native materialized views, the library emulates them using:

1. **Backing Table** - A regular table created from `CREATE TABLE name AS query`
2. **Metadata Table** - `_dp_materialized_views` tracks emulated views and their refresh state

**Refresh Methods:**

```python
from declaro_persistum.abstractions import refresh_matview_sql

# Atomic refresh (DELETE + INSERT in transaction)
statements = refresh_matview_sql("monthly_stats", query, atomic=True)

# Non-atomic refresh (DROP + CREATE, loses indexes)
statements = refresh_matview_sql("monthly_stats", query, atomic=False)
```

**Trigger-Based Auto-Refresh:**

With `refresh = "trigger"` and `trigger_sources`, triggers are created to auto-refresh when source tables change:

```python
from declaro_persistum.abstractions import generate_refresh_trigger_sql

# Creates AFTER INSERT/UPDATE/DELETE triggers on source table
triggers = generate_refresh_trigger_sql(
    matview_name="monthly_stats",
    source_table="orders",
    query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
)
```

**Introspection:**

The SQLite and Turso inspectors read `_dp_materialized_views` to tell an emulated materialized view from an ordinary one, and exclude its backing table from the schema. Neither did before 2026-08-14: every view came back `materialized: False` and the backing table came back as a user table nobody had declared, so each migration proposed dropping it and discarding the view's contents.

```python
schema, views = await inspector.introspect(conn, include_views=True)

for name, view in views.items():
    if view.get("materialized"):
        print(f"{name}: refresh={view.get('refresh')}")
```

### Column Types

| Type | PostgreSQL | SQLite | Turso |
|------|------------|--------|-------|
| `uuid` | UUID | TEXT | TEXT |
| `text` | TEXT | TEXT | TEXT |
| `varchar(n)` | VARCHAR(n) | TEXT | TEXT |
| `integer` | INTEGER | INTEGER | INTEGER |
| `bigint` | BIGINT | INTEGER | INTEGER |
| `boolean` | BOOLEAN | INTEGER | INTEGER |
| `timestamptz` | TIMESTAMPTZ | TEXT | TEXT |
| `timestamp` | TIMESTAMP | TEXT | TEXT |
| `jsonb` | JSONB | TEXT | TEXT |
| `numeric(p,s)` | NUMERIC(p,s) | REAL | REAL |
| `bytea` | BYTEA | BLOB | BLOB |

## CLI Usage

### Environment Setup

```bash
# Set database connection (or use -c flag)
export DECLARO_DATABASE_URL="postgresql://user:pass@localhost/mydb"
```

### View Pending Changes

```bash
# Show what would change
declaro diff

# With verbose output
declaro diff -v

# Specify schema directory
declaro diff -s ./my_schema
```

### Apply Migrations

```bash
# Interactive mode (confirms before applying)
declaro apply

# Unattended mode (for CI/CD)
declaro apply --unattended

# Dry run (show SQL without executing)
declaro apply --dry-run
```

### Generate SQL

```bash
# Output to stdout
declaro generate

# Save to file
declaro generate -o migration.sql
```

### Update Snapshot

After manual database changes or initial setup:

```bash
# Capture current DB state as baseline
declaro snapshot

# Force overwrite existing snapshot
declaro snapshot --force
```

### Validate Schema

Check schema files without database connection:

```bash
declaro validate

# Fail on warnings too
declaro validate --strict
```

### Connection Options

```bash
# PostgreSQL
declaro diff -c "postgresql://user:pass@localhost/mydb"

# SQLite
declaro diff -c "sqlite:///path/to/db.sqlite"

# Turso
declaro diff -c "libsql://your-db.turso.io" -d turso

# Force dialect
declaro diff -c "$DATABASE_URL" -d postgresql
```

## Opening a Database

There is no pool. A `Database` is a value: a path, the dialect, and the callables that open and close one connection. `reading` and `writing` open a connection for the span of a block and close it, so nothing holds a live handle you could reach around.

This section described `ConnectionPool.postgresql(...)`, `pool.acquire()`, `MirrorPool` and four `Pool*Error` types. None of them exist. Every example below was run before it was written down.

### Opening

```python
from declaro_persistum import open_postgresql, open_sqlite, open_turso

db = await open_sqlite("./app.db", shutdown="exit_immediately", busy_timeout_s=5.0)

db = await open_postgresql(
    "postgresql://user:pass@localhost/mydb",
    shutdown="exit_immediately",
    busy_timeout_s=5.0,
)

# Turso, local only
db = await open_turso("./app.db", shutdown="exit_immediately")

# Turso Cloud: a local replica kept in conformity with a cloud primary.
# Adding the primary is the only difference.
db = await open_turso(
    "./app.db",
    primary="libsql://your-db.turso.io",
    token="your-token",
    shutdown="replicate",
)
```

`shutdown` has no default, and that is deliberate. persistum's usual home has ephemeral disk, where anything unreplicated when the process dies is gone, so a default would silently pick the losing side of the failure it exists to prevent. `"replicate"` traps SIGTERM and SIGINT and replicates to completion before exiting; `"exit_immediately"` installs no handler.

There is no `max_size`. Concurrency on a local database comes from the crew (see Write Queue), which bounds how many writes are in flight rather than how many connections exist.

### Reading and writing

```python
from declaro_persistum.database import close, flush, reading, writing

async with writing(db) as conn:
    await conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, active INTEGER)")
    await conn.execute("INSERT INTO users VALUES ('u1', 1)")
    await conn.commit()

async with reading(db) as conn:
    cursor = await conn.execute("SELECT id FROM users WHERE active = 1")
    rows = await cursor.fetchall()

await flush(db)   # block until local writes have reached the primary
await close(db)   # replicate whatever is left, then release everything
```

**`writing` is a transaction scope: it commits when the block ends cleanly and rolls back if it raises.** It did not until 2026-08-14. Before that it opened a connection, yielded it and closed it, so a block that forgot `await conn.commit()` had its writes discarded and reported nothing — `execute_fk_ordered` omitted the commit and every one of its writes was rolled back while `cursor.rowcount` still said success. Measured on both DB-API engines, two writes through two blocks with the second uncommitted, one row survived. A second commit is harmless, so code that commits explicitly is unaffected. Committing cannot be a call to `conn.commit()` in the general case, because asyncpg has none; the per-engine answer lives in `writers.COMMIT`.

A reader takes no lock and waits for nothing: the local file is a full copy, so reads never touch the primary. On a replicated database writers are serialised, because the engine admits one.

### Mirroring two databases

`mirror` pairs two databases so a write goes to both and a read can be compared. It is a value like everything else, and the functions over it return a new one.

```python
from declaro_persistum import detach, mirror, parallel_write, promote

m = mirror(primary, replica, fail_open=True, compare_on_read=True)

m = await parallel_write(m, "INSERT INTO users (id, name) VALUES (?, ?)", ("u1", "Alice"))
m["divergences"]        # [] when the two agreed

m = promote(m)          # swap: the replica becomes the primary
db = detach(m)          # done verifying; keep the primary alone
```

A `Divergence` records `sql`, `primary` and `replica`. `fail_open=True` continues on the primary when the replica fails; `compare_on_read=True` compares SELECT results.

### Errors

```python
from declaro_persistum import (
    DatabaseError,              # base for the database layer
    DatabaseClosedError,        # used after close()
    ConnectionsExhaustedError,  # no connection available
    ConnectionFailedError,      # could not open one
)

try:
    async with writing(db) as conn:
        ...
except DatabaseClosedError:
    ...
```

`PoolError`, `PoolClosedError`, `PoolExhaustedError` and `PoolConnectionError` were the old names. The first has no successor; the other three are `DatabaseClosedError`, `ConnectionsExhaustedError` and `ConnectionFailedError`.

### Async only

`SyncConnectionPool` and the replica connection types were removed in 2026-03-08. Use `pytest-asyncio` and `asyncio.run()`.

## Instrumentation

`instrumentation.py` gives you everything needed to record a query's duration, op type and outcome. **It is not wired into the write path.** Nothing in the package calls these functions; you call them around your own `writing(db)` block.

### What this section used to say, and why it was wrong

It described `ConnectionPool.turso(..., instrumentation=True)`, `pool.configure_instrumentation(...)` and `pool._latency_logger`. `ConnectionPool` no longer exists, `configure_instrumentation` is defined nowhere in the package, and there is no `_latency_logger`. Following those instructions produced an `AttributeError`. The functions below are what is actually there, and the example was run before being written down.

### Recording a query

```python
import io, logging, time

from declaro_persistum import open_sqlite
from declaro_persistum.database import close, writing
from declaro_persistum.instrumentation import build_record, classify_sql, emit_record

logger = logging.getLogger("latency")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

db = await open_sqlite("./app.db", shutdown="exit_immediately", busy_timeout_s=5.0)

sql = "CREATE TABLE t (id INTEGER)"
started = time.perf_counter()
async with writing(db) as conn:
    await conn.execute(sql)
    await conn.commit()

emit_record(logger, build_record(
    tier="my-app",
    op=classify_sql(sql),
    duration_ms=(time.perf_counter() - started) * 1000,
    success=True,
    sql=sql,
))
```

emits one JSONL line:

```json
{"ts": "2026-08-14T03:19:18.332541+00:00", "tier": "my-app", "op": "create", "duration_ms": 7.913, "success": true, "sql": "CREATE TABLE t (id INTEGER)", "error": ""}
```

Where that line goes is the logger's business, which is why there is no `latency_sink` or `latency_path` argument: a `FileHandler` writes it to a file, a custom handler forwards it to a metrics system.

### The pieces

| function | takes | returns |
|---|---|---|
| `classify_sql(sql)` | a statement | `select` \| `insert` \| `update` \| `delete` \| `create` \| `alter` \| `other` |
| `is_write_op(op)` | a classified op | whether it writes |
| `has_returning_clause(sql)` | a statement | whether it has `RETURNING` |
| `build_record(...)` | tier, op, duration_ms, success, sql, error | a `LatencyRecord` |
| `format_jsonl(record)` | a record | one JSON line |
| `emit_record(logger, record)` | a logger and a record | nothing; logs at INFO |

All six are pure except `emit_record`, which is the single output boundary.

### LatencyRecord

```python
{
    "ts": "2026-08-14T03:19:18.332541+00:00",  # ISO 8601, UTC
    "tier": "my-app",                           # whatever label you pass
    "op": "insert",                             # from classify_sql
    "duration_ms": 842.31,
    "success": True,
    "sql": "INSERT INTO cards (id, name...",    # first 120 chars
    "error": "",                                # first 200 chars on failure
}
```

### Overhead when you do not use it

Zero, because nothing calls it. There is no flag to leave off and no branch on a hot path: the cost is exactly the calls you write yourself.

---

## Write Queue

The WAL is already the queue. A write is durable once it is in the log, and the engine applies it to the main file later — which is why a local commit takes under a millisecond.

So the only job left is to buffer callers who arrive at the same instant and hand their writes to the log in order. That is all this is: a waiting room.

It does **not** serialise the database. Turso supports concurrent writers through MVCC and `BEGIN CONCURRENT`, and a crew opens one connection per drainer. The room buffers callers; the engine still writes them concurrently.

Nothing is stored in it. It is empty except during the microseconds when callers overlap. There is no persistence, no retry, and no pending list that survives a failure.

```python
from declaro_persistum import new_room, deposit, collect, drain

room = new_room()

ticket = deposit(room, {"sql": "INSERT INTO users (id, name) VALUES (?, ?)",
                        "params": (1, "ada")})
# ... the caller is free here ...
receipt = await collect(room, ticket)      # {"id": ticket, "ok": True, "error": ""}
```

`deposit` returns a ticket immediately. `collect` awaits that ticket and gives back the same ticket with a success or failure code. That is what makes it asynchronous rather than a lock: a lock makes you wait at the moment of writing, whereas here you can deposit several writes, keep working, and collect when you actually need the answer.

Deposit order is preserved, so a write that depends on an earlier one — a foreign key, say — is safe to deposit straight after it.

### You run the appender

The library never starts a task. `drain` appends everything waiting, one at a time, and you decide when it runs:

```python
async def appender():
    while True:
        await drain(room, execute, retry)
        await asyncio.sleep(0)
```

`execute` is your own write function, so the room never touches a connection:

```python
async def execute(w):
    async with writing(db) as conn:
        await conn.execute(w["sql"], w["params"])
        await conn.commit()
```

Or let a crew do it. `start_crew(room, db, size, retry, idle_s)` runs `size` drainers, each holding one connection for its whole life, and `stop_crew` waits for the write in flight. It opens all `size` connections before it returns, so a connection that cannot open raises to you instead of killing one drainer quietly.

### Failure

A failure belongs to the caller that deposited it and goes back down that caller's ticket:

```python
receipt = await collect(room, ticket)
if not receipt["ok"]:
    logger.error("write %s failed: %s", receipt["id"], receipt["error"])
```

It is not retried, because a real error — a constraint violation — will fail again, and it belongs to the caller that caused it. One caller's failure does not affect any other caller's write.

## Programmatic Usage

### Introspecting a Database

One function, dispatched on the dialect. `PostgreSQLInspector`, `SQLiteInspector` and `TursoInspector` were stateless classes and are gone.

```python
from declaro_persistum.inspector import introspect, table_exists
```

#### Basic Introspection

```python
import asyncio
import asyncpg
from declaro_persistum.inspector import introspect

async def main():
    conn = await asyncpg.connect("postgresql://localhost/mydb")

    # Introspect all tables in the schema
    schema = await introspect(conn, "postgresql")

    for table_name, table in schema.items():
        print(f"Table: {table_name}")

        # Columns
        for col_name, col in table["columns"].items():
            print(f"  {col_name}: {col['type']}")
            if col.get("primary_key"):
                print(f"    PRIMARY KEY")
            if col.get("nullable") is False:
                print(f"    NOT NULL")
            if col.get("unique"):
                print(f"    UNIQUE")
            if col.get("references"):
                print(f"    REFERENCES {col['references']}")
            if col.get("default"):
                print(f"    DEFAULT {col['default']}")

        # Composite primary key (if any)
        if "primary_key" in table:
            print(f"  PRIMARY KEY: {table['primary_key']}")

        # Indexes
        if "indexes" in table:
            for idx_name, idx in table["indexes"].items():
                unique = "UNIQUE " if idx.get("unique") else ""
                using = f" USING {idx['using']}" if idx.get("using") else ""
                where = f" WHERE {idx['where']}" if idx.get("where") else ""
                print(f"  INDEX {unique}{idx_name} ({', '.join(idx['columns'])}){using}{where}")

    await conn.close()

asyncio.run(main())
```

#### Schema Structure

The introspected schema is a dictionary with this structure:

```python
Schema = dict[str, Table]

Table = {
    "columns": dict[str, Column],
    "primary_key": list[str],  # Only for composite PKs
    "indexes": dict[str, Index],  # Optional
}

Column = {
    "type": str,                    # e.g., "text", "integer", "uuid"
    "nullable": bool,               # Only present if False
    "primary_key": bool,            # Only for single-column PKs
    "unique": bool,                 # Optional
    "default": str,                 # Optional
    "references": str,              # e.g., "users.id"
    "on_delete": str,               # "cascade", "set null", "restrict", "no action"
    "on_update": str,               # Same options as on_delete
}

Index = {
    "columns": list[str],
    "unique": bool,                 # Optional
    "where": str,                   # Partial index condition (optional)
    "using": str,                   # Index method, e.g., "gin", "gist" (PostgreSQL)
}
```

#### PostgreSQL Schema Support

PostgreSQL supports multiple schemas. By default, introspection uses the `public` schema:

```python
# Introspect public schema (default)
schema = await inspector.introspect(conn)

# Introspect a specific schema
schema = await inspector.introspect(conn, schema_name="myapp")

# Check table existence in a specific schema
exists = await inspector.table_exists(conn, "users", schema_name="myapp")
```

#### Helper Methods

All inspectors provide these utility methods:

```python
# Check if a table exists
exists = await inspector.table_exists(conn, "users")

# Get columns for a specific table (raises DeclaroError if table doesn't exist)
columns = await inspector.get_table_columns(conn, "users")
for col_name, col in columns.items():
    print(f"{col_name}: {col['type']}")

# Get the dialect identifier
dialect = inspector.get_dialect()  # "postgresql", "sqlite", or "turso"
```

#### SQLite and Turso Introspection

SQLite and Turso use PRAGMA statements for introspection:

```python
import aiosqlite
from declaro_persistum.inspector import introspect, table_exists

async def introspect_sqlite():
    async with aiosqlite.connect("./app.db") as conn:
        schema = await introspect(conn, "sqlite")
        # Same schema structure as PostgreSQL
        exists = await table_exists(conn, "sqlite", "users")
```

Turso takes the same call with a different dialect. Every PRAGMA it needs goes through `abstractions/pragma_compat.py`, which emulates the ones the engine does not implement natively. Turso Database is a Rust rewrite, not libSQL, and SQLite compatibility is its aim rather than a guarantee.

```python
import turso
from declaro_persistum.inspector import introspect

def introspect_turso():
    conn = turso.connect("./replica.db")
    schema = asyncio.run(introspect(conn, "turso"))
```

### Introspecting Views

Use `include_views=True` to introspect both tables and views:

```python
async def introspect_with_views():
    conn = await asyncpg.connect("postgresql://localhost/mydb")

    # Returns tuple of (schema, views) when include_views=True
    schema, views = await introspect(conn, "postgresql", include_views=True)

    print("Tables:")
    for table_name in schema:
        print(f"  {table_name}")

    print("Views:")
    for view_name, view in views.items():
        print(f"  {view_name}")
        print(f"    Query: {view['query'][:50]}...")
        print(f"    Materialized: {view.get('materialized', False)}")

    await conn.close()
```

Or use `introspect_views()` directly:

```python
views = await inspector.introspect_views(conn)
for name, view in views.items():
    print(f"{name}: {'MATERIALIZED' if view.get('materialized') else 'VIEW'}")
```

#### View Structure

```python
View = {
    "name": str,           # View name
    "query": str,          # SELECT query (normalized)
    "materialized": bool,  # True for materialized views (PostgreSQL only)
}
```

### Computing a Diff

```python
from declaro_persistum.differ import diff
from declaro_persistum import load_schema_from_models

# Load target schema from Pydantic models
target = load_schema_from_models("./models")

# Current schema from introspection (see above)
current = await inspector.introspect(conn)

# Compute diff
result = diff(current, target, dialect="postgresql")

print(f"Operations: {len(result['operations'])}")
for i in result["execution_order"]:
    op = result["operations"][i]
    print(f"  {op['op']} on {op['table']}")
```

### Applying Migrations

```python
from declaro_persistum.applier import apply

# Apply the diff. The dialect is an argument, not a class.
apply_result = await apply(
    conn,
    result["operations"],
    result["execution_order"],
    "postgresql",
)

if apply_result["success"]:
    print(f"Applied {apply_result['operations_applied']} operations")
else:
    print(f"Failed: {apply_result['error']}")
```

### Query Builder

The query builder uses schema-validated dot notation. Typos like `users.emial` are caught immediately at query build time, not when the SQL hits the database.

```python
from declaro_persistum import open_postgresql
from declaro_persistum.loader import load_schema
from declaro_persistum.query.table import table

schema = load_schema("./schema")
db = await open_postgresql(
    "postgresql://localhost/mydb",
    shutdown="exit_immediately",
    busy_timeout_s=5.0,
)

# `table(name, schema)` looks a table up and fails loudly if it is absent.
# It is not bound to a database: a query is data, and the database is passed
# to whatever executes it.
users = table("users", schema)
orders = table("orders", schema)
```

#### SELECT Queries

```python
# Basic SELECT with dot notation — no conn parameter
results = await (
    users
    .select(users.id, users.email, users.name)
    .where(users.status == "active")
    .order_by(users.created_at.desc())
    .limit(10)
    .execute()
)
# results is list[dict[str, Any]]

# Single row
user = await (
    users
    .select(users.id, users.email)
    .where(users.id == ":id")
    .params(id=user_id)
    .execute_one()
)
# user is dict[str, Any] | None

# With JOIN (column-to-column comparison in ON clause)
results = await (
    orders
    .select(orders.id, orders.total, users.email)
    .join(users, on=orders.user_id == users.id)
    .where(orders.status == "pending")
    .execute()
)
# Generates: ... INNER JOIN users ON orders.user_id = users.id ...

# LEFT JOIN
results = await (
    orders
    .select(orders.id, users.email)
    .join(users, on=orders.user_id == users.id, type="left")
    .execute()
)

# All comparison operators work for column-to-column:
#   orders.user_id == users.id   →  orders.user_id = users.id
#   orders.user_id != users.id   →  orders.user_id != users.id
#   orders.amount > users.limit  →  orders.amount > users.limit

# Complex conditions with AND/OR
results = await (
    users
    .select(users.id, users.email)
    .where(
        (users.status == "active") &
        (users.role.in_(["admin", "editor"]) | users.is_superuser == True)
    )
    .execute()
)
```

#### INSERT Queries

```python
await (
    users
    .insert(
        email=":email",
        name=":name",
        created_at=now_(),  # Dialect-aware function
    )
    .params(email="alice@example.com", name="Alice")
    .execute()
)
```

#### UPDATE Queries

```python
await (
    users
    .update(name=":name", updated_at=now_())
    .where(users.id == ":id")
    .params(id=user_id, name="New Name")
    .execute()
)
```

#### DELETE Queries

```python
await (
    users
    .delete()
    .where(users.id == ":id")
    .params(id=user_id)
    .execute()
)
```

#### Scalar Queries

```python
from declaro_persistum.query import count_

# Count with condition
count = await (
    users
    .select(count_("*"))
    .where(users.status == "active")
    .execute_scalar()
)
print(f"Active users: {count}")
```

#### Column Methods

```python
# Comparisons
users.age == 25           # =
users.age != 25           # !=
users.age > 18            # >
users.age >= 18           # >=
users.age < 65            # <
users.age <= 65           # <=

# Pattern matching
users.email.like("%@example.com")
users.name.ilike("%alice%")  # Case-insensitive (PostgreSQL)

# NULL checks
users.deleted_at.is_null()
users.email.is_not_null()

# Range
users.age.between(18, 65)

# IN clause
users.status.in_(["active", "pending"])

# Ordering
users.created_at.desc()
users.name.asc()
```

#### Type Safety with TypedDict

Results are plain dicts. For IDE autocomplete, use TypedDict:

```python
from typing import TypedDict

class UserRow(TypedDict):
    id: str
    email: str
    name: str

user: UserRow | None = await (
    users
    .select(users.id, users.email, users.name)
    .where(users.id == ":id")
    .params(id=user_id)
    .execute_one()
)

if user:
    print(user["email"])  # IDE knows this is str
```

## Alternative Query Styles

**There are none, and there is one style left.** This section documented a Django-style `users.objects.filter(...)`, a Prisma-style `users.prisma.find_many(...)` and a SQLAlchemy-style `Session`. All three were deleted: the Django and Prisma surfaces in `264cedd`, the SQLAlchemy layer in `9ff59e7` ("delete 1450 LOC that no entry point reached"). Each example here bound a table to a `pool`, which does not exist either.

They were on-ramps for teams arriving from those tools. What replaced them is not a fourth style but the removal of the need for one: a query is a value, so the shape a team is used to is a function they write over it rather than a surface this package has to carry.

```python
from declaro_persistum import execute, reading, select

# The Django-style filter, as a function over the one query builder.
def filter_by(table: str, **lookups) -> Query:
    return select("*", from_table=table, where=lookups)

async with reading(db) as conn:
    active = await execute(filter_by("users", status="active"), conn)
```

`select` takes `where` as data — a dict of column to value, or a SQL string with `params` — plus `order_by`, `limit`, `offset`, `joins`, `group_by` and `having`. See [Query Builder](#query-builder).

## Handling Ambiguities

When the differ detects potential renames, it raises ambiguities that need resolution.

### Interactive Mode (Default for TTY)

```bash
$ declaro diff

1 ambiguous change(s) detected:

1. Column 'users.username' dropped and 'users.user_name' added (87% similar)
   [1] Rename (preserves data)
   [2] Drop + Add (loses data)
   Choice [1/2]: 1

Decisions saved to schema/migrations/pending.toml
```

### Unattended Mode (CI/CD)

```bash
$ declaro diff --unattended
Error: Unresolved ambiguities. Run interactively or provide decisions.
```

### Pre-declaring Decisions

Create `models/migrations/pending.toml`:

```toml
[users_username]
type = "rename"
table = "users"
from_column = "username"
to_column = "user_name"
decided_at = "2024-01-15T10:30:00Z"
```

### Using Migration Hints in Schema

```python
# Explicit rename hint
@table("users")
class User(BaseModel):
    user_name: str = field(renamed_from="username")

    # Explicit new column hint (prevents rename detection)
    display_name: str = field(is_new=True)
```

## Drift Detection

The snapshot tracks the expected database state. If someone modifies the database directly, declaro detects the drift:

```bash
$ declaro diff

Warning: Database schema has drifted from expected state

  Differences detected:
    + Column 'users.temp_field' exists in DB but not in snapshot
    - Column 'users.old_field' in snapshot but not in DB

  Options:
    1. Run 'declaro snapshot' to update snapshot to current DB state
    2. Run with --force to proceed anyway
    3. Manually reconcile the differences
```

## Database-Specific Notes

### PostgreSQL

- Full support for schemas (default: `public`)
- Transactional DDL (all-or-nothing migrations)
- Supports all PostgreSQL types including arrays, JSONB, custom types

```bash
declaro diff -c "postgresql://user:pass@host/db?sslmode=require"
```

### SQLite

- Single schema (`main`)
- Limited ALTER TABLE support (no DROP COLUMN before 3.35)
- Types mapped to SQLite affinities

```bash
declaro diff -c "sqlite:///./data/app.db"
```

### Turso (Embedded)

- SQLite-compatible database written in Rust
- Features: vector search, CDC, async I/O with io_uring
- Requires `pyturso` package

```bash
# Local embedded database
declaro diff -c "sqlite:///./data/app.db" -d turso
```

### Turso Cloud

- SQLite-compatible with replication
- Uses `pyturso`; no separate cloud package
- Same limitations as SQLite

```bash
export TURSO_AUTH_TOKEN="your-token"
declaro diff -c "libsql://your-db.turso.io" -d turso
```

## Multi-Tenant Architecture (Turso Cloud)

**There is no `TursoCloudManager`.** This section described one with eight methods: `create_database`, `delete_database`, `get_or_create_database`, `database_exists`, `list_databases`, `get_pool`, `close` and a constructor. The class was deleted in `7a5c9ac` and nothing replaced it, so every call it documented raised `ImportError`.

Provisioning a tenant database is a Turso platform API call, and this package does not make it. What it gives you is one `Database` per tenant, each a local replica of that tenant's own primary. Hold them in a dict:

```python
import os

from declaro_persistum import Database, close, open_turso, reading, writing

TENANTS: dict[str, Database] = {}

async def tenant_db(tenant_id: str) -> Database:
    """One replica per tenant, opened once and kept."""
    if tenant_id not in TENANTS:
        TENANTS[tenant_id] = await open_turso(
            f"./replicas/{tenant_id}.db",
            primary=f"https://{tenant_id}-myorg.turso.io",
            token=os.environ["TURSO_AUTH_TOKEN"],
            shutdown="replicate",
        )
    return TENANTS[tenant_id]
```

`open_turso` takes the replica path, the tenant's primary URL and its auth token. `shutdown` is required and says what happens to writes that have not reached the primary when the process stops: `"replicate"` waits for them, `"exit_immediately"` abandons them. There is no default, because the right answer is a property of the deployment (see [Opening a Database](#opening-a-database)).

The replica directory must exist before the first open. `open_turso` does not create it, and pyturso reports the absence as `turso.IoError: open: NotFound`.

### FastAPI Integration

```python
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    for db in TENANTS.values():
        await close(db)

app = FastAPI(lifespan=lifespan)

async def get_tenant_conn(request: Request):
    db = await tenant_db(request.state.tenant_id)  # tenant from auth middleware
    async with reading(db) as conn:
        yield conn

@app.get("/users")
async def list_users(conn=Depends(get_tenant_conn)):
    cursor = await conn.execute("SELECT * FROM users")
    return await cursor.fetchall()
```

`close` is what makes `shutdown="replicate"` mean anything: it is the call that waits. A process that exits without it drops whatever had not replicated, whatever the policy said.

Writes go through `writing(db)`, which serialises. A replicated database gets no write concurrency from either journal mode, so `start_crew` refuses one outright — see [Write Queue](#write-queue) for the measurements.

### Database Credentials by Backend

Each backend requires different environment variables:

| Backend | Required Credentials | Example Env Vars |
|---------|---------------------|------------------|
| **PostgreSQL** | Connection URL | `DATABASE_URL=postgresql://user:pass@host:5432/dbname` |
| **SQLite** | File path only | `DATABASE_PATH=./app.db` |
| **Turso (embedded)** | File path only | `DATABASE_PATH=./app.db` |
| **Turso Cloud** | Local path + primary URL + auth token | `DATABASE_PATH=./app.db`, `TURSO_DATABASE_URL=https://db-org.turso.io`, `TURSO_AUTH_TOKEN` |

## Example Applications

Four complete Todo apps demonstrate different query styles, each supporting all 4 database backends:

| Example | Port | Query Style |
|---------|------|-------------|
| `examples/todo_app_native/` | 7777 | Built-in fluent query builder |
| `examples/todo_app_django_style/` | 7778 | QuerySet-like API with lookups |
| `examples/todo_app_prisma_style/` | 7779 | Dict-based queries |
| `examples/todo_app_sqlalchemy/` | 7780 | Declarative models with Session |

### Running an Example

```bash
cd examples/todo_app_native
uv run uvicorn app:app --reload --port 7777
```

Visit http://localhost:7777 to use the app, or http://localhost:7777/db to switch databases.

### Runtime Database Switching

Each example app supports hot-swapping databases at runtime via the `/db` endpoint:

- **SQLite** - Local file database with configurable path
- **PostgreSQL** - Production database with host/port/credentials
- **Turso Embedded** - Rust-based SQLite with configurable path
- **Turso Cloud** - Edge-hosted SQLite with URL + auth token

### Configurable Database Paths

For local databases (SQLite and Turso Embedded), you can specify custom file paths:

```python
from db import get_sqlite_config, get_turso_embedded_config

# Default paths are relative to the app directory
# SQLite: ./todos.db
# Turso Embedded: ./todos_turso.db

# Custom paths
sqlite_config = get_sqlite_config("/path/to/my/database.db")
turso_config = get_turso_embedded_config("/path/to/my/turso.db")
```

The `/db` endpoint UI allows entering custom paths for local databases, making it easy to test with different database files without code changes.

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Database Migration

on:
  push:
    branches: [main]
    paths:
      - 'schema/**'

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install declaro_persistum asyncpg

      - name: Check for pending migrations
        run: declaro diff --unattended
        env:
          DECLARO_DATABASE_URL: ${{ secrets.DATABASE_URL }}

      - name: Apply migrations
        run: declaro apply --unattended
        env:
          DECLARO_DATABASE_URL: ${{ secrets.DATABASE_URL }}
```

### Pre-commit Validation

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: validate-schema
        name: Validate database schema
        entry: declaro validate --strict
        language: system
        files: ^models/.*\.py$
```

## API Reference

### Type Definitions

All data structures are TypedDict - no classes with state. This ensures serialization to/from JSON.

#### Schema Types

```python
from declaro_persistum.types import (
    Schema,      # dict[str, Table] - mapping of table names to definitions
    Table,       # Table definition with columns, indexes, constraints
    Column,      # Column definition with type, constraints, foreign keys
    Index,       # Index definition with columns, uniqueness, conditions
    Constraint,  # Named constraint (check, unique, exclude)
    View,        # View definition with query, materialized flag
    Enum,        # Enum type definition
    Trigger,     # Trigger definition (PostgreSQL only)
    Procedure,   # Stored procedure definition (PostgreSQL only)
)
```

#### Column TypedDict

```python
Column = {
    # Required
    "type": str,                    # SQL type (text, integer, uuid, timestamptz, etc.)

    # Optional - constraints
    "nullable": bool,               # Whether NULL allowed (default: True, only set if False)
    "primary_key": bool,            # Whether this is the primary key
    "unique": bool,                 # Whether has unique constraint
    "check": str,                   # CHECK constraint expression

    # Optional - defaults
    "default": Any,                 # Default value (SQL expression as string)

    # Optional - foreign keys
    "references": str,              # FK target as "table.column"
    "on_delete": Literal["cascade", "set null", "restrict", "no action"],
    "on_update": Literal["cascade", "set null", "restrict", "no action"],

    # Migration hints (not persisted to DB)
    "renamed_from": str,            # Indicates column was renamed from this name
    "is_new": bool,                 # Confirms intentionally new column (prevents rename detection)
}
```

#### Table TypedDict

```python
Table = {
    "columns": dict[str, Column],           # Required - column definitions
    "primary_key": list[str],               # Composite PK columns (if not in column)
    "indexes": dict[str, Index],            # Named index definitions
    "constraints": dict[str, Constraint],   # Named constraint definitions
    "renamed_from": str,                    # Migration hint - table was renamed
}
```

#### Index TypedDict

```python
Index = {
    "columns": list[str],   # Required - columns in the index
    "unique": bool,         # Whether unique index (default: False)
    "where": str,           # Partial index condition (SQL expression)
    "using": str,           # Index method (btree, hash, gin, gist - PostgreSQL)
}
```

#### View TypedDict

```python
View = {
    "name": str,                                    # View name
    "query": str,                                   # SELECT query
    "materialized": bool,                           # Whether materialized (PostgreSQL only)
    "refresh": Literal["on_demand", "on_commit"],   # Refresh strategy
    "depends_on": list[str],                        # Tables/views this view references
}
```

#### Operation Types

```python
from declaro_persistum.types import Operation, DiffResult, ApplyResult

Operation = {
    "op": Literal[
        "create_table", "drop_table", "rename_table",
        "add_column", "drop_column", "rename_column", "alter_column",
        "add_index", "drop_index",
        "add_constraint", "drop_constraint",
        "add_foreign_key", "drop_foreign_key",
        "create_view", "drop_view",
    ],
    "table": str,           # Table this operation affects
    "details": dict,        # Operation-specific parameters
}

DiffResult = {
    "operations": list[Operation],      # All operations to execute
    "dependencies": dict[int, list[int]], # Operation dependencies
    "execution_order": list[int],       # Topologically sorted indices
    "ambiguities": list[Ambiguity],     # Unresolved ambiguous changes
}

ApplyResult = {
    "success": bool,                # Whether all operations succeeded
    "executed_sql": list[str],      # SQL statements executed
    "operations_applied": int,      # Number of operations applied
    "error": str | None,            # Error message if failed
    "error_operation": int | None,  # Index of failed operation
}
```

### Exception Hierarchy

All exceptions inherit from `DeclaroError` for easy catching:

```python
from declaro_persistum.exceptions import (
    DeclaroError,        # Base exception for all errors
    SchemaError,         # Schema definition or validation error
    ValidationError,     # Validation failed (FK references, concurrent refresh requirements)
    LoaderError,         # File loading error (TOML syntax, file not found)
    AmbiguityError,      # Unresolved ambiguous changes in unattended mode
    CycleError,          # Circular dependencies in operations
    DriftError,          # Database differs from expected snapshot
    ConnectionError,     # Database connection failure
    MigrationError,      # DDL operation failed
    RollbackError,       # Both migration and rollback failed (critical)
    DatabaseError,              # Base for the database layer
    DatabaseClosedError,        # Used after close()
    ConnectionsExhaustedError,  # No connection available
    ConnectionFailedError,      # Could not open one
    TransferError,              # Bulk transfer failed
)
```

#### Exception Usage Examples

```python
from declaro_persistum.exceptions import (
    DeclaroError, MigrationError, AmbiguityError, DriftError
)

try:
    result = await apply(conn, operations, order, "postgresql")
except AmbiguityError as e:
    print(f"Unresolved ambiguities: {len(e.ambiguities)}")
    for amb in e.ambiguities:
        print(f"  {amb['table']}: {amb['message']}")
except DriftError as e:
    print(f"Database drifted from snapshot:")
    for diff in e.differences:
        print(f"  {diff['symbol']} {diff['description']}")
except MigrationError as e:
    print(f"Migration failed: {e}")
    if e.operation:
        print(f"  Operation: {e.operation['op']} on {e.operation['table']}")
    if e.sql:
        print(f"  SQL: {e.sql[:200]}...")
except DeclaroError as e:
    print(f"General error: {e}")
```

### Schema Loading

Load schema from Pydantic model files:

```python
from declaro_persistum import (
    load_schema_from_models,  # Load table schemas from Pydantic models
    load_models_from_module,  # Load from a single Python module
)
from declaro_persistum.loader import (
    load_snapshot,     # Load last-applied snapshot
    save_snapshot,     # Save current schema as snapshot
    load_decisions,    # Load pending ambiguity decisions
    save_decisions,    # Save decisions for later apply
)
```

There is no `clear_decisions`. It was deleted in `162d967` ("delete every function nothing calls"), and nothing took over the job: `migrations/pending.toml` survives a successful apply, and `load_decisions` reads it again on the next diff. Delete the file yourself once the migration lands.

#### Directory Structure

```
models/
├── users.py          # Pydantic models with @table decorator
├── orders.py
├── views.py          # View values, typed as `View`
└── snapshot.toml     # Auto-generated: last applied state

migrations/
└── pending.toml      # Ephemeral: ambiguity decisions
```

#### Loading Examples

```python
from declaro_persistum import load_schema_from_models
from declaro_persistum.loader import load_snapshot, LoaderError

# Load target schema from Pydantic models
target = load_schema_from_models("./models")
print(f"Loaded {len(target)} tables")

# Load last-applied state
try:
    snapshot = load_snapshot("./models")
except LoaderError:
    snapshot = {}  # No previous snapshot
```

### Differ

The differ computes operations needed to transform current schema to target:

```python
from declaro_persistum.differ import diff

result = diff(current_schema, target_schema, dialect="postgresql", decisions=decisions)

# Result structure
print(f"Operations: {len(result['operations'])}")
print(f"Ambiguities: {len(result['ambiguities'])}")

# Execute in order
for idx in result['execution_order']:
    op = result['operations'][idx]
    print(f"{op['op']} on {op['table']}")
```

#### How Diffing Works

Set theory operations determine changes:

```
Let C = current table names, T = target table names

dropped = C - T    (tables to drop)
added = T - C      (tables to create)
modified = C ∩ T   (tables to compare column-by-column)
```

For each modified table, column sets are compared similarly.

#### Handling Ambiguities

When column is dropped and similar one added, it might be a rename:

```python
result = diff(current, target, dialect="postgresql")

if result['ambiguities']:
    for amb in result['ambiguities']:
        print(f"Ambiguous: {amb['type']}")
        print(f"  Table: {amb['table']}")
        if amb['type'] == 'possible_rename':
            print(f"  {amb['from_column']} -> {amb['to_column']}")
            print(f"  Confidence: {amb['confidence']:.0%}")
```

Resolve with migration hints in the Pydantic model:

```python
@table("users")
class User(BaseModel):
    # Explicit rename hint
    user_name: str = field(renamed_from="username")

    # Confirm this is intentionally new (not a rename)
    display_name: str = field(is_new=True)
```

Or pre-declare decisions in `models/migrations/pending.toml`:

```toml
[decisions.users_username]
type = "rename"
table = "users"
from_column = "username"
to_column = "user_name"
```

### Appliers

Appliers execute DDL operations against databases:

```python
from declaro_persistum.applier import apply
```

One function, dispatched on the dialect. `PostgreSQLApplier`, `SQLiteApplier` and `TursoApplier` were stateless classes and are gone; the dialect is the fourth argument.

#### Applying Migrations

```python
from declaro_persistum.applier import apply
from declaro_persistum.applier.postgresql import get_transaction_mode

# Check transaction mode
print(f"Mode: {get_transaction_mode()}")  # "all_or_nothing" or "per_operation"

# Apply operations
result = await apply(
    conn,
    diff_result['operations'],
    diff_result['execution_order'],
    "postgresql",
    dry_run=False,
)

if result['success']:
    print(f"Applied {result['operations_applied']} operations")
    for sql in result['executed_sql']:
        print(f"  {sql[:80]}...")
else:
    print(f"Failed: {result['error']}")
    print(f"Failed at operation {result['error_operation']}")
```

#### Dry Run Mode

Generate SQL without executing:

```python
result = await apply(
    conn,
    operations,
    execution_order,
    dry_run=True,
)

print("SQL that would be executed:")
for sql in result['executed_sql']:
    print(sql)
    print("---")
```

#### Transaction Modes

| Backend | Mode | Behavior |
|---------|------|----------|
| PostgreSQL | `all_or_nothing` | All operations in single transaction; rollback on failure |
| SQLite | `per_operation` | Each operation commits separately; partial apply possible |
| Turso | `per_operation` | Same as SQLite |

### SQL Generation

Generate SQL without applying:

```python
from declaro_persistum.applier.postgresql import generate_operation_sql, generate_sql

# Generate SQL for a single operation
sql = generate_operation_sql({
    "op": "add_column",
    "table": "users",
    "details": {
        "column": "phone",
        "definition": {"type": "text", "nullable": True}
    }
})
print(sql)  # ALTER TABLE "users" ADD COLUMN "phone" text

# Generate all SQL in execution order
sql_list = generate_sql(operations, execution_order)
for sql in sql_list:
    print(sql)
```

#### View SQL Generation

```python
from declaro_persistum.applier.postgresql import (
    generate_create_view,
    generate_drop_view,
)

# Create regular view
sql = generate_create_view({
    "name": "active_users",
    "query": "SELECT * FROM users WHERE status = 'active'",
    "materialized": False,
})
# CREATE VIEW "active_users" AS SELECT * FROM users WHERE status = 'active'

# Create materialized view
sql = generate_create_view({
    "name": "user_stats",
    "query": "SELECT status, COUNT(*) FROM users GROUP BY status",
    "materialized": True,
})
# CREATE MATERIALIZED VIEW "user_stats" AS SELECT ...

# Drop view
sql = generate_drop_view("active_users", materialized=False)
# DROP VIEW IF EXISTS "active_users"

```

There is no `generate_refresh_materialized_view`. On PostgreSQL, issue the `REFRESH` yourself; on SQLite and Turso use `abstractions.materialized_views.refresh_matview_sql(name, query)`, which returns the DELETE, INSERT and metadata statements the emulation needs.

### Validator

Validate schema before applying:

```python
from declaro_persistum.validator import validate_schema

# Validate schema structure, including that every `references` points at a
# table and column that exist. There is no separate `validate_references`.
errors = validate_schema(schema)
for error in errors:
    print(f"Error: {error}")
```

## Troubleshooting

### "Unsupported connection type"

Ensure you have the correct database driver installed:
```bash
pip install asyncpg      # PostgreSQL
pip install aiosqlite    # SQLite
pip install pyturso      # Turso (embedded)
# Turso Cloud needs no extra package — it is pyturso with a remote_url
```

### "Cannot detect dialect"

Specify the dialect explicitly:
```bash
declaro diff -c "$DATABASE_URL" -d postgresql
```

### Type Errors in Schema

Run validation to check your schema files:
```bash
declaro validate -v
```

### Foreign Key Errors

Ensure referenced tables are defined. The differ topologically sorts operations to create tables before their dependents.

### SQLite ALTER TABLE Limitations

SQLite before 3.35 doesn't support DROP COLUMN. The applier will raise `NotImplementedError` for unsupported operations. Workaround: recreate the table.
