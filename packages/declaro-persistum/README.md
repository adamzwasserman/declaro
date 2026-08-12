# declaro_persistum


> ## ⚠️ DEPRECATED — POISONOUS PRACTICE
>
> **Every example below that hands a `ConnectionPool` to the consumer is poisonous practice. Do not copy it, and do not write new code in this shape.**
>
> The pool decision must never reach the consumer or the common syntax. The consumer chooses **async (default) or sync**, and nothing else. Whether a pool exists behind that choice, whether a write reuses a connection, and whether the engine runs MVCC or WAL are all internal, owned by exactly one writer, and invisible above that boundary.
>
> A pool exposed as a surface is promiscuous mutable state with no determinable owner — measured directly on this codebase 2026-08-11, where L1.18b reported the pool's holder fields as `unresolved, drives a decision`. It is the reason every conditional about MVCC kept ending up inside the pool: with no single owner, a branch had no outside to live in.
>
> Binding constraint: **[docs/design/state-ownership-and-the-pool-boundary.md](docs/design/state-ownership-and-the-pool-boundary.md)**
>
> This document is retained as a record. It is not guidance.

You declare the state you want; the library works out how to reach it — across PostgreSQL, SQLite and Turso, without rewriting your application to move between them.

## Architectural priorities

These are in priority order. Everything in this package is either one of these three or exists to serve one of them, and when a design decision is contested the higher priority wins.

### 1. Declarative

You declare *what* you want and the library determines *how*. That is what the name means, and it is the first principle of the whole Declaro stack.

In practice you never write a migration step. You edit your models to say what the schema should be; the library reads the live database, compares it against your declaration, and derives the operations itself. The same instinct governs queries: you describe the result you want rather than assembling SQL by hand.

Anything that pushes you back into writing procedures instead of declaring outcomes is a defect here, however convenient it looks.

### 2. One surface over every supported database

A single API spans PostgreSQL, SQLite and Turso, so that changing database is a change of configuration rather than a refactor. An application written against this package should move from SQLite in development, to PostgreSQL in production, to Turso at the edge, without its code being touched.

This is why two of the largest parts of the package exist. Neither is a feature in its own right:

- The **connection pool** is not "a pool". It is the single entry point that makes several different drivers answer to one API.
- The **compatibility layer** is not a bag of tricks. It supplies capabilities a given database lacks — arrays, maps, ranges, enums, hierarchies, materialized views, CHECK constraints — so that the difference between databases never reaches your application.

Where a database genuinely cannot do something, the gap is closed here rather than handed to the caller.

### 3. Migrations that survive a team

Alembic becomes close to unusable once several people work at once, and the reason is structural: it records a linear chain of revisions. Two developers branch from main, each generates a migration, and now two revisions claim the same parent. Someone resolves that by hand, every time.

There is no chain here to collide. Each branch carries its own declared schema, and the difference is computed against the real database at the moment migrations run. Two branches merge the way ordinary code merges — in the models file — and the resulting schema is simply whatever the merged declaration says.

## What is subordinate

Named explicitly, so that their size in the codebase is not mistaken for their importance:

- **The query builder** is an expression of priority 1, not a goal of its own. It exists so that queries can be declared rather than concatenated. The several styles it offers (native, Django-like, Prisma-like, SQLAlchemy-like) are on-ramps for teams arriving from those tools — not four competing APIs to choose between.
- **The connection pool** serves priority 2.
- **The compatibility layer** serves priority 2.
- **Pure functions, TypedDicts and the absence of hidden state** are how this code is written, not what it is for. They make the three priorities achievable and testable; they are not themselves the architecture.

## Installation

```bash
pip install declaro_persistum

# With PostgreSQL support
pip install declaro_persistum[postgresql]

# With SQLite support
pip install declaro_persistum[sqlite]

# With all databases
pip install declaro_persistum[all]
```

## Quick Start

### Define Schema (Pydantic)

```python
# models/user.py
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: UUID = field(primary=True, default="gen_random_uuid()")
    email: str = field(unique=True)
```

### Run Migrations

```bash
# Show proposed changes
declaro diff -c postgresql://localhost/mydb

# Apply migrations
declaro apply -c postgresql://localhost/mydb

# Generate SQL without executing
declaro generate -c postgresql://localhost/mydb > migration.sql
```

### Query with Connection Pool

```python
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table
from declaro_persistum.loader import load_schema

# Create a connection pool
pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")
schema = load_schema("./schema")

# Bind table to pool — no connection on the caller surface
users = table("users", schema, pool)

results = await (
    users
    .select(users.id, users.email)
    .where(users.status == "active")
    .execute()
)

await pool.close()
```

## Philosophy & Getting Started

Declaro is part of a larger functional-Python stack that shuns hidden state and prefers pure functions. If you haven't read it yet, the [Declaro Manifesto](../../MANIFESTO.md) lays out the fundamental ideas (banana/monkey/jungle, caching policy, anti-OOP, etc.).

This package is the persistence layer. Its three architectural priorities are stated at the top of this file, and they are the frame for everything below: what you read here as a list of features is, in every case, either a way of letting you declare an outcome instead of a procedure (priority 1), or a way of holding one API steady across three different databases (priority 2), or the team-safe migration engine (priority 3).

The functional style — pure functions, TypedDicts, no hidden state — is the discipline that makes those three achievable, not a fourth goal competing with them.

Caching inside this package is deliberately narrow (pools, schemas, prepared statements). Application-specific result caching belongs in an adjacent package such as `tablix`, or in your own code.

### Quick Start

```bash
pip install declaro-persistum[all]
```

```python
from uuid import uuid4
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table
from declaro_persistum.loader import load_schema

schema = load_schema("./schema")
pool = await ConnectionPool.sqlite("./app.db")

# Bind table to pool — pool is a required parameter
users = table("users", schema, pool)

# All query methods acquire connections internally
async with pool.acquire() as conn:
    await conn.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)")
    await conn.commit()

await users.insert(id=str(uuid4()), name="alice").execute()
rows = await users.select().execute()
print(rows)
await pool.close()
```

For more examples and migration commands see the top‑level README.

## Features

### Connection Pool

Unified connection pool with consistent API across all backends:

```python
from declaro_persistum import ConnectionPool

# PostgreSQL (wraps asyncpg pool)
pool = await ConnectionPool.postgresql(
    "postgresql://localhost/mydb",
    min_size=5,
    max_size=20,
)

# SQLite (semaphore-based for WAL mode)
pool = await ConnectionPool.sqlite("./app.db", max_size=5)

# Turso embedded (pyturso - SQLite-compatible with vector search & CDC)
# Provides async interface via dedicated thread pool
pool = await ConnectionPool.turso("./app.db", max_size=5)

# Turso Cloud — a local replica kept in sync with a cloud primary.
# Same factory as embedded Turso; adding remote_url is the only change.
pool = await ConnectionPool.turso(
    "./app.db",                          # local replica path
    remote_url="libsql://your-db.turso.io",
    auth_token="...",
)

# Bind table to pool, then execute without managing connections
users = table("users", schema, pool)
results = await users.select().execute()

await pool.close()
```

### Schema-Validated Queries

Typos caught at build time, not runtime:

```python
users = table("users", schema, pool)
users.emial  # AttributeError: Table 'users' has no column 'emial'
```

### Atomic Increment & Bulk Updates

Atomic counter math at the storage layer — no read-modify-write round trip, no race window:

```python
from declaro_persistum import increment

# Single-row atomic increment via the Prisma-style API:
await db.tags.update_one(
    where={"tag_id": tag_id},
    increment={"card_count": 1},
)
# → UPDATE tags SET card_count = card_count + :inc_card_count WHERE tag_id = :tag_id

# Bulk update — apply the same delta to every row matching an IN clause:
removed = await db.tags.update_many(
    where={"tag_id": {"in": list(removed_tags)}},
    increment={"card_count": -1},
)
# → UPDATE tags SET card_count = card_count + :inc_card_count WHERE tag_id IN (?,?,...)
# Returns the number of rows updated (int).

# data= and increment= compose in a single UPDATE statement:
await db.tags.update_one(
    where={"tag_id": tag_id},
    data={"last_touched": "now()"},
    increment={"card_count": -1},
)
```

The native query layer accepts `increment(delta)` directly as a column value, so update-many is also expressible without the Prisma shortcut:

```python
from declaro_persistum import increment
from declaro_persistum.query.table import table

tags = table("tags", schema, pool)
await (
    tags.update(card_count=increment(1))
        .where(tags.tag_id.in_(added_tag_ids))
        .execute()
)
```

Negative deltas are supported (`increment(-1)`). The emitted SQL stays `col = col + :param` with the negative value bound to the parameter — no special-casing of subtraction, no separate decrement function. The operation is atomic at the storage layer regardless of dialect.

### Query Hooks (pre / post)

Pass functions in — don't register them. `table_factory(...)` returns a closure that produces `TableProxy` instances with your pre-hook and post-hook pre-wired. Pre-hooks transform the query builder *before* SQL is built; post-hooks transform rows *after* the DB returns them.

```python
from declaro_persistum import table_factory
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.table import table

# Your app-defined hook — a pure function, testable without declaro.
def apply_rls(query):
    user = current_user_id.get()
    if isinstance(query, SelectQuery):
        proxy = table(query._table, query._schema, query._pool)
        return query.where(proxy.owner == user)
    return query

def log_audit(rows, meta):
    audit_log.append({"sql": meta["sql"], "rows": len(rows)})
    return rows

# Bind once at app startup:
get_table = table_factory(schema, pool, pre=apply_rls, post=log_audit)

# Use normally — hooks fire automatically on every .execute():
items = get_table("items")
rows = await items.select().where(items.owner == user_id).execute()
```

Because hooks are just function arguments, nothing is registered globally, nothing runs at import time, and you can compose them with ordinary Python — different scopes use different factories with different hook functions. Pre-hooks can structurally rewrite queries (DELETE → UPDATE for soft delete) by returning a different query type; the executor runs whatever comes back.

Full API + RLS / audit / soft-delete recipes: [`docs/hooks.md`](docs/hooks.md).

### Enum Support via Literal Types

Use Python's `Literal` type for enum fields - declaro_persistum automatically creates lookup tables with foreign key constraints (providing consistent enum enforcement across all backends):

```python
from typing import Literal

OrderStatus = Literal["pending", "confirmed", "shipped", "delivered"]

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary=True)
    status: OrderStatus = "pending"
```

This generates:
```sql
-- Lookup table (auto-generated)
CREATE TABLE _dp_enum_orders_status (value TEXT PRIMARY KEY);
INSERT INTO _dp_enum_orders_status VALUES ('pending'), ('confirmed'), ('shipped'), ('delivered');

-- Orders table with FK constraint
CREATE TABLE orders (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending' REFERENCES _dp_enum_orders_status(value)
);
```

Adding or removing enum values is handled automatically during migrations.

### Multiple Query Styles

These are on-ramps, not four APIs to choose between. All of them build the same declared query and run through the same executor; the alternative surfaces exist so a team arriving from Django, Prisma or SQLAlchemy can port code without rewriting every call site first. The native surface is the one to write new code against.

```python
# All styles — pool bound at table creation, no conn on caller surface

# Native fluent API
results = await users.select().where(users.active == True).execute()

# Django-style
results = await users.objects.filter(status="active").all()

# Prisma-style
results = await users.prisma.find_many(where={"status": "active"})
```

### Latency Instrumentation

Record every query's duration, op type, and success/failure:

```python
pool = await ConnectionPool.turso(
    "./app.db",                          # local replica path
    remote_url="libsql://your-db.turso.io",
    auth_token="...",
    instrumentation=True,
    tier_label="project",
    latency_sink="jsonl",
    latency_path="./data/db_latency.jsonl",
)
```

Or attach a callable sink (Prometheus, StatsD, etc.):

```python
pool = await ConnectionPool.sqlite("./app.db")
pool.configure_instrumentation(
    tier_label="my-app",
    callable_sink=lambda record: metrics.record(record),
)
```

Each record is a `LatencyRecord` dict: `ts`, `tier`, `op`, `duration_ms`, `success`, `sql`, `error`.
Zero overhead when disabled — no timing, no allocations.

### Optimistic Write Queue

For high-latency backends (Turso Cloud writes can take 750–1100ms), the write queue returns data to the caller immediately while persisting in the background:

```python
pool = await ConnectionPool.turso(
    "./app.db",                          # local replica path
    remote_url="libsql://your-db.turso.io",
    auth_token="...",
    instrumentation=True,
    tier_label="project",
    write_queue_path="./data/pending_writes.jsonl",
    write_queue_threshold_ms=50.0,
)

users = table("users", schema, pool)

# Returns immediately — write continues in background if >50ms
await users.insert(id=new_id, name="alice").execute()

# Reads merge pending queue entries so inserts appear instantly
rows = await users.select().execute()
```

The queue is:
- **Transparent**: callers see no API difference
- **Durable**: pending writes survive restarts (JSONL persistence)
- **Self-healing**: supervisor retries with exponential backoff, CRITICAL log after 6 hours
- **Read-aware**: SELECT results include pending entries merged by primary key

## Supported Databases

- PostgreSQL (via asyncpg)
- SQLite (via aiosqlite)
- Turso (via pyturso) — embedded SQLite-compatible engine, with optional cloud sync via `remote_url`

All three answer to the same API (priority 2 above): moving between them is configuration, not a code change.

libsql is no longer used. `ConnectionPool.libsql()` does not exist; Turso Cloud is reached with `ConnectionPool.turso(local_path, remote_url=...)`, which keeps a local replica in sync with the cloud primary. A `libsql://` URL is still a valid Turso Cloud address — that is Turso's own URL scheme, not the removed libsql package.

## Database Credentials

**Each database backend requires different environment variables.** Client applications must handle these differences when configuring connections.

| Backend | Required Credentials | Example Env Vars |
|---------|---------------------|------------------|
| **PostgreSQL** | Connection URL | `DATABASE_URL=postgresql://user:pass@host:5432/dbname` |
| **SQLite** | File path only | `DATABASE_PATH=./app.db` |
| **Turso (embedded)** | File path only | `DATABASE_PATH=./app.db` |
| **Turso Cloud** | Local path + URL + Auth token | See multi-tenant pattern below |

### Single-Tenant Configuration

```python
import os
from declaro_persistum import ConnectionPool

# PostgreSQL - single connection string
pool = await ConnectionPool.postgresql(os.environ["DATABASE_URL"])

# SQLite / Turso embedded - just a path
pool = await ConnectionPool.sqlite(os.environ.get("DATABASE_PATH", "./app.db"))
pool = await ConnectionPool.turso(os.environ.get("DATABASE_PATH", "./app.db"))
```

### Multi-Tenant Configuration (Turso Cloud)

**Turso cloud is designed for one database per client/tenant.** Use `TursoCloudManager` for database provisioning, token management, and connection pooling:

```python
import os
from declaro_persistum import TursoCloudManager

# Create manager with Platform API credentials
manager = TursoCloudManager(
    org=os.environ["TURSO_ORG"],          # e.g., "mycompany"
    api_token=os.environ["TURSO_API_TOKEN"],  # Platform API token
)

# Create database for new tenant
db_info = await manager.create_database("tenant-123")

# Get connection pool for tenant (cached, auto-creates token)
pool = await manager.get_pool("tenant-123")
async with pool.acquire() as conn:
    cursor = await conn.execute("SELECT * FROM users")
    users = await cursor.fetchall()

# Delete tenant database when they leave
await manager.delete_database("tenant-123")

# Clean up on shutdown
await manager.close()
```

Or via CLI:
```bash
turso db create my-db
turso db tokens create my-db
turso db destroy my-db --yes
```

## Example Applications

Four complete Todo apps demonstrating different query styles, each supporting **all 4 database backends**:

| Example | Port | Query Style |
|---------|------|-------------|
| [Native Fluent SQL](examples/todo_app_native/) | 7777 | Built-in fluent query builder |
| [Django-style](examples/todo_app_django_style/) | 7778 | QuerySet-like API with lookups |
| [Prisma-style](examples/todo_app_prisma_style/) | 7779 | Dict-based queries |
| [SQLAlchemy-style](examples/todo_app_sqlalchemy/) | 7780 | Declarative models with Session |

### Running Examples

```bash
cd examples/todo_app_native
uv run uvicorn app:app --reload --port 7777
```

### Runtime Database Switching

Each example app supports hot-swapping databases at runtime via the `/db` endpoint:

- **SQLite** - Local file database (configurable path)
- **PostgreSQL** - Production database (host/port/credentials)
- **Turso Embedded** - Rust-based SQLite with vector search (configurable path)
- **Turso Cloud** - Edge-hosted SQLite (URL + auth token)

Visit `http://localhost:7777/db` to switch between backends without restarting the app.

## Documentation

See [docs/usage.md](docs/usage.md) for comprehensive documentation.

## License

MIT
