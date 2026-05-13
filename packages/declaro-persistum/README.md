# declaro_persistum

Pure functional SQL library with declarative schema migrations.

## Overview

A replacement for SQLAlchemy ORM and Alembic that uses:
- **Schema as Data**: Pydantic models with `@table` decorator
- **State Diffing**: Migrations computed by diffing desired state vs actual database state
- **Pure Functions**: No sessions, no identity maps, no hidden state
- **Branch-Friendly**: No linear revision chain; each branch carries its own schema state

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

Declaro is part of a larger functional‑Python stack that shuns hidden state and prefers pure functions.
If you haven't read it yet, the [Declaro Manifesto](../../MANIFESTO.md) lays out the fundamental ideas (banana/monkey/jungle, caching policy, anti‑OOP, etc.).

This package is the persistence layer; it provides a **polymorphic facade** over SQLite, PostgreSQL, Turso, and LibSQL.
Caching inside this package is intentionally narrow (pools, schemas, prepared statements) – any application‑specific result caching belongs in an adjacent package such as `tablix` or in your own code.

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

# LibSQL (Turso cloud connections)
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
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

Choose the API that feels natural:

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
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
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
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
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
- Turso (via pyturso) - embedded SQLite-compatible with vector search & CDC
- LibSQL (via libsql-experimental) - Turso cloud connections

## Database Credentials

**Each database backend requires different environment variables.** Client applications must handle these differences when configuring connections.

| Backend | Required Credentials | Example Env Vars |
|---------|---------------------|------------------|
| **PostgreSQL** | Connection URL | `DATABASE_URL=postgresql://user:pass@host:5432/dbname` |
| **SQLite** | File path only | `DATABASE_PATH=./app.db` |
| **Turso (embedded)** | File path only | `DATABASE_PATH=./app.db` |
| **LibSQL (cloud)** | URL + Auth token | See multi-tenant pattern below |

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
