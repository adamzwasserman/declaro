# declaro_persistum Usage Guide

A Functional Persistence Layer (FPL) for Python. Taking the O out of ORM. Pure functions. Declarative schema migrations. No migration files. No revision chains. Just declare what you want; the diff engine figures out the rest.

## Installation

```bash
pip install declaro_persistum

# With database drivers
pip install declaro_persistum asyncpg        # PostgreSQL
pip install declaro_persistum aiosqlite      # SQLite
pip install declaro_persistum pyturso        # Turso (embedded)
pip install declaro_persistum libsql-experimental  # LibSQL (Turso cloud)
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
    id: UUID = field(primary=True)
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
    id: UUID = field(primary=True)
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
    id: UUID = field(primary=True)
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

Define views using Pydantic models with the `@view` decorator:

```python
# models/views.py
from declaro_persistum import view

@view("active_users")
class ActiveUsersView:
    query = "SELECT id, email, name FROM users WHERE status = 'active'"
```

#### Materialized Views

Materialized views are supported on all databases:

**PostgreSQL** - Uses native `CREATE MATERIALIZED VIEW`:

```python
# models/views.py
from declaro_persistum import view

@view("user_stats")
class UserStatsView:
    query = "SELECT COUNT(*) as total, status FROM users GROUP BY status"
    materialized = True
    refresh = "on_demand"  # or "on_commit" (not yet implemented)
```

**SQLite / Turso / LibSQL** - Uses table-based emulation:

```python
# models/views.py
from declaro_persistum import view

@view("monthly_stats")
class MonthlyStatsView:
    query = "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
    materialized = True
    refresh = "manual"  # or "trigger" or "hybrid"
    depends_on = ["orders"]
    trigger_sources = ["orders"]  # Only for trigger/hybrid refresh
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
@view("user_stats")
class UserStatsView:
    query = "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
    materialized = True
    depends_on = ["orders"]  # View depends on orders table
```

For views that reference other views:

```python
# models/views.py
@view("top_users")
class TopUsersView:
    query = "SELECT * FROM user_stats WHERE order_count > 10"
    depends_on = ["user_stats"]  # Depends on another view
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

The library validates this requirement before generating `CONCURRENTLY` SQL:

```python
from declaro_persistum.applier.postgresql import validate_concurrent_refresh

# Raises ValidationError if view lacks unique index
await validate_concurrent_refresh(conn, "user_stats")

# Then safely refresh
sql = generate_refresh_materialized_view("user_stats", concurrently=True)
await conn.execute(sql)
```

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

The SQLite inspector automatically detects emulated materialized views by checking the metadata table:

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

## Connection Pool

declaro_persistum provides a unified `ConnectionPool` abstraction that works consistently across PostgreSQL, SQLite, Turso, and LibSQL.

### Creating a Pool

```python
from declaro_persistum import ConnectionPool

# PostgreSQL - wraps asyncpg's native pool
pool = await ConnectionPool.postgresql(
    "postgresql://user:pass@localhost/mydb",
    min_size=5,      # Minimum connections to keep open
    max_size=20,     # Maximum connections allowed
    acquire_timeout=30.0,  # Timeout for acquiring connection
)

# SQLite - semaphore-based limiting (connections are cheap)
pool = await ConnectionPool.sqlite(
    "./app.db",
    max_size=5,      # Max concurrent connections (WAL mode supports ~5 writers)
    acquire_timeout=30.0,
)

# Turso - embedded SQLite-compatible database (pyturso)
# Features: vector search, CDC, async I/O with io_uring
pool = await ConnectionPool.turso(
    "./app.db",
    max_size=5,
    acquire_timeout=30.0,
)

# LibSQL - Turso cloud connections (libsql-experimental)
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
    auth_token="your-token",
    max_size=10,
    acquire_timeout=30.0,
)
```

### Using the Pool

Pool is bound at `table()` creation time. No connection parameter on the caller surface:

```python
schema = load_schema("./schema")
pool = await ConnectionPool.sqlite("./app.db")

# Bind table to pool — pool is a required parameter
users = table("users", schema, pool)

# Execute — no conn parameter, connection managed internally
results = await users.select().where(users.active == True).execute()
```

Raw connection access is still available when needed (e.g. schema setup):

```python
async with pool.acquire() as conn:
    await conn.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY)")
    await conn.commit()
```

### Pool Lifecycle

```python
# Create pool at application startup
pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")

# Bind tables — no conn needed at query time
users = table("users", schema, pool)

# Use throughout application lifetime — no acquire/release on caller
results = await users.select().execute()

# Close pool at shutdown — also flushes write queue if attached
await pool.close()
```

### FastAPI Integration

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table
from declaro_persistum.loader import load_schema

_pool = None
_users = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool, _users
    schema = load_schema("./schema")
    _pool = await ConnectionPool.postgresql(
        "postgresql://localhost/mydb",
        min_size=5,
        max_size=20,
    )
    _users = table("users", schema, _pool)
    yield
    await _pool.close()

app = FastAPI(lifespan=lifespan)

@app.get("/users")
async def list_users():
    return await _users.select().where(_users.active == True).execute()
```

### Pool Properties

```python
pool.closed     # bool - Whether pool has been closed
pool.size       # int - Current/max pool size
pool.available  # int - Available connection slots
```

### Connection Methods

All connection types (PostgreSQL, SQLite, Turso, LibSQL) provide these async methods:

| Method | Description |
|--------|-------------|
| `execute(sql, params)` | Execute SQL query, returns cursor |
| `executemany(sql, params_list)` | Execute with multiple parameter sets |
| `commit()` | Commit the current transaction |
| `rollback()` | Rollback the current transaction |
| `close()` | Close the connection |

Turso and LibSQL connections also support:

| Method | Description |
|--------|-------------|
| `sync()` | Sync database state (useful for replication/CDC) |

```python
# Example: Using sync() with Turso
pool = await ConnectionPool.turso("./app.db")
async with pool.acquire() as conn:
    await conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
    await conn.commit()
    await conn.sync()  # Ensure data is synced
```

### Async-Only

declaro-persistum is async-only. `SyncConnectionPool` and all sync connection types were removed in 2026-03-08. Use `pytest-asyncio` and `asyncio.run()` for tests.

### Mirror Pool (Replication Verification)

`MirrorPool` wraps two database pools for replication verification. Writes go to both in parallel, reads compare results and log any disagreements:

```python
from declaro_persistum import ConnectionPool, MirrorPool

# Create primary and mirror pools (can be different database types)
primary = await ConnectionPool.postgresql("postgresql://primary/db")
mirror = await ConnectionPool.sqlite("./mirror.db")

# Create mirror pool
pool = MirrorPool(primary, mirror)

async with pool.acquire() as conn:
    # Writes go to both databases in parallel
    await conn.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Alice"))
    await conn.commit()

    # Reads compare results, return primary data, log disagreements
    cursor = await conn.execute("SELECT * FROM users WHERE id = ?", (1,))
    rows = await cursor.fetchall()

await pool.close()
```

When disagreement is detected, an error is logged with full context:
```
ERROR:declaro_persistum.mirror:DATA DISAGREEMENT DETECTED
  timestamp: 2025-01-15T10:30:45.123456+00:00
  sql: 'SELECT id, name FROM users WHERE id = ?'
  parameters: (1,)
  primary_row_count: 1
  mirror_row_count: 1
  primary_data: [(1, 'Alice')]
  mirror_data: [(1, 'Alicia')]
  diff: only_in_primary=[(1, 'Alice')]; only_in_mirror=[(1, 'Alicia')]
```

**Configuration Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `fail_open` | `True` | Continue with primary if mirror fails |
| `compare_on_read` | `True` | Compare SELECT results between primary and mirror |
| `logger` | `None` | Custom logger (defaults to `declaro_persistum.mirror`) |

### Pool Exceptions

```python
from declaro_persistum import (
    PoolError,           # Base exception for all pool errors
    PoolClosedError,     # Pool has been closed
    PoolExhaustedError,  # Acquire timeout (no connections available)
    PoolConnectionError, # Failed to create connection
)

try:
    async with pool.acquire() as conn:
        # ...
except PoolExhaustedError:
    # All connections in use and timeout exceeded
    pass
except PoolClosedError:
    # Tried to use pool after close()
    pass
```

## Instrumentation

Record every query's duration, op type, and success/failure.

### Enabling Instrumentation

```python
# At pool creation
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
    auth_token="...",
    instrumentation=True,
    tier_label="project",
    latency_sink="jsonl",
    latency_path="./data/db_latency.jsonl",
)

# Or post-creation with a callable sink
pool = await ConnectionPool.sqlite("./app.db")
pool.configure_instrumentation(
    tier_label="my-app",
    callable_sink=lambda record: metrics.record(record),
)
```

### LatencyRecord

Each recorded query produces a `LatencyRecord` TypedDict:

```python
{
    "ts": "2026-03-11T14:22:01-0500",  # ISO 8601
    "tier": "project",                  # Pool label
    "op": "insert",                     # select|insert|update|delete|create|alter|other
    "duration_ms": 842.31,
    "success": True,
    "sql": "INSERT INTO cards (id, name...",  # First 120 chars
    "error": "",                               # First 200 chars on failure
}
```

### Zero Overhead When Disabled

When `configure_instrumentation()` has not been called, `pool._latency_logger` is `None`. The record path is never entered — no timing, no allocations.

---

## Write Queue

For high-latency backends (Turso Cloud writes can take 750–1100ms), the write queue returns data to the caller immediately and persists in the background.

### Enabling the Write Queue

```python
pool = await ConnectionPool.libsql(
    "libsql://your-db.turso.io",
    auth_token="...",
    instrumentation=True,
    tier_label="project",
    write_queue_path="./data/pending_writes.jsonl",
    write_queue_threshold_ms=50.0,    # Queue writes that take >50ms
    write_queue_concurrency=3,        # Max concurrent drain tasks
)
```

Or post-creation:

```python
pool.configure_write_queue(
    persistence_path="./data/pending_writes.jsonl",
    threshold_ms=50.0,
    max_concurrent_drains=3,
)
```

### Usage

The queue is completely transparent. No API changes on the caller:

```python
users = table("users", schema, pool)

# Returns immediately if write takes >50ms — data returned before DB confirms
await users.insert(id=new_id, name="alice").execute()

# Reads merge pending queue entries — insert appears in results immediately
rows = await users.select().execute()
```

### How It Works

1. `execute_with_pool` races the write against the threshold: `asyncio.wait_for(asyncio.shield(task), timeout=0.05)`
2. Fast writes (under threshold): queue never touched
3. Slow writes: `enqueue()` called immediately, data returned to caller; write continues in background
4. On background write success: entry removed from queue
5. On background write failure: supervisor retries with exponential backoff (cap 60s)
6. CRITICAL log after 6 hours of continuous failure (prefix: `WRITE_QUEUE_EXHAUSTED`)
7. SELECT results merge pending entries by primary key and re-sort using original ORDER BY
8. JOIN results: pending entries matched via FK schema knowledge

### Disk Persistence

Queue is atomically persisted (tmp + rename) to a JSONL file on every change. Pending writes survive `kill -9`. On `pool.close()`, the supervisor is stopped and a final flush is attempted.

---

## Programmatic Usage

### Introspecting a Database

Each database backend has its own inspector class:

```python
from declaro_persistum.inspector.postgresql import PostgreSQLInspector
from declaro_persistum.inspector.sqlite import SQLiteInspector
from declaro_persistum.inspector.turso import TursoInspector
```

#### Basic Introspection

```python
import asyncio
import asyncpg
from declaro_persistum.inspector.postgresql import PostgreSQLInspector

async def main():
    conn = await asyncpg.connect("postgresql://localhost/mydb")
    inspector = PostgreSQLInspector()

    # Introspect all tables in the schema
    schema = await inspector.introspect(conn)

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
from declaro_persistum.inspector.sqlite import SQLiteInspector

async def introspect_sqlite():
    async with aiosqlite.connect("./app.db") as conn:
        inspector = SQLiteInspector()
        schema = await inspector.introspect(conn)
        # Same schema structure as PostgreSQL

# Turso (libsql) uses synchronous API internally
from declaro_persistum.inspector.turso import TursoInspector
import libsql_experimental as libsql

def introspect_turso():
    conn = libsql.connect("libsql://your-db.turso.io", auth_token="...")
    inspector = TursoInspector()
    # Note: TursoInspector methods are async but use sync libsql calls
    schema = asyncio.run(inspector.introspect(conn))
```

### Introspecting Views

Use `include_views=True` to introspect both tables and views:

```python
async def introspect_with_views():
    conn = await asyncpg.connect("postgresql://localhost/mydb")
    inspector = PostgreSQLInspector()

    # Returns tuple of (schema, views) when include_views=True
    schema, views = await inspector.introspect(conn, include_views=True)

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
result = diff(current, target)

print(f"Operations: {len(result['operations'])}")
for i in result["execution_order"]:
    op = result["operations"][i]
    print(f"  {op['op']} on {op['table']}")
```

### Applying Migrations

```python
from declaro_persistum.applier.postgresql import PostgreSQLApplier

applier = PostgreSQLApplier()

# Apply the diff
apply_result = await applier.apply(
    conn,
    result["operations"],
    result["execution_order"],
)

if apply_result["success"]:
    print(f"Applied {apply_result['operations_applied']} operations")
else:
    print(f"Failed: {apply_result['error']}")
```

### Query Builder

The query builder uses schema-validated dot notation. Typos like `users.emial` are caught immediately at query build time, not when the SQL hits the database.

```python
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table
from declaro_persistum.loader import load_schema

# Load schema and create pool at startup
schema = load_schema("./schema")
pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")

# Bind table proxies to pool — pool is a required parameter
users = table("users", schema, pool)
orders = table("orders", schema, pool)
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

declaro_persistum supports multiple query API styles. Choose the one that feels most natural to you.

### Django-Style API

Familiar to Django developers with QuerySet-like patterns:

```python
from declaro_persistum.query import table

# Pool is required — bound at table creation
users = table("users", schema, pool)

# Filter with lookups — no conn parameter
active = await users.objects.filter(status="active").all()

# Django-style field lookups
recent = await users.objects.filter(created_at__gte="2024-01-01").all()
search = await users.objects.filter(name__contains="alice").all()
nulls = await users.objects.filter(deleted_at__isnull=True).all()

# Comparison lookups
adults = await users.objects.filter(age__gt=18).all()
seniors = await users.objects.filter(age__gte=65).all()

# Ordering (prefix with - for DESC)
ordered = await users.objects.order("-created_at").all()

# Chaining
result = await (
    users.objects
    .filter(status="active")
    .exclude(role="banned")
    .order("-created_at")[:10]
    .all()
)

# Single object
user = await users.objects.get(id=user_id)

# Count
count = await users.objects.filter(status="active").count()
```

#### Supported Lookups

| Lookup | SQL Equivalent |
|--------|---------------|
| `field=value` | `field = value` |
| `field__gt=value` | `field > value` |
| `field__gte=value` | `field >= value` |
| `field__lt=value` | `field < value` |
| `field__lte=value` | `field <= value` |
| `field__contains=value` | `field LIKE '%value%'` |
| `field__isnull=True` | `field IS NULL` |

### Prisma-Style API

Dict-based queries familiar to Prisma/TypeScript developers:

```python
from declaro_persistum.query import table

# Pool is required — bound at table creation
users = table("users", schema, pool)

# Find many with filtering and pagination — no conn parameter
active = await users.prisma.find_many(
    where={"status": "active"},
    order_by={"created_at": "desc"},
    take=10,
    skip=0
)

# Nested where conditions
results = await users.prisma.find_many(
    where={
        "status": "active",
        "age": {"gt": 18},
        "email": {"contains": "@example.com"}
    }
)

# Find unique record
user = await users.prisma.find_one(where={"id": user_id})

# Find first matching
first = await users.prisma.find_first(
    where={"status": "active"},
    order_by={"created_at": "desc"}
)

# Create
new_user = await users.prisma.create(
    data={"id": str(uuid4()), "email": "alice@example.com", "name": "Alice"}
)

# Update
updated = await users.prisma.update(
    where={"id": user_id},
    data={"name": "New Name"}
)

# Upsert
result = await users.prisma.upsert(
    where={"email": "alice@example.com"},
    create={"id": str(uuid4()), "email": "alice@example.com", "name": "Alice"},
    update={"name": "Alice Updated"}
)

# Delete
deleted = await users.prisma.delete(where={"id": user_id})

# Count
count = await users.prisma.count(where={"status": "active"})
```

#### Supported Operators

| Operator | Example |
|----------|---------|
| Equality | `{"field": value}` |
| Greater than | `{"field": {"gt": value}}` |
| Greater or equal | `{"field": {"gte": value}}` |
| Less than | `{"field": {"lt": value}}` |
| Less or equal | `{"field": {"lte": value}}` |
| Contains | `{"field": {"contains": value}}` |
| Starts with | `{"field": {"startsWith": value}}` |
| Ends with | `{"field": {"endsWith": value}}` |

### SQLAlchemy-Style API

> **Note**: The SQLAlchemy-style API (`declarative_base`, `Column`, `Session`) is not yet implemented. This section documents the planned API surface.

Declarative models familiar to SQLAlchemy developers (planned):

```python
from declaro_persistum.query import (
    declarative_base, Column, String, Boolean, DateTime, Session
)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True)
    email = Column(String, nullable=False, unique=True)
    name = Column(String)
    status = Column(String, default="active")
    created_at = Column(DateTime, server_default="now()")

# Using Session (planned API)
async with Session(pool) as session:
    users = await session.query(User).all()
    active = await session.query(User).filter_by(status="active").all()
```

#### Supported Column Types

| Type | PostgreSQL | SQLite |
|------|------------|--------|
| `Integer` | INTEGER | INTEGER |
| `BigInteger` | BIGINT | INTEGER |
| `String(n)` | VARCHAR(n) | TEXT |
| `Text` | TEXT | TEXT |
| `Boolean` | BOOLEAN | INTEGER |
| `Float` | REAL | REAL |
| `Numeric(p,s)` | NUMERIC(p,s) | REAL |
| `DateTime` | TIMESTAMP | TEXT |
| `Date` | DATE | TEXT |
| `UUID` | UUID | TEXT |
| `JSON` / `JSONB` | JSON/JSONB | TEXT |

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

### LibSQL (Turso Cloud)

- SQLite-compatible with cloud sync
- Requires `libsql-experimental` package
- Same limitations as SQLite

```bash
export TURSO_AUTH_TOKEN="your-token"
declaro diff -c "libsql://your-db.turso.io" -d libsql
```

## Multi-Tenant Architecture (Turso Cloud)

Turso cloud is designed for one database per tenant. The `TursoCloudManager` handles database provisioning, token management, and connection pooling for multi-tenant applications.

### Configuration

```python
import os
from declaro_persistum import TursoCloudManager

# Environment variables (shared across all tenants)
manager = TursoCloudManager(
    org=os.environ["TURSO_ORG"],          # e.g., "mycompany"
    api_token=os.environ["TURSO_API_TOKEN"],  # Platform API token
)
```

### Tenant Lifecycle

```python
# Create database for new tenant
db_info = await manager.create_database("tenant-123")
print(f"Created: {db_info['Hostname']}")

# Get connection pool for tenant
pool = await manager.get_pool("tenant-123")
async with pool.acquire() as conn:
    # Run queries against tenant's isolated database
    cursor = await conn.execute("SELECT 1")
    result = await cursor.fetchone()

# Delete tenant database when they leave
await manager.delete_database("tenant-123")
```

### Idempotent Operations

```python
# Get or create - safe to call multiple times
db_info = await manager.get_or_create_database("tenant-456")

# Check if database exists
if await manager.database_exists("tenant-456"):
    pool = await manager.get_pool("tenant-456")

# List all tenant databases
databases = await manager.list_databases()
for db in databases:
    print(f"  {db['Name']}: {db['Hostname']}")
```

### FastAPI Integration

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request
from declaro_persistum import TursoCloudManager

manager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    manager = TursoCloudManager(
        org=os.environ["TURSO_ORG"],
        api_token=os.environ["TURSO_API_TOKEN"],
    )
    yield
    await manager.close()

app = FastAPI(lifespan=lifespan)

async def get_tenant_conn(request: Request):
    """Get connection for authenticated tenant."""
    tenant_id = request.state.tenant_id  # From auth middleware
    pool = await manager.get_pool(tenant_id)
    async with pool.acquire() as conn:
        yield conn

@app.get("/users")
async def list_users(conn=Depends(get_tenant_conn)):
    cursor = await conn.execute("SELECT * FROM users")
    return await cursor.fetchall()
```

### Database Credentials by Backend

Each backend requires different environment variables:

| Backend | Required Credentials | Example Env Vars |
|---------|---------------------|------------------|
| **PostgreSQL** | Connection URL | `DATABASE_URL=postgresql://user:pass@host:5432/dbname` |
| **SQLite** | File path only | `DATABASE_PATH=./app.db` |
| **Turso (embedded)** | File path only | `DATABASE_PATH=./app.db` |
| **LibSQL (cloud)** | URL + Auth token | `TURSO_ORG`, `TURSO_API_TOKEN` (see multi-tenant pattern) |

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
    PoolError,           # Connection pool error base
    PoolClosedError,     # Pool has been closed
    PoolExhaustedError,  # Acquire timeout
    PoolConnectionError, # Failed to create connection
)
```

#### Exception Usage Examples

```python
from declaro_persistum.exceptions import (
    DeclaroError, MigrationError, AmbiguityError, DriftError
)

try:
    result = await applier.apply(conn, operations, order)
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
    clear_decisions,   # Clear decisions after successful migration
)
```

#### Directory Structure

```
models/
├── users.py          # Pydantic models with @table decorator
├── orders.py
├── views.py          # View definitions with @view decorator
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

result = diff(current_schema, target_schema, decisions=decisions)

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
result = diff(current, target)

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
from declaro_persistum.applier.postgresql import PostgreSQLApplier
from declaro_persistum.applier.sqlite import SQLiteApplier
from declaro_persistum.applier.turso import TursoApplier
```

#### Applying Migrations

```python
applier = PostgreSQLApplier()

# Check transaction mode
mode = applier.get_transaction_mode()
print(f"Mode: {mode}")  # "all_or_nothing" (PostgreSQL) or "per_operation" (SQLite)

# Apply operations
result = await applier.apply(
    conn,
    diff_result['operations'],
    diff_result['execution_order'],
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
result = await applier.apply(
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
from declaro_persistum.applier.postgresql import PostgreSQLApplier

applier = PostgreSQLApplier()

# Generate SQL for a single operation
sql = applier.generate_operation_sql({
    "op": "add_column",
    "table": "users",
    "details": {
        "column": "phone",
        "definition": {"type": "text", "nullable": True}
    }
})
print(sql)  # ALTER TABLE "users" ADD COLUMN "phone" text

# Generate all SQL in execution order
sql_list = applier.generate_sql(operations, execution_order)
for sql in sql_list:
    print(sql)
```

#### View SQL Generation

```python
from declaro_persistum.applier.postgresql import (
    generate_create_view,
    generate_drop_view,
    generate_refresh_materialized_view,
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

# Refresh materialized view
sql = generate_refresh_materialized_view("user_stats", concurrently=True)
# REFRESH MATERIALIZED VIEW CONCURRENTLY "user_stats"
```

### Validator

Validate schema before applying:

```python
from declaro_persistum.validator import validate_schema, validate_references

# Validate schema structure
errors = validate_schema(schema)
for error in errors:
    print(f"Error: {error}")

# Validate foreign key references
# Raises ValidationError if references point to non-existent tables/columns
validate_references(schema)
```

## Troubleshooting

### "Unsupported connection type"

Ensure you have the correct database driver installed:
```bash
pip install asyncpg      # PostgreSQL
pip install aiosqlite    # SQLite
pip install pyturso      # Turso (embedded)
pip install libsql-experimental  # LibSQL (Turso cloud)
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
