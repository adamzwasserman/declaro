# declaro_persistum

Pure functional SQL library with declarative schema migrations.

## Overview

A replacement for SQLAlchemy ORM and Alembic that uses:
- **Schema as Data**: TypedDict structures defined in TOML files
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

### Define Schema (TOML)

```toml
# schema/tables/users.toml
[users]
primary_key = ["id"]

[users.columns.id]
type = "uuid"
nullable = false
default = "gen_random_uuid()"

[users.columns.email]
type = "text"
nullable = false
unique = true
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
from declaro_persistum.query import table, load_default_schema

# Load schema once at startup
load_default_schema("./schema")

# Create a connection pool
pool = await ConnectionPool.postgresql("postgresql://localhost/mydb")

# Query with the fluent API
users = table("users")
async with pool.acquire() as conn:
    results = await (
        users
        .select(users.id, users.email)
        .where(users.status == "active")
        .execute(conn)
    )

await pool.close()
```

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

# Use with any backend
async with pool.acquire() as conn:
    results = await users.select().execute(conn)

await pool.close()
```

### Synchronous Pools (for Testing)

For tests that don't work well with async:

```python
from declaro_persistum import SyncConnectionPool

# SQLite (uses stdlib sqlite3)
pool = SyncConnectionPool.sqlite("./test.db")
with pool.acquire() as conn:
    conn.execute("SELECT 1")
    conn.commit()
pool.close()

# Turso (pyturso)
pool = SyncConnectionPool.turso("./test.db")

# LibSQL (Turso cloud)
pool = SyncConnectionPool.libsql("libsql://...", auth_token="...")
```

### Schema-Validated Queries

Typos caught at build time, not runtime:

```python
users = table("users")
users.emial  # AttributeError: Table 'users' has no column 'emial'
```

### Multiple Query Styles

Choose the API that feels natural:

```python
# Native fluent API
results = await users.select().where(users.active == True).execute(conn)

# Django-style
results = await users.objects.filter(status="active").execute(conn)

# Prisma-style
results = await users.prisma.find_many(where={"status": "active"})
```

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
