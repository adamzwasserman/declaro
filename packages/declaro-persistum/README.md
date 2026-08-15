# declaro_persistum

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

- **`open_sqlite` / `open_postgresql` / `open_turso`** each return the same `Database` value, so `reading`, `writing` and the query layer are written once and work on all three.
- The **compatibility layer** is not a bag of tricks. It supplies capabilities a given database lacks — arrays, maps, ranges, enums, hierarchies, materialized views, CHECK constraints — so that the difference between databases never reaches your application.

Where a database genuinely cannot do something, the gap is closed here rather than handed to the caller.

### 3. Migrations that survive a team

Alembic becomes close to unusable once several people work at once, and the reason is structural: it records a linear chain of revisions. Two developers branch from main, each generates a migration, and now two revisions claim the same parent. Someone resolves that by hand, every time.

There is no chain here to collide. Each branch carries its own declared schema, and the difference is computed against the real database at the moment migrations run. Two branches merge the way ordinary code merges — in the models file — and the resulting schema is simply whatever the merged declaration says.

## What is subordinate

Named explicitly, so that their size in the codebase is not mistaken for their importance:

- **The query builder** is an expression of priority 1, not a goal of its own. It exists so that queries can be declared rather than concatenated. It offers one style. The Django-like, Prisma-like and SQLAlchemy-like surfaces were on-ramps for teams arriving from those tools and are gone; `264cedd` and `9ff59e7` deleted them.
- **The three `open_*` functions** serve priority 2.
- **The compatibility layer** serves priority 2.
- **Pure functions, TypedDicts and the absence of hidden state** are how this code is written, not what it is for. They make the three priorities achievable and testable; they are not themselves the architecture.

## Installation

```bash
pip install declaro_persistum

pip install declaro_persistum asyncpg     # PostgreSQL
pip install declaro_persistum aiosqlite   # SQLite
pip install declaro_persistum pyturso     # Turso, embedded or cloud
```

Turso Cloud needs no extra package. It is `pyturso` pointed at a primary.

## Quick Start

### Declare the schema

```python
# models/user.py
from uuid import UUID

from pydantic import BaseModel

from declaro_persistum import field, table

@table("users")
class User(BaseModel):
    id: UUID = field(primary_key=True)
    email: str = field(unique=True)
```

`@table` sets the name the loader looks for. `field()` carries what a column MEANS: `primary_key`, `unique`, `references`, `on_delete`, `check`, `default`, `db_type`. A misspelled property is a `TypeError` at import rather than a column property that silently vanishes.

### Run the migration

```bash
declaro diff -c postgresql://localhost/mydb        # show the proposed change
declaro apply -c postgresql://localhost/mydb       # apply it
declaro generate -c postgresql://localhost/mydb    # SQL only, nothing executed
declaro validate -s ./schema                       # check the model files
```

### Query

```python
import asyncio
from uuid import uuid4

from declaro_persistum import (
    close, execute, insert, open_sqlite, reading, select, writing,
)

async def main():
    db = await open_sqlite("./app.db", shutdown="exit_immediately", busy_timeout_s=5.0)

    async with writing(db) as conn:
        await execute(
            insert("users", {"id": str(uuid4()), "email": "alice@example.com"}), conn
        )
        await conn.commit()

    async with reading(db) as conn:
        rows = await execute(
            select("id", "email", from_table="users",
                   where={"email": "alice@example.com"}),
            conn,
        )

    await close(db)
    return rows

asyncio.run(main())
```

`select`, `insert`, `update`, `delete` and `raw` build a `Query`. `execute(query, conn)` runs it. A query is data until something executes it, which is why it can be built, inspected and tested without a database.

**`writing` commits when the block ends cleanly, and rolls back if it raises.** The `await conn.commit()` above is therefore optional; a second commit is harmless, so writing it explicitly still works. It did NOT commit until 2026-08-14, and a block that forgot lost its write and reported nothing.

## Opening a Database

```python
db = await open_sqlite("./app.db", shutdown="exit_immediately", busy_timeout_s=5.0)
db = await open_postgresql(os.environ["DATABASE_URL"],
                           shutdown="exit_immediately", busy_timeout_s=5.0)
db = await open_turso("./app.db", shutdown="exit_immediately")

# Turso Cloud: a local replica kept in sync with a cloud primary.
db = await open_turso(
    "./replica.db",
    primary="https://your-db-org.turso.io",
    token=os.environ["TURSO_AUTH_TOKEN"],
    shutdown="replicate",
)
```

`Database` is a TypedDict, not an object with methods. `reading(db)`, `writing(db)` and `close(db)` are functions that take one.

There is no pool, and that is a constraint rather than an omission. A pool exposed as a surface is mutable state with no determinable owner, which is why every conditional about MVCC used to end up inside it: with no single owner, a branch had no outside to live in. Measured on this codebase 2026-08-11: L1.18b reported the pool's holder fields as `unresolved, drives a decision`.

**`shutdown` is required and has no default.** It says what happens to writes that have not reached the primary when the process stops: `"replicate"` waits for them, `"exit_immediately"` abandons them. Which one is right is a property of the deployment, and a default here would pick one silently. `busy_timeout_s` is required for the same reason.

## Features

### Enum support via Literal types

Declare a column as a `Literal` and the migration builds a lookup table with a foreign key, so the constraint holds identically on every backend:

```python
from typing import Literal

OrderStatus = Literal["pending", "confirmed", "shipped", "delivered"]

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary_key=True)
    status: OrderStatus = "pending"
```

The apply path expands it through `expand_schema_enums` into:

```sql
CREATE TABLE "orders" (
    "id" TEXT PRIMARY KEY NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'pending' REFERENCES "_dp_enum_orders_status"("value")
);

CREATE TABLE "_dp_enum_orders_status" ("value" TEXT PRIMARY KEY NOT NULL);
```

Adding or removing values is a migration like any other. The expansion happens in `apply_migrations_async`, not in the loader: calling `diff` on a loaded schema directly gives a plain `TEXT` column, because `literal_values` is still sitting on it unexpanded.

### One column vocabulary, one spelling per engine

`db_type` says what a column means. How it is spelled is the engine's business:

| Declared | PostgreSQL | SQLite / Turso |
|----------|------------|----------------|
| `uuid` | `uuid` | `TEXT` |
| `timestamptz` | `timestamptz` | `TEXT` |
| `jsonb` | `jsonb` | `TEXT` |
| `boolean` | `boolean` | `INTEGER` |
| `numeric(10,2)` | `numeric(10,2)` | `REAL` |
| `bytea` | `bytea` | `BLOB` |

`map_type(declared, dialect)` does the translation and takes the dialect as a required argument. An unknown dialect raises rather than quietly receiving PostgreSQL SQL.

### The write queue

A waiting room in front of the WAL, for callers who arrive at the same instant. The WAL is already the queue, since a write is durable once it is in the log, so the only job left is to hand overlapping callers to the log in order.

```python
from declaro_persistum import (
    collect, deposit, migrating, new_room, start_crew, stop_crew,
)

async with migrating(db) as conn:       # DDL takes its own WAL connection
    await conn.execute("CREATE TABLE hits (id INTEGER)")
    await conn.commit()

room = new_room()
crew = await start_crew(room, db, size=4, retry=retry, idle_s=0.05)

tickets = [deposit(room, {"sql": "INSERT INTO hits VALUES (?)", "params": (i,)})
           for i in range(20)]
receipts = await asyncio.gather(*(collect(room, t) for t in tickets))

await stop_crew(crew)
```

`deposit` returns a ticket immediately; `collect` waits for that write to land. Nothing is stored: the room is empty except during the microseconds when callers overlap, so there is no pending list to survive a failure and every write has a caller holding its ticket.

`migrating` is a context manager because the journal mode belongs to the FILE, not to a connection. It forces WAL for the DDL and gives the mode back on exit. Leaving the database in WAL used to undo the mode settled at open, silently.

`start_crew` opens all `size` connections before it returns, so a connection that cannot open raises to you rather than killing one drainer quietly. On this laptop `size` above 1 opens cleanly but cannot write concurrently: pyturso is thread-per-connection with a blocking driver, and two drainers on one local file hit the engine's busy-wait inside a worker thread. The crew's throughput numbers were measured on Render.

DDL does not go through the crew, and `declaro apply` does not choose its own connection either: the door is a field on the `Database`, settled at open. A local Turso database sends DDL through `migrating`, because a table created on an MVCC connection is invisible to any connection that has already read. A replicated one sends it through `writing`, because only the held connection reaches the primary. Migration must finish before a crew starts, and on a local database it now says so out loud: `migrating` needs the file to itself and raises "database is locked" rather than migrating around a live reader.

### Latency instrumentation

`build_record`, `emit_record`, `format_jsonl` and `classify_sql` in `instrumentation.py` turn a timed call into a `LatencyRecord`: `ts`, `tier`, `op`, `duration_ms`, `success`, `sql`, `error`.

**Nothing in the package calls them.** There is no `instrumentation=True` switch and no automatic timing; you time the call and build the record yourself. They are pure functions, so they cost nothing when you do not.

## Supported Databases

- PostgreSQL, via asyncpg
- SQLite, via aiosqlite
- Turso, via pyturso: the embedded engine, or a local replica synced to a cloud primary

All three answer to the same API, which is priority 2 above. Moving between them is configuration, not a code change.

libsql is gone. Turso Cloud is reached with `open_turso(path, primary=..., token=...)`. A `libsql://` URL is still a valid Turso Cloud address, since that is Turso's own URL scheme rather than the removed libsql package.

## Database Credentials

| Backend | Required | Example |
|---------|----------|---------|
| PostgreSQL | connection URL | `DATABASE_URL=postgresql://user:pass@host:5432/dbname` |
| SQLite | file path | `DATABASE_PATH=./app.db` |
| Turso, embedded | file path | `DATABASE_PATH=./app.db` |
| Turso Cloud | replica path, primary URL, token | `DATABASE_PATH`, `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN` |

Provisioning a Turso Cloud database is a platform API call and this package does not make it. Use the Turso CLI:

```bash
turso db create my-db
turso db tokens create my-db
turso db destroy my-db --yes
```

For one database per tenant, hold one `Database` per tenant in a dict. See [docs/usage.md](docs/usage.md).

## Example Applications

Three Todo apps, each running on all supported backends:

| Example | Port |
|---------|------|
| [`examples/todo_app_native/`](examples/todo_app_native/) | 7777 |
| [`examples/todo_app_django_style/`](examples/todo_app_django_style/) | 7778 |
| [`examples/todo_app_prisma_style/`](examples/todo_app_prisma_style/) | 7779 |

```bash
cd examples/todo_app_native
uv run uvicorn app:app --reload --port 7777
```

`examples/todo_app_sqlalchemy/` is listed in older docs as a fourth app on port 7780. It holds templates and a stale `todos.db` and no Python at all, and the SQLAlchemy-style API it demonstrated was deleted in `9ff59e7`.

Each app switches backend at runtime through its `/db` endpoint at `http://localhost:7777/db`.

## Documentation

[docs/usage.md](docs/usage.md) is the full reference.

## License

MIT
