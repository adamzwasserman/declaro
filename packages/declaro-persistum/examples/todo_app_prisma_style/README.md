# Todo App Demo (Prisma-style)

A simple todo application demonstrating declaro_persistum with **Prisma-style queries**.

## Features

- Create, toggle, and delete todos
- Real-time UI updates with HTMX (no page refreshes)
- **4 database backends**: SQLite, PostgreSQL, Turso Embedded (pyturso), Turso Cloud (libsql)
- Hot-swappable database at runtime via `/db` endpoint
- Configurable file paths for local databases (SQLite, Turso Embedded)
- Clean, responsive design
- Prisma-style dict-based query API

## Query Style

This demo uses **Prisma-style** queries:

```python
from declaro_persistum import ConnectionPool
from declaro_persistum.query.table import table
from declaro_persistum.loader import load_schema

schema = load_schema("./schema")
pool = await ConnectionPool.sqlite("./todos.db")

# Pool bound at table creation — no connection on the caller surface
todos = table("todos", schema, pool)

# Find many with where, order, pagination — no conn parameter
active = await todos.prisma.find_many(
    where={"completed": 0},
    order={"created_at": "desc"},
    take=10,
    skip=0
)

# Find one by unique field
todo = await todos.prisma.find_one(where={"id": todo_id})

# Find first matching
first = await todos.prisma.find_first(
    where={"completed": 0},
    order={"created_at": "desc"}
)

# Create
new_todo = await todos.prisma.create(
    data={"id": str(uuid4()), "title": "New task"}
)

# Update
updated = await todos.prisma.update(
    where={"id": todo_id},
    data={"completed": True}
)

# Delete
deleted = await todos.prisma.delete(where={"id": todo_id})

# Count
count = await todos.prisma.count(where={"completed": 0})

# Upsert
result = await todos.prisma.upsert(
    where={"id": todo_id},
    create={"id": todo_id, "title": "New"},
    update={"title": "Updated"}
)
```

## Setup

```bash
cd examples/todo_app_prisma_style
uv run uvicorn app:app --reload --port 7779
```

Open http://localhost:7779 in your browser.

## Project Structure

```
todo_app_prisma_style/
├── app.py              # FastAPI application with Prisma-style queries
├── schema/
│   └── todos.toml      # declaro_persistum schema
├── templates/
│   ├── index.html
│   └── partials/
│       ├── todo_item.html
│       └── todo_count.html
└── todos.db            # SQLite database (created on first run)
```
