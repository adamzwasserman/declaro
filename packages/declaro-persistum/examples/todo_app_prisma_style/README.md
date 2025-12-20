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
from declaro_persistum.query.table import table, set_default_schema

todos = table("todos")

# Find many with where, order, pagination
active = await todos.prisma.find_many(
    db,
    where={"completed": 0},
    order={"created_at": "desc"},
    take=10,
    skip=0
)

# Find one by unique field
todo = await todos.prisma.find_one(db, where={"id": todo_id})

# Find first matching
first = await todos.prisma.find_first(
    db,
    where={"completed": 0},
    order={"created_at": "desc"}
)

# Create
new_todo = await todos.prisma.create(
    db,
    data={"id": str(uuid4()), "title": "New task"}
)

# Update
updated = await todos.prisma.update(
    db,
    where={"id": todo_id},
    data={"completed": True}
)

# Delete
deleted = await todos.prisma.delete(db, where={"id": todo_id})

# Count
count = await todos.prisma.count(db, where={"completed": 0})

# Upsert
result = await todos.prisma.upsert(
    db,
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
