# Todo App Demo (Native Fluent SQL)

A simple todo application demonstrating declaro_persistum with **native fluent SQL API**.

## Features

- Create, toggle, and delete todos
- Real-time UI updates with HTMX (no page refreshes)
- **4 database backends**: SQLite, PostgreSQL, Turso Embedded (pyturso), Turso Cloud (libsql)
- Hot-swappable database at runtime via `/db` endpoint
- Configurable file paths for local databases (SQLite, Turso Embedded)
- Clean, responsive design
- Native fluent SQL-like query builder

## Query Style

This demo uses the **native fluent SQL** API - declaro_persistum's built-in query builder:

```python
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table, count_
from declaro_persistum.loader import load_schema

schema = load_schema("./schema")
pool = await ConnectionPool.sqlite("./todos.db")

# Pool bound at table creation — no connection on the caller surface
todos = table("todos", schema, pool)

# Select with where and order — no conn parameter
results = await (
    todos
    .select(todos.id, todos.title, todos.completed)
    .where(todos.completed == 0)
    .order_by(todos.created_at.desc())
    .execute()
)

# Insert
await todos.insert(id=todo_id, title=title, completed=0).execute()

# Update with where
await (
    todos
    .update(completed=1)
    .where(todos.id == todo_id)
    .execute()
)

# Delete with where
await todos.delete().where(todos.id == todo_id).execute()

# Aggregate functions
result = await (
    todos
    .select(count_(todos.id))
    .where(todos.completed == 0)
    .execute_one()
)
```

## Setup

```bash
cd examples/todo_app_native
uv run uvicorn app:app --reload --port 7777
```

Open http://localhost:7777 in your browser.

## Project Structure

```
todo_app_native/
├── app.py              # FastAPI application with native fluent SQL
├── models/
│   └── todos.py        # Pydantic models with @table decorator
├── templates/
│   ├── index.html
│   └── partials/
│       ├── todo_item.html
│       └── todo_count.html
└── todos.db            # SQLite database (created on first run)
```
