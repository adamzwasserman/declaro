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
from declaro_persistum.query import table, set_default_schema, count_

todos = table("todos")

# Select with where and order
results = await (
    todos
    .select(todos.id, todos.title, todos.completed)
    .where(todos.completed == 0)
    .order_by(todos.created_at.desc())
    .execute(conn)
)

# Insert
await todos.insert(id=todo_id, title=title, completed=0).execute(conn)

# Update with where
await (
    todos
    .update(completed=1)
    .where(todos.id == todo_id)
    .execute(conn)
)

# Delete with where
await todos.delete().where(todos.id == todo_id).execute(conn)

# Aggregate functions
result = await (
    todos
    .select(count_(todos.id))
    .where(todos.completed == 0)
    .execute_one(conn)
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
├── schema/
│   └── todos.toml      # declaro_persistum schema
├── templates/
│   ├── index.html
│   └── partials/
│       ├── todo_item.html
│       └── todo_count.html
└── todos.db            # SQLite database (created on first run)
```
