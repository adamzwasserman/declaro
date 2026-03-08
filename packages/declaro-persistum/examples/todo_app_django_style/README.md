# Todo App Demo (Django-style)

A simple todo application demonstrating declaro_persistum with **Django-style queries**.

## Features

- Create, toggle, and delete todos
- Real-time UI updates with HTMX (no page refreshes)
- **4 database backends**: SQLite, PostgreSQL, Turso Embedded (pyturso), Turso Cloud (libsql)
- Hot-swappable database at runtime via `/db` endpoint
- Configurable file paths for local databases (SQLite, Turso Embedded)
- Clean, responsive design
- Django ORM-style query API

## Query Style

This demo uses **Django-style** queries:

```python
from declaro_persistum.query.table import table
from declaro_persistum.loader import load_schema

schema = load_schema("./schema")
todos = table("todos", schema=schema)

# Filter with lookups
active = await todos.objects.filter(completed=0).all(db)

# Django-style lookups
recent = await todos.objects.filter(created_at__gte="2024-01-01").all(db)
search = await todos.objects.filter(title__contains="important").all(db)

# Ordering (prefix with - for DESC)
ordered = await todos.objects.order("-created_at").all(db)

# Chaining
result = await todos.objects.filter(completed=0).order("-created_at")[:10].all(db)

# Single object
todo = await todos.objects.get(db, id=todo_id)

# Count
count = await todos.objects.filter(completed=0).count(db)
```

## Setup

```bash
cd examples/todo_app_django_style
uv run uvicorn app:app --reload --port 7778
```

Open http://localhost:7778 in your browser.

## Project Structure

```
todo_app_django_style/
├── app.py              # FastAPI application with Django-style queries
├── schema/
│   └── todos.toml      # declaro_persistum schema
├── templates/
│   ├── index.html
│   └── partials/
│       ├── todo_item.html
│       └── todo_count.html
└── todos.db            # SQLite database (created on first run)
```
