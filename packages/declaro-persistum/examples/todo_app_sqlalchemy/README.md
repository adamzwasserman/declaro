# Todo App Demo (SQLAlchemy-style)

A simple todo application demonstrating declaro_persistum with **SQLAlchemy-style queries**.

## Features

- Create, toggle, and delete todos
- Real-time UI updates with HTMX (no page refreshes)
- **4 database backends**: SQLite, PostgreSQL, Turso Embedded (pyturso), Turso Cloud (libsql)
- Hot-swappable database at runtime via `/db` endpoint
- Configurable file paths for local databases (SQLite, Turso Embedded)
- Clean, responsive design
- SQLAlchemy ORM-style query API with declarative models

## Query Style

This demo uses **SQLAlchemy-style** queries:

```python
from declaro_persistum.query import (
    declarative_base, Column, String, Boolean, DateTime, Session
)

Base = declarative_base()

class Todo(Base):
    __tablename__ = "todos"
    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    completed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default="now()")

# Using Session (SQLAlchemy-style)
async with Session(db) as session:
    # Query all
    todos = await session.query(Todo).all()

    # Filter
    active = await session.query(Todo).filter_by(completed=False).all()

    # Order
    recent = await session.query(Todo).order_by("-created_at").all()

    # Single object
    todo = await session.query(Todo).filter_by(id=todo_id).first()

    # Count
    count = await session.query(Todo).filter_by(completed=False).count()

    # Create
    new_todo = Todo(id=uuid4(), title="New task")
    session.add(new_todo)
    await session.commit()
```

## Setup

```bash
cd examples/todo_app_sqlalchemy
uv run uvicorn app:app --reload --port 7780
```

Open http://localhost:7780 in your browser.

## Project Structure

```
todo_app_sqlalchemy/
├── app.py              # FastAPI application with SQLAlchemy-style queries
├── schema/
│   └── todos.toml      # declaro_persistum schema
├── templates/
│   ├── index.html
│   └── partials/
│       ├── todo_item.html
│       └── todo_count.html
└── todos.db            # SQLite database (created on first run)
```
