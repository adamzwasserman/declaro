"""
Todo App Demo using declaro_persistum SQLAlchemy-style queries.

This demonstrates the SQLAlchemy-style query API:
    session.query(Model).filter_by(completed=0).order_by("-created_at").all()
    session.add(Model(...))
    session.delete(instance)
    session.commit()

Run with: uv run uvicorn app:app --reload --port 7780
"""

import uuid
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# Database abstraction - handles SQLite, PostgreSQL, Turso Cloud, Turso Embedded
from db import (
    get_connection,
    get_database,
    set_database,
    init_schema,
    test_connection,
    check_driver_available,
    get_sqlite_config,
    get_turso_embedded_config,
    get_postgres_config,
    get_turso_cloud_config,
    DEFAULT_SQLITE_PATH,
    DEFAULT_TURSO_EMBEDDED_PATH,
)

# SQLAlchemy-compatible imports from declaro_persistum
from declaro_persistum.query import (
    declarative_base,
    Column,
    String,
    Boolean,
    DateTime,
    Session,
)

SCHEMA_DIR = Path(__file__).parent / "schema"

# Create declarative base (SQLAlchemy-style)
Base = declarative_base()


# Define model (SQLAlchemy-style)
class Todo(Base):
    """Todo model using SQLAlchemy-style column definitions."""
    __tablename__ = "todos"

    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    completed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default="datetime('now')")
    updated_at = Column(DateTime, server_default="datetime('now')")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - initialize DB on startup."""
    await init_schema(get_database())
    yield


app = FastAPI(title="Todo App (SQLAlchemy-style)", lifespan=lifespan)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


# =============================================================================
# Database Selection Endpoints
# =============================================================================

@app.get("/db", response_class=HTMLResponse)
async def db_settings(request: Request):
    """Show database selection page."""
    current = get_database()
    sqlite_ok, sqlite_msg = check_driver_available("sqlite")
    pg_ok, pg_msg = check_driver_available("postgresql")
    turso_cloud_ok, turso_cloud_msg = check_driver_available("turso_cloud")
    turso_embedded_ok, turso_embedded_msg = check_driver_available("turso_embedded")

    return templates.TemplateResponse("db_select.html", {
        "request": request,
        "current_db": current,
        "sqlite_available": sqlite_ok,
        "sqlite_message": sqlite_msg,
        "sqlite_default_path": DEFAULT_SQLITE_PATH,
        "postgres_available": pg_ok,
        "postgres_message": pg_msg,
        "turso_cloud_available": turso_cloud_ok,
        "turso_cloud_message": turso_cloud_msg,
        "turso_embedded_available": turso_embedded_ok,
        "turso_embedded_message": turso_embedded_msg,
        "turso_embedded_default_path": DEFAULT_TURSO_EMBEDDED_PATH,
    })


@app.post("/db/sqlite", response_class=HTMLResponse)
async def use_sqlite(request: Request, database_path: str = Form(DEFAULT_SQLITE_PATH)):
    """Switch to SQLite."""
    config = get_sqlite_config(database_path)
    success, message = await test_connection(config)

    if success:
        set_database(config)
        await init_schema(config)

    return templates.TemplateResponse("db_status.html", {
        "request": request,
        "success": success,
        "message": message if not success else "Switched to SQLite",
        "db_name": config.display_name,
    })


@app.post("/db/postgres", response_class=HTMLResponse)
async def use_postgres(
    request: Request,
    host: str = Form("localhost"),
    port: int = Form(5432),
    user: str = Form("postgres"),
    password: str = Form(""),
    database: str = Form("todos"),
):
    """Switch to PostgreSQL."""
    config = get_postgres_config(host, port, user, password, database)
    success, message = await test_connection(config)

    if success:
        set_database(config)
        await init_schema(config)

    return templates.TemplateResponse("db_status.html", {
        "request": request,
        "success": success,
        "message": message,
        "db_name": config.display_name if success else "PostgreSQL",
    })


@app.post("/db/turso_cloud", response_class=HTMLResponse)
async def use_turso_cloud(
    request: Request,
    url: str = Form(...),
    auth_token: str = Form(""),
):
    """Switch to Turso Cloud (libsql)."""
    import os
    if auth_token:
        os.environ["TURSO_AUTH_TOKEN"] = auth_token

    config = get_turso_cloud_config(url, auth_token)
    success, message = await test_connection(config)

    if success:
        set_database(config)
        await init_schema(config)

    return templates.TemplateResponse("db_status.html", {
        "request": request,
        "success": success,
        "message": message,
        "db_name": config.display_name if success else "Turso Cloud",
    })


@app.post("/db/turso_embedded", response_class=HTMLResponse)
async def use_turso_embedded(request: Request, database_path: str = Form(DEFAULT_TURSO_EMBEDDED_PATH)):
    """Switch to Turso Embedded (pyturso)."""
    config = get_turso_embedded_config(database_path)
    success, message = await test_connection(config)

    if success:
        set_database(config)
        await init_schema(config)

    return templates.TemplateResponse("db_status.html", {
        "request": request,
        "success": success,
        "message": message if not success else "Switched to Turso Embedded",
        "db_name": config.display_name,
    })


# =============================================================================
# Todo CRUD - SAME CODE FOR ALL DATABASES (SQLAlchemy-style)
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main page with all todos."""
    async with get_connection() as conn:
        async with Session(conn) as session:
            # SQLAlchemy-style: session.query(Model).order_by(...).all()
            todos = await session.query(Todo).order_by("-created_at").all()

    return templates.TemplateResponse("index.html", {
        "request": request,
        "todos": todos,
        "current_db": get_database().display_name,
    })


@app.post("/todos", response_class=HTMLResponse)
async def create_todo(request: Request, title: str = Form(...)):
    """Create a new todo and return the todo item HTML."""
    todo_id = str(uuid.uuid4())

    async with get_connection() as conn:
        async with Session(conn) as session:
            # SQLAlchemy-style: create model instance and add to session
            todo = Todo(id=todo_id, title=title, completed=False)
            session.add(todo)
            await session.commit()

            # SQLAlchemy-style: session.query(Model).filter_by(...).first()
            todo_data = await session.query(Todo).filter_by(id=todo_id).first()

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo_data,
    })


@app.put("/todos/{todo_id}/toggle", response_class=HTMLResponse)
async def toggle_todo(request: Request, todo_id: str):
    """Toggle todo completion status."""
    async with get_connection() as conn:
        async with Session(conn) as session:
            # SQLAlchemy-style: query to get current state
            current = await session.query(Todo).filter_by(id=todo_id).first()
            new_completed = 0 if current["completed"] else 1

            # SQLAlchemy 2.0-style: session.execute() with update
            dialect = get_database().dialect
            if dialect == "sqlite":
                await conn.execute(
                    "UPDATE todos SET completed = ?, updated_at = datetime('now') WHERE id = ?",
                    (new_completed, todo_id)
                )
            elif dialect == "postgresql":
                await conn.execute(
                    "UPDATE todos SET completed = $1, updated_at = now() WHERE id = $2",
                    new_completed, todo_id
                )
            elif dialect in ("turso_cloud", "turso_embedded"):
                conn.execute(
                    "UPDATE todos SET completed = ?, updated_at = datetime('now') WHERE id = ?",
                    (new_completed, todo_id)
                )

            await session.commit()

            # SQLAlchemy-style: fetch updated record
            todo = await session.query(Todo).filter_by(id=todo_id).first()

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo,
    })


@app.delete("/todos/{todo_id}", response_class=HTMLResponse)
async def delete_todo(todo_id: str):
    """Delete a todo."""
    async with get_connection() as conn:
        async with Session(conn) as session:
            # SQLAlchemy-style: fetch then delete
            todo = await session.query(Todo).filter_by(id=todo_id).first()
            if todo:
                # Create instance to delete
                instance = Todo(**todo)
                session.delete(instance)
                await session.commit()

    return ""  # Return empty to remove the element


@app.get("/todos/count", response_class=HTMLResponse)
async def get_count(request: Request):
    """Get todo counts for the footer."""
    async with get_connection() as conn:
        async with Session(conn) as session:
            # SQLAlchemy-style: session.query(Model).filter_by(...).count()
            active_count = await session.query(Todo).filter_by(completed=0).count()

    return templates.TemplateResponse("partials/todo_count.html", {
        "request": request,
        "active_count": active_count,
    })
