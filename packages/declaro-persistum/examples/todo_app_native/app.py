"""
Todo App Demo using declaro_persistum native fluent SQL API.

NOTE: All database operations use the same code regardless of backend.
      The db.py module handles SQLite/PostgreSQL/Turso differences.

Run with: uv run uvicorn app:app --reload
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

# Import native fluent SQL API
from declaro_persistum.query.table import table, set_default_schema
from declaro_persistum.loader import load_schema

SCHEMA_DIR = Path(__file__).parent / "schema"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - initialize DB and schema on startup."""
    await init_schema(get_database())
    schema = load_schema(str(SCHEMA_DIR))
    set_default_schema(schema)
    yield


app = FastAPI(title="Todo App (Native Fluent SQL)", lifespan=lifespan)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


def get_todos_table():
    return table("todos")


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
# Todo CRUD - SAME CODE FOR ALL DATABASES
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main page with all todos."""
    async with get_connection() as conn:
        todos = get_todos_table()
        results = await (
            todos
            .select(todos.id, todos.title, todos.completed, todos.created_at)
            .order_by(todos.created_at.desc())
            .execute(conn)
        )

    return templates.TemplateResponse("index.html", {
        "request": request,
        "todos": results,
        "current_db": get_database().display_name,
    })


@app.post("/todos", response_class=HTMLResponse)
async def create_todo(request: Request, title: str = Form(...)):
    """Create a new todo."""
    todo_id = str(uuid.uuid4())

    async with get_connection() as conn:
        todos = get_todos_table()
        await todos.insert(id=todo_id, title=title, completed=0).execute(conn)

        dialect = get_database().dialect
        if dialect == "sqlite":
            await conn.commit()
        elif dialect in ("turso_cloud", "turso_embedded"):
            conn.commit()

        todo = await (
            todos
            .select(todos.id, todos.title, todos.completed)
            .where(todos.id == todo_id)
            .execute_one(conn)
        )

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo,
    })


@app.put("/todos/{todo_id}/toggle", response_class=HTMLResponse)
async def toggle_todo(request: Request, todo_id: str):
    """Toggle todo completion status."""
    async with get_connection() as conn:
        todos = get_todos_table()

        current = await (
            todos
            .select(todos.completed)
            .where(todos.id == todo_id)
            .execute_one(conn)
        )
        new_completed = 0 if current["completed"] else 1

        await (
            todos
            .update(completed=new_completed)
            .where(todos.id == todo_id)
            .execute(conn)
        )

        dialect = get_database().dialect
        if dialect == "sqlite":
            await conn.commit()
        elif dialect in ("turso_cloud", "turso_embedded"):
            conn.commit()

        todo = await (
            todos
            .select(todos.id, todos.title, todos.completed)
            .where(todos.id == todo_id)
            .execute_one(conn)
        )

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo,
    })


@app.delete("/todos/{todo_id}", response_class=HTMLResponse)
async def delete_todo(todo_id: str):
    """Delete a todo."""
    async with get_connection() as conn:
        todos = get_todos_table()
        await todos.delete().where(todos.id == todo_id).execute(conn)

        dialect = get_database().dialect
        if dialect == "sqlite":
            await conn.commit()
        elif dialect in ("turso_cloud", "turso_embedded"):
            conn.commit()

    return ""


@app.get("/todos/count", response_class=HTMLResponse)
async def get_count(request: Request):
    """Get todo counts for the footer."""
    async with get_connection() as conn:
        todos = get_todos_table()
        from declaro_persistum.query import count_
        result = await (
            todos
            .select(count_(todos.id))
            .where(todos.completed == 0)
            .execute_one(conn)
        )
        active_count = list(result.values())[0] if result else 0

    return templates.TemplateResponse("partials/todo_count.html", {
        "request": request,
        "active_count": active_count,
    })
