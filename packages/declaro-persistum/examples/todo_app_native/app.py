"""Todo app demo. One query API, four database backends.

THIS FILE USED TO ADVERTISE A fluent SQL API:

    todos.select(todos.id).where(todos.id == x).execute(conn)

None of those surfaces exists. They were deleted deliberately -- the Django and
Prisma ones in 264cedd, the SQLAlchemy layer in 9ff59e7 -- and `docs/usage.md`
records the decision: "There are none, and there is one style left." The three
example apps were not updated with it, so every data route here raised

    AttributeError: 'dict' object has no attribute 'delete'

until 2026-08-17. Listing, creating, toggling and deleting a todo all failed,
in all three apps, measured by calling the route functions.

They use the one API the package has now: `select`, `insert`, `update` and
`delete` build a query, `execute` / `execute_one` / `execute_scalar` run it.
That makes the three apps identical from `index` onward, which is the honest
outcome of having one query API and is why their names no longer mean what
they used to.

Every database operation is the same whatever the backend. `db.py` holds the
SQLite, PostgreSQL, Turso Cloud and Turso Embedded differences, as rows in a
table rather than as branches.

Run with: uv run uvicorn app:app --reload --port 7777
"""

import uuid
from contextlib import asynccontextmanager
from pathlib import Path

# Database abstraction - handles SQLite, PostgreSQL, Turso Cloud, Turso Embedded
from db import (
    DEFAULT_SQLITE_PATH,
    DEFAULT_TURSO_EMBEDDED_PATH,
    check_driver_available,
    commit,
    get_connection,
    get_database,
    get_postgres_config,
    get_sqlite_config,
    get_turso_cloud_config,
    get_turso_embedded_config,
    init_schema,
    set_database,
    test_connection,
)
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from declaro_persistum.loader import load_schema

# Import native fluent SQL API
from declaro_persistum.query import (
    delete,
    execute,
    execute_one,
    execute_scalar,
    insert,
    select,
    update,
)
from declaro_persistum.types import NOTHING

SCHEMA_DIR = Path(__file__).parent / "schema"

_schema = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - initialize DB and schema on startup."""
    global _schema
    await init_schema(get_database())
    _schema = load_schema(str(SCHEMA_DIR))
    yield


app = FastAPI(title="Todo App (Native Fluent SQL)", lifespan=lifespan)
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
        "db_name": config["display_name"],
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
        "db_name": config["display_name"] if success else "PostgreSQL",
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
        "db_name": config["display_name"] if success else "Turso Cloud",
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
        "db_name": config["display_name"],
    })


# =============================================================================
# Todo CRUD - SAME CODE FOR ALL DATABASES
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main page with all todos."""
    async with get_connection() as conn:
        todos = await execute(
            select(
                "id",
                "title",
                "completed",
                "created_at",
                from_table="todos",
                order_by=["-created_at"], where=NOTHING, params=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
            conn, row_factory=NOTHING)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "todos": todos,
        "current_db": get_database()["display_name"],
    })

@app.post("/todos", response_class=HTMLResponse)
async def create_todo(request: Request, title: str = Form(...)):
    """Create a new todo."""
    todo_id = str(uuid.uuid4())

    async with get_connection() as conn:
        await execute(
            insert("todos", {"id": todo_id, "title": title, "completed": 0}, returning=NOTHING, on_conflict=NOTHING),
            conn, row_factory=NOTHING)
        await commit(conn)

        todo = await execute_one(
            select("id", "title", "completed", from_table="todos",
                   where={"id": todo_id}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
            conn, row_factory=NOTHING)

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo,
    })

@app.put("/todos/{todo_id}/toggle", response_class=HTMLResponse)
async def toggle_todo(request: Request, todo_id: str):
    """Toggle todo completion status."""
    async with get_connection() as conn:
        current = await execute_one(
            select("completed", from_table="todos", where={"id": todo_id}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
            conn, row_factory=NOTHING)
        new_completed = 0 if current["completed"] else 1

        await execute(
            update("todos", {"completed": new_completed}, where={"id": todo_id}, params=NOTHING, returning=NOTHING),
            conn, row_factory=NOTHING)
        await commit(conn)

        todo = await execute_one(
            select("id", "title", "completed", from_table="todos",
                   where={"id": todo_id}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
            conn, row_factory=NOTHING)

    return templates.TemplateResponse("partials/todo_item.html", {
        "request": request,
        "todo": todo,
    })

@app.delete("/todos/{todo_id}", response_class=HTMLResponse)
async def delete_todo(todo_id: str):
    """Delete a todo."""
    async with get_connection() as conn:
        await execute(delete("todos", where={"id": todo_id}, params=NOTHING, returning=NOTHING), conn, row_factory=NOTHING)
        await commit(conn)

    return ""

@app.get("/todos/count", response_class=HTMLResponse)
async def get_count(request: Request):
    """Get todo counts for the footer."""
    async with get_connection() as conn:
        active_count = await execute_scalar(
            select("COUNT(*) AS c", from_table="todos", where={"completed": 0}, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING),
            conn,
        )

    return templates.TemplateResponse("partials/todo_count.html", {
        "request": request,
        "active_count": active_count,
    })
