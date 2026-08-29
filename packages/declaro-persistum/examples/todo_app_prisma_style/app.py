"""Todo app demo. One query API, four database backends.

THIS FILE USED TO ADVERTISE A PRISMA-STYLE API:

    todos.prisma.find_many(where={"completed": 0})
    todos.prisma.update(where={"id": "..."}, data={"completed": True})

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

Run with: uv run uvicorn app:app --reload --port 7779
"""

import uuid
from contextlib import asynccontextmanager
from pathlib import Path

# Database abstraction - handles SQLite, PostgreSQL, Turso Cloud, Turso Embedded
from db import (
    DEFAULT_SQLITE_PATH,
    DEFAULT_TURSO_EMBEDDED_PATH,
    SQLITE_CONFIG,
    check_driver_available,
    commit,
    get_connection,
    get_postgres_config,
    get_sqlite_config,
    get_turso_cloud_config,
    get_turso_embedded_config,
    init_schema,
    test_connection,
)
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from declaro_persistum.loader import load_schema

# Import Prisma-style query API
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
    """Application lifespan - initialize DB and schema on startup.

    WHICH DATABASE THIS APP IS TALKING TO IS THE APP'S, and it lives in
    `app.state`. It used to be a module global inside db.py, which meant every
    function that opened a connection read state no caller could see.
    """
    global _schema
    app.state.database = SQLITE_CONFIG
    await init_schema(app.state.database)
    _schema = load_schema(str(SCHEMA_DIR))
    yield


app = FastAPI(title="Todo App (Prisma-style)", lifespan=lifespan)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


# =============================================================================
# Database Selection Endpoints
# =============================================================================


@app.get("/db", response_class=HTMLResponse)
async def db_settings(request: Request):
    """Show database selection page."""
    current = request.app.state.database
    sqlite_ok, sqlite_msg = check_driver_available("sqlite")
    pg_ok, pg_msg = check_driver_available("postgresql")
    turso_cloud_ok, turso_cloud_msg = check_driver_available("turso_cloud")
    turso_embedded_ok, turso_embedded_msg = check_driver_available("turso_embedded")

    return templates.TemplateResponse(
        "db_select.html",
        {
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
        },
    )


async def _switch_to(request, config, fallback_name: str, said_on_success: str):
    """Adopt a database if it answers, and say what happened either way.

    FOUR ROUTES WROTE THESE SIX LINES EACH: build the config, test the
    connection, adopt it and create the table if it answered, then render the
    same template with the same four keys. The config and the words were the
    only things that differed.

    `fallback_name` is what to call the engine when the connection failed and
    there is no display name to read off a config that never worked.

    `said_on_success` HAS NO DEFAULT, and the empty string is a real value: it
    means "report whatever the connection said". A default here would let a
    caller who forgot look like one who chose, and two of these four routes do
    want their own wording.
    """
    success, message = await test_connection(config)
    if success:
        request.app.state.database = config
        await init_schema(config)

    return templates.TemplateResponse(
        "db_status.html",
        {
            "request": request,
            "success": success,
            "message": (said_on_success or message) if success else message,
            "db_name": config["display_name"] if success else fallback_name,
        },
    )


# honest-code-allow: L1.21.1 - these four are the rows, and a route has to be its own function: FastAPI registers it by the decorator and the path, so generating them from a table would hide the URL map, which is the thing an example most needs to show. Their bodies already share _switch_to and are four lines each
@app.post("/db/sqlite", response_class=HTMLResponse)
async def use_sqlite(request: Request, database_path: str = Form(DEFAULT_SQLITE_PATH)):
    """Switch to SQLite."""
    return await _switch_to(
        request,
        get_sqlite_config(database_path),
        "SQLite",
        said_on_success="Switched to SQLite",
    )


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
    return await _switch_to(
        request,
        get_postgres_config(host, port, user, password, database),
        "PostgreSQL",
        said_on_success="",
    )


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

    return await _switch_to(
        request,
        get_turso_cloud_config(url, auth_token),
        "Turso Cloud",
        said_on_success="",
    )


@app.post("/db/turso_embedded", response_class=HTMLResponse)
async def use_turso_embedded(
    request: Request, database_path: str = Form(DEFAULT_TURSO_EMBEDDED_PATH)
):
    """Switch to Turso Embedded (pyturso)."""
    return await _switch_to(
        request,
        get_turso_embedded_config(database_path),
        "Turso Embedded",
        said_on_success="Switched to Turso Embedded",
    )


# =============================================================================
# Todo CRUD - SAME CODE FOR ALL DATABASES (Prisma-style)
# =============================================================================


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Render the main page with all todos."""
    async with get_connection(request.app.state.database) as (conn, dialect):
        todos = await execute(
            select(
                "id",
                "title",
                "completed",
                "created_at",
                from_table="todos",
                order_by=["-created_at"],
                where=NOTHING,
                params=NOTHING,
                limit=NOTHING,
                offset=NOTHING,
                joins=NOTHING,
                group_by=NOTHING,
                having=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "todos": todos,
            "current_db": request.app.state.database["display_name"],
        },
    )


@app.post("/todos", response_class=HTMLResponse)
async def create_todo(request: Request, title: str = Form(...)):
    """Create a new todo."""
    todo_id = str(uuid.uuid4())

    async with get_connection(request.app.state.database) as (conn, dialect):
        await execute(
            insert(
                "todos",
                {"id": todo_id, "title": title, "completed": 0},
                returning=NOTHING,
                on_conflict=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )
        await commit(conn, dialect)

        todo = await execute_one(
            select(
                "id",
                "title",
                "completed",
                from_table="todos",
                where={"id": todo_id},
                params=NOTHING,
                order_by=NOTHING,
                limit=NOTHING,
                offset=NOTHING,
                joins=NOTHING,
                group_by=NOTHING,
                having=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )

    return templates.TemplateResponse(
        "partials/todo_item.html",
        {
            "request": request,
            "todo": todo,
        },
    )


@app.put("/todos/{todo_id}/toggle", response_class=HTMLResponse)
async def toggle_todo(request: Request, todo_id: str):
    """Toggle todo completion status."""
    async with get_connection(request.app.state.database) as (conn, dialect):
        current = await execute_one(
            select(
                "completed",
                from_table="todos",
                where={"id": todo_id},
                params=NOTHING,
                order_by=NOTHING,
                limit=NOTHING,
                offset=NOTHING,
                joins=NOTHING,
                group_by=NOTHING,
                having=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )
        new_completed = 0 if current["completed"] else 1

        await execute(
            update(
                "todos",
                {"completed": new_completed},
                where={"id": todo_id},
                params=NOTHING,
                returning=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )
        await commit(conn, dialect)

        todo = await execute_one(
            select(
                "id",
                "title",
                "completed",
                from_table="todos",
                where={"id": todo_id},
                params=NOTHING,
                order_by=NOTHING,
                limit=NOTHING,
                offset=NOTHING,
                joins=NOTHING,
                group_by=NOTHING,
                having=NOTHING,
            ),
            conn,
            row_factory=NOTHING,
        )

    return templates.TemplateResponse(
        "partials/todo_item.html",
        {
            "request": request,
            "todo": todo,
        },
    )


@app.delete("/todos/{todo_id}", response_class=HTMLResponse)
async def delete_todo(request: Request, todo_id: str):
    """Delete a todo."""
    async with get_connection(request.app.state.database) as (conn, dialect):
        await execute(
            delete("todos", where={"id": todo_id}, params=NOTHING, returning=NOTHING),
            conn,
            row_factory=NOTHING,
        )
        await commit(conn, dialect)

    return ""


@app.get("/todos/count", response_class=HTMLResponse)
async def get_count(request: Request):
    """Get todo counts for the footer."""
    async with get_connection(request.app.state.database) as (conn, dialect):
        active_count = await execute_scalar(
            select(
                "COUNT(*) AS c",
                from_table="todos",
                where={"completed": 0},
                params=NOTHING,
                order_by=NOTHING,
                limit=NOTHING,
                offset=NOTHING,
                joins=NOTHING,
                group_by=NOTHING,
                having=NOTHING,
            ),
            conn,
        )

    return templates.TemplateResponse(
        "partials/todo_count.html",
        {
            "request": request,
            "active_count": active_count,
        },
    )
