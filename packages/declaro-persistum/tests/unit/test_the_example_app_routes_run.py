"""Every data route in the shipped example apps raises today.

MEASURED 2026-08-17, by calling a route function directly:

    DELETE /todos/{id} -> AttributeError: 'dict' object has no attribute 'delete'

`table("todos", schema=...)` returns `{"columns": {...}}`. The routes call
`todos.select(...)`, `todos.insert(...)`, `todos.update(...)` and
`todos.delete()` on it, a fluent TableProxy API that no longer exists. Listing,
creating, toggling and deleting a todo all raise, in all three apps.

THE DIRECTION WAS ALREADY DECIDED AND WRITTEN DOWN. `docs/usage.md` says, of
the query styles:

    "There are none, and there is one style left. This section documented a
    Django-style `users.objects.filter(...)`, a Prisma-style
    `users.prisma.find_many(...)` and a SQLAlchemy-style `Session`. All three
    were deleted: the Django and Prisma surfaces in 264cedd, the SQLAlchemy
    layer in 9ff59e7."

So this is not a question about which API the examples should use. The package
has one, the document says so, and the apps were left behind. They are moved
onto `select`/`insert`/`update`/`delete` plus `execute`, which is the surface
a reader will actually find when they go looking.

WHAT THIS FILE CANNOT SEE: the templates. It calls the route functions rather
than serving HTTP, because `starlette.testclient` needs `httpx` and this
package does not depend on it. A route that returns the wrong context to a
template still passes here.
"""

from __future__ import annotations

import importlib
import pathlib
import sys
import tempfile

import pytest

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"
APPS = sorted(p.parent.name for p in EXAMPLES.glob("*/app.py"))


def _load(app_dir: str):
    """Import an example app the way `uvicorn app:app` does, from its own dir."""
    path = EXAMPLES / app_dir
    sys.path.insert(0, str(path))
    try:
        for name in ("app", "db"):
            sys.modules.pop(name, None)
        db = importlib.import_module("db")
        app = importlib.import_module("app")
        return app, db
    finally:
        sys.path.remove(str(path))


async def _ready(app_dir: str):
    """An example app pointed at a fresh SQLite file, schema created."""
    app, db = _load(app_dir)
    config = db.get_sqlite_config(tempfile.mktemp(suffix=".db"))
    await db.init_schema(config)
    # WHERE A ROUTE LOOKS FOR IT. The lifespan sets this when the app starts;
    # these tests call the routes directly, so they set it the same way.
    app.app.state.database = config
    app._schema = {
        "todos": {
            "columns": {
                "id": {"type": "text", "primary_key": True},
                "title": {"type": "text"},
                "completed": {"type": "integer"},
                "created_at": {"type": "text"},
                "updated_at": {"type": "text"},
            }
        }
    }
    return app, db


def test_there_are_apps_to_test() -> None:
    """A glob that finds nothing passes forever."""
    assert len(APPS) >= 3, APPS


@pytest.mark.parametrize("app_dir", APPS)
async def test_creating_a_todo_stores_it(app_dir: str) -> None:
    """The route a reader clicks first."""
    app, db = await _ready(app_dir)

    await app.create_todo(_request(app.app), title="buy milk")

    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT title FROM todos")
        assert await cursor.fetchall() == [("buy milk",)]


@pytest.mark.parametrize("app_dir", APPS)
async def test_deleting_a_todo_removes_it(app_dir: str) -> None:
    """The route that measured the defect."""
    app, db = await _ready(app_dir)
    await app.create_todo(_request(app.app), title="buy milk")

    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT id FROM todos")
        todo_id = (await cursor.fetchone())[0]

    await app.delete_todo(_request(app.app), todo_id)

    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT COUNT(*) FROM todos")
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.parametrize("app_dir", APPS)
async def test_toggling_a_todo_flips_it_and_flips_it_back(app_dir: str) -> None:
    """Toggle reads, computes and writes, so it exercises all three verbs."""
    app, db = await _ready(app_dir)
    await app.create_todo(_request(app.app), title="buy milk")

    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT id, completed FROM todos")
        todo_id, completed = await cursor.fetchone()
    assert completed == 0

    await app.toggle_todo(_request(app.app), todo_id)
    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT completed FROM todos")
        assert (await cursor.fetchone())[0] == 1

    await app.toggle_todo(_request(app.app), todo_id)
    async with db.get_connection(app.app.state.database) as (conn, engine):
        cursor = await conn.execute("SELECT completed FROM todos")
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.parametrize("app_dir", APPS)
async def test_the_index_lists_what_was_created(app_dir: str) -> None:
    app, db = await _ready(app_dir)
    await app.create_todo(_request(app.app), title="one")
    await app.create_todo(_request(app.app), title="two")

    response = await app.index(_request(app.app))

    titles = {t["title"] for t in response.context["todos"]}
    assert titles == {"one", "two"}


@pytest.mark.parametrize("app_dir", APPS)
async def test_the_index_lists_the_newest_first(app_dir: str) -> None:
    """FOUND BY A MUTATION CONTROL. The test above compares a SET.

    Reversing `order_by=["-created_at"]` to `["created_at"]` left every test
    passing, because a set has no order and nothing else looked.

    The timestamps are written here rather than by creating two todos through
    the route: `created_at` defaults to `datetime('now')`, which has second
    resolution, so two creates in one test tie and the assertion would pass or
    fail on how fast the machine is.
    """
    app, db = await _ready(app_dir)

    async with db.get_connection(app.app.state.database) as (conn, engine):
        await conn.execute(
            "INSERT INTO todos (id, title, completed, created_at) VALUES "
            "('1', 'older', 0, '2026-01-01 00:00:00'), "
            "('2', 'newer', 0, '2026-06-01 00:00:00')"
        )
        await conn.commit()

    response = await app.index(_request(app.app))

    assert [t["title"] for t in response.context["todos"]] == ["newer", "older"], (
        "the index is not newest-first, so a reader's most recent todo is at the bottom of the page"
    )


@pytest.mark.parametrize("app_dir", APPS)
async def test_the_count_route_counts(app_dir: str) -> None:
    """This one runs through `count_`, which returned a string until today."""
    app, db = await _ready(app_dir)
    await app.create_todo(_request(app.app), title="one")
    await app.create_todo(_request(app.app), title="two")

    response = await app.get_count(_request(app.app))

    assert response.context["active_count"] == 2


def _request(app=None):
    """The minimum a TemplateResponse needs, without serving HTTP.

    THE APP IS REAL WHEN A ROUTE READS ITS STATE. Which database the app is
    talking to lives in `app.state`, so a route reaching for it needs the app
    the test set up rather than None, and passing the real one is what makes
    these scenarios exercise the same lookup a served request would.
    """
    from starlette.datastructures import Headers
    from starlette.requests import Request

    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": Headers({}).raw,
            "query_string": b"",
            "app": app,
        }
    )
