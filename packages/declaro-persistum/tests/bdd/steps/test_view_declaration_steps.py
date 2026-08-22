"""Steps for schema/view_declaration.feature.

THE DECLARATION PATH, NOT THE GENERATORS. `test_materialized_views_steps.py`
drives the pure SQL builders and asserts the strings they return, and its own
docstring argues that going through the applier "would add a layer without
adding a claim". The layer is the claim. A generator can return text that is
correct and that no engine will execute, and these steps exist because that is
what it does for a trigger-refreshed view.

So every scenario writes a real models file, runs the real migration, and then
asks the database what it holds. No mocks: the loader reads a file, the
applier writes to a database, and both are the things under test.

EVERY STEP IS SYNCHRONOUS AND DRIVES THE LOOP ITSELF, which is the convention
`test_opening_steps.py` already set. pytest-bdd does not await a coroutine
returned by a step: it calls the function, gets a coroutine object, and moves
on. My first draft used `async def` throughout and fourteen scenarios reported
a pass having executed none of their assertions, with the evidence buried in a
RuntimeWarning. A step that cannot fail is worse than a missing one.

Turso and PostgreSQL skip when they are not reachable, naming what they tried.
SQLite always runs.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import (
    close,
    open_postgresql,
    open_sqlite,
    open_turso,
    reading,
    writing,
)
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.migrations import apply_migrations_async
from declaro_persistum.pydantic_loader import load_views_from_models
from declaro_persistum.types import NOTHING

scenarios("../features/schema/view_declaration.feature")

_TABLE = '''from declaro_persistum import table
from pydantic import BaseModel


@table("orders")
class Orders(BaseModel):
    id: int
    user_id: int
    amount: int
'''

_AGGREGATE = "SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id"
_PLAIN = "SELECT user_id, amount FROM orders WHERE amount > 0"
_NAMES = ("v_plain", "v_totals", "v_live")


class Ctx(TypedDict, total=False):
    directory: pathlib.Path
    models: pathlib.Path
    databases: dict[str, Any]
    results: list[dict[str, Any]]
    error: Exception | None
    views: dict[str, Any]
    engine: str
    postgresql_url: str
    postgresql_schema_name: str
    run: Any


@pytest.fixture(scope="function")
def loop():
    made = asyncio.new_event_loop()
    asyncio.set_event_loop(made)
    yield made
    made.close()


@pytest.fixture
def ctx(loop, tmp_path: pathlib.Path, postgresql_schema) -> Ctx:
    made = Ctx(
        directory=tmp_path,
        databases={},
        results=[],
        error=None,
        views={},
        run=loop.run_until_complete,
        postgresql_url=postgresql_schema[0],
        postgresql_schema_name=postgresql_schema[1],
    )
    yield made

    # NOTHING TO SCRUB. The schema this scenario was given is dropped whole
    # by the `postgresql_schema` fixture, so the tables go with it.
    for database in made["databases"].values():
        loop.run_until_complete(close(database))


def _declare(ctx: Ctx, *views: str) -> None:
    """Write the models file: the table, then each view dict verbatim."""
    ctx["models"] = ctx["directory"] / "models.py"
    ctx["models"].write_text(_TABLE + "\n" + "\n".join(views))


# THE ENGINE IS OPENED ONCE PER SCENARIO AND KEPT, because a second migration
# in the same scenario has to meet the state the first one left. Opening again
# would give SQLite a fresh file and quietly turn "migrate again" into
# "migrate for the first time".
async def _database(ctx: Ctx, engine: str) -> Any:
    if engine in ctx["databases"]:
        return ctx["databases"][engine]

    if engine == "sqlite":
        database = await open_sqlite(
            str(ctx["directory"] / "s.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
    elif engine == "turso":
        try:
            database = await open_turso(
                str(ctx["directory"] / "u.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"no Turso: {type(exc).__name__}: {exc}")
    else:
        # A SCHEMA OF THIS SCENARIO'S OWN. Twenty-four test files shared one
        # PostgreSQL database and collided on table names, so the suite passed
        # in the order it happened to run in. These scenarios create `orders`,
        # which eight other places also create.
        url = ctx["postgresql_url"]
        try:
            database = await open_postgresql(
                url, shutdown="exit_immediately", busy_timeout_s=5.0
            )
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"no PostgreSQL at {url}: {type(exc).__name__}: {exc}")
    ctx["databases"][engine] = database
    return database


async def _migrate(ctx: Ctx, engine: str) -> dict[str, Any]:
    ctx["engine"] = engine
    database = await _database(ctx, engine)
    # FORCED, BECAUSE THE STAMP OUTLIVES THE SCENARIO. Skip-if-clean hashes
    # the models FILE and returns before the view diff runs, and PostgreSQL's
    # probe database is shared with every other test, so a stamp written by
    # an earlier run made a later migration report success having done
    # nothing. SQLite and Turso get a fresh file per scenario and never showed
    # it. The last scenario still asserts that a second migration applies no
    # operations, which is true with or without the stamp.
    result = await apply_migrations_async(
        database, engine, ctx["models"], force=True, expand_enums=NOTHING)
    ctx["results"].append(result)
    return result


async def _objects(ctx: Ctx, engine: str) -> dict[str, str]:
    """What the database holds, as name to kind."""
    database = await _database(ctx, engine)
    async with reading(database) as conn:
        if engine == "postgresql":
            # THE SCENARIO'S OWN SCHEMA, not `public`. The migration writes
            # where `search_path` points, and asking `public` found only what
            # other tests had left there.
            schema = ctx["postgresql_schema_name"]
            rows = await conn.fetch(
                "SELECT viewname AS n, 'view' AS k FROM pg_views "
                "WHERE schemaname = $1 "
                "UNION ALL "
                "SELECT matviewname, 'matview' FROM pg_matviews "
                "WHERE schemaname = $1 "
                "UNION ALL "
                "SELECT tablename, 'table' FROM pg_tables "
                "WHERE schemaname = $1",
                schema,
            )
            return {row["n"]: row["k"] for row in rows}
        cursor = await conn.execute(
            "SELECT name, type FROM sqlite_master "
            "WHERE type IN ('view', 'table', 'trigger')"
        )
        return dict(await cursor.fetchall())


async def _insert_order(ctx: Ctx, engine: str) -> None:
    database = await _database(ctx, engine)
    async with writing(database) as conn:
        await conn.execute('INSERT INTO "orders" VALUES (1, 7, 25)')
        if engine != "postgresql":
            await conn.commit()


async def _rows(ctx: Ctx, engine: str, name: str) -> list[tuple]:
    database = await _database(ctx, engine)
    async with reading(database) as conn:
        if engine == "postgresql":
            return [tuple(row) for row in await conn.fetch(f'SELECT * FROM "{name}"')]
        cursor = await conn.execute(f'SELECT * FROM "{name}"')
        return [tuple(row) for row in await cursor.fetchall()]


# ---------------------------------------------------------------------- given


@given('a models file declaring a table "orders"')
def _table_only(ctx: Ctx) -> None:
    _declare(ctx)


@given(
    parsers.parse('the models file declares a view "{name}" selecting from "orders"')
)
def _plain_view(ctx: Ctx, name: str) -> None:
    _declare(ctx, f'V = {{"name": "{name}", "query": "{_PLAIN}"}}')


@given("the models file declares a dict with a name and no query")
def _no_query(ctx: Ctx) -> None:
    _declare(ctx, 'V = {"name": "v_plain"}')


@given(parsers.parse('the models file declares a view carrying the key "{key}"'))
def _unknown_key(ctx: Ctx, key: str) -> None:
    _declare(ctx, f'V = {{"name": "v_plain", "query": "{_PLAIN}", "{key}": "manual"}}')


@given(
    parsers.parse(
        'the models file declares a materialized view "{name}" refreshed "{refresh}"'
    )
)
def _materialized(ctx: Ctx, name: str, refresh: str) -> None:
    _declare(
        ctx,
        f'V = {{"name": "{name}", "query": "{_AGGREGATE}", '
        f'"materialized": True, "refresh": "{refresh}"}}',
    )


@given(
    parsers.parse(
        'the models file declares a materialized view "{name}" depending on "{source}"'
    )
)
def _with_depends_on(ctx: Ctx, name: str, source: str) -> None:
    _declare(
        ctx,
        f'V = {{"name": "{name}", "query": "{_AGGREGATE}", '
        f'"materialized": True, "refresh": "manual", "depends_on": ["{source}"]}}',
    )


@given(
    parsers.parse(
        'the models file declares a materialized view "{name}" refreshed '
        '"{refresh}" watching "{source}"'
    )
)
def _triggered(ctx: Ctx, name: str, refresh: str, source: str) -> None:
    _declare(
        ctx,
        f'V = {{"name": "{name}", "query": "{_AGGREGATE}", '
        f'"materialized": True, "refresh": "{refresh}", '
        f'"depends_on": ["{source}"], "trigger_sources": ["{source}"]}}',
    )


@given(parsers.parse("the schema is migrated to {engine}"))
def _given_migrated(ctx: Ctx, engine: str) -> None:
    ctx["run"](_migrate(ctx, engine))


# ----------------------------------------------------------------------- when


@when(parsers.parse("the schema is migrated to {engine} again"))
def _migrate_again(ctx: Ctx, engine: str) -> None:
    ctx["run"](_migrate(ctx, engine))


@when(parsers.parse("the schema is migrated to {engine}"))
def _when_migrated(ctx: Ctx, engine: str) -> None:
    try:
        ctx["run"](_migrate(ctx, engine))
    except Exception as exc:  # noqa: BLE001
        ctx["error"] = exc


@when("the views are loaded")
def _load_views(ctx: Ctx) -> None:
    try:
        ctx["views"] = load_views_from_models(ctx["directory"])
    except LoaderError as exc:
        ctx["error"] = exc


@when("the declared query is changed")
def _change_query(ctx: Ctx) -> None:
    ctx["models"].write_text(
        ctx["models"].read_text().replace("amount > 0", "amount > 100")
    )


@when("the view declaration is deleted from the models file")
def _delete_view(ctx: Ctx) -> None:
    ctx["models"].write_text(_TABLE)


@when(parsers.parse('a row is inserted into "{table}"'))
def _insert_row(ctx: Ctx, table: str) -> None:
    assert table == "orders", f"only the declared table is written here, not {table}"
    ctx["run"](_insert_order(ctx, "sqlite"))


# ----------------------------------------------------------------------- then


@then(parsers.parse('{engine} holds a view named "{name}"'))
def _holds_view(ctx: Ctx, engine: str, name: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    assert objects.get(name) == "view", f"{engine} holds {name} as {objects.get(name)!r}"


@then(parsers.parse('{engine} holds no view named "{name}"'))
def _holds_no_view(ctx: Ctx, engine: str, name: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    assert name not in objects, f"{engine} still holds {name} as {objects[name]!r}"


@then(parsers.parse('{engine} holds a materialized view named "{name}"'))
def _holds_matview(ctx: Ctx, engine: str, name: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    # Native on PostgreSQL, a table on the engines that emulate it.
    expected = "matview" if engine == "postgresql" else "table"
    assert objects.get(name) == expected, (
        f"{engine} holds {name} as {objects.get(name)!r}, expected {expected!r}"
    )


@then(parsers.parse('{engine} holds "{name}" as a native materialized view'))
def _native_matview(ctx: Ctx, engine: str, name: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    assert objects.get(name) == "matview", f"holds {objects.get(name)!r}"


@then(
    parsers.parse(
        '{engine} holds "{name}" as a table with a row in the matview registry'
    )
)
def _emulated_matview(ctx: Ctx, engine: str, name: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    assert objects.get(name) == "table", f"holds {objects.get(name)!r}"

    async def registered() -> Any:
        database = await _database(ctx, engine)
        async with reading(database) as conn:
            cursor = await conn.execute(
                "SELECT name FROM _dp_materialized_views WHERE name = ?", (name,)
            )
            return await cursor.fetchone()

    assert ctx["run"](registered()) is not None, f"{name} is not registered"


@then('selecting from "v_plain" returns the rows the query describes')
def _plain_answers(ctx: Ctx) -> None:
    """The view filters, so one of the two rows must not come back."""
    engine = ctx["engine"]

    async def populate() -> None:
        database = await _database(ctx, engine)
        async with writing(database) as conn:
            await conn.execute('INSERT INTO "orders" VALUES (1, 7, 25)')
            await conn.execute('INSERT INTO "orders" VALUES (2, 7, 0)')
            if engine != "postgresql":
                await conn.commit()

    ctx["run"](populate())
    rows = ctx["run"](_rows(ctx, engine, "v_plain"))

    assert rows == [(7, 25)], f"v_plain answered {rows}"


@then(parsers.parse('"{name}" holds the aggregate rows at the moment it was built'))
def _matview_holds(ctx: Ctx, name: str) -> None:
    """A snapshot, which is the whole difference from a plain view.

    The table is empty when the migration builds this, so a row inserted
    afterwards must NOT appear: `refresh: manual` means the contents change
    when somebody asks and at no other time. Asserting only that the object
    can be selected from would pass just as well against a plain view.
    """
    engine = ctx["engine"]
    ctx["run"](_insert_order(ctx, engine))
    rows = ctx["run"](_rows(ctx, engine, name))

    assert rows == [], (
        f"{name} shows {rows} after an insert it was never refreshed for, "
        f"so it is not a snapshot"
    )


@then(parsers.parse('the recorded dependencies of "{name}" are "{source}"'))
def _recorded_depends_on(ctx: Ctx, name: str, source: str) -> None:
    async def read() -> Any:
        database = await _database(ctx, "sqlite")
        async with reading(database) as conn:
            cursor = await conn.execute(
                "SELECT depends_on FROM _dp_materialized_views WHERE name = ?",
                (name,),
            )
            return await cursor.fetchone()

    row = ctx["run"](read())
    assert row is not None, f"{name} is not registered"
    assert json.loads(row[0]) == [source]


@then("no view is found")
def _no_view_found(ctx: Ctx) -> None:
    assert ctx["error"] is None, f"loading raised {ctx['error']!r}"
    assert ctx["views"] == {}


@then(
    parsers.parse('loading fails naming "{key}" and listing the keys a view may carry')
)
def _refused_by_name(ctx: Ctx, key: str) -> None:
    assert isinstance(ctx["error"], LoaderError), (
        f"loading returned {ctx['views']!r} instead of raising"
    )
    message = str(ctx["error"])
    assert key in message
    for allowed in ("name", "query", "materialized", "refresh", "depends_on"):
        assert allowed in message, f"the message does not list {allowed}"


@then("the migration reports no error")
def _no_error(ctx: Ctx) -> None:
    assert ctx["error"] is None, f"the migration raised {ctx['error']!r}"
    assert ctx["results"][-1]["error"] is None, (
        f"the migration reported {ctx['results'][-1]['error']!r}"
    )


@then(
    parsers.parse(
        '{engine} holds a refresh trigger on "{source}" for insert, update and delete'
    )
)
def _holds_triggers(ctx: Ctx, engine: str, source: str) -> None:
    objects = ctx["run"](_objects(ctx, engine))
    triggers = [name for name, kind in objects.items() if kind == "trigger"]
    for event in ("insert", "update", "delete"):
        assert any(event in name and source in name for name in triggers), (
            f"no {event} trigger on {source}; {engine} holds {triggers}"
        )


@then(parsers.parse('"{name}" reflects the new row'))
def _reflects(ctx: Ctx, name: str) -> None:
    rows = ctx["run"](_rows(ctx, "sqlite", name))
    assert rows == [(7, 25)], f"{name} holds {rows}"


@then(parsers.parse('"{name}" answers with the new query'))
def _answers_new_query(ctx: Ctx, name: str) -> None:
    """The edited query keeps only rows over 100, so the 25 must vanish."""
    ctx["run"](_insert_order(ctx, "sqlite"))
    rows = ctx["run"](_rows(ctx, "sqlite", name))

    assert rows == [], f"{name} still answers with the old query: {rows}"


@then("the second migration applies no operations")
def _second_is_empty(ctx: Ctx) -> None:
    assert len(ctx["results"]) >= 2, "only one migration ran"
    assert ctx["results"][-1]["operations_applied"] == 0, (
        f"the second migration applied "
        f"{ctx['results'][-1]['operations_applied']} operations"
    )


@then(parsers.parse('{engine} holds nothing named "{name}"'))
def _holds_nothing(ctx: Ctx, engine: str, name: str) -> None:
    """Turso accepts CREATE TABLE AS SELECT and creates nothing.

    MEASURED 2026-08-21 straight against the driver: two CTAS statements
    returned without error and `sqlite_master` held only the source table. The
    materialized-view emulation is built on CTAS, so every materialized view
    on Turso is missing while the migration reports success.

    This step asserts the DEFECT, so it fails the day pyturso implements CTAS
    or the emulation stops relying on it. Either is the signal to delete it.
    """
    objects = ctx["run"](_objects(ctx, engine))

    assert name not in objects, (
        f"{engine} now holds {name} as {objects[name]!r}; the CTAS gap is "
        f"closed, so delete this scenario and put {engine} back in the outline"
    )
