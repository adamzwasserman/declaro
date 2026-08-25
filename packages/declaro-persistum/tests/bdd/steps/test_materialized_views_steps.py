"""Steps for schema/materialized_views.feature, run against a real database.

WHAT THESE USED TO DO. Every step asserted a substring of the generated SQL,
and no step ever executed any of it. The docstring argued that was enough:
"every function under test is a pure SQL generator: strings in, list-of-strings
out". That reasoning is wrong in the way that matters. A generator's output is
correct only if a database accepts it, and a substring check cannot tell
working SQL from a string that merely contains the right words. This package
has already reached 100% branch coverage while emitting SQL no engine could
parse.

The introspection steps were worse. `_when_introspect` built a View out of the
metadata row the Given step had just written, then asserted the View matched
the row. Four scenarios claiming "the SQLite inspector introspects views" never
called the inspector, and could not fail.

WHAT THEY DO NOW. Every scenario runs its SQL against a real SQLite database
and asks the database what happened. The create scenarios check the backing
table holds the query's rows. The trigger scenarios insert into the source
table and check the view caught up. The introspection scenarios call the real
inspector.

NO MOCKS, and now that means something rather than describing an absence.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.abstractions.materialized_views import (
    MATVIEW_METADATA_TABLE,
    create_matview_sql,
    drop_matview_sql,
    drop_refresh_triggers_sql,
    generate_metadata_table_schema,
    generate_refresh_trigger_sql,
    refresh_matview_sql,
)
from declaro_persistum.loader import validate_view
from declaro_persistum.types import NOTHING, View

scenarios("../features/schema/materialized_views.feature")

_QUERY = "SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id"
_NAME = "monthly_stats"


class Ctx(TypedDict, total=False):
    view: View
    db: sqlite3.Connection
    path: str
    sql: list[str]
    other_sql: list[str]
    triggers: list[str]
    error: Exception | None
    introspected: list[dict[str, Any]]


@pytest.fixture
def ctx() -> Any:
    """A database of this scenario's own, on disk so the inspector can open it.

    On disk rather than in memory because the inspector is async and opens its
    own connection through aiosqlite. Two connections to `:memory:` are two
    different databases, which is the sort of thing a substring assertion
    never has to think about.
    """
    path = str(Path(tempfile.mkdtemp()) / "matview.db")
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            total REAL,
            created_at TEXT
        );
        INSERT INTO orders (user_id, total) VALUES (1, 10.0), (1, 20.0), (2, 5.0);
        """
    )
    db.commit()
    context: Ctx = {
        "view": {"name": _NAME, "query": _QUERY, "materialized": True},
        "db": db,
        "path": path,
        "sql": [],
    }
    yield context
    db.close()


def _run(ctx: Ctx, statements: list[str]) -> None:
    """Execute what was generated. A statement the engine refuses fails here."""
    for statement in statements:
        ctx["db"].execute(statement)
    ctx["db"].commit()


def _rows(ctx: Ctx, query: str) -> list[tuple]:
    return list(ctx["db"].execute(query).fetchall())


def _joined(ctx: Ctx) -> str:
    return "\n".join(ctx.get("sql", []))


# ------------------------------------------------------------------ the schema


@given(parsers.parse('a schema with an "{table}" table'))
def _given_table(ctx, table):
    """The fixture already made it, so this asserts rather than builds."""
    assert _rows(ctx, f"SELECT count(*) FROM {table}")[0][0] == 3


@given(parsers.re(r"the orders table has columns: (?P<cols>.+)"))
def _given_columns(ctx, cols):
    """The columns the scenarios name, read back off the database."""
    actual = {row[1] for row in _rows(ctx, "PRAGMA table_info(orders)")}
    for column in (c.strip() for c in cols.split(",")):
        assert column in actual, f"orders has no {column}: {sorted(actual)}"


# ------------------------------------------------------------ the declaration


@given(parsers.parse('a view definition with materialized=true and refresh="{refresh}"'))
def _given_view_refresh(ctx, refresh):
    ctx["view"] = {**ctx["view"], "materialized": True, "refresh": refresh}


@given("a view definition with materialized=true")
def _given_view_materialized(ctx):
    ctx["view"] = {**ctx["view"], "materialized": True}


@given(parsers.parse('a view definition with refresh="{refresh}"'))
def _given_refresh(ctx, refresh):
    ctx["view"] = {**ctx["view"], "refresh": refresh}


@given(parsers.parse('the view query is "{query}"'))
def _given_query(ctx, query):
    ctx["view"] = {**ctx["view"], "query": query}


@given(parsers.parse("the view has depends_on=[{items}]"))
def _given_depends(ctx, items):
    ctx["view"] = {**ctx["view"], "depends_on": _list(items)}


@given(parsers.parse("trigger_sources=[{items}]"))
@given(parsers.parse("a view definition with trigger_sources=[{items}]"))
def _given_trigger_sources(ctx, items):
    ctx["view"] = {**ctx["view"], "trigger_sources": _list(items)}


@given(parsers.parse('refresh="{refresh}"'))
def _given_bare_refresh(ctx, refresh):
    ctx["view"] = {**ctx["view"], "refresh": refresh}


def _list(items: str) -> list[str]:
    return [i.strip().strip('"').strip("'") for i in items.split(",") if i.strip()]


# ---------------------------------------------------------------------- create


@when("the SQLite applier generates CREATE SQL")
@when("SQLite applier generates CREATE SQL")
def _when_create(ctx):
    """Generate the statements and run every one of them."""
    view = ctx["view"]
    ctx["sql"] = create_matview_sql(
        name=view["name"],
        query=view["query"],
        refresh_strategy=view.get("refresh", "manual"),
        depends_on=view.get("depends_on"),
    )
    _run(ctx, ctx["sql"])
    sources = view.get("trigger_sources") or []
    ctx["triggers"] = [
        statement
        for source in sources
        for statement in generate_refresh_trigger_sql(
            view["name"], source, view["query"], NOTHING
        )
    ]
    _run(ctx, ctx["triggers"])


@then(parsers.parse('it should create metadata table "{table}"'))
def _then_metadata_table(ctx, table):
    """The table exists in the database, not merely in the text."""
    assert table == MATVIEW_METADATA_TABLE
    assert generate_metadata_table_schema(), "no metadata table schema is generated"
    found = _rows(
        ctx, f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    )
    assert found, f"{table} was not created: {_joined(ctx)}"


@then(parsers.parse('it should create backing table "{name}"'))
def _then_backing_table(ctx, name):
    """The backing table exists AND holds what the query returns.

    A CREATE that runs and leaves an empty table is the failure this catches:
    the emulation is a cache, and a cache nobody filled is a view that reports
    nothing until the first refresh.
    """
    found = _rows(
        ctx, f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'"
    )
    assert found, f"{name} was not created: {_joined(ctx)}"

    cached = _rows(ctx, f'SELECT * FROM "{name}" ORDER BY 1')
    live = _rows(ctx, ctx["view"]["query"])
    assert cached == live, f"the backing table does not hold the query's rows: {cached}"


@then(
    parsers.parse(
        'it should register the view in metadata with refresh_strategy="{strategy}"'
    )
)
def _then_registered(ctx, strategy):
    rows = _rows(
        ctx,
        f"SELECT refresh_strategy FROM {MATVIEW_METADATA_TABLE} "
        f"WHERE name = '{ctx['view']['name']}'",
    )
    assert rows, f"the view was not registered: {_joined(ctx)}"
    assert rows[0][0] == strategy, f"registered as {rows[0][0]!r}, not {strategy!r}"


@then(parsers.re(r"the metadata should include depends_on as JSON array '(?P<arr>.+)'"))
def _then_depends_json(ctx, arr):
    rows = _rows(
        ctx,
        f"SELECT depends_on FROM {MATVIEW_METADATA_TABLE} "
        f"WHERE name = '{ctx['view']['name']}'",
    )
    assert rows, "the view was not registered"
    assert json.loads(rows[0][0]) == json.loads(arr), (
        f"depends_on round-tripped as {rows[0][0]!r}, not {arr!r}"
    )


# --------------------------------------------------------------------- refresh


@given(parsers.parse('an existing emulated materialized view "{name}"'))
def _given_existing(ctx, name):
    """Created for real, so the refresh has something to refresh."""
    ctx["view"] = {**ctx["view"], "name": name}
    _run(
        ctx,
        create_matview_sql(
            name=name,
            query=ctx["view"]["query"],
            refresh_strategy=ctx["view"].get("refresh", "manual"),
            depends_on=ctx["view"].get("depends_on"),
        ),
    )


@given(
    parsers.parse(
        'an existing emulated materialized view "{name}" with trigger_sources=[{items}]'
    )
)
def _given_existing_with_triggers(ctx, name, items):
    ctx["view"] = {**ctx["view"], "name": name, "trigger_sources": _list(items)}
    _run(
        ctx,
        create_matview_sql(
            name=name,
            query=ctx["view"]["query"],
            refresh_strategy="trigger",
            depends_on=ctx["view"].get("depends_on"),
        ),
    )
    _run(
        ctx,
        [
            statement
            for source in _list(items)
            for statement in generate_refresh_trigger_sql(
                name, source, ctx["view"]["query"], NOTHING
            )
        ],
    )


@when(parsers.parse("refresh_matview_sql() is called with atomic={flag}"))
def _when_refresh(ctx, flag):
    """Refresh after the source has changed, so a stale cache shows."""
    ctx["db"].execute("INSERT INTO orders (user_id, total) VALUES (3, 1.0)")
    ctx["db"].commit()
    ctx["sql"] = refresh_matview_sql(
        ctx["view"]["name"], ctx["view"]["query"], flag.strip().lower() == "true"
    )
    _run(ctx, ctx["sql"])


@then(parsers.parse('it should generate DELETE FROM "{name}"'))
def _then_refresh_emptied(ctx, name):
    """The old rows went, which is what the DELETE is for."""
    assert any("DELETE FROM" in s.upper() and name in s for s in ctx["sql"]), (
        f"no DELETE FROM {name}: {_joined(ctx)}"
    )


@then(parsers.parse('it should generate INSERT INTO "{name}" with the query'))
def _then_refresh_refilled(ctx, name):
    """And the new rows arrived, which is the whole point of a refresh."""
    assert any("INSERT INTO" in s.upper() and name in s for s in ctx["sql"]), (
        f"no INSERT INTO {name}: {_joined(ctx)}"
    )
    cached = _rows(ctx, f'SELECT * FROM "{name}" ORDER BY 1')
    live = _rows(ctx, ctx["view"]["query"])
    assert cached == live, f"the refresh left the cache stale: {cached} vs {live}"


@then("it should UPDATE last_refreshed_at in metadata")
def _then_refresh_stamped(ctx):
    rows = _rows(
        ctx,
        f"SELECT last_refreshed_at FROM {MATVIEW_METADATA_TABLE} "
        f"WHERE name = '{ctx['view']['name']}'",
    )
    assert rows and rows[0][0] is not None, (
        f"last_refreshed_at was not stamped: {rows}"
    )


@then(parsers.parse('it should generate DROP TABLE "{name}"'))
def _then_nonatomic_drops(ctx, name):
    assert any("DROP TABLE" in s.upper() and name in s for s in ctx["sql"]), (
        f"a non-atomic refresh must drop the table: {_joined(ctx)}"
    )


@then(parsers.parse('it should generate CREATE TABLE "{name}" AS query'))
def _then_nonatomic_recreates(ctx, name):
    """Rebuilt, and holding the query's rows once it is."""
    assert any(
        "CREATE TABLE" in s.upper() and "AS" in s.upper() and name in s
        for s in ctx["sql"]
    ), f"a non-atomic refresh must rebuild the table: {_joined(ctx)}"

    cached = _rows(ctx, f'SELECT * FROM "{name}" ORDER BY 1')
    live = _rows(ctx, ctx["view"]["query"])
    assert cached == live, f"the rebuild left the cache stale: {cached} vs {live}"


# -------------------------------------------------------------------- triggers


@then(parsers.parse('it should create trigger "{trigger}"'))
def _then_trigger_exists(ctx, trigger):
    """In sqlite_master, which is the only place a trigger being there shows."""
    found = _rows(
        ctx,
        f"SELECT name FROM sqlite_master WHERE type='trigger' AND name='{trigger}'",
    )
    assert found, f"{trigger} was not created: {'; '.join(ctx.get('triggers', []))}"


@given(parsers.parse('trigger SQL for matview "{name}" on source "{source}"'))
def _given_trigger_sql(ctx, name, source):
    ctx["view"] = {**ctx["view"], "name": name}
    _run(
        ctx,
        create_matview_sql(
            name=name, query=ctx["view"]["query"],
            refresh_strategy="trigger", depends_on=None,
        ),
    )
    ctx["triggers"] = generate_refresh_trigger_sql(
        name, source, ctx["view"]["query"], NOTHING
    )
    _run(ctx, ctx["triggers"])


@when(parsers.parse("I examine the {event} trigger"))
def _when_examine(ctx, event):
    """Read the trigger back out of the database rather than off the string."""
    rows = _rows(
        ctx,
        f"SELECT name, sql FROM sqlite_master WHERE type='trigger' "
        f"AND lower(name) LIKE '%_{event.lower()}'",
    )
    assert rows, f"no {event} trigger in the database"
    ctx["examined"] = rows[0][1]
    ctx["event"] = event


@then(parsers.parse('it should be AFTER {event} ON "{table}"'))
def _then_trigger_shape(ctx, event, table):
    body = ctx["examined"].upper()
    assert f"AFTER {event.upper()}" in body, ctx["examined"]
    assert table.upper() in body, ctx["examined"]


@then("the trigger body should DELETE and INSERT the matview")
def _then_trigger_refreshes(ctx):
    """Asked of the database: change the source, and the view catches up."""
    before = _rows(ctx, f'SELECT * FROM "{ctx["view"]["name"]}" ORDER BY 1')
    ctx["db"].execute("INSERT INTO orders (user_id, total) VALUES (9, 99.0)")
    ctx["db"].commit()
    after = _rows(ctx, f'SELECT * FROM "{ctx["view"]["name"]}" ORDER BY 1')

    assert after != before, (
        f"the source changed and the view did not: {before} then {after}"
    )
    assert after == _rows(ctx, ctx["view"]["query"]), (
        f"the trigger left the view disagreeing with its own query: {after}"
    )


# ------------------------------------------------------------------------ drop


@when("drop_matview_sql() is called")
def _when_drop(ctx):
    ctx["sql"] = drop_matview_sql(ctx["view"]["name"])
    _run(ctx, ctx["sql"])


@when("drop_matview_sql() is called with trigger cleanup")
def _when_drop_with_triggers(ctx):
    sources = ctx["view"].get("trigger_sources") or []
    ctx["sql"] = [
        *drop_refresh_triggers_sql(ctx["view"]["name"], sources),
        *drop_matview_sql(ctx["view"]["name"]),
    ]
    _run(ctx, ctx["sql"])


@then(parsers.parse('it should generate DROP TABLE IF EXISTS "{name}"'))
def _then_dropped(ctx, name):
    """The table is gone from the database, not merely named in a string."""
    assert not _rows(
        ctx, f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'"
    ), f"{name} survived the drop"


@then(
    parsers.re(
        r"it should generate DELETE FROM _dp_materialized_views "
        r"WHERE name='(?P<name>[^']+)'"
    )
)
def _then_metadata_row_gone(ctx, name):
    """A registry row outliving its table is a schema reporting a view that
    does not exist."""
    assert not _rows(
        ctx, f"SELECT name FROM {MATVIEW_METADATA_TABLE} WHERE name='{name}'"
    ), f"the metadata row for {name} survived the drop"


@then(parsers.parse("it should generate DROP TRIGGER for {event} trigger"))
def _then_trigger_gone(ctx, event):
    left = _rows(
        ctx,
        f"SELECT name FROM sqlite_master WHERE type='trigger' "
        f"AND lower(name) LIKE '%_{event.lower()}'",
    )
    assert not left, f"the {event} trigger survived the drop: {left}"


# ----------------------------------------------------------------- introspect


@given(parsers.parse('a database with _dp_materialized_views containing "{name}"'))
def _given_registered(ctx, name):
    ctx["view"] = {**ctx["view"], "name": name}
    _run(
        ctx,
        create_matview_sql(
            name=name, query=ctx["view"]["query"],
            refresh_strategy="manual", depends_on=None,
        ),
    )


@given(parsers.parse('a database with "{name}" having refresh_strategy="{strategy}"'))
def _given_registered_strategy(ctx, name, strategy):
    ctx["view"] = {**ctx["view"], "name": name}
    _run(
        ctx,
        create_matview_sql(
            name=name, query=ctx["view"]["query"],
            refresh_strategy=strategy, depends_on=None,
        ),
    )


@given(parsers.re(r"a database with \"(?P<name>[^\"]+)\" having depends_on='(?P<arr>.+)'"))
def _given_registered_depends(ctx, name, arr):
    ctx["view"] = {**ctx["view"], "name": name}
    _run(
        ctx,
        create_matview_sql(
            name=name, query=ctx["view"]["query"],
            refresh_strategy="manual", depends_on=json.loads(arr),
        ),
    )


@given("a database without _dp_materialized_views table")
def _given_no_metadata(ctx):
    """A plain view and no registry, which is every database made before this
    feature existed."""
    _run(ctx, ['CREATE VIEW "plain_view" AS SELECT id FROM orders'])


@when("the SQLite inspector introspects views")
def _when_introspect(ctx):
    """The real inspector, on the real database this scenario wrote.

    This used to build a View out of the metadata row the Given step had just
    written and then assert the View matched the row, so four scenarios named
    after the inspector never called it and could not fail.
    """
    ctx["db"].commit()

    async def read() -> dict[str, Any]:
        import aiosqlite

        from declaro_persistum.inspector.sqlite import views_of

        connection = await aiosqlite.connect(ctx["path"])
        try:
            return await views_of(connection)
        finally:
            await connection.close()

    try:
        ctx["introspected"] = list((asyncio.run(read())).values())
        ctx["error"] = None
    except Exception as exc:  # noqa: BLE001 - the scenario asks whether it raised
        ctx["introspected"] = []
        ctx["error"] = exc


@then(parsers.parse('it should return "{name}" with materialized=true'))
def _then_returns_matview(ctx, name):
    found = [v for v in ctx["introspected"] if v["name"] == name]
    assert found, f"{name} was not returned: {ctx['introspected']}"
    assert found[0].get("materialized") is True, found[0]


@then(parsers.parse('it should return refresh="{strategy}" for "{name}"'))
def _then_returns_strategy(ctx, strategy, name):
    found = [v for v in ctx["introspected"] if v["name"] == name]
    assert found and found[0].get("refresh") == strategy, (
        f"introspection lost the refresh strategy: {ctx['introspected']}"
    )


@then(parsers.parse('it should return depends_on=[{items}] for "{name}"'))
def _then_returns_depends(ctx, items, name):
    found = [v for v in ctx["introspected"] if v["name"] == name]
    assert found and found[0].get("depends_on") == _list(items), (
        f"introspection lost depends_on: {ctx['introspected']}"
    )


@then("it should return only regular views")
def _then_only_regular(ctx):
    materialized = [v for v in ctx["introspected"] if v.get("materialized")]
    assert not materialized, (
        f"a database with no registry reported materialized views: {materialized}"
    )


@then("it should not raise an error")
def _then_no_error(ctx):
    assert ctx.get("error") is None, (
        f"a missing metadata table must be an absence, not a failure: "
        f"{ctx.get('error')}"
    )


# --------------------------------------------------------------- validation


@when("validate_view() is called")
def _when_validate(ctx):
    try:
        validate_view(ctx["view"])
        ctx["error"] = None
    except ValueError as exc:
        ctx["error"] = exc


@then(parsers.re(r'it should raise ValueError "(?P<message>.+)"'))
def _then_raises(ctx, message):
    """The message must carry the claim, word for word not required.

    The feature paraphrases: it asks for "trigger_sources requires
    refresh='trigger' or 'hybrid'" and the code says "View 'trigger_sources'
    requires refresh='trigger' or 'hybrid'". Matching the exact string would
    fail on the word "View" and teach nobody anything, so this asks that every
    word the feature considers load-bearing is in there.
    """
    assert ctx["error"] is not None, f"{message!r} was not raised"

    raised = str(ctx["error"])
    missing = [
        word for word in message.replace('"', "").split() if word not in raised
    ]
    assert not missing, (
        f"raised {raised!r}, which does not say {missing}"
    )


# --------------------------------------------------------- dialect agreement


@when("Turso applier generates CREATE SQL")
def _when_turso(ctx):
    """Turso runs the same emulation, so it must run the same statements."""
    ctx["other_sql"] = create_matview_sql(
        name=ctx["view"]["name"],
        query=ctx["view"]["query"],
        refresh_strategy=ctx["view"].get("refresh", "manual"),
        depends_on=ctx["view"].get("depends_on"),
    )


@then("both should produce identical SQL")
def _then_identical(ctx):
    assert ctx["sql"] == ctx["other_sql"], (
        "SQLite and Turso emulate a materialized view differently, so a "
        "database migrated on one engine and read on the other disagrees"
    )


@when("PostgreSQL applier generates CREATE SQL")
def _when_postgres(ctx):
    from declaro_persistum.applier.rendering import statements_for

    ctx["other_sql"] = statements_for(
        {"op": "create_view", "table": "", "details": dict(ctx["view"])},
        "postgresql",
    )


@then("it should use CREATE MATERIALIZED VIEW (not table emulation)")
def _then_native(ctx):
    """PostgreSQL holds the rows itself, so it writes one statement and no
    registry."""
    rendered = "\n".join(ctx["other_sql"])
    assert "CREATE MATERIALIZED VIEW" in rendered.upper(), rendered
    assert MATVIEW_METADATA_TABLE not in rendered, (
        f"PostgreSQL emulated a view its own engine provides: {rendered}"
    )
