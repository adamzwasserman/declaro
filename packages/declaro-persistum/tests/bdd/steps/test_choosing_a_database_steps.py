"""Steps for examples/choosing_a_database.feature.

The example's own db.py is imported and driven, because the point of an
example is what it does when run, and a reader copies it.
"""

from __future__ import annotations

import importlib.util
import pathlib
from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

scenarios("../features/examples/choosing_a_database.feature")

EXAMPLES = pathlib.Path(__file__).resolve().parents[3] / "examples"


def _the_example_module() -> Any:
    """`examples/todo_app_native/db.py`, imported as the app imports it."""
    path = EXAMPLES / "todo_app_native" / "db.py"
    spec = importlib.util.spec_from_file_location("example_db_for_configs", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Ctx(TypedDict, total=False):
    config: Any
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"config": None, "refusal": None}


@when(parsers.parse('a config is built for "{engine}" at "{path}"'))
def _when_path_config(ctx, engine, path):
    db = _the_example_module()
    builders = {
        "sqlite": db.get_sqlite_config,
        "turso_embedded": db.get_turso_embedded_config,
    }
    ctx["config"] = builders[engine](path)


@when(parsers.parse('a PostgreSQL config is built for "{database}" on "{host}"'))
def _when_pg_config(ctx, database, host):
    db = _the_example_module()
    ctx["config"] = db.get_postgres_config(host, 5432, "postgres", "", database)


@when(parsers.parse('a Turso Cloud config is built with the token "{token}"'))
def _when_cloud_config(ctx, token):
    db = _the_example_module()
    ctx["config"] = db.get_turso_cloud_config("libsql://example.turso.io", token)


@when(parsers.parse('a config is built for "{engine}" with no path at all'))
def _when_no_path(ctx, engine):
    db = _the_example_module()
    try:
        ctx["config"] = db.get_sqlite_config()
        ctx["refusal"] = None
    except TypeError as refused:
        ctx["config"] = None
        ctx["refusal"] = refused


@then(parsers.parse('its dialect is "{dialect}"'))
def _then_dialect(ctx, dialect):
    assert ctx["config"]["dialect"] == dialect, ctx["config"]


@then(parsers.parse('it is shown as "{shown}"'))
def _then_shown(ctx, shown):
    assert ctx["config"]["display_name"] == shown, ctx["config"]["display_name"]


@then("it carries no auth token")
def _then_no_token(ctx):
    assert ctx["config"]["auth_token"] is None, ctx["config"]


@then("its connection string names the host and the database")
def _then_url(ctx):
    url = ctx["config"]["connection_string"]
    assert "db.example.com" in url and url.endswith("/todos"), url


@then("it carries the token it was given")
def _then_token(ctx):
    assert ctx["config"]["auth_token"] == "secret", ctx["config"]


@then("it refuses")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, (
        f"it invented a path and returned {ctx['config']!r}; a builder that "
        f"defaults cannot tell a caller who chose from one who forgot"
    )
