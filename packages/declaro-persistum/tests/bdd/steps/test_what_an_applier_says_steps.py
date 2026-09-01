"""Steps for applier/what_an_applier_says_about_itself.feature.

Reached by module rather than by import, because the claim is about all three
appliers agreeing on a shape. Importing three pairs of names by hand would
let a fourth applier arrive with neither and nothing notice.
"""

from __future__ import annotations

import importlib
from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

scenarios("../features/applier/what_an_applier_says_about_itself.feature")

ENGINES = ("sqlite", "turso", "postgresql")

# The two the runner knows how to act on. A third would be a mode nothing reads.
MODES = ("all_or_nothing", "per_operation")


class Ctx(TypedDict, total=False):
    said: str
    all_of_them: dict[str, Any]


@pytest.fixture
def ctx() -> Ctx:
    return {"said": "", "all_of_them": {}}


def _applier(engine: str) -> Any:
    return importlib.import_module(f"declaro_persistum.applier.{engine}")


@when(parsers.parse('the "{engine}" applier is asked which engine it is'))
def _when_engine(ctx, engine):
    ctx["said"] = _applier(engine).get_engine()


@when(parsers.parse('the "{engine}" applier is asked how it scopes a batch'))
def _when_mode(ctx, engine):
    ctx["said"] = _applier(engine).get_transaction_mode()


@when("the appliers are listed")
def _when_listed(ctx):
    ctx["all_of_them"] = {engine: _applier(engine) for engine in ENGINES}


@then(parsers.parse('it says "{expected}"'))
def _then_says(ctx, expected):
    assert ctx["said"] == expected, ctx["said"]


@then("each one says which engine it is and how it scopes a batch")
def _then_both(ctx):
    for engine, module in ctx["all_of_them"].items():
        assert module.get_engine() == engine, (
            f"the {engine} applier calls itself {module.get_engine()!r}"
        )
        assert module.get_transaction_mode(), (
            f"the {engine} applier does not say how it scopes a batch"
        )


@then("no applier names a mode the runner cannot act on")
def _then_known_mode(ctx):
    for engine, module in ctx["all_of_them"].items():
        mode = module.get_transaction_mode()
        assert mode in MODES, (
            f"the {engine} applier scopes a batch {mode!r}, which is neither "
            f"of the two the runner knows: {MODES}"
        )


@when("the skip summary is asked about an empty list")
def _when_no_skips(ctx):
    from declaro_persistum.applier.turso import _SKIP_SUMMARY

    ctx["said"] = _SKIP_SUMMARY[False]([])


@when("the skip summary is asked about two skipped operations")
def _when_two_skips(ctx):
    from declaro_persistum.applier.turso import _SKIP_SUMMARY

    skipped = ["create_table users: no such type", "add_column t.c: refused"]
    ctx["said"] = _SKIP_SUMMARY[True](skipped)


@then("the error field is empty")
def _then_error_empty(ctx):
    assert ctx["said"] is None, (
        f"the result reports {ctx['said']!r} as its error when nothing was "
        f"skipped; ApplyResult['error'] is str | None and None is what a "
        f"caller reads as no error"
    )


@then("the error field counts them and names both")
def _then_error_names(ctx):
    assert ctx["said"] is not None, "two operations were skipped and the result says nothing"
    assert "2 operation(s) skipped" in ctx["said"], ctx["said"]
    assert "no such type" in ctx["said"] and "refused" in ctx["said"], ctx["said"]


@when("the rendering tables are compared with the operations the differ emits")
def _when_tables_compared(ctx):
    from declaro_persistum.applier.templates import TEMPLATES

    ctx["tables"] = {engine: set(rows) for engine, rows in TEMPLATES.items()}


@then("each engine can render every one of them")
def _then_all_renderable(ctx):
    # SQLite's rows are the floor: PostgreSQL is built from them, so anything
    # SQLite renders and PostgreSQL does not is a gap rather than a choice.
    # `alter_column` is the one deliberate absence and is supervised instead.
    floor = ctx["tables"]["sqlite"] - {"alter_column"}
    for engine, rows in ctx["tables"].items():
        missing = sorted(floor - rows)
        assert not missing, (
            f"the {engine} table cannot render {missing}, which SQLite's can. "
            f"An operation added to one table and forgotten for another is an "
            f"operation the runner meets at deploy time."
        )


@when("the PostgreSQL rendering table is read")
def _when_pg_table(ctx):
    from declaro_persistum.applier.templates import TEMPLATES

    ctx["tables"] = {"postgresql": set(TEMPLATES["postgresql"])}


@then("it has no whole-table rebuild for a column change")
def _then_no_rebuild(ctx):
    rows = ctx["tables"]["postgresql"]
    assert "alter_column" not in rows, (
        "PostgreSQL carries a whole-table alter_column row. It alters a column "
        "where it stands, one statement per property changed, and the "
        "supervisor renders those from the alter_column:* rows instead."
    )
    assert any(row.startswith("alter_column:") for row in rows), (
        f"PostgreSQL has neither the whole-table row nor the per-property "
        f"ones, so a column change renders nothing: {sorted(rows)}"
    )
