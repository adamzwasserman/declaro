"""Steps for session_connect.feature.

Merged on 2026-09-21 from test_what_a_failed_migration_says_steps.py, test_what_an_applier_says_steps.py.
One feature, one step file: pytest-bdd resolves a step to whichever module
imported last when two bind the same file, so exactly one is the rule.
Each part keeps its own context type and fixture, under its own name,
because all of them were called Ctx and ctx and the last would have won.
"""

from __future__ import annotations

import importlib
from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.exceptions import MigrationError, RollbackError
from declaro_persistum.types import NOTHING

scenarios("../features/session/session_connect.feature")

# ---- from test_what_a_failed_migration_says_steps.py ----------------


class Ctx_what_a_failed_migration_says(TypedDict, total=False):
    error: Any
    said: str


@pytest.fixture
def ctx_what_a_failed_migration_says() -> Ctx_what_a_failed_migration_says:
    return {"error": None, "said": ""}


def _an_operation(op: str) -> Any:
    return {"op": op, "table": "users"}


@when(parsers.parse('a migration fails on "{op}" running "{sql}" with a disk error'))
def _when_failed(ctx_what_a_failed_migration_says, op, sql):
    ctx_what_a_failed_migration_says["error"] = MigrationError(
        "Migration failed",
        operation=_an_operation(op),
        sql=sql,
        original_error=OSError("the disk went away"),
    )
    ctx_what_a_failed_migration_says["said"] = str(ctx_what_a_failed_migration_says["error"])


@when("a migration fails with nothing else to report")
def _when_bare(ctx_what_a_failed_migration_says):
    ctx_what_a_failed_migration_says["error"] = MigrationError(
        "Migration failed", operation=NOTHING, sql=NOTHING, original_error=NOTHING
    )
    ctx_what_a_failed_migration_says["said"] = str(ctx_what_a_failed_migration_says["error"])


@when("a migration fails and the rollback fails too")
def _when_rollback_failed(ctx_what_a_failed_migration_says):
    ctx_what_a_failed_migration_says["error"] = RollbackError(
        "Migration failed",
        operation=_an_operation("create_table"),
        sql="CREATE TABLE users",
        original_error=OSError("the disk went away"),
        rollback_error=OSError("and the undo went away too"),
    )
    ctx_what_a_failed_migration_says["said"] = str(ctx_what_a_failed_migration_says["error"])


@when("a migration fails and the rollback works")
def _when_rollback_worked(ctx_what_a_failed_migration_says):
    ctx_what_a_failed_migration_says["error"] = RollbackError(
        "Migration failed",
        operation=_an_operation("create_table"),
        sql="CREATE TABLE users",
        original_error=OSError("the disk went away"),
        rollback_error=NOTHING,
    )
    ctx_what_a_failed_migration_says["said"] = str(ctx_what_a_failed_migration_says["error"])


@then("the message names the operation")
def _then_names_op(ctx_what_a_failed_migration_says):
    assert "create_table" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )
    assert "users" in ctx_what_a_failed_migration_says["said"], ctx_what_a_failed_migration_says[
        "said"
    ]


@then("the message names the SQL")
def _then_names_sql(ctx_what_a_failed_migration_says):
    assert "CREATE TABLE users" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )


@then("the message names what the engine said")
def _then_names_cause(ctx_what_a_failed_migration_says):
    assert "the disk went away" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )
    assert "OSError" in ctx_what_a_failed_migration_says["said"], ctx_what_a_failed_migration_says[
        "said"
    ]


@then("the message carries no operation, SQL or cause")
def _then_bare(ctx_what_a_failed_migration_says):
    assert ctx_what_a_failed_migration_says["said"] == "Migration failed", (
        f"a failure with nothing to report invented something: {ctx_what_a_failed_migration_says['said']!r}"
    )


@then("it says the rollback also failed")
def _then_rollback_named(ctx_what_a_failed_migration_says):
    assert "Rollback also failed" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )
    assert "and the undo went away too" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )


@then("it says a person has to look at the database")
def _then_manual(ctx_what_a_failed_migration_says):
    assert "Manual database inspection required" in ctx_what_a_failed_migration_says["said"], (
        ctx_what_a_failed_migration_says["said"]
    )


@then("it does not say the rollback failed")
def _then_no_rollback(ctx_what_a_failed_migration_says):
    assert "Rollback also failed" not in ctx_what_a_failed_migration_says["said"], (
        f"a rollback that worked was reported as one that failed: {ctx_what_a_failed_migration_says['said']}"
    )


@then("catching a migration failure catches it")
def _then_is_a_migration_error(ctx_what_a_failed_migration_says):
    assert isinstance(ctx_what_a_failed_migration_says["error"], MigrationError), (
        "a rollback failure is not caught by `except MigrationError`, so a "
        "caller handling migration failures misses the worst one"
    )


# ---- from test_what_an_applier_says_steps.py ------------------------


ENGINES = ("sqlite", "turso", "postgresql")

# The two the runner knows how to act on. A third would be a mode nothing reads.
MODES = ("all_or_nothing", "per_operation")


class Ctx_what_an_applier_says(TypedDict, total=False):
    said: str
    all_of_them: dict[str, Any]


@pytest.fixture
def ctx_what_an_applier_says() -> Ctx_what_an_applier_says:
    return {"said": "", "all_of_them": {}}


def _applier(engine: str) -> Any:
    return importlib.import_module(f"declaro_persistum.applier.{engine}")


@when(parsers.parse('the "{engine}" applier is asked which engine it is'))
def _when_engine(ctx_what_an_applier_says, engine):
    ctx_what_an_applier_says["said"] = _applier(engine).get_engine()


@when(parsers.parse('the "{engine}" applier is asked how it scopes a batch'))
def _when_mode(ctx_what_an_applier_says, engine):
    ctx_what_an_applier_says["said"] = _applier(engine).get_transaction_mode()


@when("the appliers are listed")
def _when_listed(ctx_what_an_applier_says):
    ctx_what_an_applier_says["all_of_them"] = {engine: _applier(engine) for engine in ENGINES}


@then(parsers.parse('it says "{expected}"'))
def _then_says(ctx_what_an_applier_says, expected):
    assert ctx_what_an_applier_says["said"] == expected, ctx_what_an_applier_says["said"]


@then("each one says which engine it is and how it scopes a batch")
def _then_both(ctx_what_an_applier_says):
    for engine, module in ctx_what_an_applier_says["all_of_them"].items():
        assert module.get_engine() == engine, (
            f"the {engine} applier calls itself {module.get_engine()!r}"
        )
        assert module.get_transaction_mode(), (
            f"the {engine} applier does not say how it scopes a batch"
        )


@then("no applier names a mode the runner cannot act on")
def _then_known_mode(ctx_what_an_applier_says):
    for engine, module in ctx_what_an_applier_says["all_of_them"].items():
        mode = module.get_transaction_mode()
        assert mode in MODES, (
            f"the {engine} applier scopes a batch {mode!r}, which is neither "
            f"of the two the runner knows: {MODES}"
        )


@when("the skip summary is asked about an empty list")
def _when_no_skips(ctx_what_an_applier_says):
    from declaro_persistum.applier.turso import _SKIP_SUMMARY

    ctx_what_an_applier_says["said"] = _SKIP_SUMMARY[False]([])


@when("the skip summary is asked about two skipped operations")
def _when_two_skips(ctx_what_an_applier_says):
    from declaro_persistum.applier.turso import _SKIP_SUMMARY

    skipped = ["create_table users: no such type", "add_column t.c: refused"]
    ctx_what_an_applier_says["said"] = _SKIP_SUMMARY[True](skipped)


@then("the error field is empty")
def _then_error_empty(ctx_what_an_applier_says):
    assert ctx_what_an_applier_says["said"] is None, (
        f"the result reports {ctx_what_an_applier_says['said']!r} as its error when nothing was "
        f"skipped; ApplyResult['error'] is str | None and None is what a "
        f"caller reads as no error"
    )


@then("the error field counts them and names both")
def _then_error_names(ctx_what_an_applier_says):
    assert ctx_what_an_applier_says["said"] is not None, (
        "two operations were skipped and the result says nothing"
    )
    assert "2 operation(s) skipped" in ctx_what_an_applier_says["said"], ctx_what_an_applier_says[
        "said"
    ]
    assert (
        "no such type" in ctx_what_an_applier_says["said"]
        and "refused" in ctx_what_an_applier_says["said"]
    ), ctx_what_an_applier_says["said"]


@when("the rendering tables are compared with the operations the differ emits")
def _when_tables_compared(ctx_what_an_applier_says):
    from declaro_persistum.applier.templates import TEMPLATES

    ctx_what_an_applier_says["tables"] = {engine: set(rows) for engine, rows in TEMPLATES.items()}


@then("each engine can render every one of them")
def _then_all_renderable(ctx_what_an_applier_says):
    # SQLite's rows are the floor: PostgreSQL is built from them, so anything
    # SQLite renders and PostgreSQL does not is a gap rather than a choice.
    # `alter_column` is the one deliberate absence and is supervised instead.
    floor = ctx_what_an_applier_says["tables"]["sqlite"] - {"alter_column"}
    for engine, rows in ctx_what_an_applier_says["tables"].items():
        missing = sorted(floor - rows)
        assert not missing, (
            f"the {engine} table cannot render {missing}, which SQLite's can. "
            f"An operation added to one table and forgotten for another is an "
            f"operation the runner meets at deploy time."
        )


@when("the PostgreSQL rendering table is read")
def _when_pg_table(ctx_what_an_applier_says):
    from declaro_persistum.applier.templates import TEMPLATES

    ctx_what_an_applier_says["tables"] = {"postgresql": set(TEMPLATES["postgresql"])}


@then("it has no whole-table rebuild for a column change")
def _then_no_rebuild(ctx_what_an_applier_says):
    rows = ctx_what_an_applier_says["tables"]["postgresql"]
    assert "alter_column" not in rows, (
        "PostgreSQL carries a whole-table alter_column row. It alters a column "
        "where it stands, one statement per property changed, and the "
        "supervisor renders those from the alter_column:* rows instead."
    )
    assert any(row.startswith("alter_column:") for row in rows), (
        f"PostgreSQL has neither the whole-table row nor the per-property "
        f"ones, so a column change renders nothing: {sorted(rows)}"
    )
