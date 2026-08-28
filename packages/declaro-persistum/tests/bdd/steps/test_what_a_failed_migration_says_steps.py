"""Steps for exceptions/what_a_failed_migration_says.feature.

Both errors are built and their message read, because the message IS the
product: it is what an operator sees when a deploy stops, and a field that
silently goes missing from it is the failure worth catching.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.exceptions import MigrationError, RollbackError
from declaro_persistum.types import NOTHING

scenarios("../features/exceptions/what_a_failed_migration_says.feature")


class Ctx(TypedDict, total=False):
    error: Any
    said: str


@pytest.fixture
def ctx() -> Ctx:
    return {"error": None, "said": ""}


def _an_operation(op: str) -> Any:
    return {"op": op, "table": "users"}


@when(parsers.parse('a migration fails on "{op}" running "{sql}" with a disk error'))
def _when_failed(ctx, op, sql):
    ctx["error"] = MigrationError(
        "Migration failed",
        operation=_an_operation(op),
        sql=sql,
        original_error=OSError("the disk went away"),
    )
    ctx["said"] = str(ctx["error"])


@when("a migration fails with nothing else to report")
def _when_bare(ctx):
    ctx["error"] = MigrationError(
        "Migration failed", operation=NOTHING, sql=NOTHING, original_error=NOTHING
    )
    ctx["said"] = str(ctx["error"])


@when("a migration fails and the rollback fails too")
def _when_rollback_failed(ctx):
    ctx["error"] = RollbackError(
        "Migration failed",
        operation=_an_operation("create_table"),
        sql="CREATE TABLE users",
        original_error=OSError("the disk went away"),
        rollback_error=OSError("and the undo went away too"),
    )
    ctx["said"] = str(ctx["error"])


@when("a migration fails and the rollback works")
def _when_rollback_worked(ctx):
    ctx["error"] = RollbackError(
        "Migration failed",
        operation=_an_operation("create_table"),
        sql="CREATE TABLE users",
        original_error=OSError("the disk went away"),
        rollback_error=NOTHING,
    )
    ctx["said"] = str(ctx["error"])


@then("the message names the operation")
def _then_names_op(ctx):
    assert "create_table" in ctx["said"], ctx["said"]
    assert "users" in ctx["said"], ctx["said"]


@then("the message names the SQL")
def _then_names_sql(ctx):
    assert "CREATE TABLE users" in ctx["said"], ctx["said"]


@then("the message names what the engine said")
def _then_names_cause(ctx):
    assert "the disk went away" in ctx["said"], ctx["said"]
    assert "OSError" in ctx["said"], ctx["said"]


@then("the message carries no operation, SQL or cause")
def _then_bare(ctx):
    assert ctx["said"] == "Migration failed", (
        f"a failure with nothing to report invented something: {ctx['said']!r}"
    )


@then("it says the rollback also failed")
def _then_rollback_named(ctx):
    assert "Rollback also failed" in ctx["said"], ctx["said"]
    assert "and the undo went away too" in ctx["said"], ctx["said"]


@then("it says a person has to look at the database")
def _then_manual(ctx):
    assert "Manual database inspection required" in ctx["said"], ctx["said"]


@then("it does not say the rollback failed")
def _then_no_rollback(ctx):
    assert "Rollback also failed" not in ctx["said"], (
        f"a rollback that worked was reported as one that failed: {ctx['said']}"
    )


@then("catching a migration failure catches it")
def _then_is_a_migration_error(ctx):
    assert isinstance(ctx["error"], MigrationError), (
        "a rollback failure is not caught by `except MigrationError`, so a "
        "caller handling migration failures misses the worst one"
    )
