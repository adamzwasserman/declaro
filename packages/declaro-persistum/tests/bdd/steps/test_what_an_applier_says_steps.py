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
    ctx["said"] = _applier(engine).get_dialect()


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
        assert module.get_dialect() == engine, (
            f"the {engine} applier calls itself {module.get_dialect()!r}"
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
