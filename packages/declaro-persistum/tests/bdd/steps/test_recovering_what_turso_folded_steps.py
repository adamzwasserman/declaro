"""Steps for inspector/recovering_what_turso_folded.feature.

THE LAST SCENARIO IS THE ONE THAT WOULD CATCH A REWRITE GOING WRONG. Both of
these build a new column rather than mutating the one they were handed, and a
version that mutated would pass the first two and fail the third.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.inspector import turso as inspector

scenarios("../features/inspector/recovering_what_turso_folded.feature")

# WHAT A PRAGMA HANDS BACK, and nothing more. If a rewrite drops a key, the
# differ sees a column that changed and rebuilds a table that did not.
_FROM_A_PRAGMA: dict[str, Any] = {
    "type": "integer",
    "nullable": False,
    "primary_key": False,
    "default": None,
}


class Ctx(TypedDict, total=False):
    given: dict[str, Any]
    before: dict[str, Any]
    result: dict[str, Any]


@pytest.fixture
def ctx() -> Ctx:
    return {"given": {}, "before": {}, "result": {}}


@given("a column read back from a pragma")
def _given_column(ctx):
    ctx["given"] = dict(_FROM_A_PRAGMA)
    ctx["before"] = dict(_FROM_A_PRAGMA)


@when(parsers.parse('the reference "{target}" is put back on it'))
def _when_reference(ctx, target):
    ctx["result"] = inspector._referring_to(ctx["given"], target)


@when(parsers.parse('the check "{expression}" is put back on it'))
def _when_check(ctx, expression):
    ctx["result"] = inspector._with_check(ctx["given"], expression)


@then(parsers.parse('it carries reference "{target}"'))
def _then_reference(ctx, target):
    assert ctx["result"]["references"] == target, ctx["result"]


@then(parsers.parse('it carries check "{expression}"'))
def _then_check(ctx, expression):
    assert ctx["result"]["check"] == expression, ctx["result"]


@then("everything the pragma said is still there")
def _then_kept(ctx):
    for key, value in _FROM_A_PRAGMA.items():
        assert ctx["result"][key] == value, (
            f"{key} came back as {ctx['result'].get(key)!r} and the pragma "
            f"said {value!r}; the differ would read that as a change"
        )


@then("the column it was given is unchanged")
def _then_not_mutated(ctx):
    assert ctx["given"] == ctx["before"], (
        "the column handed in was modified in place; a caller still holding "
        "it now sees a reference it never asked for"
    )
