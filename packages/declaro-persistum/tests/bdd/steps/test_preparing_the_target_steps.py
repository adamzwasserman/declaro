"""Steps for transfer/preparing_the_target.feature: the loader is handed in.

WHAT THIS REPLACES. Three tests swapped the whole loader table on the module
so that dropping foreign key checks would fail. Nothing needed swapping: every
function here takes the loader as its first argument, so the test hands one in.

A loader is a dict of callables, and the real ones are dicts of callables too.
Building one is passing an argument, not standing in for anything.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.transfer_steps import (
    _leave_fk_checks_on,
    _leave_the_checks_as_they_are,
    _turn_fk_checks_back_on,
    _turn_fk_checks_off,
)
from declaro_persistum.types import is_nothing

scenarios("../features/transfer/preparing_the_target.feature")

_A_CYCLE = ["authors", "books"]


class Ctx(TypedDict, total=False):
    loader: Any
    asked: list[str]
    answer: Any
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"loader": None, "asked": [], "answer": None, "refusal": None}


def _it_works(asked: list[str], what: str) -> Any:
    """A target that does what it is asked, and records being asked.

    IT RECORDS BECAUSE OTHERWISE NOTHING WOULD NOTICE. An empty body returning
    None matched the real loader step, which also returns None, so the shape
    looked right. But then "answered 1 connection" passed whether or not the
    target was ever asked. Measured 2026-08-28 by deleting the call: only the
    refusal scenario failed, and the success one stayed green.
    """

    async def _step(_conn: Any) -> None:
        asked.append(what)

    return _step


def _it_refuses(what: str) -> Any:
    """A target that will not do it, and says so the way a driver would."""

    async def _step(_conn: Any) -> None:
        raise RuntimeError(f"the target refused to {what}")

    return _step


@given("a loader whose target drops its checks")
def _given_drops(ctx):
    ctx["loader"] = {"disable_fk_checks": _it_works(ctx["asked"], "drop its checks")}


@given("a loader whose target refuses to drop its checks")
def _given_refuses_drop(ctx):
    ctx["loader"] = {"disable_fk_checks": _it_refuses("drop its checks")}


@given("a loader whose target restores its checks")
def _given_restores(ctx):
    ctx["loader"] = {"enable_fk_checks": _it_works(ctx["asked"], "restore its checks")}


@given("a loader whose target refuses to restore its checks")
def _given_refuses_restore(ctx):
    ctx["loader"] = {"enable_fk_checks": _it_refuses("restore its checks")}


def _run(ctx: Ctx, coroutine: Any) -> None:
    try:
        ctx["answer"] = asyncio.run(coroutine)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - the refusal is the answer
        ctx["answer"] = None
        ctx["refusal"] = refused


@when("the checks are turned off for two tables that reference each other")
def _when_turned_off(ctx):
    _run(ctx, _turn_fk_checks_off(ctx["loader"], None, _A_CYCLE))


@when("there is no cycle so nothing is turned off")
def _when_no_cycle(ctx):
    _run(ctx, _leave_fk_checks_on(ctx["loader"], None, []))


@when("the checks are turned back on")
def _when_turned_back_on(ctx):
    _run(ctx, _turn_fk_checks_back_on(ctx["loader"], None))


@when("nothing was turned off so nothing is restored")
def _when_nothing_restored(ctx):
    _run(ctx, _leave_the_checks_as_they_are(ctx["loader"], None))


@then(parsers.parse("it answers {expected:d} connection"))
@then(parsers.parse("it answers {expected:d} connections"))
def _then_count(ctx, expected):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert ctx["answer"] == expected, (
        f"answered {ctx['answer']!r}; the pair exists to say how many "
        f"connections had their checks turned off"
    )
    # AND THE TARGET WAS ACTUALLY ASKED. The count alone passed with the call
    # to the loader deleted, so a step that answered 1 without doing anything
    # was indistinguishable from one that worked.
    assert len(ctx["asked"]) == expected, (
        f"it answered {expected} but asked the target {ctx['asked']}"
    )


@then("it refuses")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r} instead"


@then("it names both tables")
def _then_names_both(ctx):
    message = str(ctx["refusal"])
    assert "authors" in message and "books" in message, message


@then("it says transferring now would blame those tables")
def _then_says_why(ctx):
    assert "report those tables as the problem" in str(ctx["refusal"]), ctx["refusal"]


@then("nothing came back")
def _then_nothing(ctx):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert is_nothing(ctx["answer"]), ctx["answer"]


@then("the refusal came back")
def _then_refusal_returned(ctx):
    assert ctx["refusal"] is None, (
        "the restore step raised; it runs in a finally and must return "
        f"instead: {ctx['refusal']}"
    )
    assert isinstance(ctx["answer"], RuntimeError), ctx["answer"]


@when("_left_unenforced is given a refusal and nothing else is going wrong")
def _when_left_unenforced(ctx):
    from declaro_persistum.transfer import _left_unenforced

    ctx["answer"] = _left_unenforced(RuntimeError("the target refused"))
    ctx["refusal"] = None


@when("_left_unenforced is given no refusal")
def _when_left_enforced(ctx):
    from declaro_persistum.transfer import _left_unenforced
    from declaro_persistum.types import NOTHING

    ctx["answer"] = _left_unenforced(NOTHING)
    ctx["refusal"] = None


@when("the result is built from a refusal")
def _when_result_from_refusal(ctx):
    from declaro_persistum.transfer import _UNENFORCED_ERROR

    ctx["errors"] = _UNENFORCED_ERROR[True](RuntimeError("the target refused"))


@when("the result is built from no refusal")
def _when_result_from_nothing(ctx):
    from declaro_persistum.transfer import _UNENFORCED_ERROR
    from declaro_persistum.types import NOTHING

    ctx["errors"] = _UNENFORCED_ERROR[False](NOTHING)


@then("it names the target as unchecked")
def _then_names_unchecked(ctx):
    message = ctx["errors"]["_restore_fk_checks"]
    assert "NOT checked against its constraints" in message, message


@then("it names what the target said")
def _then_names_cause(ctx):
    assert "the target refused" in ctx["errors"]["_restore_fk_checks"], ctx["errors"]


@then("it adds nothing to the errors")
def _then_adds_nothing(ctx):
    assert ctx["errors"] == {}, ctx["errors"]
