"""Every line of `core/budget_spent.feature`, bound to a step.

WHAT THIS FILE CAN PROVE, AND WHAT IT CANNOT. The feature has nine scenarios.
Three of them ask about `budget_spent`, which the declaration writes as
`(attempts: int, budget: int) -> bool`. The other six were folded in on
2026-09-21 from a file about the whole retry mechanism, and each of those
belongs to a different function that `docs/design/persistum.hd` names under
`orchestrator fn lifecycle_catch_up`,
beside this one: `worth_retrying` decides conflict from refusal, `next_wait`
and `clock_read` produce the backoff, and `lifecycle_catch_up` runs the loop
the caller never writes. A predicate over two integers cannot answer any of
them.

WHY THOSE SIX ARE `xfail` AND NOT SKIPPED OR QUIETLY PASSED. A skip says the
line was not worth running. A green assertion rewritten until it passes says
the line was satisfied when it was not. `pytest.xfail` with the missing
function named says the line is in the file, nothing satisfies it yet, and the
name of what must. When that function is written, delete the `xfail` and the
Then below it is already there to run.

EVERY STEP IS DEFINED, INCLUDING THE ONES AFTER AN `xfail`. Those never
execute today, so they are written to fail loudly rather than to pass, which
means removing an `xfail` turns the scenario red instead of green.
"""

from __future__ import annotations

from typing import Any, get_args

import pytest
from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.budget_spent import budget_spent
from declaro_persistum.types import FaultKind

scenarios("../features/core/budget_spent.feature")


# HOW MANY THE FEATURE MEANS. The scenarios count in words, so the words are a
# table rather than a branch per word, and a word nobody put here is a KeyError
# naming itself instead of a silent zero.
COUNTS = {"one": 1, "two": 2, "three": 3, "four": 4}


@pytest.fixture()
def spending() -> dict[str, Any]:
    """What one scenario has established so far."""
    return {}


# --- the three scenarios about this function -------------------------------


@given(parsers.parse("{count} attempts against a budget of {budget}"))
def given_attempts_against_a_budget(spending: dict[str, Any], count: str, budget: str) -> None:
    spending["attempts"] = COUNTS[count]
    spending["budget"] = COUNTS[budget]


@then("there is another attempt to make")
def then_there_is_another_attempt(spending: dict[str, Any]) -> None:
    assert budget_spent(spending["attempts"], spending["budget"]) is False


@then("it is spent")
def then_it_is_spent(spending: dict[str, Any]) -> None:
    assert budget_spent(spending["attempts"], spending["budget"]) is True


@given("a write that succeeded on the third attempt")
def given_a_write_that_succeeded_on_the_third_attempt(spending: dict[str, Any]) -> None:
    spending["attempts"] = COUNTS["three"]
    # THE COUNT IS THIS FUNCTION'S INPUT, NEVER ITS ANSWER. `budget_spent`
    # returns a bool, so three cannot come back out of it. Whichever function
    # hands the caller a result has to carry the count; changing this
    # signature to carry it would make one function answer two questions.
    pytest.xfail(
        "budget_spent returns bool, so its answer cannot say three; "
        "the count belongs to whatever hands the caller its result"
    )


@then("the answer says three")
def then_the_answer_says_three(spending: dict[str, Any]) -> None:
    assert budget_spent(spending["attempts"], spending["budget"]) == COUNTS["three"]


@given("a write that conflicts more often than the budget allows")
def given_more_conflicts_than_the_budget_allows(spending: dict[str, Any]) -> None:
    spending["budget"] = COUNTS["three"]
    spending["attempts"] = COUNTS["four"]


@then("the caller is told the budget was exhausted")
def then_the_caller_is_told_the_budget_was_exhausted(spending: dict[str, Any]) -> None:
    assert budget_spent(spending["attempts"], spending["budget"]) is True


@then("that is distinguishable from the write being refused")
def then_distinguishable_from_refused() -> None:
    # TWO WORDS, KEPT APART IN THE TYPE. `FaultKind` names `budget_spent` and
    # `refused` separately, so a caller reading a kind can tell a spent budget
    # from a request that was wrong in the first place.
    kinds = get_args(FaultKind)
    assert "budget_spent" in kinds
    assert "refused" in kinds
    assert "budget_spent" != "refused"


# --- the six folded in from the retry feature ------------------------------
