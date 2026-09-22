"""Whether a failure is worth another attempt, asked of the fault's kind.

WHAT THIS BINDS. `tests/bdd/features/core/worth_retrying.feature`, and nothing
else. The first three scenarios are about `worth_retrying` itself. The fourth
was folded in on 2026-09-21 from a feature about the retry loop, and it is
bound here to the part of it `worth_retrying` settles: a conflict is worth
another attempt and a refusal is not.
"""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.types import Fault, FaultKind
from declaro_persistum.worth_retrying import worth_retrying

pytestmark = pytest.mark.bdd

scenarios("../features/core/worth_retrying.feature")


# --- what a failure of each kind looks like --------------------------------
#
# THE MESSAGE IS FILLED IN WITH SOMETHING MISLEADING WHEREVER IT CAN BE. The
# rule under test is that the decision is asked of `kind` and never of the
# prose, so a message that points the other way is the useful message.


def _fault(kind: FaultKind, what_went_wrong: str) -> Fault:
    return {
        "kind": kind,
        "what_went_wrong": what_went_wrong,
        "what_to_do_instead": "read the kind, not this sentence",
    }


Outcome = Fault | None


@pytest.fixture
def decided() -> dict[str, object]:
    """What one scenario has established so far."""
    return {}


# --- a conflict is retried -------------------------------------------------


@given("a failure of the conflict kind")
def _given_a_conflict(decided: dict[str, object]) -> None:
    decided["answer"] = worth_retrying(_fault("conflict", "conflict at commit"))


@then("it is worth trying again")
def _then_worth_trying_again(decided: dict[str, object]) -> None:
    assert decided["answer"] is True


# --- a refusal is not ------------------------------------------------------


@given("a failure the engine refused outright")
def _given_a_refusal(decided: dict[str, object]) -> None:
    decided["answer"] = worth_retrying(_fault("refused", "column total has no default"))


@then("it is handed back at once")
def _then_handed_back_at_once(decided: dict[str, object]) -> None:
    assert decided["answer"] is False


# --- the message is never read ---------------------------------------------


@given("a conflict whose message says nothing about conflicts")
def _given_a_conflict_with_a_misleading_message(decided: dict[str, object]) -> None:
    misleading = _fault("conflict", "database is locked: disk I/O error on page 41")
    assert "conflict" not in misleading["what_went_wrong"]
    decided["answer"] = worth_retrying(misleading)


@then("it is still retried")
def _then_still_retried(decided: dict[str, object]) -> None:
    assert decided["answer"] is True


# --- a caller never writes a retry loop ------------------------------------


# --- a conflict is retried, and a refusal is not ---------------------------


@given("a write that conflicts at commit")
def _given_a_write_that_conflicts(decided: dict[str, object]) -> None:
    decided["first"] = worth_retrying(_fault("conflict", "serialization failure"))


@given("a write that violates a constraint")
def _given_a_write_that_violates_a_constraint(decided: dict[str, object]) -> None:
    decided["second"] = worth_retrying(_fault("refused", "unique constraint ledger_pkey"))


@then("the first is retried and the second is handed back at once")
def _then_first_retried_second_not(decided: dict[str, object]) -> None:
    assert (decided["first"], decided["second"]) == (True, False)


# --- retries back off, with jitter -----------------------------------------


# --- the budget is bounded and the exhaustion is an answer -----------------


# --- how many times it tried is in the answer ------------------------------


# --- the budget has no default ---------------------------------------------


# `parsers` is imported because the binding recipe names it; no step here
# takes an argument out of its own text, so nothing calls it.
_ = parsers
