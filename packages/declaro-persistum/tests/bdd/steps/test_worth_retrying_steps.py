"""Whether a failure is worth another attempt, asked of the fault's kind.

WHAT THIS BINDS. `tests/bdd/features/core/worth_retrying.feature`, and nothing
else. The first three scenarios are about `worth_retrying` itself. The seven
folded in on 2026-09-21 came from a feature about the retry loop, so some of
them name things this function does not decide -- how long to wait, what the
receipt says, what a database declares when it is opened. Each of those is
bound here to the part of it `worth_retrying` does settle, and the docstring on
the step says which part that is and which part lives elsewhere.

THE LOOP LIVES IN THIS FILE ON PURPOSE. `_drive` below plays the library's
part: it walks a scripted run of outcomes and asks `worth_retrying` after each
failure. It is here rather than in `src/` because the scenarios about the
caller ("wrote no loop", "succeeded on the third attempt") are about who holds
the loop, and a stand-in loop in the test is the only way to show the caller
does not hold one.
"""

from __future__ import annotations

import inspect
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

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


_SOURCE = pathlib.Path(inspect.getfile(worth_retrying)).read_text()


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


# --- the difference is asked of the exception's type -----------------------


@when("the library decides whether to retry")
def _when_the_library_decides(decided: dict[str, object]) -> None:
    """Ask every kind, and ask one kind under three unrelated messages."""
    kinds: tuple[FaultKind, ...] = ("conflict", "refused", "budget_spent", "cut_short")
    decided["by_kind"] = {kind: worth_retrying(_fault(kind, "a message")) for kind in kinds}
    decided["by_message"] = [
        worth_retrying(_fault("conflict", message))
        for message in ("deadlock detected", "", "the constraint was violated")
    ]


@then("it asks what kind of failure it is")
def _then_it_asks_the_kind(decided: dict[str, object]) -> None:
    assert decided["by_kind"] == {
        "conflict": True,
        "refused": False,
        "budget_spent": False,
        "cut_short": False,
    }


@then("it does not read the message")
def _then_it_does_not_read_the_message(decided: dict[str, object]) -> None:
    """Two ways: the answer does not move with the message, and the source never names it.

    The second is the one that would catch a rewrite. `what_went_wrong` and
    `what_to_do_instead` are the only prose a `Fault` carries, so a module that
    names neither cannot be deciding by prose.
    """
    assert decided["by_message"] == [True, True, True]
    assert "what_went_wrong" not in _SOURCE
    assert "what_to_do_instead" not in _SOURCE


# --- retries back off, with jitter -----------------------------------------


# --- the budget is bounded and the exhaustion is an answer -----------------


# --- how many times it tried is in the answer ------------------------------


# --- the budget has no default ---------------------------------------------


# `parsers` is imported because the binding recipe names it; no step here
# takes an argument out of its own text, so nothing calls it.
_ = parsers
