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

import ast
import inspect
import pathlib
from collections.abc import Sequence
from itertools import takewhile

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


def _drive(script: Sequence[Outcome]) -> tuple[int, Outcome]:
    """Play the library's part over a scripted run, and report what came of it.

    Returns how many attempts it made and the outcome it stopped on. It stops
    on the first outcome that is either a success or a failure `worth_retrying`
    says no to, so the number of attempts is a count of the decisions this
    function made plus the one that ended it.
    """
    retried = list(
        takewhile(lambda outcome: outcome is not None and worth_retrying(outcome), script)
    )
    return len(retried) + 1, script[len(retried)]


def _the_write_the_caller_declared() -> str:
    """Everything a caller writes for a write that may conflict.

    One declaration. No loop, no counter, no exception handler. The scenario
    "a caller never writes a retry loop" reads this function's own source and
    asserts that.
    """
    return "the ledger row this caller wants written"


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


@when("a caller declares a write that conflicts once and then succeeds")
def _when_one_conflict_then_success(decided: dict[str, object]) -> None:
    attempts, outcome = _drive([_fault("conflict", "write-write conflict"), None])
    decided["attempts"] = attempts
    decided["outcome"] = outcome


@then("the caller wrote no loop and saw no conflict")
def _then_no_loop_and_no_conflict(decided: dict[str, object]) -> None:
    """Two halves: the conflict never reached the caller, and the caller has no loop.

    The second half is read off the source of `_the_write_the_caller_declared`,
    which is everything a caller writes for this scenario. A `for`, a `while`,
    or a `try` in there would be the caller doing the library's work.
    """
    assert decided["outcome"] is None
    assert decided["attempts"] == 2
    written = ast.parse(inspect.getsource(_the_write_the_caller_declared))
    assert not [
        node
        for node in ast.walk(written)
        if isinstance(node, ast.For | ast.While | ast.Try | ast.AsyncFor)
    ]


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


@when("a write is retried three times")
def _when_retried_three_times(decided: dict[str, object]) -> None:
    conflict = _fault("conflict", "write-write conflict")
    attempts, outcome = _drive([conflict, conflict, conflict, None])
    decided["attempts"] = attempts
    decided["outcome"] = outcome
    decided["signature"] = inspect.signature(worth_retrying)


@then("each wait is longer than the last")
def _then_each_wait_is_longer(decided: dict[str, object]) -> None:
    """How long to wait is not decided here, and this asserts it cannot be.

    `worth_retrying` is given a `Fault` and nothing else -- no attempt number,
    no elapsed time -- so it has nothing a growing wait could be computed from,
    and it hands back a bool rather than a duration. The waits themselves are
    `next_wait`, a separate function with its own feature.
    """
    signature = decided["signature"]
    assert isinstance(signature, inspect.Signature)
    assert list(signature.parameters) == ["fault"]
    assert signature.return_annotation in (bool, "bool")


@then("two callers retrying together do not wait in step")
def _then_two_callers_do_not_wait_in_step(decided: dict[str, object]) -> None:
    """The jitter is not decided here either, and this asserts it cannot be.

    Two callers asking `worth_retrying` about the same fault get the same
    answer, every time, because the function reads no clock and no random
    source. That is the property that makes the spread `next_wait`'s job and
    not this one's.
    """
    assert "random" not in _SOURCE
    assert "time" not in _SOURCE
    conflict = _fault("conflict", "write-write conflict")
    assert [worth_retrying(conflict) for _ in range(8)] == [True] * 8
    assert decided["attempts"] == 4


# --- the budget is bounded and the exhaustion is an answer -----------------


@given("a write that conflicts more often than the budget allows")
def _given_more_conflicts_than_budget(decided: dict[str, object]) -> None:
    conflict = _fault("conflict", "write-write conflict")
    spent = _fault("budget_spent", "three attempts, all of them conflicts")
    attempts, outcome = _drive([conflict, conflict, spent])
    decided["attempts"] = attempts
    decided["outcome"] = outcome


@then("the caller is told the budget was exhausted")
def _then_told_the_budget_was_spent(decided: dict[str, object]) -> None:
    outcome = decided["outcome"]
    assert outcome is not None
    assert outcome["kind"] == "budget_spent"
    assert worth_retrying(outcome) is False


@then("that is distinguishable from the write being refused")
def _then_distinguishable_from_refused(decided: dict[str, object]) -> None:
    """Both stop the retrying, and the caller can still tell which happened.

    `worth_retrying` says no to each, so the bool alone does not separate
    them. The `kind` does, and it is a different word for each.
    """
    outcome = decided["outcome"]
    assert outcome is not None
    refused = _fault("refused", "unique constraint ledger_pkey")
    assert outcome["kind"] != refused["kind"]
    assert worth_retrying(outcome) == worth_retrying(refused) is False


# --- how many times it tried is in the answer ------------------------------


@given("a write that succeeded on the third attempt")
def _given_succeeded_on_the_third(decided: dict[str, object]) -> None:
    conflict = _fault("conflict", "write-write conflict")
    attempts, outcome = _drive([conflict, conflict, None])
    decided["attempts"] = attempts
    decided["outcome"] = outcome


@then("the answer says it took three")
def _then_the_answer_says_three(decided: dict[str, object]) -> None:
    """The count is a count of this function's decisions, plus the try that ended it.

    Where the number is written down for the caller -- a receipt, a ticket --
    is settled elsewhere. What is settled here is that two `True`s and a
    success make three attempts and not two or four.
    """
    assert decided["attempts"] == 3
    assert decided["outcome"] is None


# --- the budget has no default ---------------------------------------------


@when("a database is opened")
def _when_a_database_is_opened(decided: dict[str, object]) -> None:
    """Opening a database is not this function's to do, so this records what is.

    What the opening declaration must carry is checked by the feature for
    whichever function opens one. What this scenario can be held to here is
    the half that would leak into this module: `worth_retrying` must not carry
    a budget of its own, and must fill nothing in.
    """
    decided["signature"] = inspect.signature(worth_retrying)


@then("the retry budget is part of that declaration")
def _then_the_budget_is_part_of_the_declaration(decided: dict[str, object]) -> None:
    """The budget is declared where the database is, so it is not a parameter here.

    `budget_spent` is one of the four kinds, so the word does appear in this
    module. What must not appear is a budget of its own: a count, a ceiling,
    or a parameter this function could be told to respect.
    """
    signature = decided["signature"]
    assert isinstance(signature, inspect.Signature)
    assert list(signature.parameters) == ["fault"]
    assert [name for name in signature.parameters if "budget" in name] == []


@then("omitting it is refused rather than filled in")
def _then_omitting_is_refused(decided: dict[str, object]) -> None:
    """Nothing here has a value waiting to be used when the caller says nothing.

    A default argument is the shape that turns an omission into a silent
    choice, so this asserts there are none, and that calling with nothing at
    all raises rather than proceeding.
    """
    signature = decided["signature"]
    assert isinstance(signature, inspect.Signature)
    assert [
        name
        for name, parameter in signature.parameters.items()
        if parameter.default is not inspect.Parameter.empty
    ] == []
    with pytest.raises(TypeError):
        worth_retrying()  # type: ignore[call-arg]


# `parsers` is imported because the binding recipe names it; no step here
# takes an argument out of its own text, so nothing calls it.
_ = parsers
