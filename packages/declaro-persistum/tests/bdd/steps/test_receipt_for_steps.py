"""Bind `tests/bdd/features/core/receipt_for.feature`.

WHAT THE FEATURE ASKS FOR. Two scenarios cover the outcomes a deposited write
can reach. A write that landed carries no failure. A write that conflicted past
the retry budget carries one, and that Fault has to tell the caller what to do
differently, not only what went wrong.

The third scenario asks for something neither verdict can answer: how many times
the engine tried. A write that succeeded on the third attempt and a write that
succeeded on the first both say `ok` true, so without a count a caller cannot
tell a clean write from one that fought for it.

The feature's own summary line says `ok` false has exactly one meaning: the
engine stayed busy for the whole budget. So `ok` may not be a second, separate
record that could disagree with the failure beside it. The Thens check that by
moving the failure and watching `ok` move with it, and by moving the ticket and
the count and watching `ok` stay put.

Nothing here tests a refusal. A refusal never reaches this function: the
collecting path raises it, and `receipt_for` is total over the two arguments the
declaration gives it.
"""

import pytest
from pytest_bdd import given, parsers, scenarios, then

scenarios("../features/core/receipt_for.feature")


@pytest.fixture
def outcome() -> dict:
    """Where a Given leaves the ticket and the failure, for a Then to read."""
    return {}


@pytest.fixture
def a_ticket() -> str:
    """What `deposit` handed back. A Ticket is a str, per the declaration."""
    return "w-0007"


@pytest.fixture
def a_budget_spent_fault() -> dict:
    """The one Fault a receipt can carry: the engine stayed busy for the whole
    retry budget. `what_to_do_instead` is the field the feature reads."""
    return {
        "kind": "budget_spent",
        "what_went_wrong": "every attempt inside the retry budget hit a busy database",
        "what_to_do_instead": "deposit the write again, or raise the retry budget",
    }


# TWO SENTENCES, ONE NUMBER APART. How many attempts a write took is the only
# thing that differs, so the sentence carries it and the step reads the row.
ATTEMPTS_TAKEN = {"first": 1, "third": 3}


@given(parsers.parse("a write that ran on the {which} attempt"))
@given(parsers.parse("a write that succeeded on the {which} attempt"))
def _a_write_that_succeeded(outcome: dict, a_ticket: str, which: str) -> None:
    outcome["ticket"] = a_ticket
    outcome["attempts"] = ATTEMPTS_TAKEN[which]
    outcome["failure"] = None


@given("a write that conflicted past the budget")
def _conflicted_past_the_budget(outcome: dict, a_ticket: str, a_budget_spent_fault: dict) -> None:
    outcome["ticket"] = a_ticket
    outcome["attempts"] = 5
    outcome["failure"] = a_budget_spent_fault


@then("the receipt says it succeeded and carries no failure")
def _says_it_succeeded(outcome: dict) -> None:
    from declaro_persistum.receipt_for import receipt_for

    receipt = receipt_for(outcome["ticket"], outcome["failure"], outcome["attempts"])

    assert receipt["ok"] is True
    assert receipt["failure"] is None
    assert receipt["id"] == outcome["ticket"], "the receipt names the write it answers for"
    assert set(receipt) == {"id", "ok", "attempts", "failure"}, (
        f"a Receipt carries four things: {receipt}"
    )


@then("the receipt says it did not land")
def _says_it_did_not_land(outcome: dict) -> None:
    from declaro_persistum.receipt_for import receipt_for

    receipt = receipt_for(outcome["ticket"], outcome["failure"], outcome["attempts"])

    assert receipt["ok"] is False
    assert receipt["id"] == outcome["ticket"], "the receipt names the write it answers for"


@then("the answer says it took three")
def _says_it_took_three(outcome: dict, a_ticket: str, a_budget_spent_fault: dict) -> None:
    from declaro_persistum.receipt_for import receipt_for

    receipt = receipt_for(outcome["ticket"], outcome["failure"], outcome["attempts"])

    assert receipt["attempts"] == 3
    assert receipt["ok"] is True, "it succeeded, on the third try"

    # The count is carried, not counted here and not inferred from the verdict.
    # A receipt that said 1 whenever it succeeded would pass the line above only
    # by accident, so the count is moved and has to move with it.
    counts = [(n, receipt_for(a_ticket, None, n)["attempts"]) for n in [1, 2, 3, 7, 100]]
    wrong = [seen for seen in counts if seen[0] != seen[1]]
    assert wrong == [], f"the count did not survive the trip: {wrong}"

    # A failed write reports its attempts too, which is how a caller learns how
    # much of the retry budget the engine spent before giving up.
    assert receipt_for(a_ticket, a_budget_spent_fault, 5)["attempts"] == 5

    # The count does not move the verdict.
    verdicts = [(n, receipt_for(a_ticket, None, n)["ok"]) for n in [0, 1, 3, 100]]
    moved = [seen for seen in verdicts if seen[1] is not True]
    assert moved == [], f"the verdict moved with the count: {moved}"


@then("its failure says what to do differently")
def _says_what_to_do_differently(outcome: dict, a_ticket: str, a_budget_spent_fault: dict) -> None:
    from declaro_persistum.receipt_for import receipt_for

    receipt = receipt_for(outcome["ticket"], outcome["failure"], outcome["attempts"])

    assert receipt["failure"] == outcome["failure"], "the Fault arrives unchanged"
    assert receipt["failure"]["what_to_do_instead"].strip() != "", (
        "a Fault that only says what went wrong leaves the caller nowhere to go"
    )

    # `ok` is derived from the failure, not recorded a second time beside it.
    # A receipt whose `ok` disagreed with its `failure` would pass both Thens
    # above and still be the bug the feature's summary line forbids.
    assert receipt_for(a_ticket, None, 1)["ok"] is True
    assert receipt_for(a_ticket, a_budget_spent_fault, 5)["ok"] is False

    # The ticket moves and the verdict stays put.
    tickets = ["w-0001", "w-0007", "another-ticket-entirely", ""]
    verdicts = [(t, receipt_for(t, a_budget_spent_fault, 5)["ok"]) for t in tickets]
    wrong = [seen for seen in verdicts if seen[1] is not False]
    assert wrong == [], f"the verdict moved with the ticket: {wrong}"

    # Reading it twice reads the same thing, since nothing outside the
    # arguments is consulted.
    assert receipt_for(a_ticket, a_budget_spent_fault, 5) == receipt_for(
        a_ticket, a_budget_spent_fault, 5
    )
