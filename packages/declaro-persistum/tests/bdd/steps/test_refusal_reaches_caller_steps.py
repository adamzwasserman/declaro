"""Bind `tests/bdd/features/core/refusal_reaches_caller.feature`.

WHAT THE FEATURE ASKS FOR. Three scenarios, and between them one decision: a
fault the engine refused outright is raised to the caller that wrote it, and a
fault that only ever met contention comes back as a receipt saying so. A caller
that cannot tell those apart retries the write that can never succeed and gives
up on the one that would have worked.

The third scenario is the one with teeth. It says the kind decides and the text
never does. So the Then holds each kind still and moves the two message fields
through eight texts, including the text of the kind that answers the other way
and a pair of empty strings. If any line of the function read
`what_went_wrong` or `what_to_do_instead`, the answer moves and the Then says
which kind it moved for.

`Fault` names four kinds and the feature names two of them by scenario. The
Thens also pin the other two, because a decision over a closed vocabulary that
answers only two of its four words raises KeyError on the first `cut_short`.
"""

import pytest
from pytest_bdd import given, parsers, scenarios, then

scenarios("../features/core/refusal_reaches_caller.feature")


def a_fault(kind: str, what_went_wrong: str, what_to_do_instead: str) -> dict:
    """A Fault, as the declaration shapes it: a kind and two sentences."""
    return {
        "kind": kind,
        "what_went_wrong": what_went_wrong,
        "what_to_do_instead": what_to_do_instead,
    }


# The two writes the feature's Givens describe, in the words it uses for them.
A_WRITE_THAT = {
    "the engine refused outright": a_fault(
        "refused",
        "UNIQUE constraint failed: person.id",
        "give the row an id no other row holds, or update the row that has it",
    ),
    "that only ever met contention": a_fault(
        "budget_spent",
        "every attempt inside the retry budget hit a busy database",
        "deposit the write again, or raise the retry budget",
    ),
}


@pytest.fixture
def the_write() -> dict:
    """Where a Given leaves its fault, for a Then to read."""
    return {}


@given(parsers.parse("a write {described}"))
def _a_write(the_write: dict, described: str) -> None:
    the_write["fault"] = A_WRITE_THAT[described]


@then("it is raised to the caller that wrote it")
def _is_raised(the_write: dict) -> None:
    from declaro_persistum.refusal_reaches_caller import refusal_reaches_caller

    assert refusal_reaches_caller(the_write["fault"]) is True

    # A refusal is not the engine staying busy, and neither is the platform
    # killing the work part way. Both reach the caller.
    assert (
        refusal_reaches_caller(a_fault("refused", "no such column: nmae", "ask for name")) is True
    )
    assert (
        refusal_reaches_caller(a_fault("cut_short", "killed mid-drain", "run the write again"))
        is True
    )


@then("it comes back as a receipt saying so")
def _comes_back_as_a_receipt(the_write: dict) -> None:
    from declaro_persistum.refusal_reaches_caller import refusal_reaches_caller

    assert refusal_reaches_caller(the_write["fault"]) is False

    # A receipt whose `ok` is false says the engine stayed busy and says
    # nothing else. A bare conflict is the engine staying busy too.
    assert (
        refusal_reaches_caller(a_fault("conflict", "two writers wrote one row", "try again"))
        is False
    )


# Eight pairs of messages, put under every kind in turn. Two of them are the
# real text of a refusal, two the real text of a spent budget, and the rest are
# empty, blank, or the other kind's word said outright.
MESSAGES = [
    ("UNIQUE constraint failed: person.id", "give the row an id no other row holds"),
    ("no such column: nmae", "Person declares id and name; ask for name"),
    ("every attempt inside the retry budget hit a busy database", "deposit the write again"),
    ("database is locked", "try again"),
    ("refused", "refused"),
    ("conflict, budget_spent, cut_short", "raise it"),
    ("", ""),
    ("   ", "\n"),
]


@then("nothing read the text of the failure")
def _nothing_read_the_text() -> None:
    from declaro_persistum.refusal_reaches_caller import refusal_reaches_caller

    kinds = ["refused", "conflict", "budget_spent", "cut_short"]

    # Hold the kind still, move the words. Any kind whose answer is not the
    # same eight times read something it had no business reading.
    moved = {
        kind: sorted(
            {refusal_reaches_caller(a_fault(kind, wrong, instead)) for wrong, instead in MESSAGES}
        )
        for kind in kinds
    }
    read_the_text = {kind: answers for kind, answers in moved.items() if len(answers) > 1}
    assert read_the_text == {}, f"the text moved the answer: {read_the_text}"

    # The decision is total over the four kinds the vocabulary names, so no
    # caller of `query_deliver` meets a KeyError instead of an answer, and it
    # is a decision rather than a constant.
    answers = {kind: refusal_reaches_caller(a_fault(kind, "", "")) for kind in kinds}
    assert all(isinstance(answer, bool) for answer in answers.values()), answers
    assert set(answers.values()) == {True, False}, (
        f"a decision that answers every kind the same way is not a decision: {answers}"
    )

    # Same fault, same answer, twice. Nothing outside the argument is read.
    settled = a_fault("refused", "UNIQUE constraint failed: person.id", "use another id")
    assert refusal_reaches_caller(settled) == refusal_reaches_caller(settled)
