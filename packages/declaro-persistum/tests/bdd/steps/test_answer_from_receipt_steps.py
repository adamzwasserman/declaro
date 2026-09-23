"""Bind `tests/bdd/features/core/answer_from_receipt.feature`, and nothing else.

WHAT THE FEATURE ASKS. A write that landed gives back its count, a write that
matched nothing gives back zero, and a write whose budget ran out gives back the
fault the crew wrote.

ZERO IS NOT A FAILURE, which is the second scenario. `HOW-IT-SHOULD-WORK.md:166`
says a change that matched nothing returns zero and zero is an answer you can
act on. A caller handed a fault for it would go looking for a problem that never
happened.

THE FAULT TRAVELS UNCHANGED. A refusal reworded twice is a refusal whose author
nobody can find, so the third scenario compares the whole fault rather than
looking for words in it.

THE LAST SCENARIO IS THE ONE THAT KEEPS THE QUEUE OFF THE SURFACE. A ticket
reaching a caller is the library's own machinery showing through, and that is
what the collect door was.
"""

from __future__ import annotations

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.answer_from_receipt import answer_from_receipt
from declaro_persistum.receipt_for import receipt_for
from declaro_persistum.types import Fault

scenarios("../features/core/answer_from_receipt.feature")

A_TICKET = "9c8b7a6d5e4f3a2b1c0d9e8f7a6b5c4d"

THE_BUDGET_RAN_OUT: Fault = {
    "kind": "budget_spent",
    "what_went_wrong": "The engine stayed busy for all four attempts.",
    "what_to_do_instead": "Hand the write over again when the database is quieter.",
}

# ONE FUNCTION OVER A TABLE. The three openings differ in what the crew did.
WHAT_THE_CREW_DID: dict[str, tuple[int, Fault | None, int]] = {
    "a write that changed three rows": (3, None, 1),
    "a write that changed no rows": (0, None, 1),
    "a write whose retry budget ran out": (0, THE_BUDGET_RAN_OUT, 4),
}


@given(parsers.re(r"^(?P<opening>a write .+)$"), target_fixture="answered")
def _what_the_crew_did(opening: str) -> object:
    touched, failure, attempts = WHAT_THE_CREW_DID[opening]
    return answer_from_receipt(receipt_for(A_TICKET, touched, failure, attempts))


@then("the caller is given three")
def _three(answered: object) -> None:
    assert answered == 3, (
        f"the write changed three rows and the caller was given {answered!r}, "
        f"which is the answer to the question they asked"
    )


@then("the caller is given zero, and it is not a failure")
def _zero(answered: object) -> None:
    assert answered == 0, f"the write matched nothing and the caller was given {answered!r}"
    assert not isinstance(answered, dict), (
        f"a write that matched nothing came back as {answered!r}, so a caller "
        f"goes looking for a problem that never happened. Zero is an answer."
    )


@then("the caller is given that fault, unchanged")
def _the_same_fault(answered: object) -> None:
    assert answered == THE_BUDGET_RAN_OUT, (
        f"the caller was given {answered!r}. A refusal reworded on its way out "
        f"is a refusal whose author nobody can find, and this one has to stay "
        f"readable as a busy engine rather than as a refused write."
    )


@when("every kind of receipt is turned into an answer", target_fixture="every_answer")
def _every_kind() -> list[object]:
    return [
        answer_from_receipt(receipt_for(A_TICKET, touched, failure, attempts))
        for touched, failure, attempts in WHAT_THE_CREW_DID.values()
    ]


@then("not one of them hands the caller a ticket")
def _no_ticket_escapes(every_answer: list[object]) -> None:
    leaked = [answer for answer in every_answer if answer == A_TICKET]
    assert not leaked, (
        f"{leaked} handed the caller its ticket. A ticket is how the crew joins "
        f"a write to its outcome and it stops here; a caller holding one is the "
        f"library's own queue showing on its surface."
    )
    assert len(every_answer) == len(WHAT_THE_CREW_DID) >= 3, (
        "fewer receipts were turned into answers than there are kinds of receipt"
    )
