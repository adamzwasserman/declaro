"""Bind `tests/bdd/features/core/receipt_for.feature`, and nothing else.

WHAT THE FEATURE ASKS. Four readings of one receipt: whether the write landed,
what its failure says, how many times it tried, and how many rows it touched.

`ok: False` MEANS ONE THING. `docs/architecture.md:152`: the engine stayed busy
through the whole retry budget. Anything the engine refused outright never
becomes a receipt, and a caller that mixes the two retries the one that can
never succeed and gives up on the one that would have worked.

A WRITE THAT MATCHED NOTHING IS NOT A FAILURE, which is the fourth scenario.
`HOW-IT-SHOULD-WORK.md:166`: a change_all that matched nothing returns zero, and
zero is an answer you can act on. A receipt that called it a failure would send
a caller looking for a fault that never happened.
"""

from __future__ import annotations

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.receipt_for import receipt_for
from declaro_persistum.types import Fault, Receipt

scenarios("../features/core/receipt_for.feature")

A_TICKET = "3f2b1c0d9e8a7b6c5d4e3f2a1b0c9d8e"

THE_BUDGET_RAN_OUT: Fault = {
    "kind": "budget_spent",
    "what_went_wrong": "The engine stayed busy for all four attempts.",
    "what_to_do_instead": "Hand the write over again when the database is quieter.",
}

# ONE FUNCTION OVER A TABLE. The four openings differ in what the engine did and
# in nothing else, so each row is the three facts a receipt is built from.
WHAT_THE_ENGINE_DID: dict[str, tuple[int, Fault | None, int]] = {
    "a write that ran on the first attempt and changed one row": (1, None, 1),
    "a write that conflicted past the budget": (0, THE_BUDGET_RAN_OUT, 4),
    "a write that succeeded on the third attempt and changed one row": (1, None, 3),
    "a write that ran on the first attempt and changed no rows": (0, None, 1),
}


@given(parsers.re(r"^(?P<opening>a write .+)$"), target_fixture="answered")
def _what_the_engine_did(opening: str) -> Receipt:
    touched, failure, attempts = WHAT_THE_ENGINE_DID[opening]
    return receipt_for(A_TICKET, touched, failure, attempts)


@then("the receipt says it succeeded and carries no failure")
def _it_landed(answered: Receipt) -> None:
    assert answered["ok"] is True, (
        f"this write says ok is {answered['ok']!r}, and ok false means one "
        f"thing only: the engine stayed busy through the whole budget. A caller "
        f"reading it would hand the write over again for no reason."
    )
    assert answered["failure"] is None, (
        f"a receipt that succeeded carries {answered['failure']!r}, so a caller "
        f"is handed a fault for a write that worked"
    )
    assert answered["id"] == A_TICKET, (
        f"the receipt is filed under {answered['id']!r} and the caller holds "
        f"{A_TICKET!r}, so it would collect somebody else's outcome"
    )


@then("the receipt says it did not land")
def _it_did_not(answered: Receipt) -> None:
    assert answered["ok"] is False, (
        f"a write that spent its whole budget says ok is {answered['ok']!r}, so "
        f"the caller believes a row is there that is not"
    )


@then("its failure says what to do differently")
def _the_failure_says_what_to_do(answered: Receipt) -> None:
    failure = answered["failure"]
    assert failure is not None, "the write did not land and nothing says why"
    assert failure["what_to_do_instead"], (
        f"the failure says {failure['what_went_wrong']!r} and nothing about what "
        f"to do instead, which is half a fault"
    )


# ONE FUNCTION OVER A TABLE. The two readings differ in the count and nothing else.
HOW_MANY_ROWS = {"one row was touched": 1, "no rows were touched": 0}


@then(parsers.re(r"^it says (?P<reading>one row was touched|no rows were touched)$"))
def _how_many_rows(answered: Receipt, reading: str) -> None:
    wanted = HOW_MANY_ROWS[reading]
    assert answered["touched"] == wanted, (
        f"the receipt says {answered['touched']} rows and the scenario says "
        f"{wanted}. That number is what a caller gets back from a write, so a "
        f"wrong one is a wrong answer rather than a missing detail."
    )


@then("the answer says it took three")
def _three_attempts(answered: Receipt) -> None:
    assert answered["attempts"] == 3, (
        f"the receipt says {answered['attempts']} attempts. A caller told its "
        f"write landed cannot tell a first attempt from a fourth, which is the "
        f"difference between a quiet database and one about to stop taking "
        f"writes at all."
    )
