"""Bind `tests/bdd/features/core/refuse_some_columns_as_an_object.feature`.

WHAT THE FEATURE ASKS. Four questions, one refused. Naming some columns and
asking for your own model cannot both be honoured: two fields of a five-field
model is not that model, and an object saying it is one would be saying
something untrue.

THE THREE THAT PASS ARE THE POINT. A refusal that fires on the whole row, or on
dictionaries, would stop the ordinary question, and the ordinary question is
almost all of them. Writing all four out is what keeps this from being a guard
that refuses more than it was asked to.

REFUSED WHILE THE CALLER STILL HOLDS THE QUESTION, so the refusal can name both
words they wrote and they can drop either one.
"""

from __future__ import annotations

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.refuse_some_columns_as_an_object import (
    refuse_some_columns_as_an_object,
)
from declaro_persistum.types import Declaration

scenarios("../features/core/refuse_some_columns_as_an_object.feature")

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})
TWO_COLUMNS = ["id", "name"]
NOTHING_ASKED = {"each": {}, "any_of": [], "none_of": []}

# ONE FUNCTION OVER A TABLE. The four openings differ in which columns were
# named and which shape was asked for, so each row is that pair.
WHAT_THE_CALLER_ASKED: dict[str, tuple[list[str], str]] = {
    "a question naming two columns and asking for objects": (TWO_COLUMNS, "objects"),
    "a question naming no columns and asking for objects": ([], "objects"),
    "a question naming two columns and asking for dictionaries": (TWO_COLUMNS, "dictionaries"),
    "a question naming no columns and asking for dictionaries": ([], "dictionaries"),
}


@given(parsers.re(r"^(?P<opening>a question naming .+)$"), target_fixture="answered")
def _the_question(opening: str) -> object:
    columns, shaping = WHAT_THE_CALLER_ASKED[opening]
    asked: Declaration = {
        "model": "Person",
        "wanted": "all",
        "columns": columns,
        "condition": NOTHING_ASKED,
        "order": {},
        "grouping": [],
        "having": NOTHING_ASKED,
        "paging": None,
        "rows": None,
        "shaping": shaping,
        "across": None,
    }
    return refuse_some_columns_as_an_object(asked)


@then("it is refused, and the refusal names both")
def _refused_naming_both(answered: object) -> None:
    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a question asking for part of a row as a whole object answered "
        f"{answered!r}, so a caller is handed an object claiming to be a Person "
        f"with three of its fields missing"
    )
    said = f"{answered['what_went_wrong']} {answered['what_to_do_instead']}"
    for column in TWO_COLUMNS:
        assert column in said, f"the refusal does not name {column!r}: {said!r}"
    assert "dictionaries" in said, (
        f"the refusal does not say the caller could take dictionaries instead, "
        f"so it names the problem and not the way out: {said!r}"
    )


@then("nothing is refused")
def _nothing_refused(answered: object) -> None:
    assert answered is None, (
        f"this question was refused with {answered!r} and there is nothing "
        f"wrong with it. A guard that stops the ordinary question stops almost "
        f"every question."
    )
