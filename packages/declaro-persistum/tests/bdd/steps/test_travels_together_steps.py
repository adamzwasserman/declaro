"""Bind `tests/bdd/features/core/travels_together.feature`.

WHAT THE FUNCTION ANSWERS. `travels_together` is handed ONE declaration and
says whether the writes inside it have to land together. One row is a deposit
and a ticket, so the answer is no. Several rows handed to one call are a
transaction, because the caller asked for all of them or none, so the answer
is yes. A read never raises the question.

WHY SEVERAL ROWS CANNOT GO THROUGH THE ROOM. `docs/architecture.md` line 150:
two deposits are two independent writes, and the second failing does not undo
the first, because the first is already in the log. So a `True` here is an
instruction to hold a connection instead of depositing.

WHERE THE FOLDED-IN SCENARIOS REACH PAST THIS FUNCTION. Seven scenarios came
from `running/several_writes_travel_together.feature`, and four of them talk
about a GROUP: several separate calls the caller wants to land together. A
group is not a `Declaration` and this function never sees one, so those
scenarios are bound to the part of each claim this function owns, and the
assertion message names the part it does not. Read the messages before
believing a green run covers rollback, refusal, or read-your-own-writes.
"""

import typing

import pytest
from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum import types as declared_types

scenarios("../features/core/travels_together.feature")


HOW_MANY = {"one": 1, "two": 2, "three": 3}

# What a pure function may not reach for: a clock, a random source, the
# environment, a file, a connection. The module is read for these by name.

# Names a `Declaration` field would use if it were carrying a connection.


def _rows(count: int) -> list[dict]:
    """`count` rows of the shape `people` would hold."""
    return [{"id": n, "name": f"person {n}"} for n in range(count)]


def _a_declaration(wanted: str, rows: list | None) -> dict:
    """One declaration, with every field the declaration names present."""
    nothing_asked = {"each": {}, "any_of": [], "none_of": []}
    return {
        "model": "people",
        "wanted": wanted,
        "columns": [],
        "condition": nothing_asked,
        "order": {},
        "grouping": [],
        "having": nothing_asked,
        "paging": None,
        "rows": rows,
        "across": None,
    }


def _answer_for(declaration: dict) -> bool:
    from declaro_persistum.travels_together import travels_together

    return travels_together(declaration)


@pytest.fixture
def seen() -> dict:
    """Where a Given or a When leaves its declarations for the Then."""
    return {}


# --- the three scenarios that ask the question directly ---------------------


@given(parsers.re(r"a declaration carrying (?P<how_many>one|two|three) rows?"))
def _a_declaration_carrying_rows(seen: dict, how_many: str) -> None:
    seen["declarations"] = [_a_declaration("add", _rows(HOW_MANY[how_many]))]


@given("a declaration asking for every person")
def _a_declaration_asking_for_every_person(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("all", None)]


@then("it does not have to travel together")
def _it_does_not_have_to_travel_together(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is False, (
        "one row is a deposit and a ticket, so nothing has to land beside it"
    )


@then("they have to land together")
def _they_have_to_land_together(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is True, (
        "the caller asked for all of these rows or none, so they are a transaction"
    )


@then("the question does not arise")
def _the_question_does_not_arise(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is False, "a read lands nothing, so nothing lands with it"

    # Every reading word answers the same way, and carrying rows does not
    # change it. A word that answered `True` here would send a read down the
    # path that holds a connection for rollback it has no use for.
    reading_words = typing.get_args(declared_types.Reading)
    wrong = [
        word
        for word in reading_words
        for carried in (None, _rows(3))
        if _answer_for(_a_declaration(word, carried)) is not False
    ]
    assert wrong == [], f"these reading words raised the question: {wrong}"


# --- folded in: a list handed to one call, against two separate calls -------


# --- folded in: the group scenarios -----------------------------------------


# NOT THE REFUSAL. Naming a column no model declares is refused by
# `refuse_undeclared_columns`, which runs first and is tested by its own
# feature. This function does not read the columns at all.
