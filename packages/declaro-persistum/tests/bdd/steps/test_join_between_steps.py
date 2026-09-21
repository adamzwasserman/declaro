"""Bind `tests/bdd/features/core/join_between.feature`.

WHAT THE FUNCTION ANSWERS. `join_between` is handed two models and the
condition a caller wrote about the far one, and it works out the join. A
foreign key already says the two are related, so nothing here asks the caller
to say it again. `HOW-IT-SHOULD-WORK.md` line 178: "You declared the
relationship once, in the model, and naming it again in every query is the
duplication this library exists to remove."

WHERE THE RELATIONSHIP IS WRITTEN DOWN. `docs/design/persistum.hd` line 125: a
foreign key is declared once, on the column that points, and that is
`Column.references`. Nothing else in the package records a relationship, so
these Thens move that one field and watch the answer move with it. A function
that read the column's name, its type or its position would pass the first
assertion of each Then and fail the ones that follow.

WHAT A SCENARIO ESTABLISHES. Each When names two models and a far-side
condition, all three from one table, so the three sentences that differ only in
what the caller asked are one step function. The Given records the column the
first scenario's sentence names, which is the "that column" its Then reads.

WHAT IS NOT TESTED HERE. Nothing runs, nothing connects, and no SQL is
written. This function decides which two columns line up; rendering that into a
statement is `compile_statement`'s answer and has its own feature.
"""

from __future__ import annotations

import ast
import pathlib
import re

from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/core/join_between.feature")


# --- the models the scenarios talk about ------------------------------------


def _a_column(name: str, declared_type: str, references: str | None) -> dict:
    """One column, with every field the declaration gives a Column."""
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": None,
        "references": references,
        "facets": {},
    }


def _a_model(table: str, columns: list[dict]) -> dict:
    """One model, with every field the declaration gives a Model."""
    return {
        "table": table,
        "columns": columns,
        "dropping": [],
        "indexes": [],
        "constraints": [],
    }


PERSON = _a_model(
    "people",
    [
        _a_column("id", "INTEGER", None),
        _a_column("name", "TEXT", None),
    ],
)

# The pointing column, and the only place the relationship is written down.
THE_COLUMN_THAT_POINTS = "person_id"
THE_COLUMN_IT_POINTS_AT = "id"

ORDER = _a_model(
    "orders",
    [
        _a_column("id", "INTEGER", None),
        _a_column(THE_COLUMN_THAT_POINTS, "INTEGER", "people.id"),
        _a_column("total", "INTEGER", None),
    ],
)

# Two models with no key between them, in either direction.
WEATHER = _a_model(
    "weather",
    [
        _a_column("id", "INTEGER", None),
        _a_column("city", "TEXT", None),
        _a_column("degrees", "INTEGER", None),
    ],
)

RECIPES = _a_model(
    "recipes",
    [
        _a_column("id", "INTEGER", None),
        _a_column("title", "TEXT", None),
    ],
)


def _nothing_asked() -> dict:
    """A condition that narrows nothing, with every field a Condition has."""
    return {"each": {}, "any_of": [], "none_of": []}


def _far_side_is(column: str, value: str | int) -> dict:
    """The condition a caller wrote about the far model."""
    return {
        "each": {column: {"operator": "eq", "value": value, "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


# ALICE IS ONE ROW OF `people`. `Order.of(person)` hands over a person, which
# narrows the far side to that person and nothing else.
ALICE = _far_side_is("id", 1)
BOB = _far_side_is("name", "bob")


def _answer_for(near: dict, far: dict, condition: dict) -> dict:
    from declaro_persistum.join_between import join_between

    return join_between(near, far, condition)


# --- what the caller asked, in one table ------------------------------------
#
# ONE STEP FUNCTION, NOT THREE. The three Whens differ only in which pair of
# models and which condition the caller handed over, so the words are data and
# the regex below is built from these keys.

WHAT_THE_CALLER_ASKED = {
    "for the orders of alice": (ORDER, PERSON, ALICE),
    'for orders where the person\'s name is "bob"': (ORDER, PERSON, BOB),
    "across two unrelated models": (WEATHER, RECIPES, _nothing_asked()),
}

_ASKED = "a caller asks (?P<asked>" + "|".join(re.escape(k) for k in WHAT_THE_CALLER_ASKED) + ")"


@given("Order declaring a column that references Person")
def _order_declares_a_column_that_references_person(scenario: dict) -> None:
    """The sentence names one column, and the Then reads it back as "that column"."""
    scenario["that_column"] = THE_COLUMN_THAT_POINTS


@when(parsers.re(_ASKED))
def _a_caller_asks(scenario: dict, asked: str) -> None:
    near, far, condition = WHAT_THE_CALLER_ASKED[asked]
    scenario["near"] = near
    scenario["far"] = far
    scenario["condition"] = condition


# --- the three results ------------------------------------------------------


@then("the join was worked out from that column")
def _worked_out_from_that_column(scenario: dict) -> None:
    join = _answer_for(scenario["near"], scenario["far"], scenario["condition"])

    assert set(join) == {"model", "by_column", "to_column", "condition"}, (
        f"a Join carries four things: {join}"
    )
    assert join["by_column"] == scenario["that_column"], (
        "the column that points is the column the join is made on"
    )
    assert join["to_column"] == THE_COLUMN_IT_POINTS_AT
    assert join["model"] == PERSON["table"], "the join names the far model"
    assert join["condition"] == scenario["condition"], "the far-side condition arrives unchanged"

    # IT READS `references` AND NOTHING ELSE. The relationship moves to another
    # column, pointing at another column, and the answer has to move with it. A
    # function reading the column's name, its type or its position would pass
    # every line above and fail here.
    moved = _a_model(
        "orders",
        [
            _a_column("id", "INTEGER", None),
            _a_column("buyer", "TEXT", "people.name"),
            _a_column(THE_COLUMN_THAT_POINTS, "INTEGER", None),
        ],
    )
    answer = _answer_for(moved, PERSON, scenario["condition"])
    assert (answer["by_column"], answer["to_column"]) == ("buyer", "name"), (
        f"the join did not follow the foreign key: {answer}"
    )

    # The same models answer the same way every time, because nothing outside
    # the three arguments is consulted.
    assert _answer_for(scenario["near"], scenario["far"], scenario["condition"]) == join

    # A pure function reads no clock, no random source, no environment, no file
    # and no connection, so the module imports nothing but the declared types.
    from declaro_persistum import join_between as the_module

    source = pathlib.Path(the_module.__file__).read_text(encoding="utf-8")
    imported = {
        (node.module or "").split(".")[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    } | {
        alias.name.split(".")[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert imported <= {"declaro_persistum", "__future__", "collections", "typing"}, (
        f"a pure function reaches for nothing outside its arguments, and this "
        f"module imports {sorted(imported)}"
    )


@then("the same column is what joined them")
def _the_same_column_joined_them(scenario: dict) -> None:
    join = _answer_for(scenario["near"], scenario["far"], scenario["condition"])

    assert (join["by_column"], join["to_column"]) == (
        THE_COLUMN_THAT_POINTS,
        THE_COLUMN_IT_POINTS_AT,
    ), f"the foreign key joins these two models however the caller narrowed them: {join}"

    # NARROWING THE FAR SIDE MOVES THE CONDITION AND NOTHING ELSE. Asking for
    # alice and asking for the people called bob are the same join, carrying
    # different conditions, so the two answers differ in one field.
    asking_for_alice = _answer_for(ORDER, PERSON, ALICE)
    differ = [field for field in join if join[field] != asking_for_alice[field]]
    assert differ == ["condition"], f"the join moved with what the caller asked: {differ}"
    assert join["condition"] == BOB, "the far-side condition arrives unchanged"


@then("a fault comes back naming both and saying no key joins them")
def _a_fault_naming_both(scenario: dict) -> None:
    fault = _answer_for(scenario["near"], scenario["far"], scenario["condition"])

    assert set(fault) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
        f"a Fault carries three things, and this is not one: {fault}"
    )
    assert fault["kind"] == "refused", (
        "no key exists to work out, and asking again will not make one appear"
    )

    said = fault["what_went_wrong"]
    named = [
        model["table"] for model in (scenario["near"], scenario["far"]) if model["table"] in said
    ]
    assert named == [scenario["near"]["table"], scenario["far"]["table"]], (
        f"the fault has to name both models, and it says: {said}"
    )
    assert "key" in said, f"the fault has to say what is missing, and it says: {said}"
    assert fault["what_to_do_instead"].strip() != "", (
        "a fault that only says what went wrong leaves the caller nowhere to go"
    )

    # THE FAILURE COMES BACK, IT IS NOT RAISED. Reaching this line is the whole
    # assertion: a function that raised would have failed the scenario in the
    # call above, before any of these lines ran.

    # And it turns on the key, not on the models being strangers. Give either
    # one a column pointing at the other and a join comes back instead.
    pointing = _a_model(
        "recipes",
        [
            _a_column("id", "INTEGER", None),
            _a_column("weather_id", "INTEGER", "weather.id"),
        ],
    )
    joined = _answer_for(WEATHER, pointing, scenario["condition"])
    assert joined["by_column"] == "id" and joined["to_column"] == "weather_id", (
        f"a key declared on the far model still joins the two: {joined}"
    )
