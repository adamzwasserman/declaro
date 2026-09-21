"""Bind `tests/bdd/features/core/refuse_undeclared_columns.feature`.

WHAT THE FEATURE ASKS FOR. Three scenarios, and between them one decision: a
column no model declares stops the caller in their own process, and the
refusal names the model and lists the columns that model does declare. A
refusal that only said "no such column: nmae" would be the engine's sentence
repeated back, and the caller would still have to go and read the model.

A DECLARATION NAMES COLUMNS IN FIVE PLACES. The columns asked for, the
condition, the order, the grouping and the having. The first Then puts the
misspelled column in each of the five in turn, because a check that reads only
`columns` passes every scenario anyone would think to write and still lets
`order` reach the engine.

THE SECOND SCENARIO IS ABOUT WHAT DID NOT HAPPEN, so it asks what comes back
and what is taken: a fault carrying the three fields `Fault` names and nothing
that looks like SQL, two arguments neither of which could be a connection, and
the same answer twice for the same question.

THE THIRD SCENARIO CANNOT BE ASKED OF FOUR SURFACES that do not exist yet, so
it is asked of the shape instead. A grammar is a thin skin over one
declaration, so four grammars asking one question hand this function one
declaration. The Then builds that declaration once per grammar the vocabulary
names and asserts the four refusals are the same data.
"""

from __future__ import annotations

import inspect
from typing import get_args

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.types import Condition, Declaration, Grammar, Model

scenarios("../features/core/refuse_undeclared_columns.feature")


NOTHING_ASKED: Condition = {"each": {}, "any_of": [], "none_of": []}


def _a_model(table: str, columns: list[str]) -> Model:
    """One model, with every field the declaration names present."""
    return {
        "table": table,
        "columns": [
            {
                "name": name,
                "declared_type": "text",
                "renamed_from": None,
                "references": None,
                "facets": {},
            }
            for name in columns
        ],
        "dropping": [],
        "indexes": [],
        "constraints": [],
    }


def _asking_for(column: str) -> Condition:
    """A condition narrowing on one column."""
    return {
        "each": {column: {"operator": "eq", "value": "bob", "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


# THE FIVE PLACES A DECLARATION NAMES COLUMNS, each row saying how one column
# gets named there. One function over a table, because a Then per place would
# be five copies of one assertion differing only in the word.
NAMES_A_COLUMN: dict[str, object] = {
    "the columns asked for": lambda column: {"columns": [column]},
    "the condition": lambda column: {"condition": _asking_for(column)},
    "the order": lambda column: {"order": {column: "ascending"}},
    "the grouping": lambda column: {"grouping": [column]},
    "the having": lambda column: {"having": _asking_for(column)},
}


def _a_declaration(model: str, **named: object) -> Declaration:
    """A declaration asking for every row of `model`, with one part replaced."""
    asking: dict[str, object] = {
        "model": model,
        "wanted": "all",
        "columns": [],
        "condition": NOTHING_ASKED,
        "order": {},
        "grouping": [],
        "having": NOTHING_ASKED,
        "paging": None,
        "rows": None,
        "across": None,
    }
    asking.update(named)
    return asking  # type: ignore[return-value]


def _the_answer_for(model: Model, declaration: Declaration) -> object:
    from declaro_persistum.refuse_undeclared_columns import refuse_undeclared_columns

    return refuse_undeclared_columns(model, declaration)


@given(parsers.parse("a model {model} declaring {columns}"))
def _a_model_declaring(scenario: dict, model: str, columns: str) -> None:
    declared = columns.split(" and ")
    scenario["model"] = _a_model(model, declared)
    scenario["declared"] = declared


@when(parsers.parse('a caller asks for "{column}"'))
def _a_caller_asks_for(scenario: dict, column: str) -> None:
    scenario["asked"] = column


@then(parsers.parse("the refusal names {model} and lists {columns}"))
def _the_refusal_names_the_model(scenario: dict, model: str, columns: str) -> None:
    asked = scenario["asked"]
    declared = scenario["declared"]
    listed = columns.split(" and ")
    assert listed == declared, f"the Given declared {declared} and the Then expects {listed}"

    # Every place a declaration can name a column, one at a time. A check that
    # reads `columns` alone passes the first row and lets the other four reach
    # the engine.
    for place, name_it in NAMES_A_COLUMN.items():
        fault = _the_answer_for(scenario["model"], _a_declaration(model, **name_it(asked)))
        assert fault is not None, f"{asked} named in {place} was not refused"
        assert fault["kind"] == "refused", f"{place}: {fault['kind']}"
        assert asked in fault["what_went_wrong"], (
            f"{place}: the refusal does not say which column was asked for: {fault}"
        )
        instead = fault["what_to_do_instead"]
        assert model in instead, f"{place}: the refusal does not name the model: {instead}"
        assert all(name in instead for name in declared), (
            f"{place}: the refusal does not list {declared}: {instead}"
        )

    # A column the model does declare is not refused, in any of the five
    # places. Otherwise "refuses everything" would pass every line above.
    for place, name_it in NAMES_A_COLUMN.items():
        for name in declared:
            answer = _the_answer_for(scenario["model"], _a_declaration(model, **name_it(name)))
            assert answer is None, f"{name} is declared and {place} refused it: {answer}"

    # Nested conditions are conditions. A column misspelled three levels down
    # is as undeclared as one at the top.
    buried: Condition = {
        "each": {},
        "any_of": [{"each": {}, "any_of": [_asking_for(asked)], "none_of": []}],
        "none_of": [],
    }
    assert (
        _the_answer_for(scenario["model"], _a_declaration(model, condition=buried)) is not None
    ), "a column named inside a nested condition reached the engine"


@then("no statement was compiled and no connection was used")
def _nothing_reached_the_database(scenario: dict) -> None:
    model = _a_model("Person", ["id", "name"])
    fault = _the_answer_for(model, _a_declaration("Person", columns=["nmae"]))

    # The answer is a fault and nothing else. A statement, a parameter list or
    # a handle riding along would mean something was built or opened.
    assert fault is not None
    assert sorted(fault) == ["kind", "what_to_do_instead", "what_went_wrong"], sorted(fault)
    said = " ".join(str(value) for value in fault.values()).lower()
    assert "select" not in said, f"the refusal carries SQL: {fault}"

    # Nothing in the signature could be a connection. Two arguments, and the
    # declaration names both.
    from declaro_persistum.refuse_undeclared_columns import refuse_undeclared_columns

    taken = list(inspect.signature(refuse_undeclared_columns).parameters)
    assert taken == ["model", "declaration"], taken

    # Asked twice, answered the same. Nothing outside the two arguments is read.
    again = _the_answer_for(model, _a_declaration("Person", columns=["nmae"]))
    assert again == fault

    scenario["fault"] = fault


@then("the four grammars give the same refusal")
def _every_grammar_gives_the_same_refusal(scenario: dict) -> None:
    grammars = get_args(Grammar)
    assert len(grammars) == 4, grammars

    model = _a_model("Person", ["id", "name"])

    # A grammar is a thin skin over one declaration, so four grammars asking
    # one question hand this function one declaration. Build it once per
    # grammar and the four refusals have to be the same data.
    refusals = {
        grammar: _the_answer_for(
            model,
            _a_declaration("Person", columns=["nmae"], condition=_asking_for("nmae")),
        )
        for grammar in grammars
    }
    first = refusals[grammars[0]]
    assert first is not None
    differing = {name: fault for name, fault in refusals.items() if fault != first}
    assert not differing, f"the refusal moved with the grammar: {differing}"

    # The refusal it gives them is the one the first scenario describes: it
    # names the model and lists the columns the model declares.
    assert first["kind"] == "refused"
    assert "nmae" in first["what_went_wrong"], first
    assert "Person" in first["what_to_do_instead"], first
    assert all(name in first["what_to_do_instead"] for name in ("id", "name")), first
