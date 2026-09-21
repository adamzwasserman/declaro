"""Bind `tests/bdd/features/schema/refuse_undeclared_loss.feature`.

WHAT THE FEATURE ASKS FOR. Five scenarios, and between them one decision: an
operation that loses something runs only where a declaration asked for that
loss by name. A column missing from a model is a typo as often as a decision,
so absence is never permission, and nobody is standing at a deploy to be asked.

THE THREE WAYS A LOSS IS ASKED FOR ARE THREE SENTENCES HERE, not one. A
surviving model names a column in `dropping`. A table whose model was deleted
has no model left to ask, so `Declared.dropping_tables` names it, and
`Declared.dropping_views` names a view. The first and third scenarios put a
column and a whole table through the same Then, because they are the same
question asked of two sizes of loss.

THE FIFTH SCENARIO IS ONE I ADDED. The four that were written each carry a
single loss, so a check that stops at the first operation it approves passes
all four. A deploy carries several changes at once. Two drops, one named in
`dropping` and one not, is the shape that loses a column quietly, so the Then
asks that the refusal name the one nobody asked for and leave the other alone.

WHAT A PASS MEANS WHEN NOTHING IS REFUSED. The plan comes back, unchanged and
whole. A check that quietly returned a smaller plan would pass "the column is
dropped" while dropping something else on the floor, so the two Thens that
expect a run compare the answer against the plan that went in.
"""

from __future__ import annotations

import inspect
import re

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.types import Column, Declared, Model, Operation, Plan

scenarios("../features/schema/refuse_undeclared_loss.feature")


THE_TABLE = "Person"
ITS_COLUMNS = ("id", "nickname", "phone")

_QUOTED = re.compile(r'"([^"]+)"')


def _a_column(name: str) -> Column:
    """One column, with every field the declaration names present."""
    return {
        "name": name,
        "declared_type": "text",
        "renamed_from": None,
        "references": None,
        "facets": {},
    }


def _a_model(table: str, dropping: list[str]) -> Model:
    """One model of `THE_TABLE`'s columns, naming what it means to lose."""
    return {
        "table": table,
        "columns": [_a_column(name) for name in ITS_COLUMNS],
        "dropping": dropping,
        "indexes": [],
        "constraints": [],
    }


def _declared(dropping: list[str], dropping_tables: list[str]) -> Declared:
    """Everything the code declares, with one model of `THE_TABLE` in it."""
    return {
        "models": [_a_model(THE_TABLE, dropping)],
        "views": [],
        "dropping_tables": dropping_tables,
        "dropping_views": [],
    }


def _dropping_columns(columns: list[str]) -> Plan:
    """A plan of one drop per column, each already marked as destroying."""
    operations: list[Operation] = [
        {
            "kind": "drop_column",
            "table": THE_TABLE,
            "detail": {"column": column},
            "destroys": True,
        }
        for column in columns
    ]
    return {
        "operations": operations,
        "destroys": [f"{THE_TABLE}.{column}" for column in columns],
    }


def _the_answer_for(plan: Plan, declared: Declared) -> object:
    from declaro_persistum.refuse_undeclared_loss import refuse_undeclared_loss

    return refuse_undeclared_loss(plan, declared)


@given(parsers.re(r'a plan that would drop the column (?P<columns>".+)'))
def _a_plan_dropping_columns(scenario: dict, columns: str) -> None:
    """One column or two, read off the sentence rather than counted in code."""
    named = _QUOTED.findall(columns)
    scenario["plan"] = _dropping_columns(named)
    scenario["losing"] = named


@given(parsers.re(r'a model naming (?:only )?"(?P<column>[^"]+)" as one it means to lose'))
def _a_model_meaning_to_lose(scenario: dict, column: str) -> None:
    """The permission, and the plan it is permission for.

    The second scenario names no plan of its own, so this builds the one its
    Then is about. The fifth scenario named a plan two columns wide already,
    and `setdefault` leaves that one alone rather than shrinking it.
    """
    scenario.setdefault("plan", _dropping_columns([column]))
    scenario.setdefault("losing", [column])
    scenario["declared"] = _declared(dropping=[column], dropping_tables=[])


@given("a plan that would drop a table no model declares")
def _a_plan_dropping_a_table(scenario: dict) -> None:
    scenario["plan"] = {
        "operations": [{"kind": "drop_table", "table": THE_TABLE, "detail": {}, "destroys": True}],
        "destroys": [THE_TABLE],
    }
    scenario["losing"] = [THE_TABLE]


# ONE FUNCTION, TWO SENTENCES. A column nobody asked to lose and a table nobody
# asked to lose are the same absence, so writing them as two step functions
# would be one assertion copied and the word changed.
@given("no model asking to lose it")
@given("nothing declaring that the table is to go")
def _nothing_declares_the_loss(scenario: dict) -> None:
    scenario["declared"] = _declared(dropping=[], dropping_tables=[])

    # The Given has to leave nothing that could be read as permission, or the
    # Then below would be satisfied by a check that never looked.
    declared = scenario["declared"]
    asked = [
        *declared["dropping_tables"],
        *declared["dropping_views"],
        *[name for model in declared["models"] for name in model["dropping"]],
    ]
    assert not asked, f"the Given meant to declare no loss and declared {asked}"


@given("a plan that only adds a column")
def _a_plan_that_only_adds(scenario: dict) -> None:
    scenario["plan"] = {
        "operations": [
            {
                "kind": "add_column",
                "table": THE_TABLE,
                "detail": {"column": "nickname"},
                "destroys": False,
            }
        ],
        "destroys": [],
    }
    scenario["declared"] = _declared(dropping=[], dropping_tables=[])
    scenario["losing"] = []


@then("the migration is refused")
def _the_migration_is_refused(scenario: dict) -> None:
    plan = scenario["plan"]
    declared = scenario["declared"]
    fault = _the_answer_for(plan, declared)

    # A fault and nothing else. A plan riding back alongside one, or a plan
    # with the offending operation quietly removed, would let the open carry
    # on with part of what it was refused.
    assert fault is not None, "the loss nobody declared was allowed through"
    assert sorted(fault) == ["kind", "what_to_do_instead", "what_went_wrong"], sorted(fault)
    assert fault["kind"] == "refused", fault["kind"]
    said = " ".join(str(value) for value in fault.values())
    assert "SELECT" not in said and "ALTER" not in said, f"the refusal carries SQL: {fault}"

    # Nothing in the signature could be a connection, a clock or an engine.
    from declaro_persistum.refuse_undeclared_loss import refuse_undeclared_loss

    taken = list(inspect.signature(refuse_undeclared_loss).parameters)
    assert taken == ["plan", "declared"], taken

    # Asked twice, answered the same, and the plan it was handed is untouched.
    before = [dict(operation) for operation in plan["operations"]]
    assert _the_answer_for(plan, declared) == fault
    assert [dict(operation) for operation in plan["operations"]] == before

    scenario["fault"] = fault


@then("the fault names the column and what to write to allow it")
def _the_fault_names_it_and_what_to_write(scenario: dict) -> None:
    fault = scenario["fault"]
    losing = scenario["losing"]

    for name in losing:
        assert name in fault["what_went_wrong"], (
            f"the refusal does not say what would be destroyed: {fault}"
        )

    # A refusal that only says no leaves a deploy stuck. The declaration to
    # write is `dropping` on the model, so the instruction has to name it and
    # name the column to put in it.
    instead = fault["what_to_do_instead"]
    assert "dropping" in instead, f"the refusal does not say what to write: {instead}"
    for name in losing:
        assert name in instead, f"the refusal does not say which name to write: {instead}"

    # The same plan, once the model asks for the loss, runs. Otherwise
    # "refuses everything" would satisfy every line above.
    allowed = _the_answer_for(scenario["plan"], _declared(dropping=losing, dropping_tables=[]))
    assert allowed == scenario["plan"], f"the declared loss was refused too: {allowed}"


@then(parsers.parse('the fault names "{refused}" and not "{allowed}"'))
def _the_fault_names_only_the_undeclared_one(scenario: dict, refused: str, allowed: str) -> None:
    fault = scenario["fault"]
    said = fault["what_went_wrong"] + " " + fault["what_to_do_instead"]

    assert refused in said, f"the refusal does not name {refused}: {fault}"
    assert allowed not in said, (
        f"{allowed} was declared in `dropping` and the refusal names it anyway: {fault}"
    )


# ONE FUNCTION, TWO SENTENCES. "The column is dropped" and "it runs" are one
# result: the plan comes back, whole and unchanged, for the migration to carry
# out. Each Given recorded the operations it built, and the Then asks for those.
@then("the column is dropped")
@then("it runs")
def _the_plan_comes_back(scenario: dict) -> None:
    plan = scenario["plan"]
    answer = _the_answer_for(plan, scenario["declared"])

    assert answer == plan, f"the plan did not come back as it went in: {answer}"

    # Not a fault wearing a plan's clothes, and not a shorter plan. A check
    # that dropped the operation it approved would pass an equality against a
    # plan it had also shortened, so the operations are counted against the
    # Given rather than against the answer.
    assert sorted(answer) == ["destroys", "operations"], sorted(answer)
    assert len(answer["operations"]) == len(plan["operations"])
    assert [operation["kind"] for operation in answer["operations"]] == [
        operation["kind"] for operation in plan["operations"]
    ]
