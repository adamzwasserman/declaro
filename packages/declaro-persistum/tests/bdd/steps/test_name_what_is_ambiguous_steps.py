"""Bind `tests/bdd/features/core/name_what_is_ambiguous.feature`.

WHAT THE FEATURE ASKS FOR. A column is gone and another has appeared. Either
somebody renamed it and the rows moved with it, or somebody dropped one column
and added another and the rows went. Comparing the models against the live
database cannot tell those two apart, so `fn name_what_is_ambiguous` reads the
answer out of the models and refuses when the models do not give one.

THE OPERATIONS ARE BUILT HERE RATHER THAN DIFFED. `fn difference` produces
them and is not written yet, so every scenario hands this function the two
operations by hand. That is also what keeps these scenarios about one
question: a drop beside an add, and what the models say about the pair.

WHY EVERY "nothing is refused" SCENARIO CARRIES A REFUSAL OF ITS OWN. A
function that refuses nothing would pass three of the five scenarios. So each
Given that expects no refusal also puts the nearest input that MUST be refused
into `but_refused`, and the Then asks for both answers. For the model that
named the column in `dropping`, that is the same change with the `dropping`
list emptied, which proves the declaration did the work rather than the
function being blind.

THE FAULT COMES BACK, IT IS NOT RAISED. So the Thens read the return value.
`Fault` carries three fields and a list of operations is a list, which is how
the second scenario tells a refusal from an answer without catching anything.
"""

from __future__ import annotations

import inspect

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.types import Column, Declared, Model, Operation, OperationKind

scenarios("../features/core/name_what_is_ambiguous.feature")


# THE SPELLINGS THE FIVE FUNCTIONS BEING WRITTEN AGAINST `Operation` AGREED ON,
# 2026-09-21. A column operation carries its column's name under `column`, which
# is the key `fn name_what_destroys` reads to say `users.nickname` rather than
# `users`. A rename is about a column that is already there, so `column` is the
# name it had and `to` is the name it takes.
COLUMN = "column"
TO = "to"

FAULT_FIELDS = ("kind", "what_to_do_instead", "what_went_wrong")


def _a_column(name: str, replaced: str | None) -> Column:
    """One declared column, which names what it replaced or names nothing."""
    return {
        "name": name,
        "declared_type": "text",
        "renamed_from": replaced,
        "references": None,
        "facets": {},
    }


def _a_model(table: str, columns: list[Column], dropping: list[str]) -> Model:
    """One model, with every field the declaration names present."""
    return {
        "table": table,
        "columns": columns,
        "dropping": dropping,
        "indexes": [],
        "constraints": [],
    }


def _declaring(models: list[Model]) -> Declared:
    """Everything the code declares, which here is models and nothing else."""
    return {"models": models, "views": [], "dropping_tables": [], "dropping_views": []}


# WHAT A COMPARISON EMITS ABOUT ONE COLUMN, keyed by the kind. A column gone
# and a column appeared are one operation apart from the kind and whether
# carrying it out loses what was in the column, so they are one row each rather
# than two functions differing by a word.
GONE: OperationKind = "drop_column"
APPEARED: OperationKind = "add_column"
LOSES_WHAT_WAS_IN_IT: dict[OperationKind, bool] = {GONE: True, APPEARED: False}


def _about_a_column(kind: OperationKind, table: str, column: str) -> Operation:
    """The operation a comparison emits for a column that is gone or appeared."""
    return {
        "kind": kind,
        "table": table,
        "detail": {COLUMN: column},
        "destroys": LOSES_WHAT_WAS_IN_IT[kind],
    }


def _the_answer_for(operations: list[Operation], declared: Declared) -> object:
    from declaro_persistum.name_what_is_ambiguous import name_what_is_ambiguous

    return name_what_is_ambiguous(operations, declared)


def _is_a_fault(answer: object) -> bool:
    """A refusal is a dict carrying the three fields `Fault` names."""
    return isinstance(answer, dict) and tuple(sorted(answer)) == FAULT_FIELDS


# =============================================================================
# What each scenario establishes
# =============================================================================
#
# ONE BUILDER, FIVE SENTENCES. Each Given below says which columns moved and
# what the model says about them, and hands the same builder the answer. The
# assertions live in the Thens, so a sixth phrasing costs one line here.


def _a_change(
    scenario: dict,
    operations: list[Operation],
    declared: Declared,
    gone: str,
    appeared: str,
    but_refused: list[tuple[list[Operation], Declared]],
) -> None:
    """What went into the comparison, and what the models say about it."""
    scenario["operations"] = operations
    scenario["declared"] = declared
    scenario["gone"] = gone
    scenario["appeared"] = appeared
    scenario["but_refused"] = but_refused


@given(parsers.parse('a column "{appeared}" declaring that it replaced "{gone}"'))
def _a_declared_rename(scenario: dict, appeared: str, gone: str) -> None:
    table = "people"
    _a_change(
        scenario,
        [_about_a_column(GONE, table, gone), _about_a_column(APPEARED, table, appeared)],
        _declaring([_a_model(table, [_a_column(appeared, gone)], [])]),
        gone,
        appeared,
        [],
    )


@given(parsers.parse('"{gone}" gone and "{appeared}" appeared, with neither naming the other'))
def _nothing_declared(scenario: dict, gone: str, appeared: str) -> None:
    table = "people"
    _a_change(
        scenario,
        [_about_a_column(GONE, table, gone), _about_a_column(APPEARED, table, appeared)],
        _declaring([_a_model(table, [_a_column(appeared, None)], [])]),
        gone,
        appeared,
        [],
    )


@given("a new column and no column gone")
def _an_unrelated_add(scenario: dict) -> None:
    table = "people"
    appeared = "nickname"
    _a_change(
        scenario,
        [_about_a_column(APPEARED, table, appeared)],
        _declaring([_a_model(table, [_a_column(appeared, None)], [])]),
        "",
        appeared,
        # The same add with a column gone beside it, which is the pair this
        # function exists to stop. Without it, a function that never refuses
        # would pass this scenario.
        [
            (
                [_about_a_column(GONE, table, "email"), _about_a_column(APPEARED, table, appeared)],
                _declaring([_a_model(table, [_a_column(appeared, None)], [])]),
            )
        ],
    )


@given(parsers.parse('"{gone}" named in the model\'s dropping and "{appeared}" appeared'))
def _a_drop_the_model_asked_for(scenario: dict, gone: str, appeared: str) -> None:
    table = "people"
    columns = [_a_column(appeared, None)]
    _a_change(
        scenario,
        [_about_a_column(GONE, table, gone), _about_a_column(APPEARED, table, appeared)],
        _declaring([_a_model(table, columns, [gone])]),
        gone,
        appeared,
        # The same change with nothing named in `dropping`. It has to be
        # refused, or naming the column there settled nothing.
        [
            (
                [_about_a_column(GONE, table, gone), _about_a_column(APPEARED, table, appeared)],
                _declaring([_a_model(table, columns, [])]),
            )
        ],
    )


@given(parsers.parse('"{gone}" gone from "{near}" and "{appeared}" appeared on "{far}"'))
def _a_change_across_two_tables(
    scenario: dict, gone: str, near: str, appeared: str, far: str
) -> None:
    _a_change(
        scenario,
        [_about_a_column(GONE, near, gone), _about_a_column(APPEARED, far, appeared)],
        _declaring(
            [
                _a_model(near, [], []),
                _a_model(far, [_a_column(appeared, None)], []),
            ]
        ),
        gone,
        appeared,
        # The same two columns on one table. A rename moves a column inside a
        # table, so the pair is ambiguous there and only there.
        [
            (
                [_about_a_column(GONE, near, gone), _about_a_column(APPEARED, near, appeared)],
                _declaring([_a_model(near, [_a_column(appeared, None)], [])]),
            )
        ],
    )


# =============================================================================
# What comes back
# =============================================================================


@then("one rename comes back, not a drop and an add")
def _one_rename_comes_back(scenario: dict) -> None:
    operations = scenario["operations"]
    gone = scenario["gone"]
    appeared = scenario["appeared"]
    answer = _the_answer_for(operations, scenario["declared"])

    assert not _is_a_fault(answer), f"a declared rename was refused: {answer}"
    assert isinstance(answer, list), answer

    kinds = [operation["kind"] for operation in answer]
    assert kinds == ["rename_column"], (
        f"the drop and the add should have become one rename, and {kinds} came back"
    )

    rename = answer[0]
    assert rename["table"] == operations[0]["table"], rename
    assert rename["detail"] == {COLUMN: gone, TO: appeared}, rename

    # WHICH WAY ROUND IT IS WRITTEN, read off the model rather than off a
    # literal. `renamed_from` sits on the new column and carries the old name,
    # and the operation reads the same way round. Two readers taking this pair
    # in opposite directions would rename a column to itself and lose the
    # other, and nothing would say so.
    declaring = scenario["declared"]["models"][0]["columns"][0]
    assert rename["detail"][COLUMN] == declaring["renamed_from"], (
        f"the old name belongs under {COLUMN}: {rename} against {declaring}"
    )
    assert rename["detail"][TO] == declaring["name"], (
        f"the new name belongs under {TO}: {rename} against {declaring}"
    )
    assert rename["destroys"] is False, f"a rename keeps the rows, so it destroys nothing: {rename}"

    # The drop is gone from the answer, not merely joined by a rename. Leaving
    # it in would rename the column and then drop it.
    assert "drop_column" not in kinds, f"the drop survived beside the rename: {answer}"
    assert "add_column" not in kinds, f"the add survived beside the rename: {answer}"

    # Asked twice, answered the same, and what went in is untouched. Two
    # arguments, and the declaration names both.
    assert _the_answer_for(operations, scenario["declared"]) == answer
    assert operations == [
        _about_a_column(GONE, "people", gone),
        _about_a_column(APPEARED, "people", appeared),
    ], f"the operations handed in were mutated: {operations}"
    from declaro_persistum.name_what_is_ambiguous import name_what_is_ambiguous

    taken = list(inspect.signature(name_what_is_ambiguous).parameters)
    assert taken == ["operations", "declared"], taken
    assert not [
        name
        for name, taken_as in inspect.signature(name_what_is_ambiguous).parameters.items()
        if taken_as.default is not inspect.Parameter.empty
    ], "an argument carries a default, which answers for a caller who said nothing"


@then("it is refused")
def _it_is_refused(scenario: dict) -> None:
    answer = _the_answer_for(scenario["operations"], scenario["declared"])

    assert _is_a_fault(answer), (
        f"a column gone beside a column appeared, with nothing declared, was not refused: {answer}"
    )
    assert answer["kind"] == "refused", answer

    scenario["fault"] = answer


@then("the fault names both columns and the declaration to write")
def _the_fault_names_both_columns(scenario: dict) -> None:
    fault = scenario["fault"]
    gone = scenario["gone"]
    appeared = scenario["appeared"]

    said = fault["what_went_wrong"]
    assert gone in said, f"the refusal does not say which column is gone: {said}"
    assert appeared in said, f"the refusal does not say which column appeared: {said}"

    # THE DECLARATION TO WRITE, not an instruction to go and think about it.
    # A reader who cannot copy a line out of this sentence has to guess, which
    # is what the refusal exists to stop them doing.
    instead = fault["what_to_do_instead"]
    assert "renamed_from" in instead, (
        f"the refusal does not say which declaration settles it: {instead}"
    )
    assert gone in instead and appeared in instead, (
        f"the refusal does not name the two columns in the line to write: {instead}"
    )

    # Nothing here is a statement, and nothing here is an engine's word. This
    # function is handed operations and models, and neither carries either.
    everything = " ".join(str(value) for value in fault.values())
    assert "SELECT" not in everything.upper(), f"the refusal carries SQL: {fault}"

    # Asked twice, answered the same.
    assert _the_answer_for(scenario["operations"], scenario["declared"]) == fault


@then("nothing is refused")
def _nothing_is_refused(scenario: dict) -> None:
    operations = scenario["operations"]
    answer = _the_answer_for(operations, scenario["declared"])

    assert not _is_a_fault(answer), f"this change was refused and should not have been: {answer}"
    assert answer == operations, (
        f"nothing here is a rename and nothing here is ambiguous, so the "
        f"operations come back as they went in: {answer}"
    )

    # The nearest change that MUST be refused, so that a function refusing
    # nothing cannot pass this scenario.
    for refusable, declared in scenario["but_refused"]:
        assert _is_a_fault(_the_answer_for(refusable, declared)), (
            f"{refusable} was not refused, so this scenario passes on a "
            f"function that refuses nothing"
        )

    # Asked twice, answered the same.
    assert _the_answer_for(operations, scenario["declared"]) == answer
