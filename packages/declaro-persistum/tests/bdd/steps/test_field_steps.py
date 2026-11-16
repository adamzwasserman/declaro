"""Bind `tests/bdd/features/schema/field.feature`.

WHAT THE FUNCTION ANSWERS. `fn field` is handed what a host says about one
column that its annotation cannot carry: the name that column had before, and
where it points. It answers with a `Field` holding both, and a fact the host did
not give comes back as the named absence rather than as a guess.

THE ANSWER IS ASKED OF A REAL PYDANTIC MODEL, NOT OF THE RETURN VALUE ALONE. A
host writes this as the column's assignment, so whatever comes back has to
survive being one. Reading the two facts straight off the return would pass
while Pydantic quietly read that same value as the column's default, which turns
a required column optional and declares a schema nobody wrote. So every scenario
here builds a model, and the facts are read back the way `schema_read_declared`
reads them, off `model_fields`.

WHY TWO SCENARIOS ASK ABOUT VALIDATION AT ALL. This function's whole job is to
add a fact the database cares about to a class whose job is validating data. If
adding the fact changes what that class accepts, the host has paid for a
migration feature with a validation bug, and that trade is never worth making.
So one scenario asks that the column stays required and one asks that a wrong
type is still refused.

NO STAND-INS. Pydantic is the library the host uses and the library this reads,
so the model is a real one and `model_validate` is the real call.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError
from pytest_bdd import given, parsers, scenarios, then, when

from tests.conftest import a_reference

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/field.feature")


THE_TWO_FACTS = ("renamed_from", "references")

# What a host writes in the one scenario that declares both.
A_TABLE_POINTED_AT = "teams"
A_COLUMN_POINTED_AT = "id"
A_DELETION_RULE = "set_null"
A_NAME_IT_HAD_BEFORE = "email"
THE_OTHER_NAME_IT_HAD_BEFORE = "team"


@pytest.fixture
def declare():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.field import field

    return field


def _a_model_whose_column_declares(said: Any) -> type[BaseModel]:
    """One Pydantic model whose column carries what the host said about it.

    THE COLUMN IS REQUIRED AND HAS NO DEFAULT, which is what makes the required
    scenario able to fail. A column given a default would stay optional whatever
    this function returned, so the test could not tell the two apart.
    """

    class Person(BaseModel):
        __tablename__ = "people"
        email_address: str = said
        name: str

    return Person


def _read_back(model: type[BaseModel], column: str) -> dict[str, Any]:
    """The two facts, read the way `schema_read_declared` reads them.

    It walks `model_fields` and takes what the host attached to the field, so
    this asks the same question of the same place rather than of the value the
    function happened to return.
    """
    extra = model.model_fields[column].json_schema_extra
    said = extra if isinstance(extra, dict) else {}
    return {fact: said.get(fact) for fact in THE_TWO_FACTS}


# =============================================================================
# What a host says about one column
# =============================================================================


POINTS_AT = a_reference(
    table=A_TABLE_POINTED_AT, column=A_COLUMN_POINTED_AT, on_delete=A_DELETION_RULE
)

# WHAT THE HOST SAID, ONE ROW PER SENTENCE. Every Given in this feature builds
# the same model and differs only in what the host declared about its column, so
# it is one step over this table rather than six functions that erase to one.
WHAT_THE_HOST_SAID: dict[str, dict[str, Any]] = {
    'a host declaring a column renamed from "email"': {
        "renamed_from": A_NAME_IT_HAD_BEFORE,
        "references": None,
    },
    'a host declaring a column referencing "id" on "teams", emptying the column on delete': {
        "renamed_from": None,
        "references": POINTS_AT,
    },
    'a host declaring a column renamed from "team" and referencing "id" on "teams", '
    "emptying the column on delete": {
        "renamed_from": THE_OTHER_NAME_IT_HAD_BEFORE,
        "references": POINTS_AT,
    },
    "a host declaring a column with neither fact": {"renamed_from": None, "references": None},
    "a model whose column declares a field and whose column has no default": {
        "renamed_from": A_NAME_IT_HAD_BEFORE,
        "references": None,
    },
    "a model whose column declares a field": {
        "renamed_from": A_NAME_IT_HAD_BEFORE,
        "references": None,
    },
}


def _one_of(sentences: tuple[str, ...]) -> str:
    """One pattern matching any of them, longest first so a prefix cannot win."""
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(WHAT_THE_HOST_SAID, key=len, reverse=True)))))
def _a_host_declaring(scenario: dict, declare, sentence: str) -> None:
    scenario["model"] = _a_model_whose_column_declares(declare(**WHAT_THE_HOST_SAID[sentence]))


@when("the field is read")
def _the_field_is_read(scenario: dict) -> None:
    scenario["read"] = _read_back(scenario["model"], "email_address")


@then(parsers.re(r'^it carries "(?P<before>\w+)" as the name that column had before$'))
def _it_carries_the_name_before(scenario: dict, before: str) -> None:
    read = scenario["read"]

    assert read["renamed_from"] == before, (
        f"the host said the column was called {before!r} and the field says "
        f"{read['renamed_from']!r}, so a rename reads as a drop and an add"
    )


@then("it says the column points nowhere")
def _it_points_nowhere(scenario: dict) -> None:
    read = scenario["read"]

    assert read["references"] is None, (
        f"the host said nothing about where this column points and the field "
        f"invented {read['references']!r}"
    )


@then("it carries that table, that column and that deletion rule")
def _it_carries_the_reference(scenario: dict) -> None:
    read = scenario["read"]
    points_at = read["references"]

    assert points_at is not None, "the host said where the column points and the field forgot"
    assert points_at == a_reference(
        table=A_TABLE_POINTED_AT, column=A_COLUMN_POINTED_AT, on_delete=A_DELETION_RULE
    ), (
        "a reference that loses any of its three parts points somewhere else or "
        f"does something else on a delete: {points_at}"
    )


@then("it says the column was never renamed")
def _it_was_never_renamed(scenario: dict) -> None:
    read = scenario["read"]

    assert read["renamed_from"] is None, (
        f"the host said nothing about a previous name and the field invented "
        f"{read['renamed_from']!r}, which would rename a column nobody renamed"
    )


@then("it carries both facts")
def _it_carries_both(scenario: dict) -> None:
    read = scenario["read"]

    assert read["renamed_from"] == THE_OTHER_NAME_IT_HAD_BEFORE, read
    assert read["references"] == a_reference(
        table=A_TABLE_POINTED_AT, column=A_COLUMN_POINTED_AT, on_delete=A_DELETION_RULE
    ), read


@then("both facts come back as the named absence rather than as a guess")
def _both_are_the_named_absence(scenario: dict) -> None:
    read = scenario["read"]

    invented = {fact: said for fact, said in read.items() if said is not None}
    assert invented == {}, (
        f"the host said neither fact and the field answered with {invented}, "
        f"which is a column declaration nobody wrote"
    )


# =============================================================================
# Declaring a fact about the database must not change what the class validates
# =============================================================================


@when("the model is asked what it requires")
def _asked_what_it_requires(scenario: dict) -> None:
    scenario["required"] = scenario["model"].model_fields["email_address"].is_required()


@then("that column is still required")
def _still_required(scenario: dict) -> None:
    assert scenario["required"] is True, (
        "declaring where a column came from made it optional, so Pydantic read "
        "the answer as the column's default and the class now accepts a row it "
        "refused before"
    )


@when("a row with the wrong type is validated against it")
def _a_row_with_the_wrong_type(scenario: dict) -> None:
    try:
        scenario["accepted"] = scenario["model"].model_validate(
            {"email_address": [], "name": "alice"}
        )
    except ValidationError as refused:
        scenario["refused"] = refused


@then("it is refused exactly as it would be without the field")
def _refused_the_same_way(scenario: dict) -> None:
    refused = scenario.get("refused")

    assert refused is not None, (
        f"a list was given where the column holds text and the model took it: "
        f"{scenario.get('accepted')}"
    )
    named = [error["loc"] for error in refused.errors()]
    assert ("email_address",) in named, (
        f"the row was refused and the complaint does not name the column that caused it: {named}"
    )
