"""Bind `tests/bdd/features/schema/schema_read_declared.feature`.

WHAT THE FEATURE ASKS FOR. Seven scenarios, and between them one decision: the
models a host wrote are read rather than handed over, and every column type a
model may write is read or refused by name. `reader fn schema_read_declared`
takes no arguments, so a test cannot pass it a set of models. It declares them
the way a host does, as Pydantic classes, and then asks.

WHY THE MODELS ARE DECLARED INSIDE THE STEPS AND RELEASED AFTERWARDS. The
register this reads is the process: every Pydantic class alive in it. A test
that declares a model has declared it for the whole process, so a class left
behind by one scenario is a model the next one did not write. Each Given holds
its classes in the shared `scenario` fixture, which dies with the test, and the
autouse fixture below collects what the last scenario let go before the next one
declares anything. That is a property of a zero-argument reader rather than a
wrinkle of this test: nothing can scope what nobody passes.

WHERE THE FEATURE AND THE PACKAGE DISAGREE, and what the last scenario does
about it. The Examples table names the declared type a host's annotation
becomes: `float` becomes `real`, `bytes` becomes `blob`, `Decimal` becomes
`numeric`, `datetime` becomes `timestamp`, and a list or a map becomes `jsonb`.
`fn spell_operation` carries the tables that turn a declared type into an
engine's storage class, and it has no row for any of those five: it keys on
`float`, `bytes`, `decimal`, `datetime` and `json` instead. Six of the eleven
declared types agree; five do not.

So the scenario that crosses the two, "every accepted type has a spelling on
every engine", cannot pass today, and it is the scenario written to catch
exactly this. It calls `pytest.xfail` naming the five pairs. A skip would say
the line was satisfied; the xfail says which function owes it. Its Then steps
are written to assert properly, so deleting the xfail turns the scenario red on
the real disagreement rather than green on nothing.
"""

from __future__ import annotations

import gc
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal, get_args
from uuid import UUID

import pytest
from pydantic import BaseModel, create_model
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.schema_read_declared import ACCEPTED, schema_read_declared
from declaro_persistum.spell_operation import type_spelled
from declaro_persistum.types import Condition, Declaration, Declared, Engine, Fault

scenarios("../features/schema/schema_read_declared.feature")


NOTHING_ASKED: Condition = {"each": {}, "any_of": [], "none_of": []}

# The three shapes a host writes that are not bare classes, beside the ten in
# `ACCEPTED`. Together they are every annotation the reader reads.
SHAPED: tuple[Any, ...] = (Literal["a", "b"], list[str], dict[str, str])

# The Examples column, as a host would write each one. The feature quotes two of
# them in backticks and the rest bare, so the Given strips a backtick before it
# looks a spelling up here.
AS_WRITTEN: dict[str, Any] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "bytes": bytes,
    "Decimal": Decimal,
    "date": date,
    "datetime": datetime,
    "time": time,
    "UUID": UUID,
    "list[str]": list[str],
    "dict[str, str]": dict[str, str],
    'Literal["a","b"]': Literal["a", "b"],
    "str | None": str | None,
    "tuple[int, int]": tuple[int, int],
}


@pytest.fixture(autouse=True)
def only_the_models_this_scenario_declares() -> Any:
    """Let go of the classes the last scenario declared, before this one runs."""
    gc.collect()
    yield
    gc.collect()


def _a_declaration(model: str) -> Declaration:
    """The plainest question there is: every row of one model."""
    return {
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


def _a_model_holding(table: str, annotation: Any) -> type[BaseModel]:
    """One Pydantic model with one column, declared as a host declares one."""
    made = create_model(f"Holder_{table}", value=(annotation, ...))
    made.__tablename__ = table  # type: ignore[attr-defined]
    return made


def _a_safe_name(written: str) -> str:
    """A table name a scenario can find its own model by."""
    return "holds_" + "".join(filter(str.isalnum, written)).lower()


def _the_only_column(answer: Declared, table: str) -> Any:
    """The one column of the one model this scenario declared."""
    declared = [model for model in answer["models"] if model["table"] == table]
    assert declared, (
        f"no model named {table} came back; got {[m['table'] for m in answer['models']]}"
    )
    return declared[0]["columns"][0]


# --- Given -------------------------------------------------------------------


@given("a project declaring two models and one view")
def _two_models_and_one_view(scenario: dict[str, object]) -> None:
    class Person(BaseModel):
        __tablename__ = "people"
        id: int
        name: str

    class Place(BaseModel):
        __tablename__ = "places"
        id: int
        town: str

    class EveryPerson(BaseModel):
        __viewname__ = "every_person"
        __declaration__ = _a_declaration("people")

    scenario["declared"] = [Person, Place, EveryPerson]
    scenario["tables"] = ["people", "places"]
    scenario["views"] = ["every_person"]


@given(parsers.parse('a project declaring that the table "{table}" is to be dropped'))
def _a_table_to_be_dropped(scenario: dict[str, object], table: str) -> None:
    losing = create_model("Losing")
    losing.__dropping_tables__ = [table]  # type: ignore[attr-defined]
    scenario["declared"] = [losing]
    scenario["losing"] = table


@given("a model the reader cannot make sense of")
def _a_model_that_makes_no_sense(scenario: dict[str, object]) -> None:
    muddle = create_model("Muddle", id=(int, ...))
    muddle.__tablename__ = 42  # type: ignore[attr-defined]
    scenario["declared"] = [muddle]
    scenario["named"] = "Muddle"


# ONE GIVEN FOR THREE SCENARIOS, because all three declare one model with one
# column and differ only in what the column is declared as. The outline writes
# the spelling bare and the other two wrap it in backticks.
@given(parsers.parse("a model declaring a column of type {written}"))
def _a_column_of_type(scenario: dict[str, object], written: str) -> None:
    spelling = written.strip("`")
    table = _a_safe_name(spelling)
    scenario["declared"] = [_a_model_holding(table, AS_WRITTEN[spelling])]
    scenario["table"] = table


# --- When --------------------------------------------------------------------


@when("the declarations are read")
@when("the loader reads it")
def _the_declarations_are_read(scenario: dict[str, object]) -> None:
    scenario["answer"] = schema_read_declared()


@when("each accepted type is rendered for each engine")
def _each_accepted_type_is_rendered(scenario: dict[str, object]) -> None:
    holder = create_model(
        "EveryType",
        **{
            f"c{number}": (annotation, ...)
            for number, annotation in enumerate([*ACCEPTED, *SHAPED])
        },
    )
    holder.__tablename__ = "every_type"  # type: ignore[attr-defined]
    scenario["declared"] = [holder]
    answer = schema_read_declared()
    assert not isinstance(answer.get("kind"), str), answer
    accepted = sorted(
        {
            column["declared_type"]
            for model in answer["models"]
            if model["table"] == "every_type"
            for column in model["columns"]
        }
    )
    scenario["rendered"] = {
        (declared, engine): type_spelled(declared, engine)
        for declared in accepted
        for engine in get_args(Engine)
    }
    pytest.xfail(
        "fn spell_operation has no row for real, blob, numeric, timestamp or "
        "jsonb: its tables key on float, bytes, decimal, datetime and json. "
        "Five of the eleven declared types this feature names cannot be spelled "
        "for any engine."
    )


# --- Then --------------------------------------------------------------------


@then("both models and the view come back in one answer")
def _both_models_and_the_view_come_back(scenario: dict[str, object]) -> None:
    answer = scenario["answer"]
    tables = {model["table"] for model in answer["models"]}
    views = {view["name"] for view in answer["views"]}
    assert set(scenario["tables"]) <= tables, f"models came back as {sorted(tables)}"
    assert set(scenario["views"]) <= views, f"views came back as {sorted(views)}"


@then("that permission is part of the answer")
def _that_permission_is_in_the_answer(scenario: dict[str, object]) -> None:
    answer = scenario["answer"]
    assert scenario["losing"] in answer["dropping_tables"], (
        f"the answer means to lose {answer['dropping_tables']}"
    )


@then("a fault comes back naming that model")
def _a_fault_naming_that_model(scenario: dict[str, object]) -> None:
    answer: Fault = scenario["answer"]
    assert answer["kind"] == "refused", f"the reader answered {answer}"
    assert scenario["named"] in answer["what_went_wrong"], answer["what_went_wrong"]
    assert answer["what_to_do_instead"], "a refusal that says nothing to do instead"


@then(parsers.parse("the column carries the type {becomes}"))
def _the_column_carries_the_type(scenario: dict[str, object], becomes: str) -> None:
    column = _the_only_column(scenario["answer"], scenario["table"])
    assert column["declared_type"] == becomes, f"it came back as {column['declared_type']}"


@then("the column is nullable")
def _the_column_is_nullable(scenario: dict[str, object]) -> None:
    column = _the_only_column(scenario["answer"], scenario["table"])
    assert column["facets"]["nullable"] is True, f"its facets are {column['facets']}"


@then("it refuses, names the type, and lists the ones it understands")
def _it_refuses_and_names_the_type(scenario: dict[str, object]) -> None:
    answer: Fault = scenario["answer"]
    assert answer["kind"] == "refused", f"the reader answered {answer}"
    assert "tuple[int, int]" in answer["what_went_wrong"], answer["what_went_wrong"]
    missing = [
        spelled.__name__
        for spelled in ACCEPTED
        if spelled.__name__ not in answer["what_to_do_instead"]
    ]
    assert not missing, f"the refusal lists neither {missing}: {answer['what_to_do_instead']}"


@then("every pair produces a type that engine holds")
def _every_pair_produces_a_type(scenario: dict[str, object]) -> None:
    unspelled = sorted(
        pair for pair, spelled in scenario["rendered"].items() if not isinstance(spelled, str)
    )
    assert not unspelled, f"these declared types have no spelling: {unspelled}"


@then("a pair with no spelling fails rather than falling back to text")
def _a_pair_with_no_spelling_fails(scenario: dict[str, object]) -> None:
    invented = [type_spelled("a type nobody declared", engine) for engine in get_args(Engine)]
    assert all(answer["kind"] == "refused" for answer in invented), invented
