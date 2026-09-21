"""Bind `tests/bdd/features/schema/schema_from_declared.feature`.

WHAT THE FEATURE ASKS FOR. Nine scenarios, and between them one decision: the
models are the desired state, so the target schema is a model turned into a
table, a declared view carried through, and nothing the database says. The
architecture document puts it under "The one path" -- introspection tells you
what is, and it cannot tell you what should be, so a column somebody
added by hand in production must not become part of the specification.

WHERE THE FEATURE AND THE DECLARATION DISAGREE, and how each Then answers it.
Six of these scenarios were folded in on 2026-09-21 from an end-to-end index
feature, and they still speak of a live database: "the database holds that
index", "the engine refuses a second row with the same pair", "bringing the
schema up to date drops it". `fn schema_from_declared` is handed a `Declared`
and nothing else, and `docs/design/persistum.hd` gives it no engine, no
connection and no live schema, so it cannot reach a database to hold anything.

What it owns is the promise those sentences depend on. An index reaches the
engine because it reached the target schema first: `fn difference` compares the
target against the live schema, `fn spell_operation` writes the statement, and
neither can create an index the target does not carry. So each of those Thens
asserts the promise in the target, and says which function does the rest. An
index missing from the target is an index no engine will ever be asked for, so
the assertion still fails on the failure the scenario is about.

THE ORDER OF AN INDEX IS TESTED BY COMPARING TWO TARGETS, because "not the same
as" is a statement about two schemas rather than about one. That scenario asks
for no When, so its Then builds both.
"""

from __future__ import annotations

import copy
import inspect
import re

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.types import Column, Condition, Declaration, Declared, Index, Model, View

scenarios("../features/schema/schema_from_declared.feature")


NOTHING_ASKED: Condition = {"each": {}, "any_of": [], "none_of": []}


def _a_column(name: str) -> Column:
    """One column, with every field a `Column` names present."""
    return {
        "name": name,
        "declared_type": "text",
        "renamed_from": None,
        "references": None,
        "facets": {},
    }


def _an_index(name: str, columns: list[str], unique: bool) -> Index:
    """One index, with every field an `Index` names present."""
    return {"name": name, "columns": columns, "unique": unique}


def _a_model(table: str, columns: list[str], indexes: list[Index]) -> Model:
    """One model, with every field a `Model` names present."""
    return {
        "table": table,
        "columns": [_a_column(name) for name in columns],
        "dropping": [],
        "indexes": indexes,
        "constraints": [],
    }


def _a_declaration(model: str) -> Declaration:
    """A declaration asking for every row of one model."""
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


def _a_view(name: str, model: str) -> View:
    """One view, with every field a `View` names present."""
    return {"name": name, "declaration": _a_declaration(model), "freshness": None}


def _declaring(models: list[Model], views: list[View]) -> Declared:
    """What the code declares, with every field a `Declared` names present."""
    return {
        "models": models,
        "views": views,
        "dropping_tables": [],
        "dropping_views": [],
    }


NOTHING_LIVE = {"tables": [], "views": []}

# WHAT EACH SENTENCE ESTABLISHES: what the code declares, and what the database
# already holds. Nine step functions differing only in the words they pasted in
# would be nine places for one setup to drift, so the words are the keys and the
# setup is the row beside each one.
#
# THE LIVE COLUMN IS EMPTY IN EIGHT ROWS AND WRITTEN DOWN ANYWAY, because "the
# database holds nothing relevant" and "nobody thought about the database" are
# different statements and a reader has to be able to tell them apart.
A_SITUATION: dict[str, dict] = {
    'a model declaring "people" with id and name': {
        "declared": _declaring([_a_model("people", ["id", "name"], [])], []),
        "live": NOTHING_LIVE,
    },
    "a view declared against that model": {
        "declared": _declaring(
            [_a_model("people", ["id", "name"], [])],
            [_a_view("active_people", "people")],
        ),
        "live": NOTHING_LIVE,
    },
    # The table in the live database is one nothing declares, which is the
    # column somebody added by hand that must not become part of the target.
    "a database holding a table no model declares": {
        "declared": _declaring([_a_model("people", ["id", "name"], [])], []),
        "live": {
            "tables": [
                {
                    "name": "audit_log",
                    "columns": [_a_column("id"), _a_column("happened_at")],
                    "indexes": [],
                    "constraints": [],
                }
            ],
            "views": [],
        },
    },
    "a model declaring a column with an index": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "email"],
                    [_an_index("people_email", ["email"], False)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
    "a model declaring an index over status and created_at": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "status", "created_at"],
                    [_an_index("people_status_created_at", ["status", "created_at"], False)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
    "an index declared over status then created_at": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "status", "created_at"],
                    [_an_index("people_status_created_at", ["status", "created_at"], False)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
    # `nmae` is the misspelling of `name`, which is the way a column an index
    # names but a model does not declare actually arrives.
    "an index declared over a column that is not there": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "name"],
                    [_an_index("people_nmae", ["nmae"], False)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
    "an index declared unique over two columns": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "status", "created_at"],
                    [_an_index("people_status_created_at", ["status", "created_at"], True)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
    "a declared index already in the database": {
        "declared": _declaring(
            [
                _a_model(
                    "people",
                    ["id", "status", "created_at"],
                    [_an_index("people_status_created_at", ["status", "created_at"], False)],
                )
            ],
            [],
        ),
        "live": NOTHING_LIVE,
    },
}


def _unchanged(declared: Declared) -> Declared:
    """The models say what they said; building the target is all that happens."""
    return declared


def _with_no_indexes(declared: Declared) -> Declared:
    """The models again, with every index taken out of them."""
    return declared | {"models": [model | {"indexes": []} for model in declared["models"]]}


# WHAT EACH `When` DOES TO THE MODELS BEFORE THE TARGET IS BUILT. Three of the
# four sentences change nothing and say so here, rather than in three step
# functions that would differ only in the sentence above them.
WHAT_HAPPENS: dict[str, object] = {
    "the target schema is built": _unchanged,
    "the schema is brought to the declared state": _unchanged,
    "it is declared": _unchanged,
    "the model no longer declares it": _with_no_indexes,
}


_ANY_SITUATION = "|".join(re.escape(sentence) for sentence in A_SITUATION)
_ANY_WHEN = "|".join(re.escape(sentence) for sentence in WHAT_HAPPENS)


@pytest.fixture
def established() -> dict[str, dict]:
    """A fresh setup per sentence, so no scenario can change another's."""
    return copy.deepcopy(A_SITUATION)


def _the_target_for(declared: Declared) -> object:
    """Ask the function for the target schema these declarations describe."""
    from declaro_persistum.schema_from_declared import schema_from_declared

    return schema_from_declared(declared)


def _the_table_in(target: object) -> dict:
    """The one table the target holds, with the shape a `Table` names checked."""
    assert isinstance(target, dict), f"the target is a {type(target).__name__}: {target!r}"
    assert sorted(target) == ["tables", "views"], (
        f"a Schema holds tables and views; this one holds {sorted(target)}"
    )
    assert len(target["tables"]) == 1, (
        f"one model was declared and {len(target['tables'])} tables came back"
    )
    table = target["tables"][0]
    assert sorted(table) == ["columns", "constraints", "indexes", "name"], (
        f"a Table names four fields; this one names {sorted(table)}"
    )
    return table


# --- what each scenario sets up ---------------------------------------------


@given(parsers.re(rf"(?P<situation>{_ANY_SITUATION})"))
def _the_situation(scenario: dict, established: dict, situation: str) -> None:
    scenario["declared"] = established[situation]["declared"]
    scenario["live"] = established[situation]["live"]


@when(parsers.re(rf"(?P<happens>{_ANY_WHEN})"))
def _the_target_is_built(scenario: dict, happens: str) -> None:
    """Build the target from the models, and from the models after the When."""
    declared = scenario["declared"]
    scenario["before"] = _the_target_for(declared)
    scenario["target"] = _the_target_for(WHAT_HAPPENS[happens](declared))


# --- what each scenario says has to be true ---------------------------------


@then('it holds a table "people" with those two columns')
def _a_model_became_a_table(scenario: dict) -> None:
    model = scenario["declared"]["models"][0]
    table = _the_table_in(scenario["target"])

    assert table["name"] == model["table"], (
        f"the model declares the table {model['table']} and the target calls it {table['name']}"
    )
    assert [column["name"] for column in table["columns"]] == ["id", "name"], (
        f"the model declares id and name; the target holds "
        f"{[column['name'] for column in table['columns']]}"
    )

    # THE COLUMNS ARRIVE WHOLE, not as names. `declared_type`, `renamed_from`,
    # `references` and `facets` are what `fn difference` reads to tell an added
    # column from a renamed one, and a target carrying names alone would make
    # every rename look like a drop and an add.
    assert table["columns"] == model["columns"], (
        f"the columns were rebuilt rather than carried: {table['columns']}"
    )

    assert scenario["target"]["views"] == [], (
        f"no view was declared and the target holds {scenario['target']['views']}"
    )


@then("the view is in it")
def _a_declared_view_is_in_the_target(scenario: dict) -> None:
    declared = scenario["declared"]
    target = scenario["target"]

    assert target["views"] == declared["views"], (
        f"one view was declared and the target holds {target['views']}"
    )
    assert len(target["views"]) == 1, "the scenario declares exactly one view"

    # A VIEW IS NOT A TABLE. `fn difference` creates a table with CREATE TABLE
    # and a view with CREATE VIEW, so a view that arrived among the tables would
    # be built as a table and hold no rows.
    assert [table["name"] for table in target["tables"]] == ["people"], (
        f"the view landed among the tables: {[t['name'] for t in target['tables']]}"
    )


@then("the target does not mention it")
def _nothing_live_reaches_the_target(scenario: dict) -> None:
    from declaro_persistum.schema_from_declared import schema_from_declared

    live = scenario["live"]["tables"][0]["name"]
    target = scenario["target"]

    assert live not in [table["name"] for table in target["tables"]], (
        f"{live} is in the database and no model declares it, and it is in the target"
    )
    assert live not in repr(target), f"{live} reached the target somewhere: {target!r}"

    # NOTHING COULD HAVE TOLD IT. One argument, and the declaration names it
    # `declared`. A second argument, or a default on this one, is a way for the
    # live schema to arrive that no scenario exercises.
    parameters = inspect.signature(schema_from_declared).parameters
    assert list(parameters) == ["declared"], (
        f"schema_from_declared takes {list(parameters)}; `fn schema_from_declared` "
        "says (declared: Declared)"
    )
    assert all(parameter.default is inspect.Parameter.empty for parameter in parameters.values()), (
        "a default argument cannot tell a caller who chose from a caller who forgot"
    )

    # Same declarations, same target, twice. Anything read from outside the one
    # argument would show up here.
    assert _the_target_for(scenario["declared"]) == target, (
        "two calls on the same declarations disagreed, so something outside them is read"
    )


@then("the database holds that index")
def _an_index_on_one_column_reaches_the_target(scenario: dict) -> None:
    # The engine holds the index because `fn difference` asked for it, and it
    # can only ask for an index the target carries. That is the half this
    # function owns, and it is the half the scenario fails on.
    declared = scenario["declared"]
    table = _the_table_in(scenario["target"])

    assert table["indexes"] == declared["models"][0]["indexes"], (
        f"the model declares {declared['models'][0]['indexes']} and the target "
        f"holds {table['indexes']}"
    )
    assert len(table["indexes"]) == 1, "the scenario declares exactly one index"

    # A model declaring no index gets none. Otherwise a target that invented an
    # index for every column would satisfy every line above.
    bare = _the_target_for(_with_no_indexes(declared))
    assert bare["tables"][0]["indexes"] == [], (
        f"a model declaring no index got {bare['tables'][0]['indexes']}"
    )


@then("the database holds one index over both, in that order")
def _an_index_over_two_columns_is_one_index(scenario: dict) -> None:
    table = _the_table_in(scenario["target"])

    assert len(table["indexes"]) == 1, (
        f"one index over two columns was declared and the target holds "
        f"{len(table['indexes'])}: {table['indexes']}"
    )
    assert table["indexes"][0]["columns"] == ["status", "created_at"], (
        f"the index promises status and created_at together and the target says "
        f"{table['indexes'][0]['columns']}"
    )


@then("the index is not the same as one over created_at then status")
def _the_order_of_the_columns_is_kept(scenario: dict) -> None:
    # This scenario asks for no When, because "not the same as" compares two
    # targets rather than reading one.
    declared = scenario["declared"]
    reversed_columns = copy.deepcopy(declared)
    reversed_columns["models"][0]["indexes"][0]["columns"] = ["created_at", "status"]

    forwards = _the_target_for(declared)
    backwards = _the_target_for(reversed_columns)

    assert forwards != backwards, (
        "an index over status then created_at and one over created_at then "
        "status came out as the same target, so the order was thrown away"
    )
    assert forwards["tables"][0]["indexes"][0]["columns"] == ["status", "created_at"]
    assert backwards["tables"][0]["indexes"][0]["columns"] == ["created_at", "status"]


@then("it is refused, and the refusal names the model")
def _an_index_over_a_column_that_is_not_there_is_refused(scenario: dict) -> None:
    model = scenario["declared"]["models"][0]
    fault = scenario["target"]

    assert sorted(fault) == ["kind", "what_to_do_instead", "what_went_wrong"], (
        f"the answer is not a Fault: {fault!r}"
    )
    assert fault["kind"] == "refused", fault["kind"]
    assert "nmae" in fault["what_went_wrong"], (
        f"the refusal does not say which column the index names: {fault}"
    )

    instead = fault["what_to_do_instead"]
    assert model["table"] in instead, f"the refusal does not name the model: {instead}"
    assert all(column["name"] in instead for column in model["columns"]), (
        f"the refusal does not list the columns {model['table']} declares: {instead}"
    )

    # NO HALF-BUILT SCHEMA CAME BACK. A refusal carrying tables would let a
    # caller that forgot to look at it migrate against a target that was refused.
    assert "tables" not in fault, f"a schema rode along with the refusal: {fault}"

    # An index naming only columns the model declares is not refused. Otherwise
    # "refuses everything" would pass every line above.
    allowed = copy.deepcopy(scenario["declared"])
    allowed["models"][0]["indexes"][0]["columns"] = ["name"]
    assert "tables" in _the_target_for(allowed), (
        "an index over a column the model does declare was refused"
    )


@then("the engine refuses a second row with the same pair")
def _a_unique_index_keeps_its_promise(scenario: dict) -> None:
    # The engine refuses the second row because the index it was given says
    # unique, and it was given that index because the target carried it.
    # `fn spell_operation` writes the word UNIQUE; what this function owns is
    # that there is a promise for it to write.
    declared = scenario["declared"]
    table = _the_table_in(scenario["target"])

    assert len(table["indexes"]) == 1, table["indexes"]
    index = table["indexes"][0]
    assert index["unique"] is True, (
        f"the model declares the index unique and the target says {index['unique']!r}, "
        "so nothing would stop the second row"
    )
    assert index["columns"] == ["status", "created_at"], (
        f"the pair is status and created_at and the promise covers {index['columns']}"
    )

    # A model that asked for no such promise does not get one. A target that
    # marked every index unique would refuse rows nobody declared unique.
    ordinary = copy.deepcopy(declared)
    ordinary["models"][0]["indexes"][0]["unique"] = False
    assert _the_target_for(ordinary)["tables"][0]["indexes"][0]["unique"] is False, (
        "an index nobody declared unique came back unique"
    )


@then("bringing the schema up to date drops it")
def _an_index_the_model_stopped_declaring_leaves_the_target(scenario: dict) -> None:
    # `fn difference` drops it because it is in the live schema and not in the
    # target. The half that decides the drop happens is this one: an index still
    # in the target is an index nothing will ever drop.
    before = scenario["before"]
    target = scenario["target"]

    assert before["tables"][0]["indexes"] != [], (
        "the Given says the index was already declared, and the target built "
        "from those models does not hold it, so the scenario proves nothing"
    )
    assert target["tables"][0]["indexes"] == [], (
        f"the model no longer declares the index and the target still holds "
        f"{target['tables'][0]['indexes']}"
    )
    assert [table["name"] for table in target["tables"]] == [
        table["name"] for table in before["tables"]
    ], "dropping an index took the table with it"
