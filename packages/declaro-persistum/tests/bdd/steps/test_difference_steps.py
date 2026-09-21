"""Every line of `core/difference.feature`, bound to a step.

WHAT THE FUNCTION ANSWERS. `fn difference` is handed the schema the database
holds and the schema the models want, and hands back the operations that turn
the first into the second. Set theory over two schemas: what is only in the
target is created, what is only in the live database is dropped, what differs
is altered. No engine appears anywhere in it, and nothing is ordered, refused
or executed here.

WHAT THIS FILE CAN PROVE, AND WHAT IT CANNOT. The feature had eighteen
scenarios when it was bound on 2026-09-21 and has eleven now. Seven asked about
ordering and ambiguity, and an eighth line asked for an ambiguity beside a
drop; all eight went to the card that owns them or were deleted as already
said there, and the feature records each one. Ten of the eleven left are
asserted below. The last turns on a table that was renamed, which nothing can
say.

WHY THE RENAMED TABLE CANNOT BE ASKED FOR. `Column` carries `renamed_from` and
`Table` does not, so a target `Schema` has nowhere to write that `users`
replaced `user`. `rename_table` is in the operation vocabulary and nothing
declared can produce one, so no other card owns that scenario either and there
is nowhere to move it to. That is the type being short, not this function
refusing, and widening `types.py` is not this card's to do.

WHY IT IS `xfail` AND NOT SKIPPED OR QUIETLY PASSED. A skip says the line was
not worth running. A green assertion rewritten until it passes says the line
was satisfied when it was not. `pytest.xfail` naming what is missing says the
line is in the file, nothing satisfies it yet, and what must. The same call is
what `test_budget_spent_steps.py` makes for the same reason.

ITS TWO THENS ARE DEFINED AND NEITHER RUNS. They raise rather than assert, so
deleting the `xfail` that guards them turns the scenario red and names the
sentence, instead of turning it green on a body nobody wrote.

ONE FUNCTION OVER A TABLE. Every Given here says the same thing in different
words -- here is a schema -- so there is one Given, keyed by the sentence. The
Thens that differ only in a word are one function over that word: `a` against
`no`, and `create_table` against `drop_table`.

THE KEY IS THE SENTENCE THE FEATURE WRITES, and the pattern each step matches
is built from those keys. A scenario reworded without a row here fails as a
step nobody defined, naming the sentence, rather than passing on a row some
other scenario left behind.

NOTHING HERE READS THE MODULE'S OWN TEXT. A claim about how the code is written
is a gate over `src/`; a scenario states what comes back.
"""

from __future__ import annotations

import re
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.difference import difference
from declaro_persistum.types import Column, Declaration, Schema, Table, View

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/difference.feature")


# =============================================================================
# The smallest schema that says what a sentence says
# =============================================================================


def _column(name: str, declared_type: str) -> Column:
    return Column(
        name=name,
        declared_type=declared_type,
        renamed_from=None,
        references=None,
        facets={},
    )


def _table(name: str, columns: list[tuple[str, str]]) -> Table:
    return Table(
        name=name,
        columns=[_column(column, declared_type) for column, declared_type in columns],
        indexes=[],
        constraints=[],
    )


def _declaration(model: str, columns: list[str]) -> Declaration:
    return Declaration(
        model=model,
        wanted="all",
        columns=columns,
        condition={"each": {}, "any_of": [], "none_of": []},
        order={},
        grouping=[],
        having={"each": {}, "any_of": [], "none_of": []},
        paging=None,
        rows=None,
        across=None,
    )


def _view(name: str, model: str, columns: list[str]) -> View:
    return View(name=name, declaration=_declaration(model, columns), freshness=None)


def _schema(tables: list[Table], views: list[View]) -> Schema:
    return Schema(tables=tables, views=views)


_NOTHING_AT_ALL = _schema([], [])
_USERS = _table("users", [("id", "integer"), ("name", "text")])
_LEGACY = _table("legacy", [("id", "integer")])
_RECENT = _view("recent", "users", ["id"])


# =============================================================================
# What a scenario established, one row per sentence the feature says
# =============================================================================
#
# A ROW SETS ONE SIDE OR BOTH. Most scenarios say the live schema in one
# sentence and the target in the next, so a row carries only the side its
# sentence names and the two merge. The rows that say both in one sentence
# carry both.
#
# `runs` IS HOW MANY TIMES THE FUNCTION IS CALLED. One, unless a scenario asks
# for two and compares them.

WHAT_IS_DECLARED: dict[str, dict[str, Any]] = {
    # --- a table on one side only ------------------------------------------
    "a current schema with no tables": {"live": _NOTHING_AT_ALL},
    'a target schema with a table "users"': {"target": _schema([_USERS], [])},
    'a live schema with "legacy" and a target without it': {
        "live": _schema([_LEGACY], []),
        "target": _NOTHING_AT_ALL,
    },
    'a current schema with a table "legacy"': {"live": _schema([_LEGACY], [])},
    "a target schema with no tables": {"target": _NOTHING_AT_ALL},
    # --- a column that differs ----------------------------------------------
    #
    # HELD AS AN INTEGER IS THE LIVE SIDE, DECLARED AS TEXT IS THE TARGET. The
    # database holds what it holds; the models are the desired state.
    "a column declared as text and held as an integer": {
        "live": _schema([_table("users", [("id", "integer"), ("name", "integer")])], []),
        "target": _schema([_table("users", [("id", "integer"), ("name", "text")])], []),
        "column": "name",
    },
    # --- nothing to do -------------------------------------------------------
    "two schemas that match": {
        "live": _schema([_USERS], [_RECENT]),
        "target": _schema([_USERS], [_RECENT]),
    },
    # --- a table on both sides ----------------------------------------------
    'a current schema with a table "users" having columns id, name': {
        "live": _schema([_USERS], []),
    },
    'a target schema with a table "users" having columns id, name, email': {
        "target": _schema(
            [_table("users", [("id", "integer"), ("name", "text"), ("email", "text")])], []
        ),
    },
    # A PAIR WORTH COMPARING TWICE. Two identical empty schemas would satisfy
    # the sentence and prove nothing, because a function returning a constant
    # would pass it. This pair creates, drops, adds and alters.
    "any current schema and any target schema": {
        "live": _schema([_USERS, _LEGACY], [_RECENT]),
        "target": _schema(
            [
                _table("users", [("id", "integer"), ("name", "integer"), ("email", "text")]),
                _table("orders", [("id", "integer")]),
            ],
            [_view("recent", "users", ["id", "name"])],
        ),
    },
    # --- a table renamed, or not --------------------------------------------
    'a current schema with a table "user"': {
        "live": _schema([_table("user", [("id", "integer")])], [])
    },
    'a target schema with a table "users" and no rename hint': {
        "target": _schema([_table("users", [("id", "integer")])], []),
    },
    # --- views ---------------------------------------------------------------
    'a view "recent" with one query': {"live": _schema([_USERS], [_RECENT])},
    'a target view "recent" with a different query': {
        "target": _schema([_USERS], [_view("recent", "users", ["id", "name"])]),
    },
    "a view that is identical in both schemas": {
        "live": _schema([_USERS], [_RECENT]),
        "target": _schema([_USERS], [_RECENT]),
    },
}


# The one sentence no argument of this function can carry, and why. It names no
# other card, because no other card owns it either: `rename_table` is in the
# operation vocabulary and nothing declared can produce one.
CANNOT_BE_SAID: dict[str, str] = {
    'a target schema with a table "users" declared as renamed_from "user"': (
        "no type carries a table's rename hint, so a target Schema cannot say "
        "that users replaced user"
    ),
}


# A sentence nothing runs yet -> what owes it. Both sit after an `xfail`, so
# they never execute today and raise rather than assert.
NOT_WRITTEN: dict[str, str] = {
    'there is a rename_table operation from "user" to "users"': "a table's rename hint",
    (
        "there is NO drop_table operation, because dropping and recreating would discard every row"
    ): "a table's rename hint",
}


# How many times a scenario asks for the answer.
HOW_MANY_RUNS: dict[str, int] = {
    "the schemas are diffed": 1,
    "the schemas are diffed twice": 2,
    "the views are diffed": 1,
    "it is diffed": 1,
}

# `a` and `no` are the two things a Then says about an operation being there.
PRESENCE: dict[str, bool] = {"a": True, "no": False}

# The word a Then writes for an operation -> the kind it names. "a drop for"
# and "a drop_table operation for" are the same claim in two spellings.
KIND: dict[str, str] = {
    "create_table": "create_table",
    "drop_table": "drop_table",
    "drop": "drop_table",
}


def _one_of(sentences: Any) -> Any:
    """A pattern matching exactly the sentences given, and nothing else."""
    return parsers.re("^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$")


# =============================================================================
# What comes back
# =============================================================================


def _answers(scenario: dict) -> list[list[dict[str, Any]]]:
    """The operations, once per run the scenario asked for.

    THE FUNCTION IS CALLED HERE RATHER THAN IN THE `When`, because it is pure:
    two schemas in, a list out, no clock, no connection and no file. Calling it
    where the answer is read is the same answer, and it lets the five scenarios
    that write no `When` at all read one.
    """
    return [difference(scenario["live"], scenario["target"]) for _ in range(scenario["runs"])]


def _answer(scenario: dict) -> list[dict[str, Any]]:
    """The operations, for a scenario that asked once."""
    (operations,) = _answers(scenario)
    return operations


def _named_by(operations: list[dict[str, Any]], kind: str) -> list[str]:
    """Every name an operation of this kind acts on."""
    return [operation["table"] for operation in operations if operation["kind"] == kind]


# =============================================================================
# Given
# =============================================================================


@given(_one_of(WHAT_IS_DECLARED))
def _what_is_declared(scenario: dict, sentence: str) -> None:
    """One step for every schema this feature declares. The sentence is the key."""
    scenario["runs"] = HOW_MANY_RUNS["the schemas are diffed"]
    scenario.update(WHAT_IS_DECLARED[sentence])


@given(_one_of(CANNOT_BE_SAID))
def _a_given_nothing_here_can_take(scenario: dict, sentence: str) -> None:
    """A Given naming something no argument of this function can carry."""
    pytest.xfail(f"difference cannot be told this: {CANNOT_BE_SAID[sentence]}")


# =============================================================================
# When
# =============================================================================


@when(_one_of(HOW_MANY_RUNS))
def _the_schemas_are_diffed(scenario: dict, sentence: str) -> None:
    """How many answers this scenario reads. Twice is how it asks for purity."""
    scenario["runs"] = HOW_MANY_RUNS[sentence]


# =============================================================================
# Then -- what the operations say
# =============================================================================


@then(
    parsers.re(
        r"^there is (?P<presence>a|no) (?P<kind>create_table|drop_table|drop)"
        r' (?:operation )?for "(?P<name>\w+)"$'
    )
)
def _there_is_an_operation_for(scenario: dict, presence: str, kind: str, name: str) -> None:
    """A table on one side only is created or dropped; a table on both is
    neither, because it is compared rather than recreated."""
    acted_on = _named_by(_answer(scenario), KIND[kind])

    assert (name in acted_on) is PRESENCE[presence], (
        f"{KIND[kind]} names {acted_on}, and the scenario says {presence} for {name!r}"
    )


@then("there is an alter for that column")
def _there_is_an_alter_for_that_column(scenario: dict) -> None:
    """A column held as one type and declared as another is altered in place.
    Dropping and adding it would empty the column on the way past."""
    altered = [
        operation["detail"]["column"]
        for operation in _answer(scenario)
        if operation["kind"] == "alter_column"
    ]

    assert altered == [scenario["column"]], f"alter_column names {altered}"


@then(parsers.re(r'^there is an add_column operation for "(?P<column>\w+)" on "(?P<table>\w+)"$'))
def _there_is_an_add_column_for(scenario: dict, column: str, table: str) -> None:
    """A column only the models declare is added to the table that holds it."""
    added = [
        (operation["table"], operation["detail"]["column"])
        for operation in _answer(scenario)
        if operation["kind"] == "add_column"
    ]

    assert (table, column) in added, f"add_column names {added}"


# TWO SCENARIOS ASK FOR NOTHING BACK, AND THEY ASK FOR THE SAME THING. One
# matches two whole schemas and one matches a view inside them, and both say
# that an empty list is an answer rather than a failure: two schemas can always
# be compared, so this function has no refusal to return and nothing about an
# empty list can be read as one.
NOTHING_CAME_BACK = (
    "nothing comes back, and that is distinguishable from a failure",
    "no operations are produced, because a migration that rewrites things nobody "
    "changed is indistinguishable from one that had a reason to",
)


@then(_one_of(NOTHING_CAME_BACK))
def _nothing_comes_back(scenario: dict, sentence: str) -> None:
    """Nothing to do comes back as an empty list, and never as a fault."""
    answer = _answer(scenario)

    assert answer == [], f"{sentence!r}, and the answer was {answer!r}"


@then(
    parsers.re(
        r"^both results are identical, because the differ performs no I/O, holds no "
        r"state, and reads no clock.+$"
    )
)
def _both_results_are_identical(scenario: dict) -> None:
    """Two calls on one pair of schemas give one answer, which is what lets a
    migration be read before it runs."""
    first, second = _answers(scenario)

    assert first == second, f"one pair of schemas gave {first!r} and then {second!r}"


@then("an operation is produced for the changed view")
def _an_operation_is_produced_for_the_changed_view(scenario: dict) -> None:
    """A view whose query changed is a view the database has to be told about."""
    acted_on = [operation["table"] for operation in _answer(scenario)]

    assert "recent" in acted_on, f"the operations name {acted_on}, and none of them is the view"


# =============================================================================
# Then -- the sentences nothing satisfies yet
# =============================================================================


@then(_one_of(NOT_WRITTEN))
def _a_then_nothing_answers_yet(scenario: dict, sentence: str) -> None:
    """Every sentence above sits after an `xfail`, so none of these run today.

    They raise rather than assert, so that deleting the `xfail` that guards one
    turns its scenario red and names the sentence, instead of turning it green
    on a body nobody wrote.
    """
    raise NotImplementedError(f"CODE NOT WRITTEN: {NOT_WRITTEN[sentence]} owes this line")
