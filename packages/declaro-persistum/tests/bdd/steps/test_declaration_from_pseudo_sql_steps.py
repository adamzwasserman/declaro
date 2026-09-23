"""Bind `tests/bdd/features/core/declaration_from_pseudo_sql.feature`.

WHAT THE FEATURE ASKS FOR. The three scenarios it was written with are three
questions about one answer: the declaration carries what the caller named, a bare
value has become an eq comparison before the declaration exists, and every string
the caller passed is sitting where it will be looked up or where it will be
bound. The rest of the scenarios were learned while building the function, and
they are in the feature rather than in a step body because each is a separate
result and a reader has to be able to see which one broke.

WHERE THE WORDS COME FROM. `people.select("id", "name").where({"name": "bob"})`
reads in SQL's word order, so the words the surface deposits are SQL's: `select`,
`where`, `ordered_by`, `grouped_by`, `having` and `page`. `model` and `wanted`
sit beside them, under those names in all four grammars, because the table a call
was written against and the asking word from HOW-IT-SHOULD-WORK.md line 108 are
nobody's phrasing.

NO STEP HERE REACHES A MODEL, AND THAT IS THE POINT.
`declaration_from_pseudo_sql` takes one argument and no model, so it cannot look
a column up. What it can do is put every key in a position that gets looked up
and every value in a position that gets bound, which is what the Thens read. The
lookup itself happens later, against the model, in `fn refuse_undeclared_columns`.

NO STEP HERE CALLS A DATABASE, a clock or a file. The function is pure, so a
scenario is an `Asked` in and a `Declaration` or a `Fault` out.
"""

from __future__ import annotations

from typing import Any, get_args, get_type_hints

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.types import Declaration, Operator, Reading, Writing

scenarios("../features/core/declaration_from_pseudo_sql.feature")


# =============================================================================
# What a caller wrote, one row per phrasing
# =============================================================================
#
# ONE FUNCTION OVER A TABLE, because every When in this feature is one step.
# Erase the phrasing from each and they all say the same thing: here is what the
# caller wrote and what to expect of it, put both where a Then can read them.
#
# THE KEY IS THE SENTENCE THE FEATURE WRITES, so a scenario reworded without a
# row here fails by KeyError naming the sentence, rather than passing on a row
# somebody else's scenario left behind.
#
# A ROW CARRIES WHAT ITS THEN NEEDS AND NOTHING MORE. `asked` is in every row.
# `column` and `comparison` are in the rows an outline over comparisons reads,
# `held` and `combined` in the rows about `or` and `not`, and `names` and
# `lists` in the rows about what a refusal has to say.

PLAIN = 'where({"name": "bob"})'

_A_PERSON: dict[str, Any] = {"model": "people", "wanted": "all"}


def _narrowing(written: Any) -> dict[str, Any]:
    """A caller who named the model, asked for every row, and narrowed."""
    return {**_A_PERSON, "where": written}


def _eq(value: Any) -> dict[str, Any]:
    """What a bare value becomes."""
    return {"operator": "eq", "value": value, "ignore_case": False}


def _just(column: str, value: Any) -> dict[str, Any]:
    """One condition holding one comparison over one column."""
    return {"each": {column: _eq(value)}, "any_of": [], "none_of": []}


THE_FIFTEEN = sorted(get_args(Operator))
THE_ASKING_WORDS = sorted(get_args(Reading) + get_args(Writing))

WHAT_A_CALLER_WROTE: dict[str, dict[str, Any]] = {
    'people.select("id", "name").where({"name": "bob"})': {
        "asked": {**_A_PERSON, "select": ["id", "name"], "where": {"name": "bob"}},
    },
    # --- one comparison at a time -------------------------------------------
    PLAIN: {
        "asked": _narrowing({"name": "bob"}),
        "column": "name",
        "comparison": _eq("bob"),
    },
    'where({"name": {"eq": "bob"}})': {
        "asked": _narrowing({"name": {"eq": "bob"}}),
        "column": "name",
        "comparison": _eq("bob"),
    },
    'where({"age": 30})': {
        "asked": _narrowing({"age": 30}),
        "column": "age",
        "comparison": _eq(30),
    },
    'where({"rate": 1.5})': {
        "asked": _narrowing({"rate": 1.5}),
        "column": "rate",
        "comparison": _eq(1.5),
    },
    'where({"paid": true})': {
        "asked": _narrowing({"paid": True}),
        "column": "paid",
        "comparison": _eq(True),
    },
    'where({"note": nothing})': {
        "asked": _narrowing({"note": None}),
        "column": "note",
        "comparison": _eq(None),
    },
    'where({"age": {"gt": 18}})': {
        "asked": _narrowing({"age": {"gt": 18}}),
        "column": "age",
        "comparison": {"operator": "gt", "value": 18, "ignore_case": False},
    },
    'where({"name": {"like": "bo%", "ignore_case": true}})': {
        "asked": _narrowing({"name": {"like": "bo%", "ignore_case": True}}),
        "column": "name",
        "comparison": {"operator": "like", "value": "bo%", "ignore_case": True},
    },
    # Case-sensitive unless the caller says otherwise, per
    # HOW-IT-SHOULD-WORK.md line 143.
    'where({"name": {"like": "bo%"}})': {
        "asked": _narrowing({"name": {"like": "bo%"}}),
        "column": "name",
        "comparison": {"operator": "like", "value": "bo%", "ignore_case": False},
    },
    # --- the two keys that combine ------------------------------------------
    'where({"or": [{"name": "bob"}, {"name": "alice"}]})': {
        "asked": _narrowing({"or": [{"name": "bob"}, {"name": "alice"}]}),
        "held": "any_of",
        "combined": [_just("name", "bob"), _just("name", "alice")],
    },
    'where({"not": {"name": "bob"}})': {
        "asked": _narrowing({"not": {"name": "bob"}}),
        "held": "none_of",
        "combined": [_just("name", "bob")],
    },
    # --- a caller who narrowed by nothing ------------------------------------
    'select("id", "name") and nothing else': {
        "asked": {**_A_PERSON, "select": ["id", "name"]},
    },
    # Three keys, because the two Thens over it have three things to follow: a
    # bare value, a named operator, and an operator with its companion.
    "a condition": {
        "asked": _narrowing(
            {"name": "bob", "age": {"gt": 18}, "note": {"like": "%o%", "ignore_case": True}}
        ),
    },
    # --- what a declaration cannot be built from ------------------------------
    'select("id", "name") naming no model': {
        "asked": {"wanted": "all", "select": ["id", "name"]},
    },
    'select("id", "name") asking for nothing': {
        "asked": {"model": "people", "select": ["id", "name"]},
    },
    'where("active = :x")': {"asked": _narrowing("active = :x")},
    'where({"age": {"roughly": 18}})': {
        "asked": _narrowing({"age": {"roughly": 18}}),
        "names": "roughly",
        "lists": THE_FIFTEEN,
    },
    'where({"age": {"gt": 18, "lt": 30}})': {
        "asked": _narrowing({"age": {"gt": 18, "lt": 30}}),
    },
    'where({"age": a set of two numbers})': {
        "asked": _narrowing({"age": {18, 30}}),
        "names": "set",
        "lists": ["string", "number", "true", "false"],
    },
    'where({"not": "active = :x"})': {"asked": _narrowing({"not": "active = :x"})},
    'where({"or": "active = :x"})': {"asked": _narrowing({"or": "active = :x"})},
    'asking for "fetch"': {
        "asked": {**_A_PERSON, "wanted": "fetch"},
        "names": "fetch",
        "lists": THE_ASKING_WORDS,
    },
}


@pytest.fixture
def declare():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.declaration_from_pseudo_sql import declaration_from_pseudo_sql

    return declaration_from_pseudo_sql


@when(parsers.parse("a caller writes {phrasing}"))
def _a_caller_writes(scenario: dict, phrasing: str) -> None:
    scenario.update(WHAT_A_CALLER_WROTE[phrasing])


# =============================================================================
# What the declaration carries
# =============================================================================


@then("the declaration names the model, those two columns and that condition")
def _names_the_model_the_columns_and_the_condition(scenario: dict, declare) -> None:
    declaration = declare(scenario["asked"])

    assert declaration["model"] == "people"
    assert declaration["columns"] == ["id", "name"]
    assert declaration["condition"] == _just("name", "bob")


@then("naming another model and other columns moves both")
def _naming_another_model_moves_both(scenario: dict, declare) -> None:
    """None of the three names above is a constant the function writes down."""
    elsewhere = declare({**scenario["asked"], "model": "orders", "select": ["id"]})

    assert elsewhere["model"] == "orders"
    assert elsewhere["columns"] == ["id"]


@then("the declaration carries every field")
def _carries_every_field(scenario: dict, declare) -> None:
    """A caller who named four still gets every field, because everything
    downstream reads them by name.

    READ OFF THE TYPE RATHER THAN LISTED HERE. A list written out is a second
    place the same fact lives, and it drifts the first time a field is added,
    which is what happened when `shaping` arrived.
    """
    declaration = declare(scenario["asked"])
    every_field = set(get_type_hints(Declaration))

    assert set(declaration) == every_field, (
        f"a Declaration is {sorted(every_field)} and this one carries "
        f"{sorted(declaration)}. Everything downstream reads a field by name, "
        f"so one that is absent raises where it is read rather than here."
    )


@then("it equals the declaration the plain spelling builds")
def _equals_the_plain_spelling(scenario: dict, declare) -> None:
    """`{"name": "bob"}` and `{"name": {"eq": "bob"}}` are one question written
    twice, so they have to arrive as one declaration. The plain spelling built a
    second time is one of the two rows, which is also what says nothing outside
    the argument was read."""
    plain = declare(WHAT_A_CALLER_WROTE[PLAIN]["asked"])

    assert declare(scenario["asked"]) == plain


# =============================================================================
# What a condition carries
# =============================================================================


@then("the condition holds an eq comparison, not a bare value")
def _holds_an_eq_comparison(scenario: dict, declare) -> None:
    each = declare(scenario["asked"])["condition"]["each"]

    assert each["name"] == {"operator": "eq", "value": "bob", "ignore_case": False}
    assert each["name"] != "bob", "the bare value survived into the declaration"


@then("the comparison is the one the caller wrote")
def _the_comparison_the_caller_wrote(scenario: dict, declare) -> None:
    """One row per shape a caller can write against a column: the five values
    this library can bind, and an operator named with and without its companion.
    `true` and `1` are told apart, which an exact-type row does and a subclass
    check would not."""
    each = declare(scenario["asked"])["condition"]["each"]

    assert each[scenario["column"]] == scenario["comparison"]


@then("the condition holds what the caller combined")
def _holds_what_the_caller_combined(scenario: dict, declare) -> None:
    """`or` and `not` are still what a caller writes; a Condition holds them as
    `any_of` and `none_of`. The comparison inside each is built the same way, so
    a bare value nested two deep is an eq comparison too."""
    condition = declare(scenario["asked"])["condition"]

    assert condition[scenario["held"]] == scenario["combined"]
    assert condition["each"] == {}, "a combining key is not also a column of that name"


@then("the condition is empty rather than absent")
def _the_condition_is_empty(scenario: dict, declare) -> None:
    """Every reader of a Declaration reads this field by name."""
    assert declare(scenario["asked"])["condition"] == {"each": {}, "any_of": [], "none_of": []}


# =============================================================================
# Where every string the caller passed ends up
# =============================================================================


@then("every key was checked against the model and every value bound")
def _every_key_checked_and_every_value_bound(scenario: dict, declare) -> None:
    each = declare(scenario["asked"])["condition"]["each"]
    written = scenario["asked"]["where"]

    # EVERY KEY IS A KEY. It arrives as the name of a comparison, which is the
    # position a column is looked up from and the only one it can be looked up
    # from. None of them became text.
    assert sorted(each) == sorted(written), f"{sorted(each)} is not {sorted(written)}"

    # EVERY VALUE IS A VALUE. It sits under `value`, alone, which is the
    # position the compiler binds a parameter from.
    assert [each[key]["value"] for key in ["name", "age", "note"]] == ["bob", 18, "%o%"]


@then("moving a dangerous value moves that value and nothing else")
def _moving_a_dangerous_value(scenario: dict, declare) -> None:
    """The way to see that nothing was pasted is to move a value that would
    break a statement if it had been."""
    declaration = declare(scenario["asked"])
    each = declaration["condition"]["each"]
    dangerous = "bob' OR 1=1 --"

    attacked = declare(
        {**scenario["asked"], "where": {**scenario["asked"]["where"], "name": dangerous}}
    )

    assert attacked["condition"]["each"]["name"]["value"] == dangerous
    assert attacked["condition"]["each"]["age"] == each["age"]
    assert attacked["condition"]["each"]["note"] == each["note"]
    assert {**attacked, "condition": declaration["condition"]} == declaration


# =============================================================================
# What comes back refused
# =============================================================================


@then("it is refused, and the refusal says what to write instead")
def _it_is_refused(scenario: dict, declare) -> None:
    """Eight things a declaration cannot be built from. Each comes back in the
    return value: nothing here raised and nothing here logged."""
    refusal = declare(scenario["asked"])

    assert set(refusal) == {"kind", "what_went_wrong", "what_to_do_instead"}, refusal
    assert refusal["kind"] == "refused"
    assert refusal["what_went_wrong"].strip() != ""
    assert refusal["what_to_do_instead"].strip() != "", (
        "a refusal that only says what went wrong leaves the caller nowhere to go"
    )


@then("the refusal names what was written and lists what it could have been")
def _the_refusal_names_and_lists(scenario: dict, declare) -> None:
    """A refusal that says only "wrong" makes the caller guess. Each of these
    names the word or the shape that arrived and then lists what it could have
    been: the fifteen operators, the eighteen asking words, or the five things
    this library can bind."""
    refusal = declare(scenario["asked"])

    assert scenario["names"] in refusal["what_went_wrong"], refusal
    missing = [word for word in scenario["lists"] if word not in refusal["what_to_do_instead"]]
    assert missing == [], f"the refusal does not list {missing}: {refusal}"
