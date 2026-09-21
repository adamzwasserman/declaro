"""Bind `tests/bdd/features/core/declaration_from_django.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. Two scenarios, and both are about one call written in
Django's phrasing. The first says a declaration comes back naming the model, the
two columns the caller asked for, and the condition it filtered by. The second
says the condition holds a comparison, so `name="bob"` arrives as an `eq` against
`"bob"` rather than as the string `"bob"` sitting where a comparison belongs.

WHAT A CALLER WRITES, AND WHAT ARRIVES. The feature writes the Django expression
`Person.objects.filter(name="bob").values("id", "name")`. The function under test
is handed an `Asked`, which is that expression already taken apart: the model it
was written against, the asking word it ended on, and one key per Django word the
caller used. `WRITTEN` below is the whole of that translation, one row per
sentence in the feature, so the two `When` steps are one function over a table
rather than two functions differing only in their words.

THE SECOND SCENARIO WRITES A FRAGMENT. `filter(name="bob")` names no model,
because the scenario is about the condition and nothing else. A declaration names
one regardless, so its row carries `Person`, the model the feature's first line
already wrote.

WHY THE THENS ASSERT MORE THAN THE SENTENCE. Each extra line is the same claim
asked of something that would pass the sentence while being wrong. "Names the
model" is satisfied by any string, including one this function invented, so the
model is moved and a call that names none is checked to come back refused. "An
eq comparison, not a bare value" is satisfied by a function that writes `eq` down
without reading the caller at all, so every one of the fifteen operators is asked
for and has to come back as itself.
"""

from __future__ import annotations

from typing import Any, get_args, get_type_hints

from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.declaration_from_django import declaration_from_django
from declaro_persistum.types import Declaration, Operator

scenarios("../features/core/declaration_from_django.feature")


# --- what the feature's two sentences mean, once each -----------------------
#
# THE KEY IS THE SENTENCE AS THE FEATURE WRITES IT. A row that drifted from the
# Gherkin would fail to be found rather than pass quietly, because the `When`
# below looks the written call up and nothing invents a row for a miss.

WRITTEN: dict[str, dict[str, Any]] = {
    'Person.objects.filter(name="bob").values("id", "name")': {
        "model": "Person",
        "wanted": "all",
        "filter": {"name": "bob"},
        "values": ["id", "name"],
    },
    'filter(name="bob")': {
        "model": "Person",
        "wanted": "all",
        "filter": {"name": "bob"},
    },
}

NOTHING_NARROWED: dict[str, Any] = {"each": {}, "any_of": [], "none_of": []}
"""A `Condition` that narrows nothing, which is what an unasked one holds."""

BOB_BY_NAME: dict[str, Any] = {
    "each": {"name": {"operator": "eq", "value": "bob", "ignore_case": False}},
    "any_of": [],
    "none_of": [],
}
"""The condition `filter(name="bob")` means, written out."""


@when(parsers.parse("a caller writes {call}"))
def _a_caller_writes(scenario: dict[str, object], call: str) -> None:
    """Take the written call apart into the `Asked` the builder is handed."""
    assert call in WRITTEN, f"the feature writes {call!r} and this file has no row for it"
    scenario["call"] = call
    scenario["asked"] = WRITTEN[call]
    scenario["built"] = declaration_from_django(WRITTEN[call])


@then("the declaration names the model, those two columns and that condition")
def _it_names_the_model_the_columns_and_the_condition(scenario: dict[str, object]) -> None:
    built = scenario["built"]
    call = scenario["call"]
    assert isinstance(built, dict), f"nothing came back to read: {built!r}"
    assert "kind" not in built, f"the call was refused: {built}"

    assert built["model"] == "Person"
    assert built["columns"] == ["id", "name"]
    assert built["condition"] == BOB_BY_NAME

    # A declaration carries ten things and this is all of them. A builder that
    # left one out would hand `compile_statement` a shape it cannot read, and
    # the three lines above would not notice.
    assert set(built) == set(get_type_hints(Declaration)), (
        f"this is not the declaration the other three grammars produce: {sorted(built)}"
    )

    # Nothing was asked for beyond the columns and the condition, so the rest of
    # the declaration says so rather than carrying something nobody wrote.
    assert built["order"] == {}
    assert built["grouping"] == []
    assert built["having"] == NOTHING_NARROWED
    assert built["paging"] is None
    assert built["rows"] is None
    assert built["across"] is None

    # THE MODEL IS READ, NOT INVENTED. Any string passes the line above, so the
    # model is moved and the answer has to move with it.
    named = [
        (model, declaration_from_django({**WRITTEN[call], "model": model})["model"])
        for call in WRITTEN
        for model in ["Person", "Order", "people"]
    ]
    wrong = [seen for seen in named if seen[0] != seen[1]]
    assert wrong == [], f"the declaration named a model the caller did not: {wrong}"

    # A CALL THAT NAMES NO MODEL CANNOT NAME ONE. The refusal comes back in the
    # return, so a caller reads it rather than catching it.
    nameless = {word: value for word, value in WRITTEN[call].items() if word != "model"}
    refused = declaration_from_django(nameless)
    assert refused["kind"] == "refused", f"a call naming no model built one anyway: {refused}"
    assert refused["what_to_do_instead"].strip() != ""

    # THE COLUMNS COME FROM ONE PLACE. `values` is where a Django caller names
    # them, and a word this builder does not read is refused rather than
    # absorbed, because absorbing it would be the second place to name a column
    # that HOW-IT-SHOULD-WORK.md says no grammar has.
    smuggled = declaration_from_django({**WRITTEN[call], "only": ["id"]})
    assert smuggled["kind"] == "refused", f"a word nobody reads was absorbed: {smuggled}"
    assert "only" in smuggled["what_went_wrong"], smuggled["what_went_wrong"]


@then("the condition holds an eq comparison, not a bare value")
def _the_condition_holds_an_eq_comparison(scenario: dict[str, object]) -> None:
    built = scenario["built"]
    assert isinstance(built, dict), f"nothing came back to read: {built!r}"
    assert "kind" not in built, f"the call was refused: {built}"

    comparison = built["condition"]["each"]["name"]
    assert comparison == {"operator": "eq", "value": "bob", "ignore_case": False}
    assert comparison != "bob", "the bare value is sitting where a comparison belongs"
    assert built["condition"] == BOB_BY_NAME

    # `{"name": "bob"}` and `{"name": {"eq": "bob"}}` are the same thing --
    # HOW-IT-SHOULD-WORK.md, "Narrowing". So the two spellings build one
    # declaration, and neither is a special case of the other.
    spelled_out = declaration_from_django(
        {**WRITTEN['filter(name="bob")'], "filter": {"name": {"eq": "bob"}}}
    )
    assert spelled_out == built, f"the two spellings of eq built different things: {spelled_out}"

    # EVERY OPERATOR IS READ, NOT ASSUMED. A function that wrote `eq` down
    # without looking would pass every line above, so each of the fifteen is
    # asked for and has to come back as itself.
    asked_for = [
        (
            operator,
            declaration_from_django(
                {**WRITTEN['filter(name="bob")'], "filter": {"age": {operator: 18}}}
            )["condition"]["each"]["age"],
        )
        for operator in get_args(Operator)
    ]
    wrong = [
        (operator, seen)
        for operator, seen in asked_for
        if seen != {"operator": operator, "value": 18, "ignore_case": False}
    ]
    assert wrong == [], f"these operators did not survive the trip: {wrong}"

    # Text matching is case-sensitive unless the caller says otherwise, and
    # saying otherwise has to reach the comparison.
    otherwise = declaration_from_django(
        {**WRITTEN['filter(name="bob")'], "filter": {"name": {"like": "bob", "ignore_case": True}}}
    )
    assert otherwise["condition"]["each"]["name"] == {
        "operator": "like",
        "value": "bob",
        "ignore_case": True,
    }

    # AN OPERATOR NOBODY DECLARED IS NOT QUIETLY A COLUMN NAMED AFTER IT. It
    # comes back refused, naming the operator and listing the fifteen.
    invented = declaration_from_django(
        {**WRITTEN['filter(name="bob")'], "filter": {"age": {"roughly": 18}}}
    )
    assert invented["kind"] == "refused", f"an invented operator was accepted: {invented}"
    assert "roughly" in invented["what_went_wrong"], invented["what_went_wrong"]
    assert "gt" in invented["what_went_wrong"], "the refusal lists the operators it declares"

    # `or` IS A WORD, NOT A COLUMN. It carries a condition, so reading it as a
    # column called "or" narrowed by a list is the silent wrong answer, and the
    # declaration that comes back holds the alternatives and not the word.
    composed = declaration_from_django(
        {**WRITTEN['filter(name="bob")'], "filter": {"or": [{"name": "alice"}]}}
    )
    assert composed["condition"]["each"] == {}, f"`or` was bound as a column: {composed}"
    assert composed["condition"]["any_of"] == [
        {
            "each": {"name": {"operator": "eq", "value": "alice", "ignore_case": False}},
            "any_of": [],
            "none_of": [],
        }
    ]
