"""Bind `tests/bdd/features/core/declaration_from_phrasing.feature`.

WHAT THE FEATURE ASKS FOR. A caller's question arrives in the words their
grammar gave them, beside a table saying which word fills which field. The
scenarios ask what comes back: that the caller's own names travel, that a field
the grammar brings no word for is still reached by the library's word, and that
what cannot be built comes back refused rather than guessed at.

WHAT THE SCENARIOS LEAVE OPEN, AND HOW IT IS SETTLED HERE. `fn
declaration_from_phrasing` takes a question and a table of words, so a scenario
that says "a grammar whose word for the condition is `where`" has to become a
table with one row. `THE_GRAMMAR_WITH_TWO_WORDS` below is the stand-in for a
grammar that spells two fields its own way and brings no word for the other
eight, which is what the backfill scenarios need.

THE WORDS ARE WRITTEN OUT HERE, NOT IMPORTED FROM THE FUNCTION. A test that
reads the same table the function reads asserts that the table equals itself.
The library's own word for each field is written in `THE_LIBRARY_WORD`, so a
word that changes in the source shows up here as a failing scenario rather than
as agreement.
"""

from __future__ import annotations

from typing import Any, get_args, get_type_hints

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.declaration_from_phrasing import declaration_from_phrasing
from declaro_persistum.types import Declaration, Reading, Writing

scenarios("../features/core/declaration_from_phrasing.feature")


THE_EIGHTEEN = [*get_args(Reading), *get_args(Writing)]

# A grammar that spells two fields its own way and brings no word for the other
# eight, which is the shape every backfill scenario is about.
THE_GRAMMAR_WITH_TWO_WORDS: dict[str, str] = {
    "condition": "filter_by",
    "columns": "with_only",
}

# THE FOUR GRAMMARS, EACH WITH ONE QUESTION WRITTEN IN ITS OWN WORDS. The four
# questions say the same thing, which is what the scenario about four callers
# asks to be true of the four declarations.
FOUR_GRAMMARS: dict[str, dict[str, Any]] = {
    "pseudo-sql": {
        "phrasing": {},
        "asked": {
            "model": "Person",
            "wanted": "all",
            "select": ["id", "name"],
            "where": {"name": "bob"},
        },
    },
    "django": {
        "phrasing": {"condition": "filter", "columns": "values"},
        "asked": {
            "model": "Person",
            "wanted": "all",
            "values": ["id", "name"],
            "filter": {"name": "bob"},
        },
    },
    "prisma": {
        "phrasing": {},
        "asked": {
            "model": "Person",
            "wanted": "all",
            "select": ["id", "name"],
            "where": {"name": "bob"},
        },
    },
    "sqlalchemy": {
        "phrasing": THE_GRAMMAR_WITH_TWO_WORDS,
        "asked": {
            "model": "Person",
            "wanted": "all",
            "with_only": ["id", "name"],
            "filter_by": {"name": "bob"},
        },
    },
}

# THE LIBRARY'S OWN WORD FOR EACH FIELD THE OUTLINE NAMES, what a caller writes
# under it, and what the declaration carries once it has been read. `having` is
# the one whose two differ, because a narrowing becomes a condition on the way.
THE_LIBRARY_WORD: dict[str, dict[str, Any]] = {
    "order": {
        "word": "ordered_by",
        "written": {"name": "ascending"},
        "carried": {"name": "ascending"},
    },
    "grouping": {
        "word": "grouped_by",
        "written": ["status"],
        "carried": ["status"],
    },
    "having": {
        "word": "having",
        "written": {"total": {"gt": 5}},
        "carried": {
            "each": {"total": {"operator": "gt", "value": 5, "ignore_case": False}},
            "any_of": [],
            "none_of": [],
        },
    },
    "paging": {
        "word": "page",
        "written": {"size": 10, "number": 1},
        "carried": {"size": 10, "number": 1},
    },
}

A_PART_NOBODY_READS = "only"
AN_ASKING_WORD_NOBODY_DECLARED = "fetch"
A_COLUMN_NO_MODEL_DECLARES = "unicorn"


def _eq(value: Any) -> dict[str, Any]:
    """What a bare value becomes."""
    return {"operator": "eq", "value": value, "ignore_case": False}


def _only(each: dict[str, Any]) -> dict[str, Any]:
    """A condition narrowing by these comparisons and combining nothing."""
    return {"each": each, "any_of": [], "none_of": []}


def _a_person(**parts: Any) -> dict[str, Any]:
    """A question about Person asking for every row, plus whatever else."""
    return {"model": "Person", "wanted": "all", **parts}


# =============================================================================
# The grammar a scenario is about
# =============================================================================


@given(parsers.parse('a grammar whose word for the {field} is "{word}"'))
def _a_grammar_with_one_word(scenario: dict, field: str, word: str) -> None:
    scenario["phrasing"] = {field: word}


@given(parsers.parse("a grammar that brings no word for {field}"))
def _a_grammar_with_no_word_for(scenario: dict, field: str) -> None:
    scenario["field"] = field
    scenario["phrasing"] = THE_GRAMMAR_WITH_TWO_WORDS


# =============================================================================
# What the caller asked
# =============================================================================


@when('a caller asks Person for id and name, narrowed by name being "bob"')
def _the_whole_question(scenario: dict) -> None:
    narrowed_by = scenario["phrasing"]["condition"]
    scenario["built"] = declaration_from_phrasing(
        _a_person(select=["id", "name"], **{narrowed_by: {"name": "bob"}}),
        scenario["phrasing"],
    )


@when("a caller names no columns")
def _a_caller_names_no_columns(scenario: dict) -> None:
    """Asked after a question that did name columns, so a declaration that
    remembered the last ones anybody named would have something to remember."""
    named = scenario["phrasing"]["columns"]
    declaration_from_phrasing(_a_person(**{named: ["id", "name"]}), scenario["phrasing"])
    scenario["built"] = declaration_from_phrasing(_a_person(), scenario["phrasing"])


@when("any question is built")
def _any_question(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing(_a_person(), {})


@when("four callers ask one question in four grammars")
def _four_callers(scenario: dict) -> None:
    scenario["built"] = [
        declaration_from_phrasing(row["asked"], row["phrasing"]) for row in FOUR_GRAMMARS.values()
    ]


@when("a caller uses the library's own word for it")
def _the_library_s_own_word(scenario: dict) -> None:
    row = THE_LIBRARY_WORD[scenario["field"]]
    scenario["carried"] = row["carried"]
    scenario["built"] = declaration_from_phrasing(
        _a_person(**{row["word"]: row["written"]}), scenario["phrasing"]
    )


@when("a caller narrows using that word")
def _a_caller_narrows_using_that_word(scenario: dict) -> None:
    narrowed_by = scenario["phrasing"]["condition"]
    scenario["built"] = declaration_from_phrasing(
        _a_person(**{narrowed_by: {"name": "bob"}}), scenario["phrasing"]
    )


# A PATTERN THAT ADMITS THE TWO PARTS AND NOTHING ELSE. `a caller leaves out the
# paging` is a different scenario with a different answer, and a looser pattern
# would swallow it and report that a declaration was refused for absent paging.
@when(parsers.re(r"a caller leaves out (?P<part>model|wanted)$"))
def _a_caller_leaves_out(scenario: dict, part: str) -> None:
    scenario["built"] = declaration_from_phrasing(
        {word: said for word, said in _a_person().items() if word != part}, {}
    )


@when("a caller leaves out the paging")
def _a_caller_leaves_out_the_paging(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing(_a_person(), {})


@when("a caller asks for something that is not one of the eighteen words")
def _an_asking_word_nobody_declared(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing(
        {"model": "Person", "wanted": AN_ASKING_WORD_NOBODY_DECLARED}, {}
    )


@when("a caller names a part no word in the table points at")
def _a_part_no_word_points_at(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing(
        _a_person(**{A_PART_NOBODY_READS: ["id"]}), THE_GRAMMAR_WITH_TWO_WORDS
    )


@when("any question is refused")
def _any_refusal(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing({"wanted": "all"}, {})


@when("a caller asks for a column no model declares")
def _a_column_no_model_declares(scenario: dict) -> None:
    scenario["built"] = declaration_from_phrasing(
        _a_person(
            select=[A_COLUMN_NO_MODEL_DECLARES],
            where={A_COLUMN_NO_MODEL_DECLARES: "bob"},
        ),
        {},
    )


@when("one question is asked twice")
def _asked_twice(scenario: dict) -> None:
    asked = _a_person(select=["id"], where={"name": "bob"})
    scenario["built"] = [
        declaration_from_phrasing(asked, {}),
        declaration_from_phrasing(asked, {}),
    ]


# =============================================================================
# What came back
# =============================================================================


@then("the declaration names Person, carries those two columns and that condition")
def _it_names_them(scenario: dict) -> None:
    assert scenario["built"] == {
        "model": "Person",
        "wanted": "all",
        "columns": ["id", "name"],
        "condition": _only({"name": _eq("bob")}),
        "order": {},
        "grouping": [],
        "having": _only({}),
        "paging": None,
        "rows": None,
        "across": None,
    }


@then("the declaration asks for none, and not for the last ones anybody named")
def _it_asks_for_no_columns(scenario: dict) -> None:
    assert scenario["built"]["columns"] == []


@then("the declaration carries all ten fields")
def _all_ten_fields(scenario: dict) -> None:
    assert set(scenario["built"]) == set(get_type_hints(Declaration))


@then("the four declarations are equal")
def _the_four_are_equal(scenario: dict) -> None:
    assert scenario["built"].count(scenario["built"][0]) == len(FOUR_GRAMMARS)


@then("none of them names a grammar")
def _none_names_a_grammar(scenario: dict) -> None:
    named = [name for name in FOUR_GRAMMARS if name in repr(scenario["built"])]
    assert named == []


@then("the declaration carries what they asked for")
def _it_carries_what_they_asked_for(scenario: dict) -> None:
    assert scenario["built"][scenario["field"]] == scenario["carried"]


@then("the declaration carries the narrowing")
def _it_carries_the_narrowing(scenario: dict) -> None:
    assert scenario["built"]["condition"] == _only({"name": _eq("bob")})


@then(parsers.re(r"the refusal names (?P<part>model|wanted) and says what to write$"))
def _the_refusal_names_the_part(scenario: dict, part: str) -> None:
    assert part in scenario["built"]["what_went_wrong"]
    assert scenario["built"]["what_to_do_instead"].strip() != ""


@then("the declaration says so without refusing")
def _absent_paging_is_an_answer(scenario: dict) -> None:
    assert scenario["built"]["paging"] is None


@then("the refusal names the word and lists the ones that exist")
def _the_refusal_names_the_word(scenario: dict) -> None:
    assert AN_ASKING_WORD_NOBODY_DECLARED in scenario["built"]["what_went_wrong"]
    unlisted = [
        word for word in THE_EIGHTEEN if word not in scenario["built"]["what_to_do_instead"]
    ]
    assert unlisted == []


@then("the refusal names that part rather than dropping it")
def _the_refusal_names_that_part(scenario: dict) -> None:
    assert A_PART_NOBODY_READS in scenario["built"]["what_went_wrong"]


@then("the refusal was returned, not raised")
def _the_refusal_came_back(scenario: dict) -> None:
    assert set(scenario["built"]) == {"kind", "what_went_wrong", "what_to_do_instead"}


@then("it says what went wrong and what to write instead")
def _it_says_both(scenario: dict) -> None:
    assert scenario["built"]["what_went_wrong"].strip() != ""
    assert scenario["built"]["what_to_do_instead"].strip() != ""


@then("the declaration carries the name and refuses nothing")
def _it_carries_the_undeclared_name(scenario: dict) -> None:
    assert scenario["built"]["columns"] == [A_COLUMN_NO_MODEL_DECLARES]


@then("the name sits where a column name is looked up, never where a value binds")
def _the_name_is_a_key(scenario: dict) -> None:
    each = scenario["built"]["condition"]["each"]
    assert list(each) == [A_COLUMN_NO_MODEL_DECLARES]
    assert [comparison["value"] for comparison in each.values()] == ["bob"]


@then("both answers are the same")
def _both_answers_are_the_same(scenario: dict) -> None:
    assert scenario["built"][0] == scenario["built"][1]
