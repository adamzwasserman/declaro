"""Bind `tests/bdd/features/core/condition_from_narrowing.feature`.

WHAT THE FUNCTION ANSWERS. A caller narrowed a question in the words their
grammar gave them. `condition_from_narrowing` is handed that narrowing and
hands back the one shape this library holds: every comparison spelled out, with
`any_of` and `none_of` present whether or not anybody wrote `or` or `not`.

WHY ONE FUNCTION AND NOT FOUR. Measured 2026-09-21: four grammar builders had
each invented this work, and two of them had already drifted from the other two
on questions nobody had decided. A condition is data, and data composes the same
way whatever spelled it.

WHAT NO STEP HERE DOES. It never opens a file, a clock or a connection, because
the function is pure: a narrowing in, a `Condition` or a `Fault` out. It never
reads the module's own text either. A claim about how the code is written is a
gate over `src/`; a scenario states what comes back.

ONE FUNCTION OVER A TABLE, TWICE OVER. Every When in this feature says the same
thing in different words -- here is what a caller wrote -- so there is one When,
keyed by the sentence. The Thens that differ only in a word are one function
over that word: `matters` against `does not matter`, `alternatives` against
`an exclusion`, and the three that name a column, an operator and a value.

THE KEY IS THE SENTENCE THE FEATURE WRITES, so a scenario reworded without a row
here fails by KeyError naming the sentence, rather than quietly passing on a row
somebody else's scenario left behind.
"""

from __future__ import annotations

from typing import Any, get_args

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.types import Operator

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/condition_from_narrowing.feature")


THE_FIFTEEN = sorted(get_args(Operator))


def _eq(value: Any) -> dict[str, Any]:
    """What a bare value becomes."""
    return {"operator": "eq", "value": value, "ignore_case": False}


def _just(column: str, value: Any) -> dict[str, Any]:
    """One condition holding one eq comparison over one column."""
    return {"each": {column: _eq(value)}, "any_of": [], "none_of": []}


# =============================================================================
# What a caller wrote, one row per sentence the feature says
# =============================================================================
#
# EVERY ROW CARRIES A LIST, because two scenarios hand the function more than
# one narrowing and ask that the answers agree. A row carries what its Then
# needs beside that list and nothing more.

WHAT_A_CALLER_WROTE: dict[str, dict[str, Any]] = {
    # --- a bare value ------------------------------------------------------
    'a caller narrows by name being "bob"': {
        "narrowings": [{"name": "bob"}],
    },
    "one caller writes the value plainly and another names eq": {
        "narrowings": [{"name": "bob"}, {"name": {"eq": "bob"}}],
    },
    "a caller narrows by name and by age at once": {
        "narrowings": [{"name": "bob", "age": 18}],
    },
    # --- a named operator --------------------------------------------------
    "a caller narrows by age being greater than 18": {
        "narrowings": [{"age": {"gt": 18}}],
    },
    "a caller narrows by age being between 18 and 30": {
        "narrowings": [{"age": {"between": [18, 30]}}],
    },
    "a caller narrows by an operator this library does not have": {
        "narrowings": [{"age": {"more_than": 18}}],
        "names": "more_than",
    },
    "a caller writes one comparison naming both gt and lt": {
        "narrowings": [{"age": {"gt": 18, "lt": 30}}],
    },
    "a caller writes a comparison with nothing in it": {
        "narrowings": [{"age": {}}],
    },
    # --- whether case matters ----------------------------------------------
    'a caller narrows by name being like "bo%"': {
        "narrowings": [{"name": {"like": "bo%"}}],
        "column": "name",
    },
    'a caller narrows by name being like "bo%", ignoring case': {
        "narrowings": [{"name": {"like": "bo%", "ignore_case": True}}],
        "column": "name",
    },
    # --- or, and not --------------------------------------------------------
    'a caller narrows by name being "alice" or "carol"': {
        "narrowings": [{"or": [{"name": "alice"}, {"name": "carol"}]}],
        "held": [_just("name", "alice"), _just("name", "carol")],
    },
    'a caller narrows by name not being "bob"': {
        "narrowings": [{"not": {"name": "bob"}}],
        "held": [_just("name", "bob")],
    },
    "a caller writes an alternative that itself narrows two ways": {
        "narrowings": [{"or": [{"name": "bob", "age": {"gt": 18}}, {"name": "carol"}]}],
    },
    # `{}` and nothing at all are the two ways a caller narrows by nothing, and
    # both are the same answer: a condition that is empty rather than absent.
    "a caller narrows by nothing": {
        "narrowings": [{}, None],
    },
    # --- what cannot be carried ---------------------------------------------
    "a caller writes their narrowing as a sentence": {
        "narrowings": ["name = 'bob' OR 1=1 --"],
    },
    "a caller narrows by something that is neither a value nor a comparison": {
        "narrowings": [{"tags": {"red", "blue"}}],
        "names": "set",
    },
    # THE FOUR GRAMMARS DIFFER IN THE WORD THEY CARRY A NARROWING UNDER --
    # `where`, `filter`, `filter_by` -- and that word never reaches this
    # function. What does reach it is the narrowing itself, and prisma spells a
    # bare value out where the other three leave it bare.
    "four callers ask one question in four grammars": {
        "narrowings": [
            {"name": "bob", "age": {"gt": 18}},
            {"name": "bob", "age": {"gt": 18}},
            {"name": {"eq": "bob"}, "age": {"gt": 18}},
            {"name": "bob", "age": {"gt": 18}},
        ],
    },
}


# The word a Then writes for the value a comparison carries -> that value.
AGAINST: dict[str, Any] = {
    '"bob"': "bob",
    "18": 18,
    "both numbers": [18, 30],
}

# The word a Then writes -> what `ignore_case` has to say.
CASE: dict[str, bool] = {"matters": False, "does not matter": True}

# The word a Then writes -> the list of a Condition it names, and how many
# conditions have to be in it.
SLOT: dict[str, str] = {"alternatives": "any_of", "an exclusion": "none_of"}
HOW_MANY: dict[str, int] = {"those two": 2, "that one": 1}

A_CONDITION = ("each", "any_of", "none_of")
A_FAULT = ("kind", "what_went_wrong", "what_to_do_instead")


@pytest.fixture
def narrow():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.condition_from_narrowing import condition_from_narrowing

    return condition_from_narrowing


def _answers(scenario: dict, narrow) -> list[Any]:
    """What came back for each narrowing this scenario's caller wrote."""
    return [narrow(written) for written in scenario["narrowings"]]


def _one_answer(scenario: dict, narrow) -> Any:
    """The answer, for a scenario whose caller wrote one narrowing."""
    (answer,) = _answers(scenario, narrow)
    return answer


def _the_refusal(scenario: dict, narrow) -> dict[str, Any]:
    """The Fault that came back, asserted to be one before it is read."""
    refusal = _one_answer(scenario, narrow)
    assert isinstance(refusal, dict) and tuple(refusal) == A_FAULT, (
        f"expected a refusal in the return, got {refusal!r}"
    )
    assert refusal["kind"] == "refused", refusal
    assert refusal["what_to_do_instead"].strip() != "", (
        "a refusal that only says what went wrong leaves the caller nowhere to go"
    )
    return refusal


@when(parsers.re(r"^(?P<sentence>.+)$"))
def _what_a_caller_wrote(scenario: dict, sentence: str) -> None:
    """One step for every When in this feature. The sentence is the key."""
    scenario.update(WHAT_A_CALLER_WROTE[sentence])


# =============================================================================
# What the condition carries
# =============================================================================


@then(
    parsers.re(
        r"^the condition compares (?P<column>\w+) with (?P<operator>\w+) "
        r"against (?P<against>.+)$"
    )
)
def _the_condition_compares(
    scenario: dict, narrow, column: str, operator: str, against: str
) -> None:
    """A bare value, a named operator and an operator taking two values all
    land in one place: a comparison under the column's own name."""
    condition = _one_answer(scenario, narrow)

    assert condition["each"][column] == {
        "operator": operator,
        "value": AGAINST[against],
        "ignore_case": False,
    }, condition


@then("neither number was pasted into anything")
def _neither_number_was_pasted(scenario: dict, narrow) -> None:
    """Both numbers arrive as numbers, in a list, under `value`. A pair joined
    into "18 AND 30" would read the same to a human and be text by the time it
    reached an engine, which is the one thing this library exists to prevent."""
    value = _one_answer(scenario, narrow)["each"]["age"]["value"]

    assert value == [18, 30], value
    assert [type(one) for one in value] == [int, int], (
        f"the two numbers stopped being numbers: {value!r}"
    )


@then("the condition carries a comparison for each, and asks for both")
def _a_comparison_for_each(scenario: dict, narrow) -> None:
    """Two keys already mean both, so `each` needs no word of its own and both
    comparisons sit in it."""
    condition = _one_answer(scenario, narrow)

    assert condition["each"] == {"name": _eq("bob"), "age": _eq(18)}, condition
    assert condition["any_of"] == [] and condition["none_of"] == [], (
        f"two keys are an and, not an alternative: {condition!r}"
    )


@then(parsers.re(r"^(?:both produce the same|the four narrowings produce one) condition$"))
def _they_produce_one_condition(scenario: dict, narrow) -> None:
    """Several spellings of one question are one condition. Nothing outside the
    argument is read, so the answers cannot differ for any other reason."""
    answers = _answers(scenario, narrow)

    first = answers[0]
    differing = [one for one in answers if one != first]
    assert differing == [], f"one question spelled several ways gave {answers!r}"


@then(parsers.re(r"^the comparison says case (?P<answer>matters|does not matter)$"))
def _the_comparison_says_case(scenario: dict, narrow, answer: str) -> None:
    """Text matching is case-sensitive unless the caller says otherwise, and a
    caller who says otherwise is carried."""
    comparison = _one_answer(scenario, narrow)["each"][scenario["column"]]

    assert comparison["ignore_case"] is CASE[answer], comparison


@then("the comparison carries an answer about case either way")
def _an_answer_about_case_either_way(scenario: dict, narrow) -> None:
    """A caller who wrote nothing about case still gets a comparison that says
    something about it, so no engine downstream decides for itself."""
    comparison = _one_answer(scenario, narrow)["each"][scenario["column"]]

    assert "ignore_case" in comparison, (
        f"a reader further down would have to check for the field: {comparison!r}"
    )
    assert isinstance(comparison["ignore_case"], bool), comparison


@then(
    parsers.re(
        r"^the condition holds (?P<how_many>those two|that one) as (?P<slot>alternatives|an exclusion)$"
    )
)
def _the_condition_holds(scenario: dict, narrow, how_many: str, slot: str) -> None:
    """`or` becomes `any_of` and `not` becomes `none_of`, and what hung under
    either is a condition of the same shape."""
    condition = _one_answer(scenario, narrow)

    assert condition[SLOT[slot]] == scenario["held"], condition
    assert len(condition[SLOT[slot]]) == HOW_MANY[how_many], condition
    assert condition["each"] == {}, (
        f"a combining key is not also a column of that name: {condition!r}"
    )


@then(parsers.re(r"^the word (?P<word>or|not) appears nowhere in it$"))
def _the_word_appears_nowhere(scenario: dict, narrow, word: str) -> None:
    """A declaration holds no word a reader further down would have to read
    again, so the caller's word is gone by the time this answers."""
    condition = _one_answer(scenario, narrow)

    def _keys(held: Any) -> list[str]:
        return [
            key for one in held["any_of"] + held["none_of"] for key in [*one, *_keys(one)]
        ] + list(held["each"])

    found = [key for key in _keys(condition) if key == word]
    assert found == [], f"{word!r} survived into the condition: {condition!r}"


@then("each level is a condition of the same shape")
def _each_level_is_a_condition(scenario: dict, narrow) -> None:
    """An alternative that itself narrows two ways nests, and the nested one
    carries all three fields, because every reader reads them by name."""
    condition = _one_answer(scenario, narrow)

    levels = [condition, *condition["any_of"]]
    wrong = [one for one in levels if tuple(one) != A_CONDITION]
    assert wrong == [], f"these levels are not conditions: {wrong!r}"

    nested, plain = condition["any_of"]
    assert nested["each"] == {
        "name": _eq("bob"),
        "age": {"operator": "gt", "value": 18, "ignore_case": False},
    }, nested
    assert plain == _just("name", "carol"), plain


@then("the condition compares nothing, and both its lists are empty")
def _the_condition_compares_nothing(scenario: dict, narrow) -> None:
    """Empty, and never absent: every reader of a declaration reads this by
    name, so an absent condition would be a KeyError one step further on."""
    answers = _answers(scenario, narrow)

    wrong = [one for one in answers if one != {"each": {}, "any_of": [], "none_of": []}]
    assert wrong == [], f"narrowing by nothing gave {wrong!r}"


# =============================================================================
# What comes back refused
# =============================================================================


@then("the refusal names what they wrote and lists the operators that exist")
def _names_what_they_wrote_and_lists_the_fifteen(scenario: dict, narrow) -> None:
    """A refusal that says only "wrong" makes the caller guess which word it
    meant, so it names the word and then lists the fifteen it could have been."""
    refusal = _the_refusal(scenario, narrow)

    assert scenario["names"] in refusal["what_went_wrong"], refusal
    missing = [word for word in THE_FIFTEEN if word not in refusal["what_to_do_instead"]]
    assert missing == [], f"the refusal does not list {missing}: {refusal}"


# THREE SENTENCES ASKING ONE THING: does the refusal say this, in this half of
# it. A refusal carries what went wrong and what to write instead, and which half
# a phrase belongs in is the whole of what these three rows differ by.
#
# Two operators and none at all are one refusal, because a comparison holds one
# operator and one value: a second has nowhere to go and a zeroth leaves the
# value with nothing to compare it by. A caller who wanted both comparisons can
# have both, so the refusal says how rather than leaving them to find it. And a
# narrowing written as a sentence is neither a name this library can look up nor
# a value it can bind, so it stops rather than being carried.
WHAT_THE_REFUSAL_SAYS: dict[str, tuple[str, str]] = {
    "the refusal says a comparison names one operator": (
        "what_went_wrong",
        "names one operator",
    ),
    "it says two keys are how you ask for both": ("what_to_do_instead", "two keys"),
    "the refusal says a condition is data": ("what_went_wrong", "a condition is data"),
}


@then(parsers.re(f"^(?P<sentence>{'|'.join(WHAT_THE_REFUSAL_SAYS)})$"))
def _what_the_refusal_says(scenario: dict, narrow, sentence: str) -> None:
    half, phrase = WHAT_THE_REFUSAL_SAYS[sentence]
    refusal = _the_refusal(scenario, narrow)

    assert phrase in refusal[half], refusal


@then("nothing was pasted into a statement")
def _nothing_was_pasted_into_a_statement(scenario: dict, narrow) -> None:
    """The sentence came back refused rather than carried. A condition built
    around it would have reached the engine as text somebody else wrote."""
    refused = _one_answer(scenario, narrow)

    assert tuple(refused) != A_CONDITION, (
        f"the sentence became a condition and is now on its way to an engine: {refused!r}"
    )


@then("the refusal names the shape they wrote")
def _names_the_shape_they_wrote(scenario: dict, narrow) -> None:
    """A shape with no rule is named by its own shape, because the caller
    cannot see which of their keys this library had no row for."""
    refusal = _the_refusal(scenario, narrow)

    assert scenario["names"] in refusal["what_went_wrong"], refusal
