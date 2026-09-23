"""Bind `tests/bdd/features/core/how_to_compare.feature`, and nothing else.

WHAT THE FEATURE ASKS. Four things per comparison: the symbol, how many values
go beside it, the pattern a value is wrapped in, and what an engine says instead
when case does not matter.

WHY THE SIXTH SCENARIO WALKS ALL FORTY-FIVE PAIRS. Five scenarios name five
operators, and a sixteenth added tomorrow satisfies all five by not being
mentioned. Walking `Operator` and `Engine` themselves is what makes a missing
row fail here rather than inside a rendered statement.

CASE IS THE ONE COLUMN THE ENGINES DISAGREE ABOUT, so the fifth scenario asks
each engine separately. `HOW-IT-SHOULD-WORK.md:143` says the same `like` matched
a name on one engine and nothing on another, silently, and that making them
agree is the library's job rather than the caller's.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_compare import how_to_compare
from declaro_persistum.types import Comparing, Engine, Operator

scenarios("../features/core/how_to_compare.feature")

EVERY_ENGINE = typing.get_args(Engine)
EVERY_OPERATOR = typing.get_args(Operator)
THE_FOUR_FIELDS = frozenset(typing.get_type_hints(Comparing))
A_TEXT_OPERATOR = "contains"
AN_OPERATOR_NOBODY_WROTE_DOWN = "sounds_like"
THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


# ONE FUNCTION OVER A TABLE. The four readings differ in how many values go
# beside the symbol and in nothing else, so the shape is the row.
HOW_MANY_VALUES = {
    "one value goes beside it": "one",
    "no value goes beside the symbol": "none",
    "two values go beside the symbol, a floor and a ceiling": "range",
}


# ONE FUNCTION OVER A TABLE. The two openings differ in which operator is asked
# for and in nothing else, so the operator is the row. Written as two, the same
# call sits in the file twice and the second one exists only to pass a word the
# first could have taken.
WHAT_EACH_OPENING_ASKS = {
    "the eq operator on any engine": "eq",
    "the contains operator on any engine": "contains",
    "the is_null operator on any engine": "is_null",
    "the between operator on any engine": "between",
    "an operator the table does not cover": AN_OPERATOR_NOBODY_WROTE_DOWN,
}


@given(
    parsers.re(
        r"^(?P<opening>the \w+ operator on any engine|an operator the table does not cover)$"
    ),
    target_fixture="answered",
)
def _one_operator(opening: str) -> Comparing:
    """Asked of SQLite, because these scenarios are about the operator.

    The scenario that walks all forty-five pairs is what proves the other two
    engines answer, so picking one here costs nothing.
    """
    return how_to_compare(WHAT_EACH_OPENING_ASKS[opening], "sqlite")  # type: ignore[arg-type,return-value]


@then(
    parsers.re(
        r"^the answer (?:gives a symbol and says |says )"
        r"(?P<reading>one value goes beside it"
        r"|no value goes beside the symbol"
        r"|two values go beside the symbol, a floor and a ceiling)$"
    )
)
def _how_many(answered: Comparing, reading: str) -> None:
    wanted = HOW_MANY_VALUES[reading]
    assert answered["shape"] == wanted, (
        f"this comparison says {answered['shape']!r} values go beside its "
        f"symbol and the scenario says {wanted!r}. A renderer binding the wrong "
        f"count hands the driver more parameters than the text has places for, "
        f"or fewer, and the engine refuses the whole statement."
    )
    assert answered["symbol"], "the comparison has no symbol to write"


@then("the answer says the value is wrapped in a pattern")
def _wrapped(answered: Comparing) -> None:
    assert "{}" in answered["wraps"], (
        f"this comparison wraps its value in {answered['wraps']!r}, which has "
        f"nowhere for the value to go, so matching part of the text would "
        f"match the pattern itself"
    )


@given("any text operator on every engine", target_fixture="per_engine")
def _a_text_operator_everywhere() -> dict[str, Comparing]:
    return {
        engine: how_to_compare(A_TEXT_OPERATOR, engine)  # type: ignore[misc]
        for engine in EVERY_ENGINE
    }


@then("each engine says what it writes instead when case does not matter")
def _each_engine_on_case(per_engine: dict[str, Comparing]) -> None:
    silent = sorted(e for e, answer in per_engine.items() if not answer["ignoring_case"])
    assert not silent, (
        f"{silent} say nothing about ignoring case, so a caller asking for it "
        f"gets the case-sensitive word and no warning"
    )

    # AND THEY DO NOT ALL SAY THE SAME WORD, which is the whole reason this
    # column exists. PostgreSQL's LIKE is case-sensitive and ILIKE is not;
    # SQLite and Turso fold case in LIKE already. Asserting only that each says
    # something passes on every engine answering LIKE, which is the silent
    # mismatch `HOW-IT-SHOULD-WORK.md:143` records: the same question matched a
    # name on one engine and nothing on another.
    words = {answer["ignoring_case"] for answer in per_engine.values()}
    assert len(words) > 1, (
        f"every engine says {words} for ignoring case. At least one of them is "
        f"case-sensitive in that word, so a caller who asked to ignore case is "
        f"being handed a question that will not match"
    )


@when("all fifteen operators are asked of all three engines", target_fixture="said")
def _every_pair() -> dict[tuple[str, str], Comparing]:
    """Walked off `Operator` and `Engine`, so a missing row fails here."""
    return {
        (operator, engine): how_to_compare(operator, engine)  # type: ignore[misc]
        for operator in EVERY_OPERATOR
        for engine in EVERY_ENGINE
    }


@then("all forty-five pairs answer")
def _all_of_them(said: dict[tuple[str, str], Comparing]) -> None:
    assert len(said) == len(EVERY_OPERATOR) * len(EVERY_ENGINE) >= 45, (
        f"only {len(said)} pairs were asked, so an operator or an engine could "
        f"be added with no row and nothing here would say so"
    )
    refused = sorted(pair for pair, answer in said.items() if set(answer) != THE_FOUR_FIELDS)
    assert not refused, (
        f"{refused} came back as something other than a comparison, so the "
        f"renderer has no rule for them and would fall through to whatever was "
        f"nearest"
    )


@then("a fault comes back naming it and the engine")
def _refused_by_name(answered: Comparing) -> None:
    assert set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"an operator nobody wrote down was answered with {answered!r} rather "
        f"than refused, so it renders as whatever the lookup fell through to"
    )
    said = f"{answered['what_went_wrong']}{answered['what_to_do_instead']}"  # type: ignore[typeddict-item]
    assert AN_OPERATOR_NOBODY_WROTE_DOWN in said, f"the refusal does not name it: {said!r}"
    assert "sqlite" in said, f"the refusal does not name the engine: {said!r}"
