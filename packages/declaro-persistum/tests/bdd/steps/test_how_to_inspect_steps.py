"""Bind `tests/bdd/features/core/how_to_inspect.feature`, and nothing else.

WHAT THE FEATURE ASKS. Five questions per engine: its tables, its columns, its
indexes, its rules and its views.

THE LAST TWO SCENARIOS ARE THE ONES THAT MATTER. A table's name arriving as
characters inside a question is the hole `HOW-IT-SHOULD-WORK.md:208` exists to
close: every string is either a declared name or a bound value, and nothing is
pasted in. These questions are the one place in the package where the library
writes SQL about a name a caller supplied, so they are where that rule is
easiest to break and hardest to see.

WHY NOT ASSERT THE SQL ITSELF. A scenario naming `sqlite_master` would be this
file agreeing with the table beside it, and both would be wrong together the day
an engine's catalogue moves. What is asserted is the shape: five questions, text
and values apart, and a bound name where a name is needed.
"""

from __future__ import annotations

import re
import typing

from pytest_bdd import given, scenarios, then, when

from declaro_persistum.how_to_inspect import how_to_inspect
from declaro_persistum.types import Engine, Inspecting

scenarios("../features/core/how_to_inspect.feature")

EVERY_ENGINE = typing.get_args(Engine)
THE_FIVE = frozenset(typing.get_type_hints(Inspecting))

# A question asked once per table has to bind that table's name. Which of the
# five those are is the engine's business, so this finds them by the parameter
# rather than by naming them here.
_A_BOUND_NAME = re.compile(r":\w+")


@given("any engine", target_fixture="answered")
def _one_engine() -> Inspecting:
    """SQLite, because this scenario is about the shape every engine gives."""
    return how_to_inspect("sqlite")


@then(
    "the answer carries a question about its tables, its columns, its indexes, "
    "its rules and its views"
)
def _all_five(answered: Inspecting) -> None:
    missing = sorted(THE_FIVE - set(answered))
    assert not missing, (
        f"this engine is asked nothing about {missing}, so a schema read back "
        f"from it arrives incomplete and the next migration proposes adding "
        f"whatever went unseen"
    )


@when("every engine the package names is asked", target_fixture="said")
def _every_engine_is_asked() -> dict[str, Inspecting]:
    """Walked off `Engine` itself, so an engine with no row fails here."""
    return {engine: how_to_inspect(engine) for engine in EVERY_ENGINE}


@then("each one gives all five")
def _every_engine_gives_five(said: dict[str, Inspecting]) -> None:
    assert len(said) == len(EVERY_ENGINE) >= 3, (
        f"only {sorted(said)} were asked, so an engine could be added with no "
        f"row and nothing here would say so"
    )
    short = {engine: sorted(THE_FIVE - set(answer)) for engine, answer in said.items()}
    assert not any(short.values()), f"these engines are asked nothing about {short}"


@then("more than one set of five comes back")
def _not_all_alike(said: dict[str, Inspecting]) -> None:
    """Two engines sharing a row is right. All three sharing one is not."""
    written = {
        tuple(sorted((name, q["text"]) for name, q in answer.items())) for answer in said.values()
    }
    assert len(written) > 1, (
        "all three engines are asked the same five questions, so the table says "
        "nothing a single constant would not have said"
    )


@then("each question carries its text and its values apart")
def _text_and_values_apart(said: dict[str, Inspecting]) -> None:
    wrong = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if sorted(question) != ["parameters", "text"] or not question["text"]
    ]
    assert not wrong, (
        f"{wrong} are not statements, so the driver has nothing to bind and the "
        f"values would have to reach the text"
    )


@then("the questions asked per table bind the name rather than writing it in")
def _the_name_is_bound(said: dict[str, Inspecting]) -> None:
    pasted = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if question["parameters"] and not _A_BOUND_NAME.search(question["text"])
    ]
    assert not pasted, (
        f"{pasted} take a value and have nowhere to bind it, so a table's name "
        f"would be pasted into the question. `HOW-IT-SHOULD-WORK.md:208`: every "
        f"string is checked against a declaration or bound as a parameter, and "
        f"there is no third case."
    )

    asked_per_table = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if question["parameters"]
    ]
    assert asked_per_table, (
        "not one question on any engine takes a value, so either nothing is "
        "asked per table or a name is being written straight into the text"
    )
