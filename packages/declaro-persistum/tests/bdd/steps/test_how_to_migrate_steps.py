"""Bind `tests/bdd/features/core/how_to_migrate.feature`, and nothing else.

WHAT THE FEATURE ASKS. One word per engine, saying whether a failed migration
rolls all the way back or leaves the operations before it standing.

WHY THE FOURTH SCENARIO WALKS THE WHOLE TABLE. Three named scenarios say what
three engines answer, and a fourth engine added tomorrow would satisfy all
three by not being mentioned. Walking `Engine` itself is what makes a missing
row fail here rather than at a migration.

THE TWO WORDS COME FROM THE DECLARATION, not from this file. `MigrationMode` is
a closed vocabulary, so a fourth word could not be asserted here without being
declared first.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_migrate import how_to_migrate
from declaro_persistum.types import Engine, MigrationMode

scenarios("../features/core/how_to_migrate.feature")


EVERY_ENGINE = typing.get_args(Engine)
THE_TWO_WORDS = frozenset(typing.get_args(MigrationMode))


@given(parsers.re(r"^(?P<engine>sqlite|postgresql|turso)$"), target_fixture="answered")
def _one_engine(engine: str) -> str:
    return how_to_migrate(engine)  # type: ignore[arg-type]


# ONE FUNCTION OVER A TABLE. The two readings differ in the word the engine has
# to answer with and in nothing else. Written as two functions, the same
# assertion sits in the file twice and a third reading would be a third copy.
# A word the table does not hold raises a KeyError naming itself.
WHAT_EACH_READING_MEANS = {
    "rolls the whole migration back": "all_or_nothing",
    "leaves the operations before it standing": "per_operation",
}

WHY_IT_MATTERS = {
    "all_or_nothing": (
        "a migration that fell over halfway is being recovered from the wrong "
        "place: everything before the failure would be re-run against a "
        "database that already has it"
    ),
    "per_operation": (
        "a recovery would start over from the top and re-run operations that already landed"
    ),
}


@then(
    parsers.re(
        r"^a failure (?P<reading>rolls the whole migration back|leaves the operations before it standing)$"
    )
)
def _a_failure(answered: str, reading: str) -> None:
    wanted = WHAT_EACH_READING_MEANS[reading]
    assert answered == wanted, (
        f"this engine answered {answered!r} and the scenario says {wanted!r}, "
        f"so {WHY_IT_MATTERS[wanted]}"
    )


@when("every engine the package names is asked", target_fixture="said")
def _every_engine_is_asked() -> dict[str, str]:
    """Walked off `Engine` itself, so an engine added with no row fails here."""
    return {engine: how_to_migrate(engine) for engine in EVERY_ENGINE}


@then("each one answers with one of the two words and nothing else")
def _every_engine_answers(said: dict[str, str]) -> None:
    assert len(said) == len(EVERY_ENGINE) >= 3, (
        f"only {sorted(said)} were asked, so an engine could be added with no "
        f"row and nothing here would say so"
    )
    unknown = {engine: word for engine, word in said.items() if word not in THE_TWO_WORDS}
    assert not unknown, (
        f"{unknown} answered with a word the declaration does not admit, so a "
        f"caller reading it has no rule to apply"
    )
