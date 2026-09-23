"""Bind `tests/bdd/features/core/how_to_write.feature`, and nothing else.

WHAT THE FEATURE ASKS. Three facts per engine: the statement that opens a
transaction, how the values go in, and whether there is a commit to call.

THE SCENARIOS READ THE ANSWER, NOT THE TABLE. A scenario asserting the dict
would pass against a table that says the right thing and a caller that never
reads it. What is asserted here is the answer to one question per engine.

WHY THE LAST SCENARIO ASKS THAT NO TWO MATCH. A table whose rows are all the
same is a table nobody needed, and it would pass every scenario above by
accident if one engine's row were copied over another's.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.types import Engine, Transacting

scenarios("../features/core/how_to_write.feature")

EVERY_ENGINE = typing.get_args(Engine)


@given(parsers.re(r"^(?P<engine>sqlite|postgresql|turso)$"), target_fixture="answered")
def _one_engine(engine: str) -> Transacting:
    return how_to_write(engine)  # type: ignore[arg-type]


# ONE FUNCTION OVER A TABLE. The three readings differ in the statement the
# engine opens a write with and in nothing else, so the statement is the row and
# the assertion is written once. A fourth engine's opening is a fourth row.
WHAT_EACH_OPENING_IS = {
    "a concurrent begin": "BEGIN CONCURRENT",
    "an immediate begin": "BEGIN IMMEDIATE",
    "no statement at all": "",
}

WHAT_THE_WRONG_ONE_COSTS = {
    "BEGIN CONCURRENT": (
        "without a concurrent begin the drainers queue behind one writer and "
        "the crew buys nothing, which `architecture.md:255` calls mandatory"
    ),
    "BEGIN IMMEDIATE": (
        "a deferred transaction takes the write lock at its first write, so a "
        "busy database fails part way through instead of at the start"
    ),
    "": (
        "this driver owns the transaction, so the statement would be run inside "
        "one that is already open"
    ),
}


@then(
    parsers.re(
        r"^the answer opens with (?P<opening>a concurrent begin|an immediate begin|no statement at all)$"
    )
)
def _the_opening(answered: Transacting, opening: str) -> None:
    wanted = WHAT_EACH_OPENING_IS[opening]
    assert answered["begin"] == wanted, (
        f"this engine opens a write with {answered['begin']!r} and the scenario "
        f"says {wanted!r}: {WHAT_THE_WRONG_ONE_COSTS[wanted]}"
    )


@then("it says there is no commit to call")
def _no_commit(answered: Transacting) -> None:
    assert answered["commits"] is False, (
        "this engine says to call a commit, and its driver commits on the way "
        "out of the block instead, so the call would find nothing to commit"
    )


@when("every engine the package names is asked", target_fixture="said")
def _every_engine_is_asked() -> dict[str, Transacting]:
    """Walked off `Engine` itself, so an engine added with no row fails here."""
    return {engine: how_to_write(engine) for engine in EVERY_ENGINE}


@then("each one answers, and no two engines get the same answer")
def _all_answered_and_differ(said: dict[str, Transacting]) -> None:
    assert len(said) == len(EVERY_ENGINE) >= 3, (
        f"only {sorted(said)} were asked, so an engine could be added with no "
        f"row and nothing here would say so"
    )
    shapes = {engine: tuple(sorted(answer.items())) for engine, answer in said.items()}
    doubled = {
        one: two for one in shapes for two in shapes if one < two and shapes[one] == shapes[two]
    }
    assert not doubled, (
        f"{doubled} were given the same answer, so one engine's row was copied "
        f"over another's and every scenario above would still pass"
    )
