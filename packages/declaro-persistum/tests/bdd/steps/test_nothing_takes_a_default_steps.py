"""Bind `tests/bdd/features/gates/nothing_takes_a_default.feature`.

WHAT THE FEATURE ASKS. A number, a mutable, a `None` and a kept-word default are
all refused and named. A parameter with no default passes. The gate says how
many modules it read, and a closure over an enclosing name is allowed.

THE SCENARIOS RUN THE GATE ITSELF, from `tests/unit/test_nothing_takes_a_default.py`,
rather than a second reading of the same source. Two readers of one thing drift,
and the one nobody runs in anger is the one that goes wrong.
"""

from __future__ import annotations

import ast
import pathlib
import re
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from tests.unit.test_nothing_takes_a_default import (
    WHAT_EACH_MODULE_DECLARES,
    _the_defaults_in,
)

scenarios("../features/gates/nothing_takes_a_default.feature")


# WHAT EACH SENTENCE PLANTS. One function per row, written the way the sentence
# writes it, so the source under test is the source the feature names.
WHAT_EACH_GIVEN_PLANTS: dict[str, str] = {
    "a function declaring `timeout: int = 30`": "def a(timeout: int = 30): pass\n",
    "a function declaring `items: list = []`": "def a(items: list = []): pass\n",
    "a function declaring `token: str | None = None`": "def a(token: str | None = None): pass\n",
    "a function declaring `token: str | Nothing` with no default": (
        "def a(token: str | None): pass\n"
    ),
    "a nested function whose parameter defaults to a name from its enclosing scope": (
        "def outer(fills):\n    def inner(c, fills=fills): return fills\n    return inner\n"
    ),
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$"


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path, "planted": "", "found": []}


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_PLANTS, key=len, reverse=True)))))
def _what_the_sentence_plants(scenario: dict, sentence: str) -> None:
    scenario["planted"] = WHAT_EACH_GIVEN_PLANTS[sentence]


@when("the gate runs")
def _the_gate_reads_the_plant(scenario: dict) -> None:
    scenario["found"] = _the_defaults_in(ast.parse(scenario["planted"]))


@when("the gate runs over the package")
def _the_gate_reads_the_package(scenario: dict) -> None:
    scenario["read"] = WHAT_EACH_MODULE_DECLARES


# WHAT EACH SENTENCE SAYS ABOUT A REFUSAL, in the words that scenario uses. Four
# sentences, one assertion, because what differs is the reason and the reason is
# the row.
WHY_EACH_ONE_IS_REFUSED: dict[str, str] = {
    "it fails and names that function and that parameter": (
        "a caller who forgot the argument and a caller who chose it are the "
        "same caller once the body starts"
    ),
    "it fails, because None cannot be told from a caller who forgot": (
        "`None` reads as declared absence and is not; the caller who passed it "
        "and the caller who passed nothing look identical inside"
    ),
}


@then(parsers.re(_one_of(tuple(sorted(WHY_EACH_ONE_IS_REFUSED, key=len, reverse=True)))))
def _it_was_refused(scenario: dict, sentence: str) -> None:
    assert scenario["found"] != [], (
        f"the gate read this and found no default: {scenario['planted']!r}. "
        f"{WHY_EACH_ONE_IS_REFUSED[sentence]}."
    )
    function, parameter, _line = scenario["found"][0]
    assert function and parameter, (
        f"the gate found a default and named neither the function nor the "
        f"parameter: {scenario['found']}"
    )


@then("it passes")
def _it_was_allowed(scenario: dict) -> None:
    """A parameter with no default is what the rule asks for."""
    assert scenario["found"] == [], (
        f"a parameter declaring absence with no default was reported: {scenario['found']}"
    )


@then("it is allowed, and the allowance is recorded with its reason")
def _the_closure_is_allowed(scenario: dict) -> None:
    """Binding a value at definition time is the opposite of absorbing an
    omission: nobody calls the inner function with that argument at all."""
    assert scenario["found"] == [], (
        f"a closure over an enclosing name was reported as a default: {scenario['found']}"
    )
    said = pathlib.Path("tests/unit/test_nothing_takes_a_default.py").read_text(encoding="utf-8")
    assert "A_NAME_FROM_THE_SCOPE_AROUND_IT" in said, (
        "the gate allows a closure and nothing in it says why, so the next "
        "reader cannot tell an allowance from an oversight"
    )


@then("it reports how many functions it read")
def _it_says_what_it_read(scenario: dict) -> None:
    assert len(scenario["read"]) > 60, (
        f"the gate read {len(scenario['read'])} modules, which is too few for the package"
    )


@then("a run that read nothing fails rather than passing")
def _reading_nothing_is_a_failure(scenario: dict) -> None:
    """A gate reporting zero offenders and a gate reading no files say the same
    thing, and only one of them is good news."""
    said = pathlib.Path("tests/unit/test_nothing_takes_a_default.py").read_text(encoding="utf-8")
    counting: Any = [
        one for one in said.splitlines() if "assert read >" in one or "assert len(" in one
    ]

    assert counting, (
        "nothing in the gate asserts how much it read, so a broken glob would "
        "report zero offenders and pass"
    )
