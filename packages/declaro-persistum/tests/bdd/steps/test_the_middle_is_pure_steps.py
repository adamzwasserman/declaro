"""Bind `tests/bdd/features/gates/the_middle_is_pure.feature`.

WHAT THE FEATURE ASKS. Every function the declaration calls pure is pure in the
code: no clock, no random source, no environment, nothing opened. A function
declared to reach outside is not reported. The gate can see what it checks, and
it finds a planted impurity of every kind it names.

THE SCENARIOS RUN THE GATE ITSELF, from `tests/unit/test_the_middle_is_pure.py`,
rather than a second reading of the same source. Two readers of one thing drift,
and the one nobody runs in anger is the one that goes wrong.
"""

from __future__ import annotations

import pathlib
import re
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from tests.unit.test_the_middle_is_pure import (
    ROLES_THAT_MAY,
    WHAT_REACHES_OUTSIDE,
    _every_pure_module,
    _how_one_module_reaches_outside,
    _the_declaration,
    _the_pure_cards,
)

scenarios("../features/gates/the_middle_is_pure.feature")


# WHAT A PLANTED MODULE HOLDS: one function per way out that a single call can
# show. `execute` and `run` need a live object to be called on, so a plant for
# them would be a plant of the driver rather than of an impurity.
WHAT_ONE_FILE_CAN_PLANT = (
    "import os, random, time, uuid, sqlite3, socket\n"
    "def reads_the_clock(): return time.time()\n"
    "def reads_a_random_source(): return random.randint(1, 2)\n"
    "def reads_another(): return uuid.uuid4()\n"
    "def reads_the_environment(): return os.environ['X']\n"
    "def opens_a_file(): return open('/tmp/x')\n"
    "def opens_a_connection(): return sqlite3.connect(':memory:')\n"
    "def opens_a_socket(): return socket.socket()\n"
)

WHAT_A_SINGLE_CALL_CANNOT_SHOW = frozenset(
    {"runs a statement on a connection", "reads rows off a connection", "starts a process"}
)


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path, "planted": None}


@given("one planted pure function for each impurity the gate names")
def _one_planted_of_each(scenario: dict) -> None:
    planted = scenario["tmp"] / "planted.py"
    planted.write_text(WHAT_ONE_FILE_CAN_PLANT, encoding="utf-8")
    scenario["planted"] = planted


@when("the gate reads the declaration and every module it names")
def _the_gate_reads_the_source(scenario: dict) -> None:
    scenario["found"] = {
        name: _how_one_module_reaches_outside(path) for name, path in _every_pure_module().items()
    }


@when("the gate runs over them")
def _the_gate_reads_the_plant(scenario: dict) -> None:
    scenario["found"] = {
        "planted": _how_one_module_reaches_outside(scenario["planted"]),
    }


def _reaching(scenario: dict, ways: frozenset[str]) -> dict[str, Any]:
    return {
        name: [(where, what) for where, what in found if what in ways]
        for name, found in scenario["found"].items()
        if [what for _where, what in found if what in ways]
    }


# WHAT EACH SENTENCE ASKS ABOUT, and what it means when the gate finds one.
# Two sentences, two sets of ways out, one function reading both. Written as two
# bodies they were one function differing by a name and a string.
WHAT_EACH_SENTENCE_LOOKS_FOR: dict[str, tuple[frozenset[str], str]] = {
    "no plain `fn` reads a clock, a random source or the environment": (
        frozenset({"reads the clock", "reads a random source", "reads the environment"}),
        "their answer can change between two identical calls",
    ),
    "no plain `fn` opens a file, a connection or a socket": (
        frozenset({"opens a file", "opens a connection", "opens a socket", "starts a process"}),
        "they open something, and a pure function holds nothing",
    ),
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$"


@then(parsers.re(_one_of(tuple(sorted(WHAT_EACH_SENTENCE_LOOKS_FOR, key=len, reverse=True)))))
def _nothing_pure_reaches_that_way(scenario: dict, sentence: str) -> None:
    ways, why = WHAT_EACH_SENTENCE_LOOKS_FOR[sentence]
    reaching = _reaching(scenario, ways)

    assert reaching == {}, f"{sorted(reaching)} are declared pure and {why}: {reaching}"


@then("no plain `fn` imports a function the declaration marks as a boundary")
def _nothing_reaches_a_boundary(scenario: dict) -> None:
    """A pure function that called a door would reach the world through it, and
    the gate reading calls in that module would never see it."""
    doors = {name for name, role in _the_declaration().items() if role in ROLES_THAT_MAY}
    reaching = {
        name: sorted(named)
        for name, path in _every_pure_module().items()
        for named in [{one for one in doors if f"import {one}\n" in path.read_text()}]
        if named
    }

    assert reaching == {}, (
        f"these pure functions import a boundary, so they reach the world through it: {reaching}"
    )


@then("a function declared `boundary_in`, `boundary_out` or `reader` is not reported")
def _a_boundary_is_left_alone(scenario: dict) -> None:
    """A gate that reported every door would be turned off within the day."""
    doors = {name for name, role in _the_declaration().items() if role in ROLES_THAT_MAY}

    assert doors, "no card carries a role, so the declaration was not read"
    assert set(_every_pure_module()) & doors == set(), (
        f"{sorted(set(_every_pure_module()) & doors)} are declared to reach "
        f"outside and the gate is checking them anyway"
    )


@then("it finds every one of them")
def _it_found_every_plant(scenario: dict) -> None:
    found = {what for _where, what in scenario["found"]["planted"]}
    wanted = set(WHAT_REACHES_OUTSIDE.values()) - WHAT_A_SINGLE_CALL_CANNOT_SHOW

    assert found == wanted, (
        f"the gate claims to catch {sorted(wanted)} and found {sorted(found)} in "
        f"a file holding one of each. A gate that reports zero offenders and a "
        f"gate that finds nothing read the same from outside."
    )


@then("it found a module for every plain `fn` the declaration declares")
def _every_pure_card_has_a_module(scenario: dict) -> None:
    missing = sorted(set(_the_pure_cards()) - set(_every_pure_module()))

    assert missing == [], (
        f"{missing} are declared pure and have no module of that name, so nothing checked them"
    )


@then("that count is more than forty")
def _the_count_is_real(scenario: dict) -> None:
    """A discovery bug would make every assertion above pass by reading nothing."""
    assert len(scenario["found"]) > 35, (
        f"the gate read {len(scenario['found'])} modules, which is too few for "
        f"the assertions above to mean anything"
    )
