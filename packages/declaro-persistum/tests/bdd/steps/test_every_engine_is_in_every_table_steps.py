"""Bind `tests/bdd/features/gates/every_engine_is_in_every_table.feature`.

WHAT THE FEATURE ASKS. Every table keyed by engine names the same engines. A table
short an engine is named along with the engine it is missing, and a table naming a
word the declaration never declared is named along with the word. The gate finds
the tables by their shape, a run that found none fails, and a lookup taking a
fallback is refused.

THE SCENARIOS RUN THE GATE ITSELF, from
`tests/unit/test_every_engine_is_in_every_table.py`, rather than reading the same
source a second way. Two readers of one thing drift apart, and the one nobody runs
in anger is the one that goes wrong.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from tests.unit.test_every_engine_is_in_every_table import (
    ENGINES,
    NAMES_OF_THE_TABLES,
    WHAT_EACH_TABLE_IS_KEYED_BY,
    _every_lookup_with_a_fallback,
    _the_tables_in,
)

scenarios("../features/gates/every_engine_is_in_every_table.feature")

_THE_GATE = pathlib.Path("tests/unit/test_every_engine_is_in_every_table.py")

# WHAT EACH SENTENCE PLANTS, written the way the sentence writes it, so what the
# gate reads is what the feature names.
WHAT_EACH_GIVEN_PLANTS: dict[str, str] = {
    "a table carrying postgresql and sqlite but not turso": (
        'WHAT_EACH_ONE_DOES = {"postgresql": 1, "sqlite": 2}\n'
    ),
    "a table carrying an engine no other table knows": (
        'WHAT_EACH_ONE_DOES = {"postgresql": 1, "sqlite": 2, "turso": 3, "duckdb": 4}\n'
    ),
    "a dispatch table read with `.get(key, something)`": (
        'WHAT_EACH_ONE_DOES = {"postgresql": 1, "sqlite": 2, "turso": 3}\n'
        "def a(engine):\n"
        "    return WHAT_EACH_ONE_DOES.get(engine, 0)\n"
    ),
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$"


@pytest.fixture
def scenario() -> dict:
    return {"planted": ast.parse("")}


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_PLANTS, key=len, reverse=True)))))
def _what_the_sentence_plants(scenario: dict, sentence: str) -> None:
    scenario["planted"] = ast.parse(WHAT_EACH_GIVEN_PLANTS[sentence])


@when("every dispatch table keyed by engine is read")
def _every_real_table(scenario: dict) -> None:
    scenario["found"] = WHAT_EACH_TABLE_IS_KEYED_BY


@when("the gate looks for dispatch tables")
def _the_gate_looks(scenario: dict) -> None:
    scenario["found"] = WHAT_EACH_TABLE_IS_KEYED_BY
    scenario["said"] = _THE_GATE.read_text(encoding="utf-8")


@when("the gate runs")
def _the_gate_reads_the_plant(scenario: dict) -> None:
    scenario["short"] = _the_tables_in("planted.py", scenario["planted"], "keyed by engine")
    scenario["stray"] = _the_tables_in(
        "planted.py", scenario["planted"], "naming an engine and something else"
    )
    scenario["falling_back"] = _every_lookup_with_a_fallback(scenario["planted"])


@when("the gate runs and finds no dispatch table at all")
def _the_gate_reads_a_file_with_no_table(scenario: dict) -> None:
    scenario["found"] = _the_tables_in(
        "nothing.py", ast.parse("WHAT_THIS_HOLDS = {}\n"), "keyed by engine"
    )
    scenario["said"] = _THE_GATE.read_text(encoding="utf-8")


@then("all of them carry the same set of keys")
def _they_all_agree(scenario: dict) -> None:
    disagreeing = {
        name: sorted(keys) for name, keys in scenario["found"].items() if keys != ENGINES
    }

    assert disagreeing == {}, (
        f"these tables are keyed by engine and name a different set of engines "
        f"than {sorted(ENGINES)}: {disagreeing}"
    )


@then("it fails and names the table and the missing engine")
def _the_short_table_is_named(scenario: dict) -> None:
    missing = {name: sorted(ENGINES - keys) for name, keys in scenario["short"].items()}

    assert missing, "a table carrying two of the three engines was read as complete"
    assert list(missing.values()) == [["turso"]], (
        f"the table has no row for turso and the gate says it is missing {missing}"
    )
    assert all("WHAT_EACH_ONE_DOES" in name for name in missing), (
        f"the gate found the fault and did not name the table: {sorted(missing)}"
    )


@then("it fails and names both")
def _the_stray_word_is_named(scenario: dict) -> None:
    """A word in one table and no other is a word the declaration never said."""
    assert scenario["stray"], (
        "a table naming duckdb beside the three engines was read as an ordinary "
        "engine table, so a stray word goes unnamed"
    )
    for name, keys in scenario["stray"].items():
        assert "WHAT_EACH_ONE_DOES" in name, f"the gate did not name the table: {name}"
        assert "duckdb" in keys, f"the gate did not name the stray word: {sorted(keys)}"


@then("it fails, because a default re-opens the space the table closed")
def _the_fallback_is_named(scenario: dict) -> None:
    assert [named for _line, named in scenario["falling_back"]] == ["WHAT_EACH_ONE_DOES"], (
        f"a table read with `.get(key, 0)` was read as {scenario['falling_back']}"
    )
    # THE PLANTED MODULE IS NOW THE ONLY POSITIVE CONTROL, AND IT IS ENOUGH. The
    # line above hands the scanner a module written to contain one fallback and
    # asserts it reads exactly that one, so a scanner that reads nothing fails
    # here. A second assertion used to demand that the live package contain a
    # fallback too, on the ground that several did and so an empty answer meant a
    # broken scan. On 2026-11-16 the last six were removed, in engine_of,
    # lifecycle_register, how_to_spell, how_to_compare and schema_read_declared,
    # and that assertion then failed for the one reason it was never meant to
    # catch: the package is clean. A liveness check whose premise is a defect
    # cannot survive the defect being fixed, so the plant carries it instead.


@then("it finds them by shape rather than from a list somebody maintains")
def _it_found_them_by_shape(scenario: dict) -> None:
    """A list of table names is the defect this gate catches, one level up."""
    listed = sorted(
        one for one in NAMES_OF_THE_TABLES if re.search(rf"\b{re.escape(one)}\b", scenario["said"])
    )

    assert listed == [], (
        f"the gate names these tables in its own source, so a table nobody added "
        f"to the list goes unchecked: {listed}"
    )
    assert len(scenario["found"]) > 15, (
        f"the gate found {len(scenario['found'])} tables by shape, which is too few for the package"
    )


@then("it fails, because a gate that read nothing agrees with everything")
def _reading_nothing_is_a_failure(scenario: dict) -> None:
    assert scenario["found"] == {}, (
        f"a file holding one empty dict was read as holding a table: {scenario['found']}"
    )
    counting = [one for one in scenario["said"].splitlines() if "assert found >" in one]

    assert counting, (
        "nothing in the gate asserts how many tables it found, so a broken glob "
        "would report no fault and pass"
    )
