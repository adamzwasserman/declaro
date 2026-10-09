"""Bind `tests/bdd/features/gates/every_field_carried_is_spelled.feature`.

WHAT THE FEATURE ASKS. Every field `fn what_an_operation_carries` says a change
carries is a field some engine's spelling uses. A change built from exactly that
row is never refused for want of a field. A new table's indexes are spelled. A
renamed column needs the new name and not the column definitions. The gate reads
the kinds off the declaration, and a run that spelled nothing fails.

THE SCENARIOS RUN THE GATE ITSELF, from
`tests/unit/test_every_field_carried_is_spelled.py`, rather than spelling
everything a second way. Two readers of one thing drift apart, and the one
nobody runs in anger is the one that goes wrong.
"""

from __future__ import annotations

import pathlib
import typing

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import types
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.what_an_operation_carries import what_an_operation_carries
from tests.conftest import a_column, an_operation
from tests.unit.test_every_field_carried_is_spelled import (
    HOW_EACH_KIND_SPELLS,
    WHAT_TO_LOOK_FOR,
    _everything_the_statements_say,
    _is_a_fault,
    _the_engines,
    _the_kinds,
)

scenarios("../features/gates/every_field_carried_is_spelled.feature")

_THE_GATE = pathlib.Path("tests/unit/test_every_field_carried_is_spelled.py")

# WHAT EACH SENTENCE BUILDS, one row per sentence, so the two Givens that build
# one change each are one function over a table rather than two functions
# differing by their words.
WHAT_EACH_GIVEN_BUILDS: dict[str, dict] = {
    "a create_table carrying two columns and one index": an_operation(
        kind="create_table",
        table="people",
        column=None,
        columns=[
            a_column(name="id", declared_type="integer", renamed_from=None, references=None),
            a_column(name="a", declared_type="text", renamed_from=None, references=None),
        ],
        indexes=[{"name": "idx_a", "columns": ["a"], "unique": False}],
        constraints=[],
        copying=[],
        values=[],
        renamed_to=None,
        view=None,
        destroys=False,
    ),
    "a rename_column carrying the new name and no column definitions": an_operation(
        kind="rename_column",
        table="people",
        column="a",
        columns=[],
        indexes=[],
        constraints=[],
        copying=[],
        values=[],
        renamed_to="b",
        view=None,
        destroys=False,
    ),
}


@pytest.fixture
def scenario() -> dict:
    return {"built": None, "spelled": {}}


@given(parsers.re(r"^one change of every kind, [^\n]+$"))
@when("each one is spelled for each engine")
def _every_kind_was_built_and_spelled(scenario: dict) -> None:
    """Building and spelling are one act here, and the gate did it at import.

    FOUR SENTENCES REACH THIS AND THEY ARE ONE SENTENCE. Two Givens say "with
    every field the table claims filled" and "carrying only the fields the table
    claims", which is the same change: one built from the row carries what the
    row claims and nothing else. The When that follows either of them asks for
    the spelling, and the spelling already happened when the gate loaded, because
    building and spelling every kind on every engine is the gate's own work and
    doing it twice would be two readers of one thing.
    """
    scenario["spelled"] = HOW_EACH_KIND_SPELLS


_ONE_CHANGE = (
    r"^(?P<sentence>a create_table carrying two columns and one index"
    r"|a rename_column carrying the new name and no column definitions)$"
)


@given(parsers.re(_ONE_CHANGE))
def _one_change_was_built(scenario: dict, sentence: str) -> None:
    scenario["built"] = WHAT_EACH_GIVEN_BUILDS[sentence]


@when("it is spelled for each engine")
def _the_one_was_spelled(scenario: dict) -> None:
    scenario["spelled"] = {
        engine: spell_operation(scenario["built"], engine) for engine in _the_engines()
    }


@when("every kind in the declaration is read")
def _the_kinds_were_read(scenario: dict) -> None:
    scenario["kinds"] = _the_kinds()
    scenario["spelled"] = HOW_EACH_KIND_SPELLS


@when("the gate looks for the kinds of change")
def _the_gate_looked(scenario: dict) -> None:
    scenario["kinds"] = _the_kinds()
    scenario["said"] = _THE_GATE.read_text(encoding="utf-8")


@when("the gate runs and spells no statement at all")
def _nothing_was_spelled(scenario: dict) -> None:
    scenario["spelled"] = {}
    scenario["said"] = _THE_GATE.read_text(encoding="utf-8")


@then("every claimed field shows up in the statements or their values, on some engine")
def _every_claimed_field_is_spelled(scenario: dict) -> None:
    unspelled = {
        kind: sorted(
            field
            for field, claimed in what_an_operation_carries(kind).items()
            if claimed
            if not any(
                WHAT_TO_LOOK_FOR[field] in _everything_the_statements_say(spelled)
                for (said, _engine), spelled in scenario["spelled"].items()
                if said == kind
                if not _is_a_fault(spelled)
            )
        )
        for kind in _the_kinds()
    }
    guilty = {kind: fields for kind, fields in unspelled.items() if fields}

    assert guilty == {}, (
        f"these changes carry a field no engine spells, so what the change says "
        f"it will do and what it does are two different things: {guilty}"
    )


@then("none of them is refused for want of a field")
def _none_was_refused(scenario: dict) -> None:
    refused = {
        f"{kind} on {engine}": spelled["what_went_wrong"]
        for (kind, engine), spelled in scenario["spelled"].items()
        if _is_a_fault(spelled)
    }

    assert refused == {}, (
        f"these were built from exactly what the table says they carry and the "
        f"writer refused them, so the row denies a field the writer needs: {refused}"
    )


@then("one statement makes the table and another makes the index")
def _the_table_and_its_index(scenario: dict) -> None:
    for engine, spelled in scenario["spelled"].items():
        assert not _is_a_fault(spelled), f"{engine} refused a new table: {spelled}"
        assert len(spelled) == 2, (
            f"{engine} spelled a table carrying one index as {len(spelled)} "
            f"statements, and an index cannot go inside a CREATE TABLE"
        )
        assert "CREATE TABLE" in spelled[0]["text"], spelled[0]["text"]
        assert "INDEX" in spelled[1]["text"], spelled[1]["text"]


@then("the index is named in the statement that makes it")
def _the_index_is_named(scenario: dict) -> None:
    for engine, spelled in scenario["spelled"].items():
        assert "idx_a" in spelled[1]["text"], (
            f"{engine} made an index and did not name it: {spelled[1]['text']}"
        )


@then("it is spelled, and the statement names both the old and the new name")
def _the_rename_names_both(scenario: dict) -> None:
    for engine, spelled in scenario["spelled"].items():
        assert not _is_a_fault(spelled), (
            f"{engine} refused a rename carrying the new name: {spelled}. The row "
            f"in `fn what_an_operation_carries` denied `renamed_to` until 2026-10-07."
        )
        said = " ".join(one["text"] for one in spelled)
        assert '"a"' in said and '"b"' in said, (
            f"{engine} spelled a rename naming one of the two names: {said}"
        )


@then("the table answers for each one")
def _the_table_answers(scenario: dict) -> None:
    missing = [kind for kind in scenario["kinds"] if not _whether_the_table_answers(kind)]

    assert missing == [], f"the declaration names {missing} and the table has no row for them"


def _whether_the_table_answers(kind: str) -> bool:
    try:
        return what_an_operation_carries(kind) is not None  # type: ignore[arg-type]
    except KeyError:
        return False


@then("the writer answers for each one on each engine")
def _the_writer_answers(scenario: dict) -> None:
    wanted = len(scenario["kinds"]) * len(_the_engines())

    assert len(scenario["spelled"]) == wanted, (
        f"the gate spelled {len(scenario['spelled'])} of {wanted} pairings, so "
        f"something was skipped rather than checked"
    )


@then("it reads `set operation_kinds` in docs/design/persistum.hd")
def _it_reads_the_declaration(scenario: dict) -> None:
    assert "set operation_kinds" in scenario["said"], (
        "the gate does not read `set operation_kinds`, so the kinds it checks "
        "come from somewhere other than the declaration"
    )
    assert set(scenario["kinds"]) == set(typing.get_args(types.OperationKind)), (
        f"the declaration names {sorted(scenario['kinds'])} and the code's "
        f"`OperationKind` admits {sorted(typing.get_args(types.OperationKind))}"
    )


@then("a kind added there is checked without anybody editing the gate")
def _a_new_kind_is_checked(scenario: dict) -> None:
    """A list of kinds inside the gate is the defect the gate catches, one level up."""
    listed = sorted(one for one in scenario["kinds"] if f'"{one}"' in scenario["said"])

    assert listed == [], (
        f"the gate names these kinds in its own source, so a kind added to the "
        f"declaration and not to the gate goes unchecked: {listed}"
    )


@then("it fails, because a gate that read nothing agrees with everything")
def _spelling_nothing_is_a_failure(scenario: dict) -> None:
    assert scenario["spelled"] == {}, scenario["spelled"]
    counting = [one for one in scenario["said"].splitlines() if "assert written >" in one]

    assert counting, (
        "nothing in the gate asserts how many statements it spelled, so a broken "
        "build would report no fault and pass"
    )
