"""Bind `tests/bdd/features/core/what_an_operation_carries.feature`, and nothing else.

WHAT THE FEATURE ASKS. Which of the seven named fields each of the thirteen
kinds of schema change fills.

WHY THIS FUNCTION EXISTS AT ALL. An `Operation` carried a `detail` bag of
anything until 2026-09-23, so an `alter_column` holding a list of enum values
was expressible and meant nothing. Every field is named now and this says which
ones a kind uses, which is what lets an operation be checked against its kind.

THE LAST SCENARIO IS THE ONE THAT BITES. Six scenarios say what six kinds carry,
and a seventh kind added tomorrow satisfies all six by not being mentioned.
Walking `OperationKind` itself is what makes a missing row fail here rather than
at a migration, and asking that no two kinds fill the same set is what catches a
row copied from the one above it.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.types import Carrying, OperationKind
from declaro_persistum.what_an_operation_carries import what_an_operation_carries

scenarios("../features/core/what_an_operation_carries.feature")

EVERY_KIND = typing.get_args(OperationKind)
THE_SEVEN_FIELDS = frozenset(typing.get_type_hints(Carrying))


def _filled(answer: Carrying) -> set[str]:
    return {field for field, carried in answer.items() if carried}


@given(parsers.re(r"^(?P<kind>[a-z_]+)$"), target_fixture="answered")
def _one_kind(kind: str) -> Carrying:
    return what_an_operation_carries(kind)  # type: ignore[arg-type]


@then("it carries columns, constraints and indexes")
def _the_whole_table(answered: Carrying) -> None:
    assert _filled(answered) >= {"columns", "constraints", "indexes"}, (
        f"creating a table fills {sorted(_filled(answered))}, so a table would "
        f"arrive without its rules or its indexes and the next migration would "
        f"propose adding them"
    )


@then("it carries no values and no new name")
def _and_nothing_else(answered: Carrying) -> None:
    assert not _filled(answered) & {"values", "renamed_to"}, (
        f"creating a table fills {sorted(_filled(answered))}, which admits an "
        f"operation carrying enum values or a rename nobody asked for"
    )


@then("it carries none of the fields")
def _nothing_at_all(answered: Carrying) -> None:
    assert not _filled(answered), (
        f"this kind fills {sorted(_filled(answered))} and needs only the name "
        f"it is given, so anything else in it is a field nobody reads"
    )


# ONE FUNCTION OVER A TABLE. Three readings, each naming what its kind fills, so
# what is named is the row. `it carries values and nothing else` joined them
# rather than being a fourth copy of the same assertion.
#
# THE RENAME NAMES THREE FIELDS SINCE 2026-10-12. A table rename on a database
# with a primary is a create under the new name, a copy and a drop, because
# `ALTER TABLE ... RENAME TO` reaches no primary, so the create reads the columns
# and the rules.
WHAT_A_SENTENCE_NAMES: dict[str, set[str]] = {
    "a new name, columns and constraints": {"renamed_to", "columns", "constraints"},
    "a view": {"view"},
    "values": {"values"},
}

WHAT_IS_LOST_WITHOUT_IT = {
    "a new name, columns and constraints": (
        "a rename on a replicated database has nothing to build the table from"
    ),
    "a view": "the view itself has nowhere to travel",
    "values": "the values have nowhere to travel",
}

_WHAT_A_KIND_MAY_NAME = parsers.re(
    r"^it carries (?P<named>a new name, columns and constraints|a view|values),?"
    r" and nothing else$"
)


@then(_WHAT_A_KIND_MAY_NAME)
def _only_those_fields(answered: Carrying, named: str) -> None:
    wanted = WHAT_A_SENTENCE_NAMES[named]
    assert _filled(answered) == wanted, (
        f"this kind fills {sorted(_filled(answered))} and the scenario says "
        f"{sorted(wanted)}, so {WHAT_IS_LOST_WITHOUT_IT[named]}"
    )


@when("all thirteen kinds are asked", target_fixture="said")
def _every_kind_is_asked() -> dict[str, Carrying]:
    """Walked off `OperationKind` itself, so a kind with no row fails here."""
    return {kind: what_an_operation_carries(kind) for kind in EVERY_KIND}


@then("each one answers about all seven fields")
def _all_answered(said: dict[str, Carrying]) -> None:
    assert len(said) == len(EVERY_KIND) >= 13, (
        f"only {sorted(said)} were asked, so a kind could be added with no row "
        f"and nothing here would say so"
    )
    wrong_shape = {
        kind: sorted(set(answer) ^ THE_SEVEN_FIELDS)
        for kind, answer in said.items()
        if set(answer) != THE_SEVEN_FIELDS
    }
    assert not wrong_shape, (
        f"{wrong_shape} answered about a different set of fields than `Carrying` "
        f"declares, so a caller asking about one of them reads nothing"
    )


@then("more than one set of fields comes back")
def _they_are_not_all_the_same(said: dict[str, Carrying]) -> None:
    """A table whose rows are all alike is a table nobody needed.

    Kinds sharing a set is right: dropping a table and dropping a view both
    carry nothing. What this refuses is every row being a copy of the first,
    which would satisfy the six named scenarios by accident.
    """
    sets = {frozenset(_filled(answer)) for answer in said.values()}
    assert len(sets) > 1, (
        f"all thirteen kinds fill {sorted(next(iter(sets)))}, so the table says "
        f"nothing a single constant would not have said"
    )
