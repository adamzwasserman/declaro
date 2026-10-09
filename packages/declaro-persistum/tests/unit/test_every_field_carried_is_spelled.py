"""Every field a change carries is a field the writer uses.

TWO FUNCTIONS AND NOTHING BETWEEN THEM. `fn what_an_operation_carries` is a
table saying which fields one kind of schema change fills.
`fn spell_operation` turns that change into statements. Each reads correctly on
its own, and a reader checking one has no reason to open the other, so they drift
and nothing says so.

WHAT THAT DRIFT COSTS, MEASURED 2026-10-07. `create_table` claims it carries
indexes. Spelling one hands back a single CREATE TABLE and the index is gone. A
deployment gets its table, no index, and a success report. Another project hit
that on persistum 0.1.11, where the differ packed a new table's indexes into the
operation and the applier never read them, and four indexes went missing in
production. It survived the rewrite.

THE SECOND ONE IS A WRONG ANSWER RATHER THAN A MISSING ONE. `rename_column`
claims `columns` and denies `renamed_to`. Measured both ways: given `renamed_to`
and no columns it writes `ALTER TABLE "people" RENAME COLUMN "a" TO "b"`, and
given columns and no `renamed_to` it refuses with "handed to sqlite without to".
Both halves of that row are backwards.

A VALUE MAY BE BOUND RATHER THAN WRITTEN, and a check that forgets that reports
a field as missing when it is there. A `Statement` is text and the values kept
beside it, so `insert_enum_values` spelling two values gives two statements with
each value as a parameter and neither in the text. The first draft of this gate
read only the text and called both of them broken.
"""

from __future__ import annotations

import pathlib
import re
import typing

import pytest

from declaro_persistum import types
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.what_an_operation_carries import what_an_operation_carries
from tests.conftest import (
    NARROWS_BY_NOTHING,
    a_column,
    a_constraint,
    a_declaration,
    a_view,
    an_operation,
)

pytestmark = pytest.mark.precommit

_HD = pathlib.Path(__file__).resolve().parents[2] / "docs" / "design" / "persistum.hd"

# WHAT FILLS EACH FIELD, one row per field a change can carry, and what to look
# for in the statements once it has been spelled. The marker is what a reader
# would search the SQL for: a quoted column name, an index's name, the words a
# rule becomes. `values` has no marker because a value is bound rather than
# written, and the check for it reads the parameters instead.
AN_INDEX_NAME = "idx_spelled"

WHAT_FILLS_EACH_FIELD: dict[str, object] = {
    "columns": [a_column(name="a", declared_type="text", renamed_from=None, references=None)],
    "indexes": [{"name": AN_INDEX_NAME, "columns": ["a"], "unique": False}],
    "constraints": [
        a_constraint(
            name="nn_a", kind="not_null", columns=["a"], rule=NARROWS_BY_NOTHING, value=None
        )
    ],
    "copying": ["a"],
    "values": ["a_value_nobody_else_writes"],
    "renamed_to": "b",
    "view": a_view(
        name="a_view_nobody_else_writes",
        declaration=a_declaration(
            model="people",
            wanted="all",
            columns=["a"],
            condition=NARROWS_BY_NOTHING,
            order={},
            grouping=[],
            having=NARROWS_BY_NOTHING,
            paging=None,
            rows=None,
            shaping="dictionaries",
            across=None,
        ),
        freshness=None,
    ),
}

WHAT_TO_LOOK_FOR: dict[str, str] = {
    "columns": '"a"',
    "indexes": AN_INDEX_NAME,
    "constraints": "NOT NULL",
    "copying": '"a"',
    "values": "a_value_nobody_else_writes",
    "renamed_to": '"b"',
    "view": "a_view_nobody_else_writes",
}

WHAT_AN_EMPTY_FIELD_LOOKS_LIKE: dict[str, object] = {
    "columns": [],
    "indexes": [],
    "constraints": [],
    "copying": [],
    "values": [],
    "renamed_to": None,
    "view": None,
}


def _the_kinds() -> tuple[str, ...]:
    """Every kind of change, read off `set operation_kinds` in the declaration.

    Read from the declaration rather than from a list here, so a kind added
    there is checked without anybody editing this file.
    """
    said = _HD.read_text(encoding="utf-8")
    line = re.search(r"^\s*set operation_kinds = \{(.*?)\}", said, re.M | re.S)
    assert line, "the declaration has no `set operation_kinds`, so this gate knows no kinds"
    return tuple(re.findall(r'"(\w+)"', line.group(1)))


def _the_engines() -> tuple[str, ...]:
    return typing.get_args(types.Engine)


def _a_change_of(kind: str, *, carrying: dict[str, bool]) -> dict:
    """One change of this kind, with the fields the table claims filled."""
    return an_operation(
        kind=kind,
        table="people",
        column="a",
        destroys=False,
        **{
            field: (WHAT_FILLS_EACH_FIELD if claimed else WHAT_AN_EMPTY_FIELD_LOOKS_LIKE)[field]
            for field, claimed in carrying.items()
        },
    )


def _everything_the_statements_say(spelled: list[dict]) -> str:
    """The text of every statement and every value bound beside it.

    A value is bound rather than written, so reading the text alone reports a
    field as missing when the driver will carry it perfectly well.
    """
    return " ".join(
        [one["text"] for one in spelled]
        + [str(value) for one in spelled for value in one["parameters"].values()]
    )


# EVERY KIND SPELLED FOR EVERY ENGINE, ONCE, WHEN THIS FILE LOADS. Collecting
# this file is the edge, and a function that builds and spells is work hidden
# where a reader of the tests below cannot see it.
HOW_EACH_KIND_SPELLS: dict[tuple[str, str], object] = {
    (kind, engine): spell_operation(
        _a_change_of(kind, carrying=what_an_operation_carries(kind)), engine
    )
    for kind in _the_kinds()
    for engine in _the_engines()
}


def _is_a_fault(answer: object) -> bool:
    return isinstance(answer, dict) and "what_went_wrong" in answer


def test_every_field_the_table_claims_is_used_by_some_engine() -> None:
    """A field carried and read by nobody is a declaration nobody carries out.

    ASKED PER KIND AND NOT PER ENGINE, AND THAT IS NOT LAXNESS. Whether a change
    needs a field is the kind's business, and whether it uses it is the engine's.
    `alter_column` carries every index the table is meant to end with, because
    SQLite and Turso alter a column by building a new table and copying into it,
    and the new table needs them back. PostgreSQL alters a column with one
    statement and needs none of them. Demanding every engine read every carried
    field would call PostgreSQL's spelling broken for being simpler.

    A field no engine at all reads is the real fault, and it is what cost another
    project four indexes on 0.1.11.
    """
    unspelled = {
        kind: sorted(
            field
            for field, claimed in what_an_operation_carries(kind).items()
            if claimed
            if not any(
                WHAT_TO_LOOK_FOR[field] in _everything_the_statements_say(spelled)  # type: ignore[arg-type]
                for (said, _engine), spelled in HOW_EACH_KIND_SPELLS.items()
                if said == kind
                if not _is_a_fault(spelled)
            )
        )
        for kind in _the_kinds()
    }
    guilty = {where: fields for where, fields in unspelled.items() if fields}

    assert not guilty, (
        "these changes carry a field the writer never uses, so what the change "
        "says it will do and what it does are two different things:\n"
        + "\n".join(
            f"    {where} carries {', '.join(fields)} and no engine spells it"
            for where, fields in sorted(guilty.items())
        )
        + "\n\nEither the writer has to spell that field, or the row in "
        "`fn what_an_operation_carries` is claiming a field the kind does not "
        "carry. `fn spell_operation` hands back a list, so a change that needs "
        "two statements has room for both."
    )


def test_a_change_carrying_what_the_table_claims_is_never_refused() -> None:
    """A row that denies a field the writer needs starves it.

    This is the other direction and it is the one that found `rename_column`
    backwards: the row denied `renamed_to`, so a change built from the row alone
    was refused for want of the very field the row said it did not need.
    """
    refused = {
        f"{kind} on {engine}": spelled["what_went_wrong"]  # type: ignore[index]
        for (kind, engine), spelled in HOW_EACH_KIND_SPELLS.items()
        if _is_a_fault(spelled)
    }

    assert not refused, (
        "these changes were built from exactly what the table says they carry, "
        "and the writer refused them:\n"
        + "\n".join(f"    {where}: {why}" for where, why in sorted(refused.items()))
        + "\n\nThe row in `fn what_an_operation_carries` denies a field the "
        "writer needs. Fix the row, not this gate."
    )


def test_the_gate_read_the_kinds_and_spelled_something() -> None:
    """A gate that spelled nothing agrees with everything."""
    kinds, engines = _the_kinds(), _the_engines()

    assert len(kinds) > 10, f"the declaration names {len(kinds)} kinds of change, which is too few"
    assert len(engines) == 3, f"the package has {len(engines)} engines and this gate expects three"
    assert len(HOW_EACH_KIND_SPELLS) == len(kinds) * len(engines), (
        f"the gate spelled {len(HOW_EACH_KIND_SPELLS)} of {len(kinds) * len(engines)} "
        f"combinations, so something was skipped rather than checked"
    )
    written = sum(
        len(spelled)
        for spelled in HOW_EACH_KIND_SPELLS.values()
        if not _is_a_fault(spelled)  # type: ignore[arg-type]
    )

    assert written > 30, (
        f"the gate spelled {written} statements in all, which is too few for "
        f"{len(kinds)} kinds across {len(engines)} engines. A count this low "
        f"means the building is broken and the tests above are reading faults."
    )


def test_the_table_answers_for_every_kind_the_declaration_names() -> None:
    """A kind the table has no row for raises, which is the refusal it owes."""
    missing = [
        kind
        for kind in _the_kinds()
        for answered in [_whether_the_table_answers(kind)]
        if not answered
    ]

    assert missing == [], (
        f"the declaration names {missing} and `fn what_an_operation_carries` has "
        f"no row for them, so nothing can say what they carry"
    )


def _whether_the_table_answers(kind: str) -> bool:
    """Whether the table has a row for this kind, asked by asking it."""
    try:
        return what_an_operation_carries(kind) is not None
    except KeyError:
        return False
