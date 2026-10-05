"""The code's shapes are the declaration's shapes, field for field.

THE FAILURE THIS EXISTS TO STOP. `docs/design/persistum.hd` is the drawing the
map is rendered from, and `src/declaro_persistum/types.py` is what the functions
actually pass around. Nothing but a person reading both makes them agree, and a
person reading both is what this package has already been through: the source
deleted on 2026-09-21 had drifted so far from its documents that all three
described a different package.

So this compares them every run. A record that gains a field in one and not the
other fails here, and the declaration is the one that is right unless a source
says otherwise.

WHAT IT DOES NOT CHECK. Types this file cannot express: `handle`, which says a
value is held and never read, and `NOTHING`, which is absence with a name. Both
are rules a reader keeps, not shapes a checker can compare.
"""

from __future__ import annotations

import pathlib
import re
import typing

import pytest

import declaro_persistum.types as types

pytestmark = pytest.mark.precommit

_HD = pathlib.Path(__file__).resolve().parents[2] / "docs" / "design" / "persistum.hd"


def _declaration() -> str:
    return _HD.read_text(encoding="utf-8")


def _records() -> dict[str, list[str]]:
    """Every `type X = { ... }` in the declaration, and the field names in it."""
    out = {}
    for match in re.finditer(r"^  type (\w+) = \{(.*?)\}", _declaration(), re.S | re.M):
        fields = re.findall(r"^\s*(\w+):", match.group(2), re.M)
        out[match.group(1)] = fields
    return out


def _vocabularies() -> dict[str, list[str]]:
    """Every vocabulary, and the members of the sets that bound it."""
    text = _declaration()
    sets = {}
    for match in re.finditer(r"^  set (\w+) = \{(.*?)\}", text, re.M):
        # a member may carry a description: `"sqlite" : "a bare path"`. The
        # member is the first string; the second is prose for the reader.
        sets[match.group(1)] = [
            m.group(1) for m in re.finditer(r'"([^"]+)"(?:\s*:\s*"[^"]*")?', match.group(2))
        ]
    out = {}
    for match in re.finditer(r"^  vocabulary (\w+) = \{ ([^}]+) \}", text, re.M):
        members = []
        for name in (n.strip() for n in match.group(2).split(",")):
            members += sets[name]
        out[match.group(1)] = members
    return out


def _members_of(name: str) -> list[str]:
    """The strings a Literal admits, however it was assembled."""
    declared = getattr(types, name)
    return [
        str(arg) for arg in typing.get_args(declared) for arg in (typing.get_args(arg) or [arg])
    ]


def test_the_declaration_is_where_this_gate_reads_from() -> None:
    """A gate that cannot find its source passes for the wrong reason."""
    assert _HD.exists(), _HD
    assert len(_records()) > 20, len(_records())
    assert len(_vocabularies()) > 5, len(_vocabularies())


# DECLARED AND NOT WRITTEN YET, with the reason, and it only ever gets shorter.
# The order here is the declaration, then the feature, then the setup, then the
# code, so a record whose field moved sits in two shapes until the code catches
# up. A name here is a record the declaration has moved and `types.py` has not;
# taking one off means the code landed.
#
# `Answering` CARRIES THE WRITING NOW, NOT A DICT. Measured 2026-10-05: the door
# used to take a word naming the spelling beside a dict a caller had taken apart,
# and it takes the text instead. So the field is `written: Written` where it was
# `asked: Asked`. Two places in the source touch it, `fn query_ask` filling it and
# `orchestrator fn query_answer` reading it, and both change when the four readers
# become one.
DECLARED_AND_NOT_WRITTEN: frozenset[str] = frozenset({"Answering"})


def _one_case(record: str) -> object:
    """One case per record, marked when the declaration has moved ahead of it.

    The mark goes on the case rather than inside the test, so there is no branch
    reading a list at runtime and a reader sees which records are waiting in the
    collected names.
    """
    return pytest.param(
        record,
        marks=[pytest.mark.skip(reason="declared ahead of the code")]
        * int(record in DECLARED_AND_NOT_WRITTEN),
    )


@pytest.mark.parametrize("record", [_one_case(one) for one in sorted(_records())])
def test_every_declared_record_exists_with_its_fields(record: str) -> None:
    declared = _records()[record]
    written = getattr(types, record, None)
    assert written is not None, f"{record} is declared and the code does not have it"
    assert list(written.__annotations__) == declared, (
        f"{record} carries {list(written.__annotations__)} and the declaration says {declared}"
    )


@pytest.mark.parametrize("vocabulary", sorted(_vocabularies()))
def test_every_vocabulary_admits_exactly_what_it_declares(vocabulary: str) -> None:
    declared = _vocabularies()[vocabulary]
    assert hasattr(types, vocabulary), f"{vocabulary} is declared and the code does not have it"
    assert sorted(_members_of(vocabulary)) == sorted(declared), (
        f"{vocabulary} admits {sorted(_members_of(vocabulary))} "
        f"and the declaration says {sorted(declared)}"
    )


def test_the_code_declares_nothing_the_declaration_does_not() -> None:
    """Drift runs both ways: a shape nobody declared is the same defect."""
    shapes = {
        n
        for n in dir(types)
        if n[0].isupper()
        and getattr(getattr(types, n), "__module__", "") == types.__name__
        and hasattr(getattr(types, n), "__annotations__")
    }
    extra = shapes - set(_records())
    assert not extra, f"these shapes are in the code and in no declaration: {sorted(extra)}"
