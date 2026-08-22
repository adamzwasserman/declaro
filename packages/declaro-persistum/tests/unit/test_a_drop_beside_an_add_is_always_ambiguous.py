"""A rename the differ did not recognise dropped the table and said nothing.

MEASURED 2026-08-21. `detect_ambiguities` decides whether a dropped table and
an added table might be one rename by scoring them: Levenshtein distance on
the names, averaged with the fraction of columns they share, gated at 50%
overlap. Below that gate it reports nothing.

    users -> members, same columns          1 ambiguity, migration stops
    users -> members, columns also changed  0 ambiguities, migration proceeds
                                            drop_table users, create_table members

The second line is the whole problem. Renaming a table while also reshaping it
is an ordinary migration, and there the score falls under the gate, so the
operator is never asked. The drop runs and the rows are gone.

A SIMILARITY SCORE WAS DECIDING WHETHER DATA LOSS GOT ANNOUNCED. That is not a
judgement a heuristic is entitled to make. A drop beside an add is ambiguous
by construction: the differ compares two schemas and cannot know whether the
author moved a thing or replaced it. Nothing about the names or the columns
changes that, so nothing should be measured.

WHAT REPLACES IT. `renamed_from` already exists on Column and Table in
`types.py`, and `ambiguity.py` already skips its inference when it is set. So
the declarative answer is present and the guessing sits in front of it. The
rule is now: a drop and an add in the same diff is ambiguous unless the author
declared which it is. No confidence, no threshold, and no case where the
question goes unasked.

This deletes `calculate_rename_confidence`, `_levenshtein_distance`, the 50%
gate, and six of the package's surviving conditionals.
"""

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.types import NOTHING

_ID = {"id": {"type": "integer"}}


def _ambiguities(current: dict, target: dict) -> list[dict]:
    return diff(current, target, dialect="sqlite", decisions=NOTHING)["ambiguities"]


@pytest.mark.parametrize(
    "target_columns",
    [
        {"id": {"type": "integer"}},
        {"uuid": {"type": "text"}, "address": {"type": "text"}},
    ],
    ids=["columns unchanged", "columns also reshaped"],
)
def test_a_dropped_table_beside_an_added_one_is_ambiguous(
    target_columns: dict,
) -> None:
    """Whether the columns match has nothing to do with it.

    The second case is the one that lost data. It scored under the old 50%
    gate, so no ambiguity was raised and the drop ran unannounced.
    """
    found = _ambiguities({"users": {"columns": _ID}}, {"members": {"columns": target_columns}})

    assert len(found) == 1, f"got {found}"
    assert found[0]["type"] == "possible_rename"
    assert "users" in found[0]["message"] and "members" in found[0]["message"]


@pytest.mark.parametrize(
    "new_name",
    ["mail", "contact_address"],
    ids=["a similar name", "an unlike name"],
)
def test_a_dropped_column_beside_an_added_one_is_ambiguous(new_name: str) -> None:
    """The same rule one level down, and name similarity is equally irrelevant."""
    found = _ambiguities(
        {"t": {"columns": {**_ID, "email": {"type": "text"}}}},
        {"t": {"columns": {**_ID, new_name: {"type": "text"}}}},
    )

    assert len(found) == 1, f"got {found}"
    assert found[0]["from_column"] == "email"
    assert found[0]["to_column"] == new_name


def test_no_score_survives_in_what_is_reported() -> None:
    """A confidence number invites the reader to trust a guess.

    The old record carried `confidence: 0.714`, computed from an edit distance
    averaged with a column overlap. There is nothing to be confident about:
    the differ does not know, which is why it is asking.
    """
    found = _ambiguities({"users": {"columns": _ID}}, {"members": {"columns": _ID}})

    assert "confidence" not in found[0], found[0]


def test_a_declared_rename_is_not_ambiguous() -> None:
    """`renamed_from` is the answer, so there is no question to ask.

    This is the behaviour that must survive: an author who says what they did
    is never interrogated about it.
    """
    found = _ambiguities(
        {"users": {"columns": _ID}},
        {"members": {"columns": _ID, "renamed_from": "users"}},
    )

    assert found == []


def test_a_plain_drop_is_not_ambiguous() -> None:
    """The negative control, so the rule is narrowed and not switched on.

    A table dropped with nothing added in its place is not a rename candidate.
    Reporting one would make every deletion require a decision.
    """
    found = _ambiguities(
        {"users": {"columns": _ID}, "keep": {"columns": _ID}},
        {"keep": {"columns": _ID}},
    )

    assert found == []


def test_a_plain_add_is_not_ambiguous() -> None:
    """And the same on the other side."""
    found = _ambiguities(
        {"keep": {"columns": _ID}},
        {"keep": {"columns": _ID}, "extra": {"columns": _ID}},
    )

    assert found == []


def test_the_scoring_functions_are_gone() -> None:
    """The heuristic must not survive as a private helper nobody calls.

    Left in place it reads as available, and the next caller that wants to
    guess at a rename will find it sitting there.
    """
    from declaro_persistum.differ import ambiguity

    for name in ("calculate_rename_confidence", "_levenshtein_distance"):
        assert not hasattr(ambiguity, name), (
            f"{name} still exists; the differ can still be asked to guess"
        )
