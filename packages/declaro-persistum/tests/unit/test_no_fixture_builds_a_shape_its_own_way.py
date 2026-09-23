"""Nobody writes a `Model` or a `Column` out by hand twice.

THE FAILURE THIS EXISTS TO STOP. Twelve step files each wrote a `Model` out
themselves and ten wrote a `Column`, through thirteen private helpers that were
each slightly different. When `Column` lost `facets` on 2026-09-22, none of the
twelve knew, because nothing in this suite compares a fixture against
`docs/design/persistum.hd`. The types gate reads `src/declaro_persistum/types.py`
and stops there.

It had already happened once. Eight files hand-built a `Room`, the room grew two
fields and lost one inside a day, and the suite stayed green throughout.

SO THE RULE IS ONE BUILDER EACH, IN `tests/conftest.py`, with every field
required. A record that gains a field then fails every call site at once, which
is the only thing that makes a fixture keep up with a declaration.
"""

from __future__ import annotations

import pathlib

import pytest

pytestmark = pytest.mark.precommit

_STEPS = pathlib.Path(__file__).resolve().parents[1] / "bdd" / "steps"

# The field names that say a dict is being built as one of the two shapes. Read
# off the declaration's own records rather than guessed: a `Column` is the only
# thing with `declared_type`, and a `Model` the only thing with `table` beside
# `columns`.
_A_COLUMN = '"declared_type"'
_A_MODEL = '"dropping"'


# EVERY STEP FILE'S TEXT, READ ONCE, WHEN THIS MODULE LOADS. The reading is up
# here rather than inside a function because pytest collecting this file is the
# edge, and a function that globs and reads is I/O buried where a reader of the
# tests below cannot see it. Nothing under this line touches a disk.
WHAT_EACH_STEP_FILE_SAYS: dict[str, str] = {
    p.name: p.read_text(encoding="utf-8") for p in _STEPS.glob("*.py")
}


def _files_writing(field: str, said: dict[str, str]) -> set[str]:
    """Which of those files write a dict carrying this field."""
    return {name for name, text in said.items() if field in text}


# EVERY FILE THAT STILL BUILDS ONE ITSELF, listed by name so this gate names the
# work rather than a number. Each comes off as it moves to the shared builder,
# and the list may only ever get shorter.
STILL_THEIR_OWN_WAY = {
    "test_spell_view_steps.py",
    "test_spell_operation_steps.py",
    "test_answer_from_rows_steps.py",
    "test_join_between_steps.py",
    "test_migration_needed_steps.py",
    "test_name_what_is_ambiguous_steps.py",
    "test_order_operations_steps.py",
    "test_refuse_owned_names_steps.py",
    "test_refuse_undeclared_columns_steps.py",
    "test_refuse_undeclared_loss_steps.py",
    "test_schema_from_declared_steps.py",
}


@pytest.mark.parametrize("field", [_A_COLUMN, _A_MODEL])
def test_no_new_file_writes_one_out_by_hand(field: str) -> None:
    """A file not on the list may not start building these itself."""
    new = _files_writing(field, WHAT_EACH_STEP_FILE_SAYS) - STILL_THEIR_OWN_WAY
    assert not new, (
        f"{sorted(new)} write a shape out by hand that {field} identifies. "
        f"`tests/conftest.py` has `a_model` and `a_column` with every field "
        f"required, so use those: a record that gains a field has to fail every "
        f"call site at once, and a dict written out here fails none of them."
    )


def test_the_list_only_ever_gets_shorter() -> None:
    """A name on the list that no longer builds one comes off it."""
    said = WHAT_EACH_STEP_FILE_SAYS
    building = _files_writing(_A_COLUMN, said) | _files_writing(_A_MODEL, said)
    stale = STILL_THEIR_OWN_WAY - building
    assert not stale, (
        f"{sorted(stale)} no longer build a model or a column by hand, so take "
        f"them off STILL_THEIR_OWN_WAY. A ratchet that does not tighten is a list."
    )
