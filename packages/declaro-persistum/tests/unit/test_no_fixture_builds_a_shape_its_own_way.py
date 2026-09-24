"""Nobody writes a `Model`, a `Column` or an `Operation` out by hand twice.

THE FAILURE THIS EXISTS TO STOP. Twelve step files each wrote a `Model` out
themselves and ten wrote a `Column`, through thirteen private helpers that were
each slightly different. When `Column` lost `facets` on 2026-09-22, none of the
twelve knew, because nothing in this suite compares a fixture against
`docs/design/persistum.hd`. The types gate reads `src/declaro_persistum/types.py`
and stops there.

It had already happened once. Eight files hand-built a `Room`, the room grew two
fields and lost one inside a day, and the suite stayed green throughout.

AND IT HAPPENED AGAIN THE NEXT DAY, TO A RECORD THIS GATE WAS NOT WATCHING. Six
step files hand-built an `Operation`, and between them they wrote four fields the
type does not have and left five off that it does. This gate watched two records
and saw none of it. A gate that names the shapes it watches goes stale the moment
a third one matters, so `Operation` is here now, and the next record added to the
declaration belongs here on the day it lands.

SO THE RULE IS ONE BUILDER EACH, IN `tests/conftest.py`, with every field
required. A record that gains a field then fails every call site at once, which
is the only thing that makes a fixture keep up with a declaration.
"""

from __future__ import annotations

import pathlib

import pytest

pytestmark = pytest.mark.precommit

_STEPS = pathlib.Path(__file__).resolve().parents[1] / "bdd" / "steps"

# The field names that say a dict is being built as one of the three shapes.
# Read off the declaration's own records rather than guessed: a `Column` is the
# only thing with `declared_type`, a `Model` the only thing with `dropping`, and
# an `Operation` the only thing with `copying`.
_A_COLUMN = "declared_type"
_A_MODEL = "dropping"
_AN_OPERATION = "copying"
# ADDED 2026-09-24, on the day `orchestrator fn session_connect` was written.
# `Connecting` has nineteen fields and one function fills them, `Schema` and
# `Declared` and `Report` and `Plan` ride inside it, and a step file writing
# `{"tables": [], "views": []}` at a call site is building a `Schema` by hand.
# The docstring above says the next record belongs here on the day it lands,
# and five of them landed together.
_A_CONNECTING = "connections_asked"
_A_DECLARED = "dropping_tables"
_A_REPORT = "applied"
_A_PLAN = "destroys"
_EVERY_SHAPE = (
    _A_COLUMN,
    _A_MODEL,
    _AN_OPERATION,
    _A_CONNECTING,
    _A_DECLARED,
    _A_REPORT,
    _A_PLAN,
)

# `Schema` IS NOT WATCHED, AND THIS IS THE REASON RATHER THAN AN OVERSIGHT. It
# has two fields, `tables` and `views`, and it owns neither: `Inspecting` also
# has `tables` and `Declared` also has `views`. Watching `tables` fired on
# `test_how_to_inspect_steps.py:160`, which keys the names each engine's answer
# has to come back under and builds no schema at all. A gate that fires on the
# wrong file gets switched off, so `Schema` waits for a field of its own.
# `tests/conftest.py` has `a_schema` and every step file should still call it.

# WHAT A BUILD LOOKS LIKE, WHICH IS NOT THE SAME AS A MENTION. Building one of
# these by hand means writing a dict literal, so the field has to appear as a
# key with a value after it. On 2026-09-24 this gate fired on a file that named
# `declared_type` in a set of names an engine has to answer under, which builds
# nothing and is exactly the kind of false positive that gets a gate switched
# off. Every instrument has a horizon; this one's is that a field named in a
# string is not a field being filled in.
#
# `declared_type=` IS NOT ONE OF THESE SPELLINGS, AND THAT IS DELIBERATE. It is
# how `a_column` is called, which is the cure rather than the disease. Adding it
# turned this gate on four files that had already been moved to the shared
# builders, which is the shape of a gate that counts the cure and gets turned
# off for it.
_ASSIGNED = ('"{field}":', "'{field}':")


# EVERY STEP FILE'S TEXT, READ ONCE, WHEN THIS MODULE LOADS. The reading is up
# here rather than inside a function because pytest collecting this file is the
# edge, and a function that globs and reads is I/O buried where a reader of the
# tests below cannot see it. Nothing under this line touches a disk.
WHAT_EACH_STEP_FILE_SAYS: dict[str, str] = {
    p.name: p.read_text(encoding="utf-8") for p in _STEPS.glob("*.py")
}


def _files_writing(field: str, said: dict[str, str]) -> set[str]:
    """Which of those files assign this field, rather than merely naming it."""
    spellings = [spelling.format(field=field) for spelling in _ASSIGNED]
    return {name for name, text in said.items() if any(spelling in text for spelling in spellings)}


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


@pytest.mark.parametrize("field", _EVERY_SHAPE)
def test_no_new_file_writes_one_out_by_hand(field: str) -> None:
    """A file not on the list may not start building these itself."""
    new = _files_writing(field, WHAT_EACH_STEP_FILE_SAYS) - STILL_THEIR_OWN_WAY
    assert not new, (
        f"{sorted(new)} write a shape out by hand that {field} identifies. "
        f"`tests/conftest.py` has `a_model`, `a_column`, `a_constraint` and "
        f"`an_operation` with every field required, so use those: a record that "
        f"gains a field has to fail every call site at once, and a dict written "
        f"out here fails none of them."
    )


def test_the_list_only_ever_gets_shorter() -> None:
    """A name on the list that no longer builds one comes off it."""
    said = WHAT_EACH_STEP_FILE_SAYS
    building = set().union(*(_files_writing(field, said) for field in _EVERY_SHAPE))
    stale = STILL_THEIR_OWN_WAY - building
    assert not stale, (
        f"{sorted(stale)} no longer build a model or a column by hand, so take "
        f"them off STILL_THEIR_OWN_WAY. A ratchet that does not tighten is a list."
    )
