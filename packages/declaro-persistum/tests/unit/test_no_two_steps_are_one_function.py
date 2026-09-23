"""Two step functions that differ only in their words are one function.

THE FAILURE THIS EXISTS TO STOP. A step file grows a function per sentence. Two
sentences that ask the same thing about different words become two functions with
the same body and different string literals, and then three, and then a change
has to be made in every copy or the copies disagree. That is the same defect this
package deletes in `src/`, and a test file is not exempt from it: four grammar
builders independently invented one function four times, and had drifted from
each other on four separate questions before anybody finished writing them.

WHY A CHECKER CANNOT BE TRUSTED TO BE OBVIOUS. The bodies are not textually
identical. They differ exactly where the words differ, which is what makes them
look like two functions to a reader and one function to this gate: erase every
name and every quoted string, and what is left is the shape. Two functions with
the same shape in the same module are one function over a table.

WHY IT IS A GATE AND NOT A REVIEW NOTE. Written 2026-09-21, after fourteen
agents were each told this rule was checked and it was not. An external checker
had been finding these by hand, one file at a time, and reporting them to a
person who then had to pass them on. A rule nobody can run is a rule that holds
only while somebody remembers it.

THE RATCHET. `KNOWN_DUPLICATE_SHAPES` records what was already there when this
was written, per file, as a count. It only goes down.
"""

from __future__ import annotations

import ast
import pathlib
from collections.abc import Callable

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_STEPS = _ROOT / "tests" / "bdd"

_STEP_DECORATORS = frozenset({"given", "when", "then"})

# file name -> how many duplicate shapes it is allowed to keep. Empty is the goal,
# and this only ever goes down.
#
# THE NINE HERE WERE MEASURED ON 2026-09-21, the moment this gate stopped
# skipping one-line steps. Most of them are steps that mark a scenario as owned
# by another card, and they go when those scenarios move to the feature of the
# function that can answer them.
KNOWN_DUPLICATE_SHAPES: dict[str, int] = {
    "common_steps.py": 1,
    "test_budget_spent_steps.py": 1,
    "test_worth_retrying_steps.py": 2,
}

# EVERY BODY IS JUDGED, INCLUDING A ONE-LINE ONE. This stood at 2 for an hour
# and found nothing, while an external checker found ten duplicates this gate had
# skipped. Measured on 2026-09-21: at 2 it reports zero groups, at 1 it reports
# nine. A one-statement step is the commonest duplicate there is, because a Given
# that records one fact and a Then that asserts one thing are exactly the steps
# somebody writes once per sentence. Excluding them excluded the whole point.
_ENOUGH_TO_BE_A_SHAPE = 1


# THE FIELDS THAT CARRY A WORD RATHER THAN A STRUCTURE. Erasing exactly these is
# what turns a function into its shape: `assert x == "alice"` and
# `assert y == "bob"` differ only here, and nowhere else.
_A_WORD: dict[str, tuple[str, ...]] = {
    "Name": ("id",),
    "Attribute": ("attr",),
    "Constant": ("value", "kind"),
    "arg": ("arg", "annotation"),
    "keyword": ("arg",),
    "alias": ("name", "asname"),
    "FunctionDef": ("name",),
    "AsyncFunctionDef": ("name",),
    "ExceptHandler": ("name",),
    "Global": ("names",),
    "Nonlocal": ("names",),
    "ImportFrom": ("module",),
}


# A NODE KIND NOT IN THE TABLE ABOVE CARRIES NO WORDS, and that is a rule rather
# than a gap, so it is a row of its own instead of a fallback on the read.
_NO_WORDS: tuple[str, ...] = ()
_WORDS_IN: dict[bool, Callable[[str], tuple[str, ...]]] = {
    True: lambda kind: _A_WORD[kind],
    False: lambda _kind: _NO_WORDS,
}


def _of_a_node(node: ast.AST) -> object:
    """The structure of one node, with every word in it erased."""
    kind = type(node).__name__
    erased = _WORDS_IN[kind in _A_WORD](kind)
    return {
        "node": kind,
        "fields": {
            field: _ERASED_OR_KEPT[field in erased](value) for field, value in ast.iter_fields(node)
        },
    }


def _of_a_list(values: list[object]) -> object:
    """A field holding several things, each taken down to its own structure."""
    return [_skeleton(value) for value in values]


def _skeleton(value: object) -> object:
    """The structure of whatever a field holds, with every word erased.

    A field holds a node, a list of nodes, or a plain thing: a string, a number,
    or nothing at all. All three are rows here, so nothing has to be guarded
    against on the way in.

    Written as functions rather than an `ast.NodeTransformer` subclass because a
    subclass puts half the behaviour in a base class nobody reading this file can
    see, and which fields carry a word is the whole of what this decides.
    """
    return _BY_WHAT_IT_HOLDS[isinstance(value, ast.AST), isinstance(value, list)](value)


_BY_WHAT_IT_HOLDS: dict[tuple[bool, bool], Callable[[object], object]] = {
    (True, False): _of_a_node,
    (False, True): _of_a_list,
    # A string, a number, or nothing. It is already its own structure.
    (False, False): lambda value: value,
    # An `ast.AST` is never a `list`, so this pair cannot arrive. The row exists
    # so the table is total and the read needs no fallback.
    (True, True): lambda value: value,
}

_ERASED_OR_KEPT: dict[bool, Callable[[object], object]] = {
    True: lambda _value: "_",
    False: _skeleton,
}


def _is_a_step(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """Whether pytest-bdd will call this as a Given, When or Then."""
    return any(
        (isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id in _STEP_DECORATORS)
        or (isinstance(d, ast.Name) and d.id in _STEP_DECORATORS)
        for d in node.decorator_list
    )


def _shape_of(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str | None:
    """The body with every name and string erased, or None if too small to judge."""
    body = [
        statement
        for statement in node.body
        if not (
            isinstance(statement, ast.Expr)
            and isinstance(statement.value, ast.Constant)
            and isinstance(statement.value.value, str)
        )
    ]
    if len(body) < _ENOUGH_TO_BE_A_SHAPE:
        # APPROVED-IF 2026-09-21 17:05 — an empty body has no shape at all, which
        # is an absence rather than a case, so there is no table to look it up in.
        return None
    return repr([_skeleton(statement) for statement in body])


def _duplicate_shapes(text: str) -> dict[str, list[str]]:
    """shape -> the step functions sharing it, for shapes shared by two or more."""
    by_shape: dict[str, list[str]] = {}
    for node in ast.walk(ast.parse(text)):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _is_a_step(node):
            shape = _shape_of(node)
            if shape is not None:
                # APPROVED-IF 2026-09-21 16:30 — `shape is None` means the body was
                # too small to have a shape, which is an absence rather than a
                # case; there is nothing to look up.
                by_shape.setdefault(shape, []).append(node.name)
    return {shape: names for shape, names in by_shape.items() if len(names) > 1}


# EVERY STEP MODULE, READ ONCE, WHEN THIS FILE LOADS. The reading is up here
# rather than inside a fixture because pytest collecting this file is the edge,
# and a function that reads a disk, called by the tests below, buries the I/O
# where neither test can see it. Nothing under this line touches a disk.
STEP_FILES: dict[pathlib.Path, str] = {
    p: p.read_text(encoding="utf-8") for p in sorted(_STEPS.rglob("*.py"))
}


def test_no_two_step_functions_share_a_shape():
    """Erase the words and two of them are the same function."""
    found = {
        path.name: sorted(names for names in _duplicate_shapes(text).values())
        for path, text in STEP_FILES.items()
        if _duplicate_shapes(text)
    }
    # A file not on the ratchet is allowed none, which is a rule and not an
    # absence, so it is a row rather than a fallback on the read.
    allowed = dict.fromkeys(found, 0) | KNOWN_DUPLICATE_SHAPES
    over = {name: groups for name, groups in found.items() if len(groups) > allowed[name]}

    assert not over, (
        "in these step files, two or more functions are one function once every "
        "name and quoted string is erased:\n"
        + "\n".join(
            f"    {name}\n" + "\n".join(f"        {group}" for group in groups)
            for name, groups in sorted(over.items())
        )
        + "\n\nMake each group one function over a table, where every row carries "
        "the words that version would have pasted in. `parsers.parse` with a "
        "placeholder is how a step reads the row."
    )


def test_the_ratchet_holds_no_name_that_is_already_clean():
    """An entry for a file with nothing wrong hides the next regression."""
    live = {path.name: len(_duplicate_shapes(text)) for path, text in STEP_FILES.items()}
    measured = dict.fromkeys(KNOWN_DUPLICATE_SHAPES, 0) | live
    stale = sorted(
        name for name, allowed in KNOWN_DUPLICATE_SHAPES.items() if measured[name] < allowed
    )

    assert not stale, (
        f"these names allow more duplicate shapes than they have: {stale}. "
        "Lower the number to what is measured, or take the name out."
    )


def test_this_gate_can_actually_see_the_steps():
    """A discovery bug would make the gate above pass on an empty set."""
    assert len(STEP_FILES) >= 20, (
        f"only {len(STEP_FILES)} step modules found under {_STEPS}; the gate "
        "above would pass by examining almost nothing"
    )
    steps = sum(
        1
        for text in STEP_FILES.values()
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _is_a_step(node)
    )
    assert steps >= 200, (
        f"only {steps} step functions found across every module; the reader is "
        "no longer recognising a Given, When or Then and nothing is measured"
    )
    judged = sum(
        1
        for text in STEP_FILES.values()
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and _is_a_step(node)
        and _shape_of(node) is not None
    )
    assert judged >= steps // 4, (
        f"only {judged} of {steps} step functions have a body big enough to "
        "judge, so almost everything is being skipped as too small"
    )
