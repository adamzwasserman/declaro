r"""A file named `test_*.py` must contain a test.

SIX STILL DO NOT. Twenty-four did. Each was a docstring, its imports,
sometimes a helper, and then blank space where the tests had been, and the
suite total said nothing about the gap.

Sixteen of the twenty-four came back by copying the last revision that held
tests and running it unchanged. Nothing was rewritten and nothing was adapted:
they pass because the modules they test still behave the way they did, which is
also the evidence that restoring them was safe. The suite went from 299 to 774.

WHAT IS LEFT, AND WHY EACH IS LEFT. Measured by restoring it and running it:

    test_views.py             48 tests   24 pass, 24 fail
    test_query_expressions.py 30 tests    0 pass, 30 fail
    test_write_queue.py       25 tests   import error: `pool` is abolished
    test_procedures.py        20 tests    9 pass, 11 fail
    test_triggers.py          20 tests    8 pass, 12 fail
    test_enums.py             17 tests    8 pass,  9 fail

160 tests. test_arrays.py and test_maps.py are no longer among them: their
four failures all passed the abolished dialect "libsql" and were right that it
must not yield PostgreSQL SQL. Fixing that in src (cfc0b1c) brought both files
back, 43 tests.

None of what is left is a copy job. Every failure is a real question about what the
rewritten module should now promise, and answering it by editing the test until
it passes is how a suite becomes decoration.

COUNTED WITH `grep -cE '^\s*(async )?def test_'`. An earlier version of this
docstring said 600 across twenty-four files. That count used a pattern without
`async def`, so it missed every asynchronous test. The true figure was 640.

A HOLLOW TEST FILE IS WORSE THAN A MISSING ONE. A missing file is an obvious
gap. A file called `test_validator.py` sitting in a green suite reads as "the
validator is tested", and every tool that counts files rather than tests agrees
with it. That is how `create_bulk_loader` shipped raising NameError for every
dialect it supported, and how `_normalize_view_query` shipped deleted with both
its callers intact: the modules had test files.

THIS RATCHETS, IT DOES NOT DEMAND. Pinned at zero it would fail the suite until
all 160 are answered, and a gate nobody can satisfy gets deleted. So it freezes
the list instead. A NEW hollow file fails. Filling or deleting one of these also
fails, so the list cannot quietly rot into a permanent excuse — every change to
it is a decision someone made on purpose.

The BDD step modules are excluded by directory, not by exception: they hold
`@given`/`@when` definitions for feature files bound elsewhere, so collecting
nothing is what they are for. `test_every_feature_is_bound` covers them.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

UNIT = pathlib.Path(__file__).resolve().parent

# Verified 2026-08-13 against `pytest --co`: this AST count and pytest's
# collection agree on zero versus non-zero for every file in this directory.
KNOWN_HOLLOW = {
    "test_enums.py",
    "test_procedures.py",
    "test_query_expressions.py",
    "test_triggers.py",
    "test_views.py",
    "test_write_queue.py",
}


def _holds_a_test(path: pathlib.Path) -> bool:
    """True if pytest would collect anything from this file.

    Counts `def test_*` at any depth, plus `scenarios(...)`, which binds an
    unknown number of feature scenarios but never zero.
    """
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith("test_"):
            return True
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "scenarios":
            return True
    return False


def test_the_gate_can_see_the_test_files_at_all() -> None:
    """A gate that reads nothing passes forever."""
    files = list(UNIT.glob("test_*.py"))
    assert len(files) > 30, f"found only {len(files)} test files in {UNIT}"


def test_no_new_test_file_is_hollow() -> None:
    hollow = {
        p.name for p in UNIT.glob("test_*.py") if not _holds_a_test(p)
    }

    appeared = hollow - KNOWN_HOLLOW
    assert not appeared, (
        f"{sorted(appeared)} contain no test. A file named test_*.py that "
        f"collects nothing reads as coverage to every reader and every tool "
        f"that counts files"
    )

    fixed = KNOWN_HOLLOW - hollow
    assert not fixed, (
        f"{sorted(fixed)} now hold tests, or were deleted. Remove them from "
        f"KNOWN_HOLLOW so the list keeps meaning what it says"
    )
