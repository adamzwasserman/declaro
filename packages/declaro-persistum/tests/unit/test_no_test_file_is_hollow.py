r"""A file named `test_*.py` must contain a test.

TWENTY-THREE DO NOT. Between them they once held 597 tests. Every one is now a
docstring, its imports, sometimes a helper, and then blank space where the tests
were, and the suite total says nothing about the gap.

    test_query_builder.py         58 tests -> 0
    test_views.py                 48 tests -> 0
    test_validator.py             45 tests -> 0
    test_enum_abstraction.py      39 tests -> 0
    test_cli_commands.py          31 tests -> 0
    ... eighteen more

COUNTED WITH `grep -cE '^\s*(async )?def test_'`. An earlier version of this
docstring said 600 across twenty-four files. That count used a pattern without
`async def`, so it missed every asynchronous test and reported
test_fk_ordering.py as 36 when it held 43. The true figure was 640 across
twenty-four. test_fk_ordering.py's 43 are restored, leaving 597 across
twenty-three.

A HOLLOW TEST FILE IS WORSE THAN A MISSING ONE. A missing file is an obvious
gap. A file called `test_validator.py` sitting in a green suite reads as "the
validator is tested", and every tool that counts files rather than tests agrees
with it. That is how `create_bulk_loader` shipped raising NameError for every
dialect it supported, and how `_normalize_view_query` shipped deleted with both
its callers intact: the modules had test files.

THIS RATCHETS, IT DOES NOT DEMAND. Pinning the gate at zero would fail the suite
until 600 tests are rebuilt, and a gate nobody can satisfy gets deleted. So it
freezes the list instead. A NEW hollow file fails. Filling or deleting one of
these also fails, so the list cannot quietly rot into a permanent excuse — every
change to it is a decision someone made on purpose.

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
    "test_arrays.py",
    "test_cli_commands.py",
    "test_cli_main.py",
    "test_differ.py",
    "test_enum_abstraction.py",
    "test_enums.py",
    "test_hierarchy.py",
    "test_loader.py",
    "test_maps.py",
    "test_materialized_views.py",
    "test_procedures.py",
    "test_query_builder.py",
    "test_query_expressions.py",
    "test_ranges.py",
    "test_reconstruction.py",
    "test_returning_dispatch.py",
    "test_schema_hash_key_isolation.py",
    "test_toposort.py",
    "test_transfer_ordering.py",
    "test_triggers.py",
    "test_types.py",
    "test_validator.py",
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
