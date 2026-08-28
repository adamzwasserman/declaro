"""Two tests with one name is one test, and Python says nothing about it.

WHAT HAPPENED. A rename across the test files turned
`test_applier_handles_alter_column_nullability` and
`test_applier_handles_alter_column_type` into the same name in the same file.
Python bound the name twice, kept the second, and discarded the first. No
error, no warning. The suite went from 2925 passing to 2924, and that single
digit was the only evidence that a test had been destroyed.

IT WOULD HAVE STAYED GONE. Nothing else notices: the file still imports, the
remaining test still passes, coverage barely moves, and the name that vanished
is the name of a case nobody is looking for. A test suite that quietly gets
smaller is the one failure a test suite cannot report about itself.
"""

from __future__ import annotations

import ast
import collections
import pathlib

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _duplicates_in(path: pathlib.Path) -> list[str]:
    """Every name defined more than once at the same level in one file."""
    tree = ast.parse(path.read_text())
    seen: collections.Counter[tuple[int, str]] = collections.Counter()

    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                seen[(id(node), child.name)] += 1

    return sorted({name for (_scope, name), count in seen.items() if count > 1})


def test_no_test_file_defines_a_name_twice() -> None:
    """The second definition wins and the first is gone."""
    offenders = [
        f"{path.relative_to(ROOT.parent)}: {names}"
        for path in sorted(ROOT.rglob("*.py"))
        for names in [_duplicates_in(path)]
        if names
    ]

    assert not offenders, (
        f"these files define a name twice, so the first definition is "
        f"discarded and whatever it tested is no longer tested: {offenders}"
    )


def test_the_gate_still_finds_a_collision() -> None:
    """A scanner matching nothing is indistinguishable from a clean tree."""
    sample = pathlib.Path(__file__).parent / "_collision_sample.py"
    sample.write_text(
        "def test_one():\n    pass\n\n\ndef test_one():\n    pass\n\n\ndef test_two():\n    pass\n"
    )
    try:
        assert _duplicates_in(sample) == ["test_one"]
    finally:
        sample.unlink()
