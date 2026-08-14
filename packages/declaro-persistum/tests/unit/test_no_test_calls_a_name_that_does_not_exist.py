"""The tests must reference only names that exist, same as src.

`test_every_name_src_calls_exists` covers `src/`. It was written after two
class-deletion sweeps each removed a definition and left its caller, and it
would have caught both. The tests had no such gate, and the same thing had
happened there:

    tests/conftest.py
        @pytest.fixture
        def mock_pg_connection() -> MockAsyncPGConnection:
            return MockAsyncPGConnection()      # deleted; NameError

Two fixtures returning two classes that no longer exist, in the file every
test in the package imports. Nothing requested either fixture, so nothing
failed — which is the same reason the `create_bulk_loader` NameError shipped.

RUFF, NOT MYPY, BECAUSE THE PROPERTY IS THE SAME AND THE COST IS NOT. mypy over
`tests/` would demand annotations this suite does not have and report hundreds
of things that are not this. Ruff's F821 asks exactly one question: does this
name resolve. That is the whole property, and it holds at zero today.

THIS GATE HAD A HOLE UNTIL bc9a5e8. Ruff parses at `target-version = "py311"`
and three step files used 3.12 f-string syntax, so ruff could not read them at
all and reported a syntax error instead of their contents. A gate that cannot
parse a file cannot check it. The quotes are fixed, so all of `tests/` is now
actually read — asserted below rather than assumed, because an instrument that
silently skips its input is how the hollow test files went unnoticed.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.precommit

TESTS = Path(__file__).resolve().parents[1]


def _ruff(*args: str) -> str:
    if shutil.which("ruff") is None:
        pytest.skip("ruff is not installed")
    return subprocess.run(
        ["ruff", "check", "--output-format=concise", *args, str(TESTS)],
        capture_output=True,
        text=True,
        cwd=TESTS.parent,
    ).stdout


def test_every_test_file_can_actually_be_parsed() -> None:
    """A file the linter cannot read is a file the gate below cannot check."""
    unreadable = [
        line for line in _ruff("--select", "F821").splitlines()
        if "invalid-syntax" in line
    ]
    assert not unreadable, (
        "ruff cannot parse these, so nothing below has checked them:\n  "
        + "\n  ".join(unreadable)
    )


def test_no_test_references_a_name_that_does_not_exist() -> None:
    undefined = [
        line for line in _ruff("--select", "F821").splitlines()
        if " F821 " in line
    ]
    assert not undefined, (
        "these reference names that do not exist, so the line raises "
        "NameError the moment it runs:\n  " + "\n  ".join(undefined)
    )
