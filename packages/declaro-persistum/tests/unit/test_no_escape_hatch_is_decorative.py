"""Every `# type: ignore` must actually suppress something.

ELEVEN OF THE TWENTY IN src SUPPRESSED NOTHING. mypy reports each one as
`unused-ignore` and has been doing so the whole time.

An escape that suppresses nothing is worse than no escape, because it is a sign
that lies. It tells the next reader "this line is hard, the type system cannot
express it, do not go poking" about a line the type system is perfectly happy
with. Four of them sat on `columns[x]["unique"] = True` and its neighbours,
which type-check cleanly.

THIS IS HOW THE bulk_loader BUG STAYED HIDDEN. `create_bulk_loader` carried
`# type: ignore[return-value]` on both branches. Neither suppressed the real
error — mypy reported `Name "PostgreSQLBulkLoader" is not defined` regardless,
and flagged one of the two ignores as unused. The comments read as "someone
looked at this and decided it was fine", when what they meant was "someone
pasted an escape here once and the code has changed underneath it since".

A LOAD-BEARING ESCAPE IS FINE AND STAYS. `writers.py` has none left, but
`WriteOne.conn: Any` is a declared, explained looseness in the one place the
type system genuinely cannot express a correlation. The rule is not "no
escapes". It is that an escape must be doing the job it claims.

Narrow on purpose, like the name gate beside it: this asserts only that no
escape is decorative, so it holds at zero from today without first resolving
every other complaint mypy has.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.precommit

SRC = Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"


def _mypy_lines() -> list[str]:
    if shutil.which("mypy") is None:
        pytest.skip("mypy is not installed")
    return subprocess.run(
        ["mypy", "--no-error-summary", "--warn-unused-ignores", str(SRC)],
        capture_output=True,
        text=True,
        cwd=SRC.parents[1],
    ).stdout.splitlines()


def test_the_gate_can_see_the_source_at_all() -> None:
    """A gate that reads nothing passes forever."""
    assert SRC.is_dir(), f"{SRC} is not a directory"
    assert len(list(SRC.rglob("*.py"))) > 20, "found almost no source to check"


def test_no_type_ignore_suppresses_nothing() -> None:
    decorative = [line for line in _mypy_lines() if "[unused-ignore]" in line]
    assert not decorative, (
        "these escapes suppress nothing, so each is a sign telling the next "
        "reader the line is hard when it is not:\n  " + "\n  ".join(decorative)
    )
