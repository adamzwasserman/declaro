"""`@boundary` must mean the function touches the world. 214 of 264 did not.

WHAT THE MARKER IS FOR. `boundary.py` says it: a function carrying it may open
a connection, read a file, take the clock, and catch what those raise. A
function without it may not, and `honest-check` reads the marker by name to
decide which is which. It is the package's written record of where it meets
the world.

WHAT IT WAS. Measured 2026-08-25 by the Slop Audit's clause 4: 213 of our 264
`@boundary` markers sat on functions that obtain nothing outside the process.
Four in five said an edge was there and it was not, so the record said nothing
at all. The same run reported "31 boundary declarations withheld a finding" --
the checker declined to flag 31 things because a marker claimed they were
edges. 152 came off the same day, leaving 61 and 129 markers.

THE TWO CHECKERS DISAGREE ABOUT 34 OF THE 61, and that is the reason this is a
ratchet rather than a gate at zero. Slop Audit asks whether a function
*obtains* anything from outside; honest-check asks whether it performs I/O at
all, and counts `print` as I/O. So 34 functions here print and nothing else:
honest-check requires `@boundary` on them, and Slop Audit calls that marker
false. Removing it fails honest-check; keeping it fails Slop Audit. Neither
tool is wrong about its own question, and the disagreement is theirs to settle
rather than ours to paper over.

The other 27 have not been looked at.

HOW THE 152 WERE FOUND SAFE TO REMOVE. Not by trusting one tool. Every marker
Slop Audit flagged was demoted, honest-check was run over the result, and
every function it objected to had its marker restored: 28 of them, doing real
I/O the audit had not seen -- `asyncpg.connect`, `aiosqlite.connect`, `input`,
`uuid.uuid4` -- or catching an exception, which is also a boundary privilege.
Two checkers, each used to bound the other.

THE COUNT COMES FROM THE TOOL, not from a second implementation of the same
question here. Deciding "does this function reach any I/O" needs the call
graph the analyzer already walks, and one measure with two owners is how this
package spent a week believing a file was 1113 lines when its own analyzer
said 336.
"""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

# 2026-08-25: 213, then 61 the same day. Only ever downwards.
BASELINE = 61

_AUDIT = shutil.which("slop-audit-l1") or str(
    pathlib.Path.home() / ".local/bin/slop-audit-l1"
)


def _decorations_that_state_an_edge_that_is_not_there() -> list[str]:
    """Every `@boundary` the analyzer says obtains nothing outside the process."""
    files = sorted(str(path) for path in SRC.rglob("*.py"))
    finished = subprocess.run(  # noqa: S603
        [_AUDIT, "--honest-code", *files],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return [
        line
        for line in finished.stderr.splitlines()
        if re.search(r"L1\.21\.4 .*declares itself a boundary", line)
    ]


@pytest.mark.skipif(
    not pathlib.Path(_AUDIT).exists(),
    reason=f"the analyzer is not installed at {_AUDIT}",
)
def test_no_more_boundary_markers_than_the_baseline() -> None:
    """The ratchet. Lower `BASELINE` when you remove some; never raise it."""
    offenders = _decorations_that_state_an_edge_that_is_not_there()

    assert len(offenders) <= BASELINE, (
        f"{len(offenders)} `@boundary` markers sit on functions that touch "
        f"nothing, up from {BASELINE}. A marker that names an edge which is "
        f"not there tells honest-check to stop asking about that function. "
        f"New since the baseline: {offenders[BASELINE:][:5]}"
    )


@pytest.mark.skipif(
    not pathlib.Path(_AUDIT).exists(),
    reason=f"the analyzer is not installed at {_AUDIT}",
)
def test_the_baseline_is_not_slack() -> None:
    """A baseline left above the real count stops ratcheting silently.

    It would pass every run while the count crept back up underneath it, which
    is the failure mode of every ratchet nobody tightens.
    """
    offenders = _decorations_that_state_an_edge_that_is_not_there()

    assert len(offenders) == BASELINE, (
        f"the real count is {len(offenders)} and BASELINE says {BASELINE}. "
        f"Set BASELINE to {len(offenders)}: a ratchet with slack in it is a "
        f"ratchet that has stopped."
    )
