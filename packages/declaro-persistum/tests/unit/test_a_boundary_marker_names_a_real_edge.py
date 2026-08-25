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

WHERE 14 COMES FROM, on the build named below. Eight are the error-handling
layer directly above the I/O: the reader inspects one function body and does
not follow a call into a function that is itself a declared `@boundary`, so it
sees nothing in a function whose whole job is catching what that boundary
raised. Six call process-level things it does not count: `os.environ.get`,
`asyncio.run`, `asyncio.create_task`, `asyncio.get_running_loop`,
`uuid.uuid4`, and `importlib` loading a file off disk and executing it.

ALL 14 ARE LOAD-BEARING FOR honest-check, which grants a boundary three
privileges -- I/O, catching, and non-determinism -- and refuses to let a
non-boundary catch at all. A function that only catches must carry the marker
for one checker and must not for the other. Removing any of the 14 trades this
finding for an HC-P002. Both were told, 2026-08-25.

SO 14 IS THE FLOOR while the two disagree, not a to-do list. If it drops,
either the analyzer learned to see one of these or somebody removed a marker
that was carrying its weight, and those want different responses.

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
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "src"

# 2026-08-25: 213, then 61, then 14, then 7, all on one unchanged tree. Only
# the analyzer moved. Record which build a baseline was set against, because
# without it a number here cannot be told apart from a number about the code.
#
# The drop to 7 is the analyzer learning that a function a dispatch table holds
# is a function something calls. It could not see that before, and this package
# reaches most of its interior exactly that way.
BASELINE = 7
BASELINE_BUILD = "2ac0fed, built 2026-08-25 14:10"

# THE OTHER HALF OF THE SAME CLAUSE, which the same change made visible: a
# function that does I/O and is called from elsewhere in its own file, so the
# I/O is in the interior rather than at the edge. 26 of these appeared the
# moment the reader could follow a table, and 24 are `logger` calls in the
# migration and transfer pipelines. Rule 4 names logging as I/O, so they are
# real; moving them out is a restructure and not a rename.
INTERIOR_IO_BASELINE = 26

_AUDIT = shutil.which("slop-audit-l1") or str(
    pathlib.Path.home() / ".local/bin/slop-audit-l1"
)


def _clause_four() -> list[str]:
    """Every clause 4 finding the analyzer reports on this package's source."""
    files = sorted(str(path) for path in SRC.rglob("*.py"))
    finished = subprocess.run(  # noqa: S603
        [_AUDIT, "--honest-code", *files],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    return [line for line in finished.stderr.splitlines() if "L1.21.4 " in line]


def _decorations_that_state_an_edge_that_is_not_there() -> list[str]:
    """Every `@boundary` the analyzer says obtains nothing outside the process."""
    return [
        line for line in _clause_four() if "declares itself a boundary" in line
    ]


def _io_in_the_interior() -> list[str]:
    """Every function doing I/O that something else in its own file calls."""
    return [line for line in _clause_four() if "performs I/O" in line]


@pytest.mark.skipif(
    not pathlib.Path(_AUDIT).exists(),
    reason=f"the analyzer is not installed at {_AUDIT}",
)
def test_no_more_boundary_markers_than_the_baseline() -> None:
    """The ratchet. Lower `BASELINE` when you remove some; never raise it."""
    offenders = _decorations_that_state_an_edge_that_is_not_there()

    assert len(offenders) <= BASELINE, (
        f"{len(offenders)} `@boundary` markers sit on functions the analyzer "
        f"sees no I/O in, up from {BASELINE} set against {BASELINE_BUILD}. "
        f"CHECK THE ANALYZER BEFORE THE CODE: this number has moved three "
        f"times on an unchanged tree because the tool was rebuilt underneath "
        f"it. `ls -la ~/.local/share/uv/tools/slop-audit-l1` gives the build "
        f"date and `--version` does not. New since the baseline: "
        f"{offenders[BASELINE:][:5]}"
    )


@pytest.mark.skipif(
    not pathlib.Path(_AUDIT).exists(),
    reason=f"the analyzer is not installed at {_AUDIT}",
)
def test_no_more_interior_io_than_the_baseline() -> None:
    """The other half. Rule 4 puts I/O at the edge, and logging is I/O."""
    offenders = _io_in_the_interior()

    assert len(offenders) <= INTERIOR_IO_BASELINE, (
        f"{len(offenders)} functions do I/O and are called from elsewhere in "
        f"their own file, up from {INTERIOR_IO_BASELINE} set against "
        f"{BASELINE_BUILD}. New: {offenders[INTERIOR_IO_BASELINE:][:5]}"
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

    assert (len(offenders), len(_io_in_the_interior())) == (
        BASELINE,
        INTERIOR_IO_BASELINE,
    ), (
        f"the real count is {len(offenders)} and BASELINE says {BASELINE}, "
        f"set against {BASELINE_BUILD}. Either somebody fixed some, or the "
        f"analyzer was rebuilt. This assertion caught the rebuild twice "
        f"today, which is the only reason the wrong figure did not go into a "
        f"report. Set BASELINE to {len(offenders)} and record the build."
    )
