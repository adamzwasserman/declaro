"""No `if`, `elif` or `else` without Adam's dated written approval.

THE RULE, given 2026-08-21. There must not be a single conditional anywhere in
this package unless Adam specifically approved that one, and the approval sits
in a comment carrying the date and time he gave it:

    # APPROVED-IF 2026-08-21 09:47 — why this cannot be a dispatch table
    if something:
        ...

WHY IT IS A RULE AND NOT A PREFERENCE. `umbra audit` on 2026-08-21 located
1721 silences in this package. 267 are unexercised branches, and
`cli/commands.py` holds 73 of them against its 80 conditionals. A branch
nothing takes is behaviour nobody has watched run, and it exists because the
code is deciding at runtime what should have been decided at declaration time.

A CONDITIONAL IS USUALLY THE SYMPTOM RATHER THAN THE DISEASE. The same audit
found input-region silences correlate with loose parameters at r = 0.749 and
with conditional count at only r = 0.383. `bulk_loader.py` has 6 conditionals
and 141 input-region silences, off 48 parameters that are Optional, a union,
untyped, or carrying a default. So when this gate refuses a branch, the answer
is nearly always a tighter type or a dispatch table at the boundary.

WHY A RATCHET RATHER THAN A HARD ZERO. 787 conditionals existed when the rule
was written and none were approved. A gate that simply failed would be red for
as long as that work takes, and a permanently red gate is one nobody reads.
This records the number instead and fails when it RISES, so the debt is
visible, cannot grow, and only ever comes down. Lowering the baseline is the
point; the test says so when you do.
"""

import ast
import pathlib

import pytest

_SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"

# The count when the rule was given. LOWER THIS as conditionals are removed.
# It must never be raised.
# 709 after a Python default's SQL spelling became a lookup on the exact
# type, instead of an if/elif chain whose order was load-bearing.
BASELINE = 683

_MARKER = "APPROVED-IF"


def _conditional_lines(tree: ast.Module) -> list[ast.If]:
    """Every `if` and `elif` node, which is every conditional keyword.

    An `elif` is an `If` sitting alone in another `If`'s `orelse`, so walking
    for `If` finds both. An `else` belongs to the `if` above it and is not
    counted separately: approving the `if` approves its whole shape.
    """
    return [node for node in ast.walk(tree) if isinstance(node, ast.If)]


def _approved(source_lines: list[str], node: ast.If) -> bool:
    """Whether a dated approval sits immediately above this conditional."""
    above = node.lineno - 2
    return above >= 0 and _MARKER in source_lines[above]


def _survey() -> tuple[int, int, dict[str, int]]:
    total = approved = 0
    per_file: dict[str, int] = {}
    for path in sorted(_SRC.rglob("*.py")):
        text = path.read_text()
        lines = text.splitlines()
        nodes = _conditional_lines(ast.parse(text))
        unapproved = [n for n in nodes if not _approved(lines, n)]
        total += len(nodes)
        approved += len(nodes) - len(unapproved)
        per_file[str(path.relative_to(_SRC))] = len(unapproved)
    return total, approved, per_file


def test_the_gate_still_finds_conditionals() -> None:
    """The floor. A scan matching nothing passes every other test here.

    Without this, a rename or a broken parse turns the gate silently green and
    it reads exactly like a codebase with no conditionals left in it.
    """
    total, _, _ = _survey()

    assert total > 0, (
        "the scan found no conditionals at all; either this package really has "
        "none, in which case delete this test and celebrate, or the scan is "
        "broken and every assertion below is meaningless"
    )


def test_no_conditional_is_added_without_approval() -> None:
    """The ratchet. The count may fall. It may not rise."""
    total, approved, per_file = _survey()
    unapproved = total - approved

    assert unapproved <= BASELINE, (
        f"{unapproved} unapproved conditionals, up from the {BASELINE} recorded "
        f"when the rule was given on 2026-08-21. Every `if` and `elif` needs "
        f"Adam's dated approval:\n"
        f"    # {_MARKER} YYYY-MM-DD HH:MM — why this cannot be a dispatch "
        f"table or a type\n"
        f"Worst files: "
        f"{sorted(per_file.items(), key=lambda kv: -kv[1])[:5]}"
    )


def test_the_baseline_is_not_stale() -> None:
    """When the count falls, the baseline comes down with it.

    A ratchet that is never re-tightened stops ratcheting: the debt shrinks,
    the recorded number does not, and the slack it leaves is room to add
    conditionals back without the gate noticing.
    """
    total, approved, _ = _survey()
    unapproved = total - approved

    assert unapproved >= BASELINE, (
        f"{unapproved} unapproved conditionals, below the recorded {BASELINE}. "
        f"Good. Set BASELINE to {unapproved} so the gate holds the new floor."
    )


@pytest.mark.parametrize("marker", [_MARKER])
def test_an_approval_needs_a_date_and_a_time(marker: str) -> None:
    """An approval without a timestamp is not the approval Adam asked for.

    The rule is a comment carrying the date and time he gave it. A bare
    marker would let anybody wave a branch through, which is the rule with its
    one enforcing detail removed.
    """
    import re

    stamped = re.compile(rf"{marker}\s+\d{{4}}-\d{{2}}-\d{{2}}\s+\d{{2}}:\d{{2}}")
    bad: list[str] = []
    for path in sorted(_SRC.rglob("*.py")):
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if marker in line and not stamped.search(line):
                bad.append(f"{path.relative_to(_SRC)}:{number}")

    assert not bad, (
        f"these approvals carry no date and time, so they approve nothing: {bad}"
    )
