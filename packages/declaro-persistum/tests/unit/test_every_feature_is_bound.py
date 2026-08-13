"""Every .feature file must be bound by exactly one `scenarios()` call.

THE FAILURE THIS EXISTS TO STOP, stated as the shape rather than the instance:

A Gherkin file is prose. It only becomes a test when a step file calls
`scenarios()` on it. Nothing in pytest-bdd, pytest, or CI notices when nobody
does. An unbound feature file is indistinguishable from a bound one by reading
it, by the file tree, or by a green suite — it simply never runs, and the suite
reports success without it.

That is worse than having no file. A missing feature announces itself the first
time someone looks for it. An unbound one asserts that the behaviour is
specified AND covered, and both halves are false.

FOUND ON THIS BRANCH, 2026-08-13, by counting rather than by reading:

    schema/materialized_views.feature    20 scenarios    bound by 0
    database/multi_backend.feature        5 scenarios    bound by 0

25 scenarios that have never executed. `database_steps.py` defines steps for
multi_backend and calls `scenarios()` nowhere at all, so it looks like a bound
feature from every angle except the one that matters.

THE OTHER DIRECTION IS ALSO A DEFECT, and it has happened here too: on the
sibling branch `pragma_compat.feature` was bound TWICE, by two different step
files. Every scenario ran twice, which wastes time and — worse — means a step
definition collision resolves silently to whichever module pytest imported
last. Exactly once is the invariant, not at least once.

THIS IS THE SAME CLASS AS test_every_module_is_reachable.py. There, a module
with tests but no production caller looked alive. Here, a feature file with
steps but no `scenarios()` call looks bound. Both are things that are only
verifiable by asking a question nobody thought to ask, so the fix in both cases
is a test that asks it every run.

KNOWN_UNBOUND is a RATCHET, not an allowlist to grow. A name in it is a
decision to ship a specification that does not execute, with the reason
written down. Removing one means the file was bound or deleted.
"""

from __future__ import annotations

import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_FEATURES = _ROOT / "tests" / "bdd" / "features"
_STEPS = _ROOT / "tests" / "bdd"

# name -> why it ships unexecuted. Empty is the goal.
KNOWN_UNBOUND: dict[str, str] = {}

# `scenarios("../features/x.feature")` and `scenarios("../features")` both bind.
# The second form binds a whole DIRECTORY, which is why the resolver below has
# to compare directories and not only filenames.
_SCENARIOS_CALL = re.compile(r"""scenarios\(\s*['"]([^'"]+)['"]""")


def _feature_files() -> list[pathlib.Path]:
    return sorted(_FEATURES.rglob("*.feature"))


def _bindings() -> dict[pathlib.Path, list[pathlib.Path]]:
    """feature file -> the step files that bind it."""
    bound: dict[pathlib.Path, list[pathlib.Path]] = {f: [] for f in _feature_files()}
    for step_file in _STEPS.rglob("*.py"):
        text = step_file.read_text(encoding="utf-8")
        for raw in _SCENARIOS_CALL.findall(text):
            target = (step_file.parent / raw).resolve()
            for feature in bound:
                if target == feature or (
                    target.is_dir() and target in feature.parents
                ):
                    bound[feature].append(step_file)
    return bound


def _scenario_count(path: pathlib.Path) -> int:
    return len(
        [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("Scenario")
        ]
    )


def test_no_feature_file_is_unbound():
    """A specification nobody runs is prose claiming to be a test."""
    bound = _bindings()
    unbound = [
        f for f, binders in bound.items()
        if not binders and f.name not in KNOWN_UNBOUND
    ]

    assert not unbound, (
        "these .feature files are bound by no scenarios() call and have NEVER "
        "executed — they read as specified-and-covered while asserting "
        "nothing:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  ({_scenario_count(f)} scenarios)"
            for f in sorted(unbound)
        )
        + "\n\nBind it from a step file, delete it, or add it to KNOWN_UNBOUND "
        "with the reason it ships unexecuted."
    )


def test_no_feature_file_is_bound_twice():
    """Twice is not safer than once; it is a silent step-collision.

    Two step files calling scenarios() on one feature run every scenario
    twice, and a step defined in both resolves to whichever module pytest
    imported last — which is import order, not intent.
    """
    doubled = {
        f: binders for f, binders in _bindings().items() if len(binders) > 1
    }

    assert not doubled, (
        "these .feature files are bound more than once, so their scenarios run "
        "repeatedly and any duplicated step resolves by import order:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  bound by "
            + ", ".join(b.name for b in binders)
            for f, binders in sorted(doubled.items())
        )
    )


def test_the_ratchet_can_actually_see_a_feature_file():
    """The guard above is worthless if it finds nothing to guard.

    A discovery bug — wrong directory, wrong glob — would make both tests pass
    by examining an empty set. That is the vacuous-pass shape this package has
    shipped repeatedly, so the ratchet asserts its own reach.
    """
    features = _feature_files()
    assert features, f"no .feature files found under {_FEATURES}"
    assert any(_scenario_count(f) > 0 for f in features), (
        "every feature file parsed as zero scenarios, so the counter is broken "
        "and the reports above would be meaningless"
    )
