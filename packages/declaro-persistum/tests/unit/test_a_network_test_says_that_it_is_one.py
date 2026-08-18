"""A test that reaches a live remote must say so in its marker.

`uv run pytest` on this machine makes network calls to a real Turso Cloud
database, and nothing at the top level says so. Several tests parse `.env`
themselves and open a replica against the primary it names -- which is how the
cloud path came to be tested at all, and is a good thing -- but a marker is
the only place a reader learns which tests those are before running them.

`turso` is not that marker. It means "requires Turso", and most tests carrying
it open a LOCAL file and touch no network. Selecting `-m "not turso"` to avoid
the remote also throws away the local ones, and selecting `-m turso` to run
the remote ones drags in dozens that do not.

So `network` is a marker of its own, and this gate keeps it honest in both
directions: a test that names TEST_TURSO_URL carries it, and nothing else
does.

WHAT THIS IS NOT. It does not stop the suite touching the remote, and it
should not: the cloud path is the one that had never been executed, and
covering it found a 155-line command nobody had run. It makes the fact
selectable, so `-m "not network"` is a complete answer.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

TESTS = pathlib.Path(__file__).resolve().parent.parent
REMOTE_VARIABLES = ("TEST_TURSO_URL", "TEST_TURSO_AUTH_TOKEN")


def _reaches_a_remote(path: pathlib.Path) -> bool:
    """A file that READS the remote credentials opens against a primary.

    PARSED, NOT GREPPED. My first version matched any mention, and
    `test_replication_steps.py` names both variables in comments recording a
    measurement taken against the remote in August. It reaches no network. A
    detector that counts prose reports the record of a thing as the thing --
    the third time today.
    """
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "attr", None) not in ("get", "environ"):
            continue
        for argument in node.args:
            if (
                isinstance(argument, ast.Constant)
                and argument.value in REMOTE_VARIABLES
            ):
                return True
    return False


def _marked_network(path: pathlib.Path) -> bool:
    """Carrying the marker, or bound to scenarios tagged `@network`.

    A pytest-bdd step file has no `def test_` to decorate: its tests come from
    the feature file, and pytest-bdd turns a Gherkin tag into a pytest marker.
    So `@network` on the scenario is how a step file carries it, and a gate
    that only looked for the decorator reported the one file that does this
    correctly.
    """
    source = path.read_text()
    if "pytest.mark.network" in source:
        return True

    for match in re.finditer(r'scenarios\(\s*"([^"]+)"', source):
        feature = (path.parent / match.group(1)).resolve()
        if feature.exists() and "@network" in feature.read_text():
            return True
    return False


def _test_files() -> list[pathlib.Path]:
    # This file names the marker throughout and reads no credential, so it
    # would report itself. A gate that cannot see past its own text is the
    # third detector today to count prose as code.
    return [
        p
        for p in sorted(TESTS.rglob("test_*.py"))
        if p != pathlib.Path(__file__).resolve()
    ]


def test_the_gate_can_see_the_files_at_all() -> None:
    assert len(_test_files()) > 50, len(_test_files())


def test_every_test_that_opens_a_remote_says_so() -> None:
    """`-m "not network"` has to be a complete answer, or it is no answer."""
    unmarked = [
        str(p.relative_to(TESTS))
        for p in _test_files()
        if _reaches_a_remote(p) and not _marked_network(p)
    ]

    assert not unmarked, (
        f"these open against a live remote and do not carry "
        f"`pytest.mark.network`, so nobody can exclude them: {unmarked}"
    )


def test_nothing_claims_the_marker_without_earning_it() -> None:
    """A marker that spreads means `-m network` stops selecting the remote."""
    overclaimed = [
        str(p.relative_to(TESTS))
        for p in _test_files()
        if _marked_network(p) and not _reaches_a_remote(p)
    ]

    assert not overclaimed, overclaimed


def test_the_marker_is_declared() -> None:
    """An undeclared marker warns on every run and is a typo away from
    silently selecting nothing."""
    pyproject = (TESTS.parent / "pyproject.toml").read_text()

    assert "network:" in pyproject, "network is not in the markers list"


def test_the_local_turso_tests_are_not_swept_up() -> None:
    """The reason `turso` could not serve as this marker.

    Most tests carrying it open a local file. If excluding the network also
    excluded those, nobody would exclude the network.
    """
    local_turso = [
        p
        for p in _test_files()
        if "pytest.mark.turso" in p.read_text() and not _reaches_a_remote(p)
    ]

    assert local_turso, "no local-only Turso tests found, so the split is moot"


# ---------------------------------------------------------------------------
# Per scenario, not per file
# ---------------------------------------------------------------------------
#
# FOUND BY A MUTATION CONTROL. `_marked_network` asks whether a feature file
# contains `@network` anywhere, so removing the tag from ONE of two scenarios
# left the gate green. A file-level answer to a per-scenario question.


def _remote_steps() -> set[str]:
    """Step texts whose implementation reads the remote credentials."""

    found: set[str] = set()
    for path in TESTS.rglob("test_*_steps.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            body = ast.unparse(node)
            if not any(v in body for v in REMOTE_VARIABLES):
                # The credentials may come from a helper this step calls.
                if "_real_primary" not in body or node.name.startswith("_real"):
                    continue
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call):
                    continue
                for argument in decorator.args:
                    if isinstance(argument, ast.Constant) and isinstance(
                        argument.value, str
                    ):
                        found.add(argument.value)
    return found


def _scenarios_with(feature: pathlib.Path, steps: set[str]) -> list[tuple[str, bool]]:
    """Each scenario using one of `steps`, and whether it is tagged."""
    # A TAG SITS ABOVE THE SCENARIO IT APPLIES TO, so it has to be held until
    # the Scenario line arrives. My first version cleared the flag ON that
    # line, which threw away the tag it had just read and reported a correctly
    # tagged scenario as untagged.
    out: list[tuple[str, bool]] = []
    name: str | None = None
    tagged = False
    uses = False
    pending = False
    for line in feature.read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("@"):
            pending = pending or "@network" in stripped
            continue
        if stripped.startswith("Scenario:"):
            if name is not None and uses:
                out.append((name, tagged))
            name = stripped[len("Scenario:") :].strip()
            tagged, uses, pending = pending, False, False
            continue
        if name and any(step in stripped for step in steps):
            uses = True
    if name is not None and uses:
        out.append((name, tagged))
    return out


def test_every_scenario_that_opens_a_remote_is_tagged() -> None:
    """A tag on one scenario does not cover the one beside it."""
    steps = _remote_steps()
    assert steps, "no remote-opening steps found, so this gate checks nothing"

    untagged = [
        f"{feature.name}: {name}"
        for feature in TESTS.rglob("*.feature")
        for name, tagged in _scenarios_with(feature, steps)
        if not tagged
    ]

    assert not untagged, (
        f"these scenarios open against a live remote and carry no @network "
        f"tag, so `-m \"not network\"` still runs them: {untagged}"
    )
