"""A step definition no scenario can reach is dead code wearing coverage.

THE FAILURE THIS EXISTS TO STOP. pytest-bdd resolves a step against the module
that bound the scenario. A step defined in `test_next_wait_steps.py` is reachable
only from the feature that module calls `scenarios()` on. Define one for a
sentence that feature never says, and nothing anywhere complains: not pytest, not
pytest-bdd, not a green suite. The file grows, it reads as though the behaviour
is covered, and the step has never run.

FOUND 2026-09-21, by counting rather than by reading. Six step files carried 70
such definitions between them, `test_travels_together_steps.py` alone holding 19
against a feature that asks for 6 sentences. They arrived during the fold: the
scenarios moved to the feature of the function that owns them, and the step
definitions stayed behind, most of them calling `pytest.skip` with a message
naming the function that really owns the behaviour. A reader opening the file saw
twenty-three steps and a feature with six.

WHY IT IS WORSE THAN AN EMPTY FILE. An unwritten step announces itself the first
time a scenario runs. This one asserts that a sentence is handled, and the
sentence is never asked.

THIS IS THE SAME CLASS AS test_every_feature_is_bound.py, one axis over. There, a
feature nobody binds looks covered. Here, a step nobody can reach looks used.
Both are answerable only by asking a question nobody thought to ask.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_BDD = _ROOT / "tests" / "bdd"
_STEPS = _BDD / "steps"

_KEYWORDS = ("Given ", "When ", "Then ", "And ", "But ")
_DECORATORS = frozenset({"given", "when", "then"})

# name -> why it ships unreachable. Empty is the goal, and it only goes down.
KNOWN_UNREACHABLE: dict[str, int] = {}


@pytest.fixture(scope="module")
def step_files() -> dict[pathlib.Path, str]:
    """Every step module, as path to text."""
    return {p: p.read_text(encoding="utf-8") for p in sorted(_STEPS.rglob("test_*.py"))}


def _features_bound_by(module: pathlib.Path, tree: ast.AST) -> list[pathlib.Path]:
    """The feature files this module calls `scenarios()` on.

    `scenarios("../features")` binds a whole directory, so a call is resolved to
    a path and then expanded rather than compared by name.
    """
    targets = [
        (module.parent / node.args[0].value).resolve()
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "scenarios"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ]
    found: list[pathlib.Path] = []
    for target in targets:
        found.extend(sorted(target.rglob("*.feature")) if target.is_dir() else [target])
    return found


def _sentences_in(feature: pathlib.Path) -> set[str]:
    """Every step sentence a feature asks for, keyword stripped."""
    return {
        line.strip().split(" ", 1)[1].strip()
        for line in feature.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith(_KEYWORDS)
    }


def _literal_steps(tree: ast.AST) -> list[str]:
    """Every step sentence this module defines as a plain string.

    A `parsers.parse(...)` step is left out on purpose: it matches a family of
    sentences, so whether any scenario reaches it cannot be settled by comparing
    text. The plain ones are where the dead weight collected.
    """
    return [
        decorator.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for decorator in node.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Name)
        and decorator.func.id in _DECORATORS
        and decorator.args
        and isinstance(decorator.args[0], ast.Constant)
        and isinstance(decorator.args[0].value, str)
    ]


def _unreachable(module: pathlib.Path, text: str) -> list[str]:
    tree = ast.parse(text)
    bound = _features_bound_by(module, tree)
    # A module that binds nothing defines steps for whoever imports it, and that
    # is a different question from this one.
    if not bound:
        # APPROVED-IF 2026-09-21 14:10 — a module binding no feature has no
        # feature to be measured against, so there is no table to look it up in.
        return []
    asked: set[str] = set()
    for feature in bound:
        asked |= _sentences_in(feature)
    return [sentence for sentence in _literal_steps(tree) if sentence not in asked]


def test_no_step_definition_is_unreachable(step_files):
    """A step its own feature never says has never run."""
    found = {
        module.name: len(dead)
        for module, text in step_files.items()
        if (dead := _unreachable(module, text))
    }
    over = {name: count for name, count in found.items() if count > KNOWN_UNREACHABLE.get(name, 0)}

    assert not over, (
        "these step modules define steps no scenario they bind can reach, so "
        "the steps have never run and the file reads as covering them:\n"
        + "\n".join(
            f"    {name}  {count} unreachable"
            + (f" (ratchet allows {KNOWN_UNREACHABLE[name]})" if name in KNOWN_UNREACHABLE else "")
            for name, count in sorted(over.items())
        )
        + "\n\nDelete the step, or move the scenario into the feature of the "
        "function that owns the behaviour and bind it there."
    )


def test_the_ratchet_holds_no_name_that_is_already_clean(step_files):
    """A ratchet entry for a file with nothing wrong hides the next regression."""
    live = {module.name: len(_unreachable(module, text)) for module, text in step_files.items()}
    stale = sorted(
        name for name, allowed in KNOWN_UNREACHABLE.items() if live.get(name, 0) < allowed
    )

    assert not stale, (
        f"these names allow more unreachable steps than they have: {stale}. "
        "Lower the number to what is measured, or take the name out."
    )


def test_this_gate_can_actually_see_the_steps(step_files):
    """A discovery bug would make the test above pass on an empty set."""
    assert len(step_files) >= 20, (
        f"only {len(step_files)} step modules found under {_STEPS}; the gate "
        "above would pass by examining almost nothing"
    )
    binding = [
        module for module, text in step_files.items() if _features_bound_by(module, ast.parse(text))
    ]
    assert len(binding) >= 15, (
        f"only {len(binding)} of {len(step_files)} modules resolve to a feature "
        "file; if `scenarios()` is spelled differently now, every module looks "
        "like it binds nothing and nothing is measured"
    )
    defining = sum(len(_literal_steps(ast.parse(text))) for text in step_files.values())
    assert defining >= 100, (
        f"only {defining} plain-string step definitions found across all "
        "modules, so the reader is no longer finding them"
    )
