"""An `async def` gherkin step is a test that asserts on nothing.

MEASURED 2026-08-27, on twelve scenarios written that afternoon. pytest-bdd
calls a step function and does not await what it returns, so an `async def`
step leaves a coroutine nobody ran:

    RuntimeWarning: coroutine '_when_replicate' was never awaited

The `When` never happened, the `Then` read an untouched context, and seven
scenarios went red at once. They went red rather than green only because the
assertions expected something to have changed. A scenario whose Then asserts
that a list is EMPTY would have passed, and passed for the reason that the
code under test never ran at all.

THE FIX IS ONE LINE PER STEP: a synchronous step that runs the coroutine
itself with `asyncio.run`. This gate holds it, because the failure is silent
in exactly the shape a test is most often written -- asserting an absence.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

STEPS = pathlib.Path(__file__).resolve().parents[1] / "bdd" / "steps"

_STEP_DECORATORS = frozenset({"given", "when", "then"})


def _decorator_names(node: ast.AsyncFunctionDef) -> list[str]:
    """Every name a decorator could be written as, on one step function."""
    return [
        decorator.func.id
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name)
        else decorator.id
        if isinstance(decorator, ast.Name)
        else decorator.func.attr
        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
        else ""
        for decorator in node.decorator_list
    ]


def _async_steps(path: pathlib.Path) -> list[str]:
    """Every `async def` carrying a gherkin step decorator, as file:line name."""
    return [
        f"{path.name}:{node.lineno} {node.name}"
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.AsyncFunctionDef) and _STEP_DECORATORS & set(_decorator_names(node))
    ]


def test_no_step_is_declared_async() -> None:
    offenders = [
        offender for path in sorted(STEPS.rglob("*.py")) for offender in _async_steps(path)
    ]

    assert offenders == [], (
        "these gherkin steps are `async def`, so pytest-bdd never runs their "
        "body and every assertion after them reads an untouched context: "
        f"{offenders}. Make the step synchronous and call `asyncio.run` in it."
    )


def test_the_scanner_finds_one_when_there_is_one(tmp_path) -> None:
    """The gate's own horizon. A scan that finds nothing must be able to find.

    Without this, "zero offenders" and "the scan matched nothing" are the
    same green, which is the failure this package has already paid for.
    """
    planted = tmp_path / "test_planted_steps.py"
    planted.write_text(
        "from pytest_bdd import when\n\n\n"
        '@when("something happens")\n'
        "async def _when_something(ctx):\n"
        "    return None\n"
    )

    found = _async_steps(planted)

    assert len(found) == 1, f"the scanner cannot see an async step: {found}"
    assert "_when_something" in found[0]
