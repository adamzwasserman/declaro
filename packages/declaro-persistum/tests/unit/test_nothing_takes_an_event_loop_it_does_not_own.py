"""A loop you did not open is a loop you cannot trust, or close.

TWO DEFECTS OF THIS SHAPE IN ONE DAY, both found through the Slop Audit
reporting test determinism at 4 of 5.

The first was production code. `shutdown.py` asked `asyncio.get_event_loop()`
for a loop to run the SIGTERM replication on. That call answers with whatever
loop object is lying around, including a closed one, and creates one when there
is none. A closed loop reports `is_running() == False`, which the handler read
as "stopped, run the work here", and `run_until_complete` on a closed loop
raises. On ephemeral disk that raises at the one moment it costs data.

The second was a test. It built a loop with
`asyncio.get_event_loop_policy().new_event_loop()` and never closed it, so the
loop was finalised whenever the collector next ran and its `__del__` raised
`ValueError: Invalid file descriptor: -1`. pytest attributed that to whichever
unrelated test was running at the time, and whether it surfaced at all varied
between runs. Each test directory was clean alone and the full suite was not.

WHAT IS ALLOWED. `asyncio.run`, which closes what it opens.
`asyncio.get_running_loop`, which returns a running loop or raises and can do
neither wrong thing. And `new_event_loop` inside a fixture that closes it,
which is how the BDD steps give a scenario a loop of its own.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[2]

# Never, anywhere. It hands back a loop somebody else owns.
_REFUSED = {"get_event_loop", "get_event_loop_policy"}

# Allowed only where the same function closes what it made.
_OWNED_IF_CLOSED = "new_event_loop"

_ALLOWED = {pathlib.Path(__file__).name}


def _calls(tree: ast.AST) -> list[tuple[int, str]]:
    return [
        (node.lineno, node.func.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    ]


def _offenders() -> list[str]:
    found = []
    for path in sorted([*(ROOT / "src").rglob("*.py"), *(ROOT / "tests").rglob("*.py")]):
        if path.name in _ALLOWED:
            continue
        text = path.read_text()
        tree = ast.parse(text)
        where = path.relative_to(ROOT)

        found += [
            f"{where}:{line} asyncio.{name}" for line, name in _calls(tree) if name in _REFUSED
        ]
        # A loop made here must be closed here. `.close()` anywhere in the same
        # function is the coarse version of that and is what a fixture does.
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            names = [n for _, n in _calls(node)]
            if _OWNED_IF_CLOSED in names and "close" not in names:
                found.append(f"{where}:{node.lineno} {node.name} never closes it")
    return found


def test_no_loop_is_taken_from_somewhere_else() -> None:
    """Use `asyncio.run`, or open one and close it."""
    offenders = _offenders()

    assert not offenders, (
        f"these take an event loop they do not own, which has produced two "
        f"defects here: a SIGTERM handler that raised instead of replicating, "
        f"and a leak that made the suite's result vary between runs. "
        f"{offenders}"
    )


def test_the_gate_still_finds_what_it_looks_for() -> None:
    """A scanner matching nothing is indistinguishable from a clean tree."""
    sample = ast.parse(
        "import asyncio\n"
        "def leaks():\n"
        "    loop = asyncio.new_event_loop()\n"
        "    loop.run_until_complete(work())\n"
        "def borrows():\n"
        "    return asyncio.get_event_loop()\n"
    )
    borrowed = [n for _, n in _calls(sample) if n in _REFUSED]
    leaked = [
        node.name
        for node in ast.walk(sample)
        if isinstance(node, ast.FunctionDef)
        and _OWNED_IF_CLOSED in [n for _, n in _calls(node)]
        and "close" not in [n for _, n in _calls(node)]
    ]

    assert borrowed == ["get_event_loop"], borrowed
    assert leaked == ["leaks"], leaked
