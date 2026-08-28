"""Steps for cli/overwriting_a_snapshot.feature.

WHETHER THERE IS A TERMINAL IS AN ARGUMENT, so both cases can be put to the
dispatcher from a test that has no terminal. It used to call
`sys.stdin.isatty()` itself while carrying a marker that said it did no I/O,
which meant the no-terminal case could only be reached by faking stdin.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.cli.parts import _WHEN_OVERWRITING

scenarios("../features/cli/overwriting_a_snapshot.feature")


class Ctx(TypedDict, total=False):
    path: Path
    exists: bool
    ran: bool
    code: int
    printed: str


@pytest.fixture
def ctx() -> Ctx:
    return {"path": Path("x"), "exists": False, "ran": False, "code": -1, "printed": ""}


@given("a snapshot already exists")
def _given_exists(ctx, tmp_path):
    ctx["path"] = tmp_path / "snap.json"
    ctx["path"].write_text("{}")
    ctx["exists"] = True


@given("no snapshot exists")
def _given_absent(ctx, tmp_path):
    ctx["path"] = tmp_path / "snap.json"
    ctx["exists"] = False


def _consider(ctx: Ctx, *, a_terminal: bool, capsys: Any) -> None:
    async def _work() -> int:
        ctx["ran"] = True
        return 0

    async def _go() -> None:
        ctx["code"] = await _WHEN_OVERWRITING[ctx["exists"]](ctx["path"], _work, a_terminal)

    asyncio.run(_go())
    out = capsys.readouterr()
    ctx["printed"] = out.out + out.err


@when("the overwrite is considered with somebody at the terminal")
def _when_terminal(ctx, capsys, monkeypatch):
    # The `input()` this reaches is the real builtin, so the scenario answers
    # it the way a person would rather than replacing the function.
    monkeypatch.setattr("builtins.input", lambda _prompt: "n")
    _consider(ctx, a_terminal=True, capsys=capsys)
    ctx["asked"] = True


@when("the overwrite is considered with no terminal")
def _when_no_terminal(ctx, capsys, monkeypatch):
    def _refuses(_prompt: str) -> str:
        raise AssertionError(
            "a question was put to a run with no terminal; input() raises "
            "EOFError on a pipe, in CI and in cron"
        )

    monkeypatch.setattr("builtins.input", _refuses)
    _consider(ctx, a_terminal=False, capsys=capsys)
    ctx["asked"] = False


@then("they are asked")
def _then_asked(ctx):
    assert ctx["asked"], ctx


@then("nobody is asked")
def _then_not_asked(ctx):
    assert not ctx["asked"], ctx


@then("the exit code is 1")
def _then_one(ctx):
    assert ctx["code"] == 1, f"{ctx['code']}, printed: {ctx['printed']}"


@then("the work ran")
def _then_ran(ctx):
    assert ctx["ran"], "there was nothing in the way and the work did not run"
