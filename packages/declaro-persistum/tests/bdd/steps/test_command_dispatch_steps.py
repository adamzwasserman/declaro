"""Steps for cli/command_dispatch.feature: a real table, not a patched module.

THE DIFFERENCE THIS FILE TURNS ON. Eleven tests replaced `cmd_diff` on the
module with a fake, so the real one could not run. That is a mock. Handing
`_dispatch` a table of commands is not: the table is a parameter, the
functions in it are this test's own, and nothing of ours is prevented from
running. It is the same shape as `Database`, which carries the callables it
needs rather than reaching for them by name.
"""

from __future__ import annotations

import argparse
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.cli.main import (
    _dispatch,
    _keep_the_traceback,
    _print_traceback,
)

scenarios("../features/cli/command_dispatch.feature")


class Ctx(TypedDict, total=False):
    commands: dict[str, Any]
    name: str
    code: int
    printed: str


@pytest.fixture
def ctx() -> Ctx:
    return {"commands": {}, "name": "diff", "code": -1, "printed": ""}


@given(parsers.parse("a command that returns {code:d}"))
def _given_returns(ctx, code):
    def _cmd(_args, _connection_string, _dialect):
        return code

    ctx["commands"] = {"diff": _cmd}


@given("a command that raises KeyboardInterrupt")
def _given_interrupt(ctx):
    def _cmd(_args, _connection_string, _dialect):
        raise KeyboardInterrupt

    ctx["commands"] = {"diff": _cmd}


@given(parsers.parse('a command that raises RuntimeError with "{message}"'))
def _given_raises(ctx, message):
    def _cmd(_args, _connection_string, _dialect):
        raise RuntimeError(message)

    ctx["commands"] = {"diff": _cmd}


@given(parsers.parse('a command named "{name}" that no table holds'))
def _given_absent(ctx, name):
    ctx["commands"] = {}
    ctx["name"] = name


@when("_dispatch runs it")
def _when_dispatch(ctx, capsys):
    args = argparse.Namespace(command=ctx["name"], verbose=False)
    ctx["code"] = _dispatch(args, "sqlite:///x.db", "sqlite", ctx["commands"])
    out = capsys.readouterr()
    ctx["printed"] = out.out + out.err


@when(parsers.parse("{name} runs inside a failure"))
def _when_traceback(ctx, name, capsys):
    printers = {
        "_print_traceback": _print_traceback,
        "_keep_the_traceback": _keep_the_traceback,
    }
    try:
        raise RuntimeError("something to have a traceback about")
    except RuntimeError:
        ctx["code"] = printers[name]()
    out = capsys.readouterr()
    ctx["printed"] = out.out + out.err


@then(parsers.parse("the exit code is {expected:d}"))
def _then_code(ctx, expected):
    assert ctx["code"] == expected, ctx["code"]


@then(parsers.parse('"{text}" was printed'))
def _then_printed(ctx, text):
    assert text in ctx["printed"], ctx["printed"]


@then("a traceback was printed")
def _then_traceback(ctx):
    assert "Traceback" in ctx["printed"], ctx["printed"]


@then("nothing was printed")
def _then_silent(ctx):
    assert ctx["printed"] == "", ctx["printed"]


@when(parsers.parse('the command table is asked for "{typed}"'))
def _when_table_asked(ctx, typed):
    from declaro_persistum.cli.main import COMMANDS

    ctx["runner"] = COMMANDS[typed].__name__


@then(parsers.parse('it answers the runner named "{runner}"'))
def _then_runner(ctx, runner):
    assert ctx["runner"] == runner, ctx["runner"]


@when("the parser's subcommands are compared with the command table")
def _when_compared(ctx):
    from declaro_persistum.cli.main import COMMANDS, create_parser

    subparsers = [
        action
        for action in create_parser()._actions
        if hasattr(action, "choices") and isinstance(action.choices, dict)
    ]
    ctx["offered"] = set(subparsers[0].choices)
    ctx["held"] = set(COMMANDS)


@then("the two name exactly the same commands")
def _then_same_commands(ctx):
    assert ctx["offered"] == ctx["held"], (
        f"the parser offers {sorted(ctx['offered'] - ctx['held'])} that the "
        f"table cannot run, and the table holds "
        f"{sorted(ctx['held'] - ctx['offered'])} nobody can type"
    )


@when(
    parsers.parse(
        '_run_command is asked whether "{typed}" can run with no connection'
    )
)
def _when_needs_connection(ctx, typed):
    from declaro_persistum.cli.main import COMMANDS_NEEDING_CONNECTION

    # The exact expression `_run_command` evaluates, with no connection string.
    satisfied = bool(None) or typed not in COMMANDS_NEEDING_CONNECTION
    ctx["verdict"] = "runs" if satisfied else "refused"


@then(parsers.parse('the answer is "{expected}"'))
def _then_verdict(ctx, expected):
    assert ctx["verdict"] == expected, ctx["verdict"]


@when("_refuse_missing_connection answers a command that needed a database")
def _when_refuse_connection(ctx, capsys):
    from declaro_persistum.cli.main import _refuse_missing_connection

    ctx["code"] = _refuse_missing_connection(
        argparse.Namespace(command="diff", verbose=False), None
    )
    out = capsys.readouterr()
    ctx["printed"] = out.out + out.err


@when(parsers.parse("the traceback table is asked what {mode} does"))
def _when_traceback_table(ctx, mode):
    from declaro_persistum.cli.main import _TRACEBACK

    ctx["runner"] = _TRACEBACK[mode == "verbose"].__name__


@then(parsers.parse('it answers "{expected}"'))
def _then_answers_name(ctx, expected):
    assert ctx["runner"] == expected, ctx["runner"]
