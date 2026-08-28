"""Steps for cli/dialect_selection.feature: no fakes, because nothing here reaches out.

WHAT THIS REPLACES. Two tests in `test_cli_main.py` replaced a command handler
with a fake, ran the whole program, and read a variable the fake had captured,
to find out which engine had been chosen. The choice is `_chosen_dialect`, a
pure function over the parsed arguments and the connection string, so the fake
sat between the test and a value already being returned.

`argparse.Namespace` is the real type the real parser produces, not a stand-in
for one: `_chosen_dialect` reads `args.dialect` and nothing else off it.
"""

from __future__ import annotations

import argparse
from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.cli.main import (
    _chosen_dialect,
    _detect_from_url,
    _keep_given_dialect,
    _refuse_unknown_prefix,
    _the_named_dialect,
    detect_dialect,
    get_connection_string,
)

scenarios("../features/cli/dialect_selection.feature")


class Ctx(TypedDict, total=False):
    answer: Any
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"answer": None, "refusal": None}


def _call(ctx: Ctx, fn: Any, *args: Any) -> None:
    """Run it and keep whatever it did, because a refusal is an outcome here."""
    try:
        ctx["answer"] = fn(*args)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - the refusal is the answer
        ctx["answer"] = None
        ctx["refusal"] = refused


@when(parsers.parse('_keep_given_dialect is given "{given}" alongside the url "{url}"'))
def _when_keep_given(ctx, given, url):
    ctx["answer"] = _keep_given_dialect(given, url)


@when(parsers.parse('_detect_from_url is given no engine alongside the url "{url}"'))
def _when_detect(ctx, url):
    ctx["answer"] = _detect_from_url(None, url)


@when(parsers.parse('an operator types --dialect "{given}" with the url "{url}"'))
def _when_typed_dialect(ctx, given, url):
    ctx["answer"] = _chosen_dialect(argparse.Namespace(dialect=given), url)


@when(parsers.parse('an operator types no --dialect with the url "{url}"'))
def _when_no_dialect(ctx, url):
    ctx["answer"] = _chosen_dialect(argparse.Namespace(dialect=None), url)


@when("an operator types no --dialect with no url")
def _when_nothing(ctx):
    ctx["answer"] = _chosen_dialect(argparse.Namespace(dialect=None), None)


@when(parsers.parse('detect_dialect is given the url "{url}"'))
def _when_detect_dialect(ctx, url):
    _call(ctx, detect_dialect, url)


@when("_refuse_unknown_prefix is given no prefix matches at all")
def _when_refuse(ctx):
    # THE EMPTY LIST IS THE INPUT, not a placeholder. `detect_dialect` builds
    # a list of every prefix that matched and hands it on; empty is the case
    # this half answers.
    _call(ctx, _refuse_unknown_prefix, [])


@when(parsers.parse('_the_named_dialect is given the prefix matches "{named}"'))
def _when_named(ctx, named):
    _call(ctx, _the_named_dialect, [named])


@when(parsers.parse('an operator types the url "{typed}" with "{env}" in the environment'))
def _when_both(ctx, typed, env, monkeypatch):
    # SETTING AN ENVIRONMENT VARIABLE IS NOT A FAKE. `get_connection_string`
    # reads this variable in production, and this sets the real one; nothing
    # of ours is replaced.
    monkeypatch.setenv("DECLARO_DATABASE_URL", env)
    ctx["answer"] = get_connection_string(argparse.Namespace(connection=typed))


@when(parsers.parse('an operator types no url with "{env}" in the environment'))
def _when_env_only(ctx, env, monkeypatch):
    monkeypatch.setenv("DECLARO_DATABASE_URL", env)
    ctx["answer"] = get_connection_string(argparse.Namespace(connection=None))


@then(parsers.parse('the engine is "{expected}"'))
def _then_engine(ctx, expected):
    assert ctx["answer"] == expected, ctx["answer"]


@then("there is no engine")
def _then_no_engine(ctx):
    assert ctx["answer"] is None, ctx["answer"]


@then("it refuses and says to pass --dialect")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r} instead"
    assert "--dialect" in str(ctx["refusal"]), ctx["refusal"]


@then(parsers.parse('the connection string is "{expected}"'))
def _then_connection(ctx, expected):
    assert ctx["answer"] == expected, ctx["answer"]
