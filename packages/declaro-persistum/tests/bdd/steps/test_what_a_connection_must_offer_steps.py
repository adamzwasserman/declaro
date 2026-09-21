"""Steps for writers/what_a_connection_must_offer.feature.

THE LAST SCENARIO IS THE ONE THAT EARNS ITS PLACE. The first two read the
Protocol and would pass if it named methods nobody calls. The third reads the
transaction scope's own source for every `conn.<method>()` it calls and
requires the contract to name each, which is the failure that actually
happened: the Protocol named `execute` and `commit`, and `rollback` was called
on a connection no contract said had one.
"""

from __future__ import annotations

import ast
import inspect
import re
from typing import TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum import writers

scenarios("../features/query/query_run_together.feature")


class Ctx(TypedDict, total=False):
    required: set[str]
    called: set[str]
    checked: int


@pytest.fixture
def ctx() -> Ctx:
    return {"required": set(), "called": set(), "checked": 0}


def _members_of(protocol: type) -> set[str]:
    """Every method the Protocol declares, by name."""
    return {
        node.name
        for node in ast.walk(ast.parse(inspect.getsource(protocol)))
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
    }


@when("the DB-API connection contract is read")
def _when_dbapi(ctx):
    ctx["required"] = _members_of(writers.DbApiConnection)


@when("the asyncpg connection contract is read")
def _when_asyncpg(ctx):
    ctx["required"] = _members_of(writers.AsyncpgConnection)


@when("the transaction scope is read for the methods it calls on a connection")
def _when_scope(ctx):
    """Each function checked against the contract ITS OWN parameter names.

    My first version compared every `conn.<method>()` in the module against
    the DB-API contract alone, and reported `transaction` as unrequired. It is
    required -- by `AsyncpgConnection`, which is the type the function calling
    it declares. The test was wrong, not the code.

    So the question is asked the way mypy asks it: read the annotation on the
    `conn` parameter, and require that Protocol to name what the body calls.
    """
    contracts = {
        "DbApiConnection": _members_of(writers.DbApiConnection),
        "AsyncpgConnection": _members_of(writers.AsyncpgConnection),
    }
    tree = ast.parse(inspect.getsource(writers))
    unrequired: set[str] = set()
    checked = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        declared = [
            ast.unparse(arg.annotation)
            for arg in node.args.args
            if arg.arg == "conn" and arg.annotation is not None
        ]
        named = [contracts[d] for d in declared if d in contracts]
        if not named:
            continue
        checked += 1
        called = set(re.findall(r"\bconn\.(\w+)\(", ast.unparse(node)))
        unrequired |= called - named[0]

    assert checked, "no function in writers.py declares a contract for `conn`"
    ctx["checked"] = checked
    ctx["called"] = unrequired
    ctx["required"] = set()


@then(parsers.parse('it requires "{method}"'))
def _then_requires(ctx, method):
    assert method in ctx["required"], (
        f"the contract does not name {method!r}; it names {sorted(ctx['required'])}"
    )


@then("every one of them is named by the DB-API contract")
def _then_all_named(ctx):
    missing = sorted(ctx["called"])
    assert not missing, (
        f"the write path calls {missing} on a connection, and no contract "
        f"says a connection has them. That is how `rollback` came to be the "
        f"method a transaction's undo rested on with nothing requiring it."
    )
