"""Steps for boundary/declaring_a_role.feature.

THE MARKERS ARE READ BY NAME, so every scenario reaches them by name off the
module rather than importing four symbols. That is also how `honest-check`
finds them, and how the four survive being written as one function over a
table: the names are module-level bindings either way.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

import declaro_persistum.boundary as boundary_module

scenarios("../features/boundary/declaring_a_role.feature")


class Ctx(TypedDict, total=False):
    given: Any
    returned: Any
    said: str
    exported: list[str]


@pytest.fixture
def ctx() -> Ctx:
    return {"given": None, "returned": None, "said": "", "exported": []}


def _a_marker(name: str) -> Any:
    marker = getattr(boundary_module, name, None)
    assert marker is not None, (
        f"the module offers no marker called {name!r}; the name is what "
        f"honest-check reads and what 972 places in src carry"
    )
    return marker


@when(parsers.parse('"{marker}" is put on a function'))
def _when_marked(ctx, marker):
    def a_function(x: int) -> int:
        """What this function is for."""
        return x + 1

    ctx["given"] = a_function
    ctx["returned"] = _a_marker(marker)(a_function)


@when(parsers.parse('"{marker}" is put on an asynchronous function'))
def _when_marked_async(ctx, marker):
    async def a_coroutine(x: int) -> int:
        """What this coroutine is for."""
        return x + 1

    ctx["given"] = a_coroutine
    ctx["returned"] = _a_marker(marker)(a_coroutine)


@when(parsers.parse('"{marker}" is asked what it declares'))
def _when_asked(ctx, marker):
    ctx["said"] = _a_marker(marker).__doc__ or ""


@when("the markers the module exports are listed")
def _when_listed(ctx):
    ctx["exported"] = sorted(boundary_module.__all__)


@then("the same function comes back")
def _then_same(ctx):
    # IDENTITY, NOT EQUALITY. A wrapper would put a frame between the caller
    # and the function, change the traceback an operator reads, and make an
    # async function's identity depend on the marker getting that right.
    assert ctx["returned"] is ctx["given"], (
        "the marker returned something other than the function it was given, "
        "so it is a wrapper rather than a declaration"
    )


@then("it keeps its own name and docstring")
def _then_keeps(ctx):
    assert ctx["returned"].__name__ == "a_function"
    assert ctx["returned"].__doc__ == "What this function is for."


@then("it is still awaitable")
def _then_awaitable(ctx):
    assert asyncio.run(ctx["returned"](1)) == 2


@then(parsers.parse('it says "{words}"'))
def _then_says(ctx, words):
    assert words in ctx["said"], (
        f"the marker's docstring does not say {words!r}; it says "
        f"{ctx['said'].strip().splitlines()[0] if ctx['said'] else '(nothing)'!r}"
    )


@then("they are exactly boundary, helper, link and recognizer")
def _then_exactly(ctx):
    assert ctx["exported"] == ["boundary", "helper", "link", "recognizer"], ctx["exported"]
