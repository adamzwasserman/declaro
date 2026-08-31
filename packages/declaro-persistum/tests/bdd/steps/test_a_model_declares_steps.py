"""Steps for declared/a_model_declares_and_does_nothing_else.feature.

NO DATABASE AND NO MOCKS. The subject is a pure function: a class in, a list of
findings out. Every model here is a real Pydantic model carrying the real habit,
because the thing under test is whether the habit is visible on the class.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict

import pytest
from pydantic import BaseModel, field_validator
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.column_types import HABITS, habits_in

scenarios("../features/declared/a_model_declares_and_does_nothing_else.feature")


class Ctx(TypedDict, total=False):
    found: list[str]
    rules: dict[str, Any]


@pytest.fixture
def ctx() -> Ctx:
    return {"found": [], "rules": {}}


def _looked_at(ctx: Ctx, model: type) -> None:
    ctx["found"] = habits_in(model)


# THREE MODELS, ONE SHAPE: a class body, looked at. The bodies differ, so the
# rows carry the bodies.
_PLAIN: dict[str, Callable[[], type]] = {
    "a model declaring a table with columns and nothing else": lambda: type(
        "Person", (BaseModel,), {"__annotations__": {"id": int, "name": str}}
    ),
    "a model whose column carries a Python default": lambda: type(
        "Person", (BaseModel,), {"__annotations__": {"id": int, "name": str}, "name": "unknown"}
    ),
    "a model inheriting from another model": lambda: type(
        "Person",
        (type("Base", (BaseModel,), {"__annotations__": {"id": int}}),),
        {"__annotations__": {"name": str}},
    ),
}


@given(
    parsers.re(
        r"(?P<said>a model (?:declaring a table with columns and nothing else"
        r"|whose column carries a Python default"
        r"|inheriting from another model))"
    ),
    target_fixture="ctx",
)
def _given_a_plain_model(ctx: Ctx, said: str) -> Ctx:
    _looked_at(ctx, _PLAIN[said]())
    return ctx


@given("a model carrying a validator", target_fixture="ctx")
def _given_validator(ctx: Ctx) -> Ctx:
    class Person(BaseModel):
        id: int
        total: int

        @field_validator("total")
        @classmethod
        def not_negative(cls, given: int) -> int:
            return given

    _looked_at(ctx, Person)
    return ctx


@given("a model carrying a method", target_fixture="ctx")
def _given_method(ctx: Ctx) -> Ctx:
    class Person(BaseModel):
        id: int
        name: str

        def shout(self) -> str:
            return self.name.upper()

    _looked_at(ctx, Person)
    return ctx


@given("a model carrying a property", target_fixture="ctx")
def _given_property(ctx: Ctx) -> Ctx:
    class Person(BaseModel):
        id: int
        name: str

        @property
        def loud(self) -> str:
            return self.name.upper()

    _looked_at(ctx, Person)
    return ctx


@given("a model carrying a validator and a method", target_fixture="ctx")
def _given_two(ctx: Ctx) -> Ctx:
    class Person(BaseModel):
        id: int
        name: str
        total: int

        @field_validator("total")
        @classmethod
        def not_negative(cls, given: int) -> int:
            return given

        def shout(self) -> str:
            return self.name.upper()

    _looked_at(ctx, Person)
    return ctx


@when("the rules are read", target_fixture="ctx")
def _when_rules(ctx: Ctx) -> Ctx:
    ctx["rules"] = HABITS
    return ctx


@then("it is accepted")
def _then_accepted(ctx: Ctx) -> None:
    assert ctx["found"] == [], ctx["found"]


@then("it is refused")
def _then_refused(ctx: Ctx) -> None:
    assert ctx["found"], "the habit was not seen at all"


# FOUR CHECKS, ONE SHAPE: is this word in what the checker said? The words
# were the only difference, so they are a table.
_SAYS: dict[str, str] = {
    "the message names the validator": "not_negative",
    "the message offers a declared check": "field(check=",
    "the message says a computed value is not a column": "not a column",
    "the message says the columns must be visible in this file": "visible",
}


@then(
    parsers.re(
        r"(?P<said>the message (?:names the validator|offers a declared check"
        r"|says a computed value is not a column"
        r"|says the columns must be visible in this file))"
    )
)
def _then_the_message(ctx: Ctx, said: str) -> None:
    wanted = _SAYS[said]
    assert any(wanted in line for line in ctx["found"]), ctx["found"]


@then("both are named")
def _then_both_named(ctx: Ctx) -> None:
    said = " ".join(ctx["found"])
    for wanted in ("not_negative", "shout"):
        assert wanted in said, f"{wanted} missing from {ctx['found']}"


@then("each row carries what to look for and what to say")
def _then_rows_carry_both(ctx: Ctx) -> None:
    """The words are a string, not a function. That is the point of the table:
    three functions with one shape, differing only by the sentence each pasted
    in, is the thing a table exists to replace."""
    for name, row in ctx["rules"].items():
        found, said = row
        assert callable(found), f"{name} has no way to look"
        assert isinstance(said, str) and said, f"{name} has no words"
        assert "{named" in said, f"{name}'s words never name what was found"


@then("the checker names none of them")
def _then_checker_is_blind() -> None:
    """The function must not mention a habit by name, or the table is decoration."""
    import inspect

    from declaro_persistum import column_types

    body = inspect.getsource(column_types.habits_in)
    for word in ("validator", "property", "default", "method", "inherit"):
        assert word not in body.lower(), f"habits_in knows about {word!r}"
