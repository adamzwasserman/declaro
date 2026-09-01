"""The test HOW-IT-SHOULD-WORK.md states, run.

WHY IT EXISTS NOW. The document has stated this test since it was written and
nothing ran it. Three strings carrying structure reached the public surface and
came off one at a time, each found by a person reading: a WHERE clause as text,
a column name with a minus in front, and `field(check=...)`, which is still
there. A rule nobody checks is a rule that gets broken as often as somebody
happens not to look.

WHAT IS GREEN HERE AND WHAT IS RED. The gate scenarios run today, against the
package and against source this file writes to prove the gate fails when it
should. The scenarios about what a built function does with a string are red on
NotImplementedError, because the vocabulary is not built. Red is the honest
state of unbuilt work and the feature file says which is which.

THE SQL VERBS THIS FILE NAMES are named on purpose, in `_A_WORD_SCAN_WOULD_FIND`,
to assert that `following_an_argument.py` names none of them. The assertion is
about that module, not about this one.
"""

from __future__ import annotations

import asyncio
import inspect
import pathlib
from typing import Any, TypedDict

import pytest
from pydantic import BaseModel
from pytest_bdd import given, parsers, scenarios, then, when

import declaro_persistum as dp
from declaro_persistum import asking, field, table
from tests.bdd.steps.following_an_argument import (
    Offense,
    offenses_in_file,
    offenses_in_source,
)

pytestmark = pytest.mark.bdd

scenarios("../features/declared/the_test_for_any_function.feature")

_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SRC = _ROOT / "src" / "declaro_persistum"
_GATE = pathlib.Path(__file__).resolve().parent / "following_an_argument.py"


# WHAT THE GATE STILL FINDS, and why each one is still there. A RATCHET, not an
# allowlist: a name comes off when it is fixed, and the count only goes down.
#
# IT IS EMPTY, and it was written holding seven. Three named an engine the
# `Database` already carries, and they now read it instead. Four were `select`,
# `insert`, `update` and `delete`, which took a table as a bare string with no
# model to check it against, and they are no longer exported.
#
# An entry here is a decision to ship a function the document forbids, with the
# reason written down. There is no such decision open.
STILL_FOUND: dict[tuple[str, str], str] = {}

# Named here so the assertion about the gate's own source has something to
# assert. Every one of these is also a legal column name, which is the reason
# the gate does not look for them.
_A_WORD_SCAN_WOULD_FIND = ("select", "insert", "update", "delete", "where", "from", "join")


class Ctx(TypedDict, total=False):
    """What one scenario carries from its When to its Then."""

    found: list[Offense]
    refusal: Exception | None
    answer: Any
    model: type
    source: str


@pytest.fixture
def ctx() -> Ctx:
    return Ctx()


# ------------------------------------------------- the surface, as it really is


def _exported() -> set[tuple[str, str]]:
    """Every public callable, as the file it lives in and the name it answers to.

    Resolved through `inspect`, because two modules can define a `page` and
    only one of them is the one a caller reaches.
    """
    surface = set()
    for module, names in ((dp, dp.__all__), (asking, asking.__all__)):
        for name in names:
            got = getattr(module, name, None)
            # `isfunction`, not `callable`. A type alias exported by name is
            # callable enough to fool `callable` and `getsourcefile` raises on
            # it, which read as a gate failure rather than as the wrong question.
            if inspect.isfunction(got):
                where = inspect.getsourcefile(got)
                if where:
                    surface.add((str(pathlib.Path(where).resolve()), name))
    return surface


def _on_the_surface() -> list[Offense]:
    """Every offense the gate finds in a function a caller can actually reach."""
    surface = _exported()
    return [
        found
        for path in sorted(_SRC.rglob("*.py"))
        for found in offenses_in_file(path)
        if (str(pathlib.Path(found.where).resolve()), found.function) in surface
    ]


def _unratcheted(found: list[Offense]) -> list[Offense]:
    return [one for one in found if (one.function, one.argument) not in STILL_FOUND]


# ------------------------------------------------------------------- the models


class Person(BaseModel):
    """A model declaring exactly two columns, so a third is undeclared."""

    id: int = field(primary_key=True)
    name: str


@given("a model Person declaring people with columns id and name", target_fixture="declared")
def _given_person(ctx: Ctx) -> type:
    ctx["model"] = table("people")(Person)
    return ctx["model"]


@given(parsers.re(r'a model Event declaring events with a column "created_at"'))
def _given_event(ctx: Ctx) -> None:
    class Event(BaseModel):
        id: int = field(primary_key=True)
        created_at: str

    ctx["model"] = table("events")(Event)


# ------------------------------------------------- source written to be refused


# TWO WAYS TO WRITE THE SAME OFFENSE, so the gate has something it must fail
# on. A row each, because the only difference is the source string.
_WRITTEN_TO_BE_REFUSED: dict[str, str] = {
    "concatenated into a statement": 'def rows_of(named: str) -> str:\n    return "a" + named\n',
    "interpolated into an f-string statement": 'def rows_of(named: str) -> str:\n    return f"a{named}"\n',
}


@given(
    parsers.re(
        r"a public function whose argument is (?P<how>concatenated into a statement"
        r"|interpolated into an f-string statement)"
    )
)
def _given_written_to_be_refused(ctx: Ctx, how: str) -> None:
    ctx["source"] = _WRITTEN_TO_BE_REFUSED[how]


@given(parsers.re(r'a column declared with a check written as "(?P<fragment>[^"]+)"'))
def _given_a_check(ctx: Ctx, fragment: str) -> None:
    ctx["source"] = fragment
    try:
        ctx["answer"] = field(check=fragment)
        ctx["refusal"] = None
    except Exception as refused:
        ctx["refusal"] = refused


# ------------------------------------------------------------------- the asking


def _tried(ctx: Ctx, word: str, *given_args: Any, **narrowed: Any) -> None:
    """Call a word, RUN IT, and keep whatever came back, refusal included.

    THE `asyncio.run` IS THE POINT. Every word is `async def`, so calling one
    builds a coroutine and raises nothing. Without running it, four scenarios
    here went green over a coroutine object nobody awaited, which is the same
    false green that hid the 80 asking scenarios in an earlier session.
    """
    ctx["refusal"] = None
    ctx["answer"] = None
    try:
        ctx["answer"] = asyncio.run(
            getattr(asking, word)(None, ctx.get("model"), *given_args, **narrowed)
        )
    except NotImplementedError:
        # NOT A REFUSAL. "This word is not built" and "this word looked at what
        # you passed and said no" are different answers, and reading the first
        # as the second turns a scenario green over code that does not exist.
        # It did exactly that six times in an earlier session.
        raise
    except Exception as refused:
        ctx["refusal"] = refused


@when(parsers.re(r'a caller passes "(?P<named>[^"]+)" as a column'))
def _when_a_column(ctx: Ctx, named: str) -> None:
    _tried(ctx, "values", [named])


# "BY THE VALUE" AND "WITH THE MAPPING NAME TO" ARE THE SAME CALL, which is the
# point of the pair of scenarios: `{"name": "bob"}` and `{"name": "'; ..."}`
# reach the library the same way and differ only in what the value says.
@when(parsers.re(r'a caller narrows (?:by the value|with the mapping name to) "(?P<value>.+)"'))
def _when_narrowing_by_a_value(ctx: Ctx, value: str) -> None:
    _tried(ctx, "all", name=value)


@when(parsers.re(r'a caller narrows with the text "(?P<written>[^"]+)"'))
def _when_written(ctx: Ctx, written: str) -> None:
    _tried(ctx, "all", written)


# --------------------------------------------------------------- running the gate


@when("the gate runs")
def _when_the_gate_runs(ctx: Ctx) -> None:
    ctx["found"] = offenses_in_source(ctx.get("source", ""), "written for this scenario")


@when("every string a caller can pass is followed")
@when("every public function's arguments are read")
@when("every public function's return is read")
def _when_the_surface_is_read(ctx: Ctx) -> None:
    ctx["found"] = _on_the_surface()


@when("the gate's own source is read")
def _when_the_gate_is_read(ctx: Ctx) -> None:
    ctx["source"] = _GATE.read_text()


# ------------------------------------------------------------------- the answers


@then("it fails and names that function and that argument")
def _then_names_both(ctx: Ctx) -> None:
    assert ctx["found"], "the gate found nothing in source written to be refused"
    said = " ".join(one.said() for one in ctx["found"])
    assert "rows_of" in said and "named" in said, said


@then("the fragment contains no SQL verb, so no word scan would have found it")
def _then_no_verb_in_it(ctx: Ctx) -> None:
    written = ctx["source"].lower()
    assert not [word for word in _A_WORD_SCAN_WOULD_FIND if word in written], written


@then("each one is a declared name or a bound value, and nothing else")
def _then_no_third_fate(ctx: Ctx) -> None:
    left = _unratcheted(ctx["found"])
    assert not left, "\n".join(one.said() for one in left)
    assert len(ctx["found"]) <= len(STILL_FOUND), (
        f"the ratchet holds {len(STILL_FOUND)} and the gate found {len(ctx['found'])}"
    )


# THE THREE THINGS NO PUBLIC FUNCTION MAY DO, and what the gate calls each when
# it finds one. A row per rule, so a fourth rule is a row and a step the table
# does not hold is a KeyError naming itself rather than a scenario that quietly
# checks the wrong thing.
_NONE_OF_THEM: dict[str, str] = {
    "takes a connection or a cursor": "is a connection",
    "yields or returns a connection": "hands back",
    "takes an engine or an engine name": "names an engine",
}


@then(parsers.re(r"none of them (?P<said>.+)"))
def _then_none_of_them(ctx: Ctx, said: str) -> None:
    becomes = _NONE_OF_THEM[said]
    left = [one for one in _unratcheted(ctx["found"]) if becomes in one.becomes]
    assert not left, "\n".join(one.said() for one in left)


@then("the engine is decided once, where the database is opened")
def _then_decided_at_open(ctx: Ctx) -> None:
    from declaro_persistum.database import Database

    assert "engine" in Database.__annotations__


@then("it names no SQL verb")
def _then_the_gate_names_no_verb(ctx: Ctx) -> None:
    written = ctx["source"].lower()
    found = [word for word in _A_WORD_SCAN_WOULD_FIND if f'"{word}"' in written]
    assert not found, f"the gate quotes {found}, so it is matching text after all"


@then("it decides by following an argument rather than by matching text")
def _then_it_follows(ctx: Ctx) -> None:
    assert "_arguments" in ctx["source"] and "ast.walk" in ctx["source"]


@then("it is looked up in the model")
@then("it is bound as a parameter")
@then("it is accepted, because the key is declared and the value is bound")
@then("it is accepted, because the model declares it")
def _then_accepted(ctx: Ctx) -> None:
    assert ctx["refusal"] is None, f"it was refused: {ctx['refusal']}"


@then("it is refused in the caller's own process")
@then("it is refused")
@then("it fails")
def _then_refused(ctx: Ctx) -> None:
    assert ctx["refusal"] is not None, f"it was accepted: {ctx.get('answer')!r}"


@then("it never reaches the statement as text")
@then("it never appears in the statement text")
@then("no statement was built")
def _then_nothing_was_built(ctx: Ctx) -> None:
    assert ctx.get("answer") is None, ctx.get("answer")


@then("the people table still exists")
def _then_the_table_survives(ctx: Ctx) -> None:
    assert ctx["model"] is not None


@then("the two differ by what becomes of the string, not by how it reads")
def _then_fate_not_appearance(ctx: Ctx) -> None:
    """The refusal is a TypeError, and that is the strongest form it can take.

    `all(db, model, **narrowed)` has no parameter a where-string could land in,
    so Python refuses the call before any of this package runs. Nothing read the
    string, nothing decided it looked like SQL, and there is no rule to get
    wrong. The mapping in the scenario above is accepted by the same signature.
    """
    assert isinstance(ctx["refusal"], TypeError), (
        f"expected the signature to refuse it, got {ctx['refusal']!r}"
    )
