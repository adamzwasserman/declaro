"""Bind `core/crew_size.feature` to `crew_size`, and to nothing else.

WHAT THIS FUNCTION CAN ANSWER. `crew_size(asked, replicated) -> int` is handed
two values and answers with a number. Three scenarios ask it exactly that: a
local database runs the number the deployment asked for, a replicated database
runs one whatever it asked for, and an unset variable is answered rather than
filled in with a number of our choosing.

WHAT IT CANNOT, AND WHY THOSE STEPS RAISE. Seven scenarios were folded in from
`running/how_many_connections_and_when.feature` on 2026-09-21, and they ask
about callers arriving at once, a read overlapping a write, a connection being
closed after a failure, and a bound on connections opened. This function sees no
caller, no connection and no time passing, so there is no assertion about
`crew_size` that would make any of them true. Other declared functions own them:
`room_deposit` and `lifecycle_catch_up` stagger the writes, `query_answer` runs
a read beside them, `session_open_connection` opens and closes the connection,
`permitted_connections` settles the bound, and `session_connect` is where an
open states its settings. Those steps raise NotImplementedError so the red names
what is missing. A step with an empty body, or one that skips, would let a
feature the declaration cannot satisfy look answered.

WHY AN UNNAMED DATABASE IS LOCAL. The third scenario gives no database. The
feature files' own convention, written down in `tests/bdd/conftest.py`, is that
a scenario naming no backend is running on SQLite, and SQLite has no primary.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_size import crew_size
from declaro_persistum.types import NOTHING

scenarios("../features/core/crew_size.feature")


# --- what the sentences mean ----------------------------------------------

A_NUMBER = {"one": 1, "eight": 8}
"""The counts this feature spells out in words."""

IS_REPLICATED = {"local": False, "replicated": True}
"""The two kinds of database the feature names, and which one holds a primary."""


@pytest.fixture
def deployment() -> dict[str, Any]:
    """What the deployment said, and what kind of database it said it about.

    `replicated` starts False because a scenario that names no database is on
    SQLite by the convention above. `asked` starts absent, so a scenario that
    forgets to say what was asked for fails on the missing key rather than on a
    number this fixture invented.
    """
    return {"replicated": False}


# --- the three scenarios crew_size answers ---------------------------------


@given(parsers.parse("a deployment asking for {number}"))
@given(parsers.parse("the same deployment asking for {number}"))
def a_deployment_asking_for(deployment: dict[str, Any], number: str) -> None:
    deployment["asked"] = A_NUMBER[number]


@given(parsers.parse("a {kind} database"))
def a_database_of_that_kind(deployment: dict[str, Any], kind: str) -> None:
    deployment["replicated"] = IS_REPLICATED[kind]


@given("no crew size set")
def no_crew_size_set(deployment: dict[str, Any]) -> None:
    deployment["asked"] = NOTHING


@then(parsers.parse("{number} drainers run"))
@then(parsers.parse("{number} drainer runs"))
def that_many_drainers_run(deployment: dict[str, Any], number: str) -> None:
    assert crew_size(deployment["asked"], deployment["replicated"]) == A_NUMBER[number]


@then("the answer says so rather than picking a number quietly")
def the_answer_says_so(deployment: dict[str, Any]) -> None:
    """Two halves, because a number can be invented in either place.

    The call must refuse, naming what was not set. And the signature must carry
    no default, because a parameter written `asked: int | None = 4` would answer
    four and never reach the refusal at all.
    """
    with pytest.raises(ValueError) as refused:
        crew_size(deployment["asked"], deployment["replicated"])
    said = str(refused.value).lower()
    assert "crew" in said
    assert "set" in said

    defaults = [p.default for p in inspect.signature(crew_size).parameters.values()]
    assert defaults == [inspect.Parameter.empty, inspect.Parameter.empty]


# --- the seven scenarios another function answers --------------------------
#
# Each row is one sentence this function cannot be asked, and the declared
# function in `docs/design/persistum.hd` that can.

NOT_THIS_FUNCTION: tuple[tuple[str, str, str], ...] = (
    ("given", "a write in flight", "room_deposit"),
    ("given", "a read that raises partway", "session_open_connection"),
    ("when", "a caller reads", "query_answer"),
    ("when", "five callers read at once, each taking time", "query_answer"),
    ("when", "many callers read and write at once", "query_answer"),
    ("when", "a thousand callers ask at once", "permitted_connections"),
    ("when", "a database is opened", "session_connect"),
    ("then", "the read answers without waiting for the write to finish", "query_answer"),
    ("then", "the five overlap rather than running one after another", "query_answer"),
    ("then", "no connection reached any caller", "query_answer"),
    (
        "then",
        "the number of connections opened stays under a stated bound",
        "permitted_connections",
    ),
    (
        "then",
        "the bound is a declared number, not whatever the machine allowed",
        "permitted_connections",
    ),
    ("then", "how many connections it may use is part of that declaration", "session_connect"),
    (
        "then",
        "it has no default, because the right number is a property of the machine",
        "session_connect",
    ),
    ("then", "the connection it used was closed", "session_open_connection"),
    ("then", "exactly one connection to the primary exists", "session_open_connection"),
    ("then", "the reads did not queue behind it", "query_answer"),
)

REGISTER = {"given": given, "when": when, "then": then}


def cannot_be_asked_here(sentence: str, owner: str) -> Callable[[], None]:
    """Build the step that says which function this sentence belongs to."""

    def step() -> None:
        raise NotImplementedError(
            f"CODE NOT WRITTEN: {sentence!r} is not something "
            f"crew_size(asked, replicated) -> int can be asked. It sees no caller, "
            f"no connection and no time passing. `{owner}` owns this, and the "
            "feature file and the declaration disagree until one of them moves."
        )

    return step


for step_type, step_sentence, step_owner in NOT_THIS_FUNCTION:
    REGISTER[step_type](step_sentence)(cannot_be_asked_here(step_sentence, step_owner))
