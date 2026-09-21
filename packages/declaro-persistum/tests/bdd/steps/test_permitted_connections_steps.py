"""Bind `features/core/permitted_connections.feature` to the function.

WHAT THE FUNCTION IS. `docs/design/persistum.hd`, `fn permitted_connections`, declares
`permitted_connections : (asked: int | NOTHING, replicated: bool) -> int | Fault`.
It settles a number and nothing else. It opens nothing, reads no environment,
and never sees a connection. The number the deployment sets arrives as `asked`,
already read by whoever opened the database.

WHAT THE FEATURE ASKS, ALL OF WHICH THIS FUNCTION ANSWERS. Every scenario is
about the number: a replica holds one, a local database holds what the
deployment set, an unset number is a case rather than a default, a thousand
callers do not raise the number, the number is declared rather than measured off
the machine, and a caller gets a count back instead of a connection.

WHAT THIS FEATURE USED TO ASK AND NO LONGER DOES. Three scenarios were about
what happens while a database is running: a read not waiting for a writer, five
reads overlapping, and reads not queueing behind the sync connection. Each
needed a connection to exist and time to pass, so each step here called
`pytest.skip` naming the function that owed the answer. A skip printed the gap
rather than hiding it, but it left this feature describing somebody else's
function. On 2026-09-21 the three moved to
`features/session/session_open_connection.feature`, which is the boundary that
opens what they are asking about.

ONE SCENARIO DECIDES A CASE THE OTHERS LEAVE OPEN. "a replicated database still
holds one sync connection" sets
no number, and then asks for exactly one connection to the primary. So
`(NOTHING, replicated=True)` is one, not a refusal: on a replica the number is
measured rather than configured, and an unset variable costs nothing there.
"""

from __future__ import annotations

from typing import Any, get_type_hints

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.permitted_connections import permitted_connections
from declaro_persistum.types import NOTHING

scenarios("../features/core/permitted_connections.feature")


@pytest.fixture
def asking() -> dict[str, Any]:
    """What one scenario has said so far. Fresh per scenario, so nothing leaks."""
    return {}


# --- what the deployment and the engine are ---------------------------------


# FOUR SENTENCES, TWO FACTS, ONE STEP. Each Given says what the deployment asked
# for and whether the database replicates, and nothing else. Written as four
# functions they were the same two assignments four times over, differing only in
# the words. `NOTHING` is the named absence here, never a zero and never a number
# to invent: a deployment that set nothing is a case of its own.
#
# The last row names no number and still expects one connection, because on a
# replica the number is measured rather than configured.
WHAT_WAS_ASKED: dict[str, tuple[int | None, bool]] = {
    "a replicated database and a deployment asking for eight": (8, True),
    "a local database and a deployment asking for eight": (8, False),
    "no number set": (NOTHING, False),
    "a replicated database": (NOTHING, True),
}


@given(parsers.re(f"^(?P<sentence>{'|'.join(WHAT_WAS_ASKED)})$"))
def _what_the_deployment_and_the_engine_are(asking: dict[str, Any], sentence: str) -> None:
    asking["asked"], asking["replicated"] = WHAT_WAS_ASKED[sentence]


# --- what the callers do ----------------------------------------------------


@when("many callers read and write at once")
def _many_callers(asking: dict[str, Any]) -> None:
    asking["callers"] = "many"


@when("a thousand callers ask at once")
def _a_thousand_callers_ask(asking: dict[str, Any]) -> None:
    # Three deployments, a thousand askers each. A thousand simultaneous callers
    # cannot talk this function into a bigger number than the one stated, because
    # the number it answers with is a function of its arguments and nothing else.
    asking["stated"] = (1, 8, 64)
    asking["answers"] = {
        stated: [permitted_connections(stated, False) for _ in range(1000)]
        for stated in asking["stated"]
    }


# --- what must be true ------------------------------------------------------


# The feature spells its numbers, so the step reads the word and looks it up.
SPELLED = {"one": 1, "eight": 8}


@then(parsers.parse("{count} is permitted"))
@then(parsers.parse("{count} are permitted"))
def _that_many_are_permitted(asking: dict[str, Any], count: str) -> None:
    assert permitted_connections(asking["asked"], asking["replicated"]) == SPELLED[count]


@then("the answer says so")
def _the_answer_says_so(asking: dict[str, Any]) -> None:
    answer = permitted_connections(asking["asked"], asking["replicated"])
    # A Fault comes back in the return value. It is not raised and not logged,
    # so a caller has to look at it to get past it.
    assert isinstance(answer, dict), f"expected a Fault, got {answer!r}"
    assert answer["kind"] == "refused"
    assert answer["what_went_wrong"]
    assert answer["what_to_do_instead"]


@then("no connection reached any caller")
def _no_connection_reached_a_caller(asking: dict[str, Any]) -> None:
    # What this function can prove: it deals in counts. Neither its arguments nor
    # its answer can carry a Connection, so no caller of it can end up holding
    # one. What it cannot prove is the same claim about the whole library, which
    # session_open_connection and session_close_connection answer.
    hints = get_type_hints(permitted_connections)
    named = " ".join(str(hint) for hint in hints.values())
    assert "Connection" not in named, f"a connection travels through: {named}"
    assert isinstance(permitted_connections(4, False), int)


@then("the number of connections opened stays under a stated bound")
def _stays_under_the_bound(asking: dict[str, Any]) -> None:
    for stated, answers in asking["answers"].items():
        assert max(answers) <= stated, f"a thousand askers raised {stated} to {max(answers)}"
        assert set(answers) == {stated}


@then("the bound is a declared number, not whatever the machine allowed")
def _the_bound_is_declared(asking: dict[str, Any]) -> None:
    for stated, answers in asking["answers"].items():
        assert answers[0] == stated
    # With nothing declared there is no number at all, which is the proof that
    # the number never came from the machine in the first place.
    assert isinstance(permitted_connections(NOTHING, False), dict)


@then("exactly one connection to the primary exists")
def _exactly_one_to_the_primary(asking: dict[str, Any]) -> None:
    # Measured 2026-08-12 against a real primary: a second answers
    # "database tape error: database is busy".
    assert permitted_connections(asking["asked"], asking["replicated"]) == 1
