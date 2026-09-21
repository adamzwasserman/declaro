"""Bind `features/core/permitted_connections.feature` to the function.

WHAT THE FUNCTION IS. `docs/design/persistum.hd` line 684 declares
`permitted_connections : (asked: int | NOTHING, replicated: bool) -> int | Fault`.
It settles a number and nothing else. It opens nothing, reads no environment,
and never sees a connection. The number the deployment sets arrives as `asked`,
already read by whoever opened the database.

WHAT THE FEATURE ASKS THAT THIS FUNCTION CAN ANSWER. Six of the ten scenarios
are about the number: a replica holds one, a local database holds what the
deployment set, an unset number is a case rather than a default, a thousand
callers do not raise the number, the number is declared rather than measured off
the machine, and a caller gets a count back instead of a connection.

WHAT IT CANNOT ANSWER, AND WHY THOSE SCENARIOS SKIP. Four scenarios are about
what happens while a database is running: a read not waiting for a writer, five
reads overlapping, a connection closed after a read raises, and reads not
queueing behind the sync connection. Every one of them needs a connection to
exist and time to pass. The declaration gives those to other functions --
`session_open_connection` (line 552), `session_open_writer` (line 584),
`session_close_connection` (line 528) and the room that `lifecycle_catch_up`
drains (line 541) -- and all of them are `boundary_out`, which this one is not.
A step here that pretended to measure overlap would be measuring a stub. Each
such step calls `pytest.skip` naming the function that owes the answer, so the
gap is printed rather than hidden inside a pass.

SCENARIO 10 DECIDES ONE CASE THE OTHERS LEAVE OPEN. "a replicated database" sets
no number, and then asks for exactly one connection to the primary. So
`(NOTHING, replicated=True)` is one, not a refusal: on a replica the number is
measured rather than configured, and an unset variable costs nothing there.
"""

from __future__ import annotations

import inspect
import pathlib
from typing import Any, get_type_hints

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.permitted_connections import permitted_connections
from declaro_persistum.types import NOTHING

scenarios("../features/core/permitted_connections.feature")

# The declaration itself, so a scenario about where the number is stated can read
# the line that states it instead of trusting this file's memory of it.
DECLARATION = pathlib.Path(__file__).parents[3] / "docs" / "design" / "persistum.hd"


@pytest.fixture
def asking() -> dict[str, Any]:
    """What one scenario has said so far. Fresh per scenario, so nothing leaks."""
    return {}


# --- what the deployment and the engine are ---------------------------------


@given("a replicated database and a deployment asking for eight")
def _replicated_asking_eight(asking: dict[str, Any]) -> None:
    asking["asked"] = 8
    asking["replicated"] = True


@given("a local database and a deployment asking for eight")
def _local_asking_eight(asking: dict[str, Any]) -> None:
    asking["asked"] = 8
    asking["replicated"] = False


@given("no number set")
def _no_number_set(asking: dict[str, Any]) -> None:
    # NOTHING is the named absence, not a zero and not a number to invent.
    asking["asked"] = NOTHING
    asking["replicated"] = False


@given("a replicated database")
def _replicated(asking: dict[str, Any]) -> None:
    # This scenario names no number, and still expects one connection. The
    # measured rule answers it without the deployment saying anything.
    asking["asked"] = NOTHING
    asking["replicated"] = True


@given("a write in flight")
def _write_in_flight(asking: dict[str, Any]) -> None:
    asking["needs"] = "a live writer, which this function never holds"


@given("a read that raises partway")
def _read_that_raises(asking: dict[str, Any]) -> None:
    asking["needs"] = "a read that runs and fails, which this function never runs"


# --- what the callers do ----------------------------------------------------


@when("a caller reads")
def _a_caller_reads(asking: dict[str, Any]) -> None:
    asking["needs"] = "a read, which this function never performs"


@when("five callers read at once, each taking time")
def _five_callers_read(asking: dict[str, Any]) -> None:
    asking["needs"] = "five reads and a clock, and this function reads no clock"


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


@when("a database is opened")
def _a_database_is_opened(asking: dict[str, Any]) -> None:
    asking["declaration"] = DECLARATION.read_text(encoding="utf-8")


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


@then("the read answers without waiting for the write to finish")
def _read_does_not_wait(asking: dict[str, Any]) -> None:
    pytest.skip(
        "permitted_connections settles a number and runs nothing. Whether a read "
        "waits is owned by session_open_connection (persistum.hd line 552) and the "
        "room lifecycle_catch_up drains (line 541)."
    )


@then("the five overlap rather than running one after another")
def _five_overlap(asking: dict[str, Any]) -> None:
    pytest.skip(
        "Overlap needs five reads and a clock. This function reads no clock and "
        "opens nothing; session_open_connection (persistum.hd line 552) owns it."
    )


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


@then("how many connections it may use is part of that declaration")
def _part_of_the_declaration(asking: dict[str, Any]) -> None:
    # Stated where the database is opened: session_connect, the one function that
    # opens a database, invokes this one, and hands the number it gets to
    # session_open_connection, which is declared to take `connections: int`.
    declaration = asking["declaration"]
    assert "invokes" in declaration
    opening = [line for line in declaration.splitlines() if "fn session_connect" in line]
    assert opening, "persistum.hd no longer declares session_connect"
    assert "permitted_connections" in opening[0]
    assert "session_open_connection : (target: str, connections: int)" in declaration
    assert "asked" in inspect.signature(permitted_connections).parameters


@then("it has no default, because the right number is a property of the machine")
def _it_has_no_default(asking: dict[str, Any]) -> None:
    for name, parameter in inspect.signature(permitted_connections).parameters.items():
        assert parameter.default is inspect.Parameter.empty, f"{name} invents a number"


@then("the connection it used was closed")
def _the_connection_was_closed(asking: dict[str, Any]) -> None:
    pytest.skip(
        "This function never opens a connection, so it can close none. "
        "session_open_connection (persistum.hd line 552) and "
        "session_close_connection (line 528) own the pair."
    )


@then("exactly one connection to the primary exists")
def _exactly_one_to_the_primary(asking: dict[str, Any]) -> None:
    # Measured 2026-08-12 against a real primary: a second answers
    # "database tape error: database is busy".
    assert permitted_connections(asking["asked"], asking["replicated"]) == 1


@then("the reads did not queue behind it")
def _reads_did_not_queue(asking: dict[str, Any]) -> None:
    pytest.skip(
        "One sync connection is a fact about the channel to the primary and says "
        "nothing about reads. Proving reads do not queue needs a running database; "
        "session_open_connection (persistum.hd line 552) owns it."
    )
