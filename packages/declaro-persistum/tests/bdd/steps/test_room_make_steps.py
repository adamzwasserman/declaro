"""Bind `tests/bdd/features/room/room_make.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `room_make() -> Room` takes nothing and hands back
the waiting room between callers and the drainers that empty it. The first
scenario asks the only question a function with no arguments can be asked: what
does it hand back. A Room carries an arrival order and the callers waiting on
tickets, and both are empty while nobody is mid-flight.

WHY THE THEN DOES MORE THAN READ TWO EMPTY LISTS. A function that returned one
room built once and handed out again would satisfy "no arrivals and nobody
waiting" on the first read and be the worst bug this function can carry: every
database in the process would share one waiting room, and two callers' writes
would queue in the same arrival order. Reading one room cannot see that, so the
Then makes two, checks they are not the same room, puts a write in one, and
reads the other. Nothing in the feature's words says "two", and nothing else in
the suite would catch it.

WHAT IT CANNOT ANSWER. The second scenario opens a database and asks that the
room arrived inside the handle. `room_make` opens nothing, is handed nothing,
and never sees a `Database`, so there is no assertion about it that would make
that scenario true. `session_connect` builds the handle and the declaration
already says it invokes `room_make` to fill the `room` field. Those two steps
raise so the red names what is missing; an empty body or a skip would let a
scenario this function cannot satisfy read as answered.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable

from pytest_bdd import scenarios, then, when

from declaro_persistum.room_make import room_make

scenarios("../features/room/room_make.feature")


AN_EMPTY_ROOM = {"arrivals": [], "waiting": []}
"""What the declaration says a Room holds, with nobody mid-flight."""

A_DEPOSITED_WRITE = {"text": "INSERT INTO tasks (title) VALUES (:title)", "parameters": {}}
"""A Statement, so the mutation probe below puts a real arrival in the room."""


@when("a room is made")
def _a_room_is_made(scenario: dict[str, object]) -> None:
    """Two of them, because one call cannot show whether two callers share one.

    The Then reads both. Making them here keeps the When as the call the
    scenario describes and leaves every assertion in the Then.
    """
    scenario["rooms"] = [room_make(), room_make()]


@then("it has no arrivals and nobody waiting")
def _it_holds_nothing(scenario: dict[str, object]) -> None:
    first, second = scenario["rooms"]

    assert first == AN_EMPTY_ROOM, f"a new room already holds something: {first}"
    assert set(first) == {"arrivals", "waiting"}, (
        f"a Room carries an arrival order and the callers waiting: {sorted(first)}"
    )

    # The declaration gives this function no arguments, so a caller cannot hand
    # it a starting room and nothing it returns depends on what was passed.
    assert list(inspect.signature(room_make).parameters) == [], (
        "room_make takes nothing; an argument here would be a room somebody else made"
    )

    # TWO CALLS, TWO ROOMS. Both the room and each of its two lists have to be
    # new, since a shared list inside two fresh dicts is the same bug one level
    # down: the arrival orders would still be one queue.
    assert first is not second, "both calls handed back the same room"
    assert first["arrivals"] is not second["arrivals"], "both rooms share one arrival order"
    assert first["waiting"] is not second["waiting"], "both rooms share one list of callers"

    # And the proof that identity is the property that matters: deposit into one
    # room and the other is still empty. A function returning a shared default
    # passes every line above that reads only the first room.
    first["arrivals"].append(A_DEPOSITED_WRITE)
    first["waiting"].append("w-0001")
    assert second == AN_EMPTY_ROOM, f"a write deposited in one room reached another: {second}"

    # A room made after that one is still empty, which is the same question
    # asked of a call that happens later rather than earlier.
    assert room_make() == AN_EMPTY_ROOM, "the room the third caller got was already used"


# --- the scenario this function cannot be asked ----------------------------
#
# One function over a table. The two sentences differ only in their words and in
# nothing else, so writing them as two step functions would be one decision
# typed twice.

NOT_THIS_FUNCTION = (
    ("when", "a database is opened", "session_connect"),
    ("then", "the room came with the handle", "session_connect"),
)

REGISTER = {"when": when, "then": then}


def cannot_be_asked_here(sentence: str, owner: str) -> Callable[[], None]:
    """Build the step that says which function this sentence belongs to."""

    def step() -> None:
        raise NotImplementedError(
            f"CODE NOT WRITTEN: {sentence!r} is not something `room_make() -> Room` "
            f"can be asked. It takes no arguments, opens nothing, and never sees a "
            f"Database. `{owner}` builds the handle and fills its `room` field, and "
            f"it is not written yet."
        )

    return step


for step_type, step_sentence, step_owner in NOT_THIS_FUNCTION:
    REGISTER[step_type](step_sentence)(cannot_be_asked_here(step_sentence, step_owner))
