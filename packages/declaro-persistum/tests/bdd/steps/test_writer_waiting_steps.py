"""Bind `tests/bdd/features/core/writer_waiting.feature`, and nothing else.

WHAT THE FEATURE ASKS. Two scenarios, one question. A room holding a write says
a sync pass would be dropped; an empty room says a pass may run. That is the
whole of `fn writer_waiting : (room: Room) -> bool`.

WHY THE TWO GIVENS ARE ONE STEP, AND THE TWO THENS ARE ONE STEP. The openings
differ only in how full the room is, and the readings differ only in the
verdict. Written as four functions, the same decision would sit in the file
four times and a fifth wording would be a fifth copy. Each pair is one function
over a table, so a word the table does not hold raises a KeyError naming itself
rather than passing on a silent default.

WHAT A DROPPED PASS COSTS, which is why the verdict is asserted with `is` and
not with truthiness. A pass dropped by mistake costs one skipped sync, and the
next free turn takes it. A pass RUN by mistake takes the connection out from
under a caller that is waiting on it. The two errors are not the same size, so
the test reads the exact bool rather than anything that merely behaves like one.

THE ROOM IS BUILT HERE FROM `Room` ITSELF. `room_make` is declared and not
written, so a scenario that needed it could not run today. A `Room` is two
lists, and writing both out keeps this file honest about which one each
scenario fills.
"""

from __future__ import annotations

from typing import cast

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.room_deposit import room_deposit
from declaro_persistum.room_make import room_make
from declaro_persistum.types import Room, Statement
from declaro_persistum.writer_waiting import writer_waiting

scenarios("../features/core/writer_waiting.feature")


# ONE DEPOSITED WRITE, as `compile_statement` produces it: text and the values
# that go with it. The statement's content decides nothing here, so it is a
# plain insert rather than anything a reader has to interpret.
A_DEPOSITED_WRITE: Statement = {
    "text": "INSERT INTO orders (id, total) VALUES (:id, :total)",
    "parameters": {"id": 1, "total": 42},
}


def _a_room_holding(statement: Statement) -> Room:
    """A room with one write in it, deposited the way a caller deposits one.

    BUILT BY DEPOSITING, NOT BY WRITING THE FIELDS OUT. A room joins a ticket to
    its write and its receipt, and hand-building one is how the shape changed
    under eight test files on 2026-09-22 without one of them noticing.
    """
    room = room_make()
    room_deposit(room, statement)
    return room


# WHAT EACH OPENING MEANS, as the room it builds. "nothing in the room" is both
# lists empty, which `docs/architecture.md:140` calls the room whenever nobody
# is mid-flight.
A_ROOM: dict[str, Room] = {
    "one write": _a_room_holding(A_DEPOSITED_WRITE),
    "nothing": room_make(),
}

# WHAT EACH READING ASSERTS. A writer waiting drops the pass, so the verdict a
# scenario states maps straight onto the bool the function returns.
THE_VERDICT = {"would be dropped": True, "may run": False}


@given(parsers.re(r"(?P<how_full>one write|nothing) in the room"))
def _the_room_holds(scenario: dict[str, object], how_full: str) -> None:
    scenario["room"] = A_ROOM[how_full]


@then(parsers.re(r"a sync pass (?P<verdict>would be dropped|may run)"))
def _a_sync_pass(scenario: dict[str, object], verdict: str) -> None:
    room = cast(Room, scenario["room"])
    answered = writer_waiting(room)
    assert answered is THE_VERDICT[verdict], (
        f"the room {room} says the pass {verdict}, so writer_waiting has to "
        f"answer {THE_VERDICT[verdict]} and it answered {answered!r}"
    )
