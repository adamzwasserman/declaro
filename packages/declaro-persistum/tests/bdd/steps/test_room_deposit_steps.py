"""Bind `tests/bdd/features/room/room_deposit.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `boundary_out fn room_deposit` puts one write in the
waiting room and hands the caller a ticket for it. Nothing here touches a
database: the call is handed a room and a statement, and neither of those is a
connection, so the write it leaves behind has not run when the ticket comes back.
A crew drains the room afterwards, and the caller collects on its ticket when it
wants to.

WHAT THE THENS READ THE ROOM WITH. `fn next_in_order` and `fn writer_waiting`
are the two functions a drainer and a sync pass already use to read a room, and
both are written and green. Asserting through them rather than reaching into the
lists is what makes these scenarios say something about the crew rather than
about a dict: a deposit that recorded the arrival somewhere else would satisfy a
hand-written read of the arrivals and still leave `fn next_in_order` answering
that there is nothing to do.

THE STAND-IN ENGINE IS A LIST. The third scenario asks how many times the engine
was asked to run the write, so the drain below hands each write to a function
that writes down what it was asked and runs nothing. A real engine would answer
the same question and need a live database to do it, which would make a fact
about a list of arrivals depend on a connection.

WHY THERE IS A FOURTH SCENARIO. One deposit cannot show that two deposits keep
their order, and it cannot show that two callers get two different tickets. Both
failures lose a caller's write and neither is visible from a single deposit: two
callers holding one ticket collect each other's outcome, and two writes drained
back to front land in the database the wrong way round. Added 2026-09-22 with
the function.

WHAT THE ROOM CANNOT BE ASKED YET. A `Room` holds an arrival order and the
tickets of the callers waiting, in two lists, and nothing joins one to the
other. So the last Then reads the two lists in step, the first write's ticket
being the first ticket waiting, which is the only join there is today. It is the
join a drainer will have to use as well, and it holds only while nothing is
taken out of the middle of either list.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable

from pytest_bdd import scenarios, then, when

from declaro_persistum.next_in_order import next_in_order
from declaro_persistum.room_deposit import room_deposit
from declaro_persistum.room_make import room_make
from declaro_persistum.types import NOTHING, Room, Statement
from declaro_persistum.writer_waiting import writer_waiting

scenarios("../features/room/room_deposit.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}
"""One caller's write, already spelled. A deposit never writes the text."""

ANOTHER_WRITE: Statement = {
    "text": "DELETE FROM tasks WHERE id = :id",
    "parameters": {"id": 7},
}
"""A second caller's write, different from the first so order can be read."""

NOTHING_TO_RUN: Statement = {"text": "", "parameters": {}}
"""A write with no statement in it: the caller has nothing for the crew."""

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _a_stand_in_engine(asked: list[Statement]) -> Callable[[Statement], int]:
    """An engine that writes down what it was asked to run and runs nothing."""

    def run(statement: Statement) -> int:
        asked.append(statement)
        return 1

    return run


def _the_crew_drains(room: Room, engine: Callable[[Statement], int]) -> None:
    """One pass: every write waiting, one at a time, in arrival order.

    `docs/architecture.md` line 142 says a drain takes everything waiting rather
    than one write, which is what makes a second copy of one deposit visible
    here: the pass goes round again and the engine is asked a second time.
    """
    while (ticket := next_in_order(room)) is not NOTHING:
        # THE TICKET NAMES THE WRITE. A drainer asks whose write is next, runs
        # the statement it finds under that ticket, and takes the ticket out.
        engine(room["writes"][ticket])
        room["arrivals"].remove(ticket)


# --- one caller, one write -------------------------------------------------
#
# ONE FUNCTION OVER A TABLE. The two sentences differ in the write the caller
# brings and in nothing else, so the write is the row and the deposit is written
# once. Both scenarios start from a room nobody has used, which is what lets the
# refusal's Then say the room is still empty.

WHAT_THE_CALLER_BRINGS: dict[str, Statement] = {
    "a caller deposits a write": A_WRITE,
    "a caller deposits a write with nothing to run": NOTHING_TO_RUN,
}


def depositing(write: Statement) -> Callable[[dict[str, object]], None]:
    """Build the step that deposits one write in a room nobody has used."""

    def step(scenario: dict[str, object]) -> None:
        room = room_make()
        scenario["room"] = room
        scenario["write"] = write
        scenario["answer"] = room_deposit(room, write)

    return step


for sentence, the_write in WHAT_THE_CALLER_BRINGS.items():
    when(sentence)(depositing(the_write))


@then("it is handed a ticket without waiting for the write")
def _a_ticket_came_back_at_once(scenario: dict[str, object]) -> None:
    answer = scenario["answer"]
    room = scenario["room"]

    assert isinstance(answer, str), f"the caller was handed {answer!r}, which is not a ticket"
    assert answer, "the caller was handed an empty ticket, which names no write"

    # NOTHING WAS WAITED FOR, AND THE SIGNATURE IS WHY. The call is handed a
    # room and a statement. A database, a connection and a crew are not among
    # its arguments, so there is nowhere for the write to have run.
    assert list(inspect.signature(room_deposit).parameters) == ["room", "statement"], (
        "room_deposit takes the room and the write; anything else here is a "
        "connection it could have run the write on before answering"
    )
    assert not inspect.iscoroutinefunction(room_deposit), (
        "the deposit is a coroutine, so a caller has to await it to get its "
        "ticket, which is the wait this function exists to remove"
    )
    assert not inspect.isawaitable(answer), f"the caller has to await {answer!r} to read its ticket"

    # And the write is still sitting in the room, undrained, which is the other
    # half of "without waiting": the ticket came back ahead of the write, not
    # after it.
    assert room["writes"][next_in_order(room)] == scenario["write"], (
        f"the crew's next write is {room['writes'].get(next_in_order(room))!r}, "
        "not the one deposited"
    )


@then("it is refused before it takes a ticket")
def _the_empty_write_was_refused(scenario: dict[str, object]) -> None:
    answer = scenario["answer"]

    assert not isinstance(answer, str), (
        f"a write with nothing to run was given the ticket {answer!r}, so a "
        f"caller is now waiting on an outcome the crew can never produce"
    )
    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"the refusal came back as {answer!r}, which is not a Fault"
    )
    assert answer["kind"] == "refused", (
        f"a write with nothing in it is refused, not {answer['kind']!r}: a "
        f"caller retrying a conflict would retry this forever"
    )
    assert answer["what_to_do_instead"], "the refusal tells the caller nothing to do instead"


@then("the crew is given nothing to do")
def _the_room_is_still_empty(scenario: dict[str, object]) -> None:
    room = scenario["room"]

    assert next_in_order(room) is NOTHING, (
        f"the crew has {next_in_order(room)!r} to run, from a deposit that was refused"
    )
    assert not writer_waiting(room), (
        "the room says somebody is mid-flight, so every sync pass is dropped "
        "for a caller who was refused and went home"
    )
    assert room == room_make(), f"a refused deposit left {room!r} behind in the room"


# --- one deposit, one drain ------------------------------------------------


@when("a caller deposits one write and the crew drains once")
def _a_write_is_deposited_and_drained(scenario: dict[str, object]) -> None:
    room = room_make()
    asked: list[Statement] = []
    room_deposit(room, A_WRITE)
    _the_crew_drains(room, _a_stand_in_engine(asked))
    scenario["asked"] = asked


@then("the engine was asked to run it once")
def _the_engine_ran_it_once(scenario: dict[str, object]) -> None:
    asked = scenario["asked"]

    assert asked == [A_WRITE], (
        f"the engine was asked to run {asked!r}. One deposit is one write: "
        f"twice is the caller's row inserted twice, and never is a caller "
        f"waiting on a ticket for a write nobody can find."
    )


# --- two callers, two writes -----------------------------------------------


@when("two callers each deposit a write")
def _two_callers_deposit(scenario: dict[str, object]) -> None:
    room = room_make()
    scenario["room"] = room
    scenario["tickets"] = [room_deposit(room, write) for write in (A_WRITE, ANOTHER_WRITE)]


@then("each caller is given a ticket of its own")
def _the_two_tickets_differ(scenario: dict[str, object]) -> None:
    first, second = scenario["tickets"]

    assert isinstance(first, str) and isinstance(second, str), (
        f"one of the two callers was refused: {first!r} and {second!r}"
    )
    assert first != second, (
        f"both callers hold the ticket {first!r}, so each collects on the "
        f"other's write and one of the two outcomes is unreachable"
    )
    assert scenario["room"]["arrivals"] == [first, second], (
        f"the room has {scenario['room']['waiting']!r} waiting, so a ticket a "
        f"caller holds is not one the crew knows anybody is waiting on"
    )


@then("the crew is handed the two writes in the order they arrived")
def _the_writes_keep_their_order(scenario: dict[str, object]) -> None:
    room = scenario["room"]
    first, second = scenario["tickets"]

    # THE ORDER IS A LIST OF TICKETS AND THE WRITES ARE LOOKED UP BY THEM, which
    # is what lets a write leave the middle without the rest losing their
    # owners.
    in_order = [room["writes"][ticket] for ticket in room["arrivals"]]

    assert in_order == [A_WRITE, ANOTHER_WRITE], (
        f"the arrival order is {in_order!r}, and a write that drains "
        f"out of order lands in the database behind one that was written later"
    )

    # Read through the function a drainer reads with, one write at a time, since
    # the arrival order the list holds and the order it hands out are two
    # different claims.
    handed: list[Statement] = []
    _the_crew_drains(room, _a_stand_in_engine(handed))
    assert handed == [A_WRITE, ANOTHER_WRITE], f"the crew was handed {handed!r}"

    # EACH TICKET NAMES ITS OWN WRITE, and that is the join. Until 2026-09-22 a
    # room held the writes and the tickets in two lists side by side, and the
    # only thing joining them was that both were appended in step, which stops
    # being true the moment a ticket leaves the middle. This survives the drain
    # above, which empties the arrival order and leaves the writes behind.
    assert room["writes"][first] == A_WRITE, (
        f"the first caller's ticket names {room['writes'][first]!r}, so its "
        "outcome would be posted against somebody else's write"
    )
    assert room["writes"][second] == ANOTHER_WRITE, (
        f"the second caller's ticket names {room['writes'][second]!r}"
    )
