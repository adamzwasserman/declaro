"""Bind `tests/bdd/features/core/progress_line.feature`, and nothing else.

WHAT THE FEATURE ASKS. Every line names where the database is and how long the
shutdown has been going, and every line is true if the process dies on it.

THE SECOND SCENARIO IS THE ONE THAT MATTERS. The platform kills the process at a
time nobody chose and nothing runs afterwards. A line saying the push finished,
written before it did, reads afterwards as a completed shutdown over data that
never left the machine.

THE HANDLE IS BUILT HERE BECAUSE NOTHING ELSE CAN BUILD ONE YET. The opener is
not written. Every field is filled, so a field added to `Database` fails here
rather than being quietly absent, and the two databases differ only in where
they are, which is the one thing the last scenario turns on.
"""

from __future__ import annotations

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.progress_line import progress_line
from declaro_persistum.types import Database

scenarios("../features/core/progress_line.feature")

FOUR_SECONDS = 4.0


def _a_database(target: str) -> Database:
    """One handle, every field said out loud."""
    return Database(
        target=target,
        engine="turso",
        connections=[],
        writers=[],
        channel=object(),
        crew=crew_make(),
        shutdown="replicate_then_exit",
    )


ONE_PLACE = "libsql://notes.turso.io"
ANOTHER_PLACE = "libsql://orders.turso.io"

# ONE FUNCTION OVER A TABLE. The two openings differ in whether the push has
# finished and in nothing else.
WHETHER_IT_FINISHED = {
    "a shutdown has been running for four seconds": False,
    "the push finishes": True,
}


@when(
    parsers.re(r"^(?P<opening>a shutdown has been running for four seconds|the push finishes)$"),
    target_fixture="line",
)
def _one_line(opening: str) -> dict:
    return progress_line(_a_database(ONE_PLACE), FOUR_SECONDS, WHETHER_IT_FINISHED[opening])


@then("the line says which database and says four seconds")
def _names_both(line: dict) -> None:
    assert ONE_PLACE in line.values(), (
        f"the line says {line!r} and names no database, so two of them exiting "
        f"at once write the same line and neither says which is holding up the exit"
    )
    assert FOUR_SECONDS in line.values(), (
        f"the line says {line!r} and carries no elapsed time, so progress and a "
        f"stall look identical"
    )


@then("a process killed after that line leaves a true last line")
def _true_if_it_is_the_last(line: dict) -> None:
    said = " ".join(str(value) for value in line.values())

    assert line["done"] is False, (
        f"a shutdown still pushing says done is {line['done']!r}, so a process "
        f"killed here leaves a line reading as a finished shutdown over data "
        f"that never left the machine"
    )
    assert "not" in said or "yet" in said, (
        f"the line says {said!r}, which read on its own says the data arrived. "
        f"Nothing runs after the kill, so this is what somebody finds."
    )


@then("the final line says the primary has everything")
def _the_last_line(line: dict) -> None:
    assert line["done"] is True, (
        f"the push finished and the line says done is {line['done']!r}, so a "
        f"shutdown that worked reads as one that was cut short"
    )
    said = " ".join(str(value) for value in line.values())
    assert "has everything" in said, f"the last line does not say it arrived: {said!r}"


@given("two databases shutting down from different places", target_fixture="two_lines")
def _two_databases() -> list[dict]:
    return [
        progress_line(_a_database(place), FOUR_SECONDS, False)
        for place in (ONE_PLACE, ANOTHER_PLACE)
    ]


@then("their lines name different databases")
def _told_apart(two_lines: list[dict]) -> None:
    named = [line["shutting_down"] for line in two_lines]

    assert named[0] != named[1], (
        f"both databases wrote {named[0]!r}, so an operator watching two exit "
        f"together cannot tell which one is stalled, which is the whole reason "
        f"a line names one at all"
    )
    assert all(named), f"a line names no database at all: {named}"
