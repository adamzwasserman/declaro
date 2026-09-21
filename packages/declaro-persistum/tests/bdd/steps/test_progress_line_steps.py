"""Bind `tests/bdd/features/core/progress_line.feature`.

WHAT THE FEATURE ASKS FOR. Four results about one line. It names the database.
It says how long the shutdown has been running. Until the push ends it says the
data is not in the cloud yet, so whichever line the kill lands after is still
true. When the push ends it says the cloud has everything. And two databases
exiting together get lines that name different databases.

THE LINE IS A VALUE, NOT SOMETHING ON A SCREEN. `fn progress_line` returns the
event, and `boundary_out fn log_write` is the only card that writes one. So
every assertion here reads a dict that came back, and nothing here captures
output.

WHY THE ELAPSED TIME IS HANDED IN. The function reads no clock; `reader fn
clock_read` does that at the edge and passes the answer inward. That is what
lets these scenarios compare exact numbers instead of ranges.
"""

from __future__ import annotations

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.progress_line import progress_line

scenarios("../features/core/progress_line.feature")


# The fields the feature asks the line to carry, each named once here so a
# rename shows up as one failing name rather than as six pasted strings.
DATABASE = "database"
ELAPSED = "elapsed_seconds"
ARRIVED = "reached_the_cloud"
SAYING = "saying"

FOUR_SECONDS = 4.0

# The two moments the feature builds a line at, as the arguments each one
# means. Two `When` steps that differed only in these two values would be one
# decision written twice, so they are one function reading this table.
A_LINE_IS_BUILT = {
    "a shutdown has been running for four seconds": (FOUR_SECONDS, False),
    "the push finishes": (FOUR_SECONDS, True),
}

# Elapsed times a kill could land after. The point is that every one of them
# leaves a line that is still true, so the spread runs from the instant the
# shutdown starts to an hour in.
ANY_ELAPSED = (0.0, 0.25, 4.0, 90.0, 3600.0)

TWO_PRIMARIES = ("libsql://orders-app.turso.io", "libsql://people-app.turso.io")


@when(parsers.re(r"(?P<moment>a shutdown has been running for four seconds|the push finishes)"))
def _a_line_is_built(scenario: dict, a_replicated_database: dict, moment: str) -> None:
    elapsed, done = A_LINE_IS_BUILT[moment]
    scenario["db"] = a_replicated_database
    scenario["elapsed"] = elapsed
    scenario["line"] = progress_line(a_replicated_database, elapsed, done)


@then("the line says which database and says four seconds")
def _the_line_names_the_database_and_the_time(scenario: dict) -> None:
    line = scenario["line"]
    db = scenario["db"]

    assert line[DATABASE] == db["primary"], (
        f"the line names {line[DATABASE]!r}, and the database exiting is "
        f"{db['primary']!r}; an operator cannot tell which one is holding up the exit"
    )
    assert line[ELAPSED] == FOUR_SECONDS, (
        f"four seconds have passed and the line says {line[ELAPSED]!r}"
    )


@then("a process killed after any line leaves a true last line")
def _every_line_before_the_end_is_true(a_replicated_database: dict) -> None:
    lines = [progress_line(a_replicated_database, elapsed, False) for elapsed in ANY_ELAPSED]

    claiming = [line for line in lines if line[ARRIVED]]
    assert not claiming, (
        "the push has not finished and these lines say the cloud has the data, "
        f"so a kill landing after one of them leaves a false last line: {claiming}"
    )

    # Fit to be the last line also means fit to be read on its own: each one
    # still has to say which database and how long it has been going.
    incomplete = [
        line
        for line, elapsed in zip(lines, ANY_ELAPSED, strict=True)
        if line[DATABASE] != a_replicated_database["primary"] or line[ELAPSED] != elapsed
    ]
    assert not incomplete, (
        "these lines do not name the database and the time they were built at, "
        f"so a kill after one of them leaves an operator nothing to read: {incomplete}"
    )


@then("the final line says Turso has everything")
def _the_final_line_says_it_arrived(scenario: dict) -> None:
    line = scenario["line"]
    assert line[ARRIVED] is True, (
        f"the push finished and the line still says {line[ARRIVED]!r}; a caller "
        "reading it cannot tell a finished shutdown from a stalled one"
    )

    # THE WORDS HAVE TO MOVE WITH THE FACT. A line that flipped the field and
    # read the same to a person would pass the assertion above and still tell an
    # operator the data has not arrived.
    still_going = progress_line(scenario["db"], scenario["elapsed"], False)
    assert line[SAYING] != still_going[SAYING], (
        f"the finished line and the unfinished one both read {line[SAYING]!r}"
    )


@given("two databases shutting down against different primaries")
def _two_databases_exiting_together(scenario: dict, a_replicated_database: dict) -> None:
    scenario["lines"] = [
        progress_line({**a_replicated_database, "primary": primary}, FOUR_SECONDS, False)
        for primary in TWO_PRIMARIES
    ]


@then("their lines name different databases")
def _their_lines_name_different_databases(scenario: dict) -> None:
    named = [line[DATABASE] for line in scenario["lines"]]
    assert named == list(TWO_PRIMARIES), (
        f"two databases exiting against {list(TWO_PRIMARIES)} produced lines "
        f"naming {named}; an operator cannot tell which one is holding up the exit"
    )
