"""Bind `tests/bdd/features/observability/clock_read.feature`, and nothing else.

WHAT THE FEATURE ASKS. The elapsed time on a shutdown line came from this
reader and not from inside the pure function that builds the line.

THE PURE FUNCTION IS THE EVIDENCE. `fn progress_line` takes the elapsed time as
an argument, so the only way it can carry a time is for a caller to have read
one and handed it in. A version that read the clock itself would take one fewer
argument, and that is what this scenario looks at: the number in the line is the
number that was passed.

WHY MONOTONIC. Every caller of this measures a duration. Wall time runs
backwards when a machine syncs its clock, which would make an elapsed time
negative and a retry's wait longer than any budget allows.
"""

from __future__ import annotations

import inspect
import time

from pytest_bdd import scenarios, then, when

from declaro_persistum.clock_read import clock_read
from declaro_persistum.crew_make import crew_make
from declaro_persistum.progress_line import progress_line
from declaro_persistum.types import Database

scenarios("../features/observability/clock_read.feature")


def _a_database() -> Database:
    return Database(
        target="libsql://notes.turso.io",
        engine="turso",
        connections=[],
        writers=[],
        channel=object(),
        crew=crew_make(),
        shutdown="replicate_then_exit",
    )


@when("a shutdown line is built", target_fixture="built")
def _a_shutdown_line() -> dict:
    """Read the clock twice around a wait, and hand the difference in."""
    began = clock_read()
    time.sleep(0.01)
    elapsed = clock_read() - began
    return {"line": progress_line(_a_database(), elapsed, False), "elapsed": elapsed}


@then("the elapsed time came from this reader, not from inside the pure function")
def _the_time_came_from_here(built: dict) -> None:
    assert built["line"]["elapsed"] == built["elapsed"], (
        f"the line says {built['line']['elapsed']} elapsed and the caller read "
        f"{built['elapsed']}, so the line is carrying a time this caller never "
        f"measured"
    )

    # THE PURE FUNCTION TAKES IT AS AN ARGUMENT, which is the only way it can
    # carry a time at all. One that read the clock itself would take one fewer.
    taken = list(inspect.signature(progress_line).parameters)
    assert "elapsed" in taken, (
        f"progress_line takes {taken} and none of them is an elapsed time, so "
        f"it is reading a clock, and a function that reads a clock is not the "
        f"same function on every machine"
    )

    # AND THIS READER MEASURES A DURATION RATHER THAN NAMING A MOMENT. Two
    # readings a moment apart go forwards, which wall time does not promise:
    # it runs backwards when a machine syncs, and an elapsed time would go
    # negative under a shutdown that was already racing a kill.
    first = clock_read()
    time.sleep(0.001)
    assert clock_read() > first, (
        "two readings a moment apart did not go forwards, so an elapsed time "
        "built from them can be negative and a retry's wait can outlast its budget"
    )
    assert built["elapsed"] > 0, f"no time passed across a wait: {built['elapsed']}"
