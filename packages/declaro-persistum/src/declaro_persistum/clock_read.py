"""Asking the clock what time it is.

    reader fn clock_read : () -> float

A PURE FUNCTION READS NO CLOCK, so the reading happens here and the answer is
handed inward. `fn next_wait` works out how long to wait from the instant its
caller read, and `fn progress_line` names the elapsed time on every shutdown
line, and neither can ask for itself without stopping being the same function on
every machine.

IT IS A READER AND NOT A DOOR. The program asks; nothing arrives on its own. It
stands with the doors, calls nothing, and heads no chain, and the orchestrators
that need the time call it and hand its answer to the pure functions.

IT CANNOT FAIL, so it returns a float and no fault arm. A machine with no clock
is not a state this library can do anything about.

MONOTONIC RATHER THAN WALL TIME. Every caller of this measures a duration:
how long a shutdown has been pushing, how long since a retry was asked for. Wall
time runs backwards when a machine syncs, which would make an elapsed time
negative and a wait longer than any budget.
"""

from __future__ import annotations

import time


def clock_read() -> float:
    """Ask the clock what time it is, in seconds, for measuring a duration."""
    return time.monotonic()
