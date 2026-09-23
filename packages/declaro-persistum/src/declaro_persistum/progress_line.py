"""One line of a shutdown, fit to be the last one.

    fn progress_line : (db: Database, elapsed: float, done: bool) -> dict<str, Value>

NOBODY IS WATCHING A DEPLOY, so this is the only account of a shutdown anybody
will read. `docs/architecture.md:314`: shutdown blocks until the primary has
everything, retrying rather than giving up, because the usual deployment has
disk that does not survive the process.

EVERY LINE HAS TO BE TRUE IF IT IS THE LAST ONE. The platform kills the process
at a time nobody chose and nothing runs afterwards, so a line saying the push
finished, written before it did, is a line that will be read as a completed
shutdown over data that never left the machine.

"REPLICATING" ALONE SAYS NOTHING. It does not tell progress from a stall, and it
does not say which database is holding up the exit. So a line carries the time
elapsed, which moves, and where the database is, which is the one thing that
names it: an engine is shared, a connection may never be read, and a crew
belongs to whichever database made it.

IT BUILDS THE FACTS AND WRITES NOTHING. `boundary_out fn log_write` takes a dict
whose values are a string, a number, a bool or nothing, so there is no formatted
message here and no exception in the vocabulary.
"""

from __future__ import annotations

from declaro_persistum.types import Database, Value

# WHAT EACH STATE SAYS, in words that stay true if the process dies on this
# line. Both are written out, so the ordinary line is a row rather than the
# answer a check happened to fall through to.
_WHETHER_IT_ARRIVED: dict[bool, str] = {
    True: "the primary has everything",
    False: "the data has not reached the primary yet",
}


def progress_line(db: Database, elapsed: float, done: bool) -> dict[str, Value]:
    """Build the facts of one shutdown line."""
    return {
        "level": "info",
        "shutting_down": db["target"],
        "elapsed": elapsed,
        "state": _WHETHER_IT_ARRIVED[done],
        "done": done,
    }
