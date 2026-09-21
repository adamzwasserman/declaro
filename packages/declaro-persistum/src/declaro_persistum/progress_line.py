"""A shutdown line fit to be the last one.

WHAT SHUTDOWN LOOKS LIKE FROM OUTSIDE. `docs/architecture.md` says shutdown
blocks until the cloud has every local commit, retrying instead of giving up,
because the usual deployment has disk that does not survive the process. So an
operator watching a process refuse to exit needs to see it working rather than
hung, and every line has to name which database is holding up the exit and how
long it has been at it. "Replicating" alone says neither.

EVERY LINE IS FIT TO BE THE LAST ONE. The platform kills the process when its
grace period runs out, and nothing runs after the kill. There is no closing line
to correct an earlier one, so a line written while the push is still going says
the data is not in the cloud yet, and that stays true however the shutdown ends.
Only the line written after the push finishes says the cloud has everything.

IT RETURNS THE LINE, IT DOES NOT WRITE ONE. `boundary_out fn log_write` takes
an event and writes it, and it is the only card in this package that does. This
one builds the event and hands it back, which is what lets a test read it.

THE ELAPSED TIME IS HANDED IN. `reader fn clock_read` asks the clock at the
edge, and the answer is passed inward. Reading a clock here would make the same
three arguments give a different line every time it ran.

THE PRIMARY IS THE NAME, BECAUSE A DATABASE HAS NO OTHER. What a `Database`
holds is an engine, a driver handle nobody may open, a room, a shutdown policy
and the primary it pushes to. Several databases exit at once and the engine is
the same word for all of them, so the primary is the only field that tells them
apart. A database with no primary never reaches this line: `fn replicates`
answers first, and a local database has nothing to push.
"""

from __future__ import annotations

from declaro_persistum.types import Database, Value

# WHAT THE LINE SAYS WHILE IT IS GOING, AND WHAT IT SAYS ONCE IT IS DONE. Two
# rows, read by whether the push has finished, because done and not done is a
# table of two rows rather than a decision made at runtime.
#
# THE FACT AND THE WORDS COME OUT OF ONE ROW. An operator reads the words and a
# later reader of the log reads the field, and they are the same claim. Holding
# them in one row is what stops the two disagreeing, which is exactly what
# happens when a message is written in one place and the flag beside it in
# another.
WHAT_THE_LINE_SAYS: dict[bool, dict[str, Value]] = {
    False: {
        "reached_the_cloud": False,
        "saying": "still pushing; the cloud does not have this database yet",
    },
    True: {
        "reached_the_cloud": True,
        "saying": "the push finished; the cloud has everything",
    },
}


def progress_line(db: Database, elapsed: float, done: bool) -> dict[str, Value]:
    """Build one shutdown line: which database, how long, and where the data is.

    It reads the primary and nothing else, so it answers about a dict rather
    than about a live database.
    """
    return {
        "database": db["primary"],
        "elapsed_seconds": elapsed,
        **WHAT_THE_LINE_SAYS[done],
    }
