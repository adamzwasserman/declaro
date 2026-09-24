"""Whether this database replicates at all.

DERIVED, NEVER STORED. A database replicates when it holds a channel to a
primary. Nothing records the answer beside the channel, because a second field
could disagree with the first, and then two callers reading two fields would get
two answers about one database.

IT READS ONE FIELD AND NOTHING ELSE. `shutdown` says `replicate_then_exit` on
a replicated database and looks like the same fact, so it is worth saying that
it is not: `shutdown` is what to do on the way out, and a caller may set it
wrong. The channel is the thing that exists or does not.

IT DOES NOT TOUCH THE CONNECTION. A `Database` carries the driver's handle,
and asking it anything would make this a function that needs a live database to
answer a question about a dict.
"""

from declaro_persistum.types import NOTHING, Database


def replicates(db: Database) -> bool:
    """True when `db` holds a channel, which is the whole of the question.

    IT READ `primary` UNTIL 2026-09-24, and `Database` has not carried one since
    the crew was rebuilt. A target and a primary were two names for where a
    database is; what this turns on is whether there is a line to one, and that
    line is the channel.
    """
    return db["channel"] is not NOTHING
