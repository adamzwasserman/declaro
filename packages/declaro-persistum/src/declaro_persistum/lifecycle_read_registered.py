"""The databases that asked to replicate at exit.

    reader fn lifecycle_read_registered : () -> list<Database> | Fault

THE SIGNAL CARRIES NO ARGUMENTS. The platform sends a shutdown and nothing
comes with it, so the door has to find out for itself which databases are
waiting to push. Without this reader it would be handed a list the module wrote
itself, which is a module holding state nobody can see.

A DATABASE THAT DID NOT ASK IS NOT ON THE LIST. `boundary_out fn
lifecycle_register` writes one down as it opens, and only one that asked to
replicate at exit gets written, so a caller who never opted in changes nothing
about what happens at exit.

NOTHING REGISTERED IS AN ANSWER. A process that opened no replicating database
has an empty list and a shutdown with nothing to do, which is an ordinary state
and not a failure.

WHAT COMES BACK IS A COPY. The list is read while a shutdown works through it,
and a database opening at that moment would otherwise change the list underneath
the loop reading it.
"""

from __future__ import annotations

from declaro_persistum.lifecycle_register import REGISTERED
from declaro_persistum.types import Database, Fault


def lifecycle_read_registered() -> list[Database] | Fault:
    """Read the databases that asked to replicate at exit."""
    return list(REGISTERED)
