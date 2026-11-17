"""The list shutdown works through, and putting one database on it.

    boundary_out fn lifecycle_register : (db: Database) -> NOTHING | Fault
    side_effect writes "shutdown list"

WHY A LIST EXISTS AT ALL. A signal carries nothing in its hands. The handler the
platform calls is handed a number and a stack frame, and neither of them says
which databases are open or which of them have local commits the primary has
never seen. So the databases that asked to replicate at exit write themselves
down as they open, and `reader fn lifecycle_read_registered` reads `REGISTERED`
back when the signal arrives.

WHAT GOES ON IT IS WHAT THE CALLER ASKED FOR. `docs/architecture.md` line 122:
the shutdown policy is a required argument with no default, because a default
cannot tell "chose this" from "never knew there was a choice", and on disk that
does not survive the process a default would silently pick the losing side. That
argument is the whole of the decision here. A caller who asked to replicate at
exit is on the list; a caller who asked only to exit is not, and opening one
changes nothing about what happens at exit.

IT READS ONE FIELD AND NOTHING ELSE. `shutdown` says what to do on the way out.
Having a primary is a different fact, answered by `fn replicates`, and this
function does not ask it: a handle that says `replicate_then_exit` with nowhere
to replicate to is a settings problem, refused at open by `fn
refuse_unusable_settings`, and re-deciding it here would be a second opinion
about one caller's request.

NO BRANCH, AND NO ROW MISSING. The two words a caller may ask for are the two
rows of the table below, so neither arm can go untaken and unseen. A word outside
those two is not a default: it comes back as a refusal naming the word that
arrived, because a handle carrying a shutdown nobody recognises is a handle
nobody can promise anything about at exit.

IT HOLDS THE HANDLE, NEVER A COPY. Shutdown pushes through the connection inside
the handle. A copy would carry a connection that was opened for somebody else, so
the list keeps exactly what the caller was given, and nothing here opens the
handle up or asks it a question.

IT NAMES NO ENGINE AND CARRIES NO SQL. A `Database` says which engine it is, and
this function never reads that field.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from declaro_persistum.types import NOTHING, Database, Fault, Shutdown

# THE SHUTDOWN LIST, IN THE ORDER THE DATABASES OPENED. One process, one list,
# because there is one set of signals and the handler answers for all of them.
# `reader fn lifecycle_read_registered` is the only thing that reads it.
REGISTERED: list[Database] = []


def _put_it_on_the_list(db: Database) -> None:
    """A caller who asked to replicate at exit."""
    REGISTERED.append(db)
    return NOTHING


def _leave_it_off(_db: Database) -> None:
    """A caller who asked only to exit, so exit has nothing to push for it."""
    return NOTHING


# WHAT EACH SHUTDOWN THE CALLER MAY ASK FOR MEANS FOR THE LIST. Both words in
# `Shutdown` are here, so there is no unexercised arm and no default.
WHAT_TO_DO_WITH_IT: dict[Shutdown, Callable[[Database], None | Fault]] = {
    "replicate_then_exit": _put_it_on_the_list,
    "exit_immediately": _leave_it_off,
}


def _no_word_for_that(asked_for: object) -> Callable[[Database], Fault]:
    """The refusal a shutdown nobody recognises earns, naming what arrived."""

    def refuse(_db: Database) -> Fault:
        return {
            "kind": "refused",
            "what_went_wrong": f"This handle asks for {asked_for!r} at shutdown, "
            "and there is no such shutdown. Nothing can be promised about this "
            "database at exit, so it was neither put on the shutdown list nor "
            "quietly left off it.",
            "what_to_do_instead": "Open the database asking for one of the two "
            "shutdowns: replicate_then_exit, which blocks at exit until the "
            "primary has every local commit, or exit_immediately, which pushes "
            "nothing and is only safe on disk that survives the process.",
        }

    return refuse


# WHETHER THERE IS A WORD FOR WHAT THIS HANDLE ASKED. `Shutdown` names two words
# and `WHAT_TO_DO_WITH_IT` answers both, so this row is never taken by a handle
# this package built. It is taken by one that arrived carrying something else,
# which is a caller reaching past the door, and that earns a refusal rather than
# a default. Asking the table for one would also write the refusal on every
# register, including every one that worked.
_WHETHER_THERE_IS_A_WORD_FOR_IT: dict[
    bool, Callable[[object], Callable[[Database], None | Fault]]
] = {
    True: lambda asked_for: WHAT_TO_DO_WITH_IT[cast("Shutdown", asked_for)],
    False: _no_word_for_that,
}


def lifecycle_register(db: Database) -> None | Fault:
    """Put this database on the shutdown list, or leave it off, as it asked."""
    asked_for = db["shutdown"]
    return _WHETHER_THERE_IS_A_WORD_FOR_IT[asked_for in WHAT_TO_DO_WITH_IT](asked_for)(db)
