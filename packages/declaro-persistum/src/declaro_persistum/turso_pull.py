"""Bringing down what Turso has and this copy does not.

`docs/design/persistum.hd` declares this as `reader fn turso_pull`: it takes a
`Database`, it reads Turso, and it hands back nothing or a `Fault`. It is one
of the two moments `docs/architecture.md` puts the sync channel on the critical
path, and it is the inbound one. `fn turso_push` is the other direction.

IT ASKS; NOTHING ARRIVES ON ITS OWN. That is what makes it a reader rather than
a door. Two callers reach it: `orchestrator fn session_connect`, when a cold
copy has no schema and is therefore unusable rather than merely stale, and
`orchestrator fn lifecycle_catch_up`, on a spare moment when no writer is
waiting.

ONE PULL IS NOT ENOUGH, AND THAT IS THE WHOLE OF THE LOOP. pyturso's `pull()`
fetches ONE batch and answers whether it fetched anything, which means "there
may be more". Measured again on 2026-09-22 against the primary at
`TEST_TURSO_URL`: a copy whose primary has moved on answers True to the first
pull and False to the second, and the row written to the primary is readable
only after the second. Calling it once and keeping the answer leaves a copy
part way through and reports it current.

THE BOUND EXISTS BECAUSE A BUSY PRIMARY CAN ANSWER "MORE" FOREVER. A copy that
is still behind after `A_BATCH_BUDGET` batches is not converging, which is a
different answer from an ordinary pull and comes back as a different `Fault`.

A FAILURE COMES BACK, NEVER RAISED. The primary is not always reachable, and
`docs/architecture.md` is explicit that no consumer request path waits on it. A
pull that cannot reach Turso returns a `Fault` and leaves the database open and
answering from its local copy, which is the whole reason a replica has one.

IT CATCHES THE DRIVER'S ERRORS AND NOTHING ELSE. `turso.Error` is what pyturso
raises when the primary refuses, when the network is gone and when the channel
is shut; measured on 2026-09-22, a refused connection, a rejected credential
and a closed channel are all `turso.Error`. A `TypeError` or an `AttributeError`
is a bug in this package, and reporting our own bug as an unreachable primary
is how a caller spends an afternoon looking at the network.

IT DOES NOT ASK WHETHER THE DATABASE REPLICATES. `fn replicates` answers that
from the primary, and whether this function is reached at all is the
orchestrator's decision. Asking again here would be a second place that decides
it, and two places can disagree.
"""

from __future__ import annotations

import turso

from declaro_persistum.types import NOTHING, Connection, Database, Fault

# A primary under continuous write can keep answering "there may be more".
# Reaching this is the copy failing to converge, which is worth saying out loud
# rather than looping in silence.
A_BATCH_BUDGET = 64


async def turso_pull(db: Database) -> Fault | None:
    """Bring down everything the primary has that this copy does not."""
    try:
        drained = await _drained(db["connection"], A_BATCH_BUDGET)
    except turso.Error as refused:
        return _could_not_reach(db["primary"], refused)
    return _WHEN_THE_PULL_STOPPED[drained](db["primary"])


async def _drained(connection: Connection, budget: int) -> bool:
    """Pull batches until the primary reports nothing further.

    True when it reported nothing further, False when the budget ran out first.
    The handle is passed to the driver and never asked anything about itself.
    """
    remaining = budget
    fetched = True
    while fetched and remaining > 0:
        fetched = await connection.pull()
        remaining -= 1
    return not fetched


def _level(_primary: str | None) -> None:
    """The copy has everything the primary had, so there is nothing to say."""
    return NOTHING


def _not_converging(primary: str | None) -> Fault:
    """The budget ran out with the primary still reporting more."""
    return {
        "kind": "budget_spent",
        "what_went_wrong": (
            f"pulled {A_BATCH_BUDGET} batches from {primary} and it still "
            f"reported more, so this copy is not catching up"
        ),
        "what_to_do_instead": (
            "read from the local copy, which holds everything that did arrive, "
            "and pull again when the primary is quieter. A copy that never "
            "converges means the primary is written faster than it can be read."
        ),
    }


def _could_not_reach(primary: str | None, refused: turso.Error) -> Fault:
    """The driver stopped the pull, so the copy is stale rather than wrong."""
    return {
        "kind": "cut_short",
        "what_went_wrong": (
            f"the pull from {primary} stopped on {type(refused).__name__}: {refused}"
        ),
        "what_to_do_instead": (
            "read from the local copy, which is still open and still answers, "
            "and pull again later. Nothing here waits on the primary, so a "
            "caller that needs the primary's latest must ask for it again."
        ),
    }


# The two ways a pull ends, as a value each. Both keys are here because a bool
# has two, so nothing is ever looked up and found missing.
_WHEN_THE_PULL_STOPPED = {True: _level, False: _not_converging}
