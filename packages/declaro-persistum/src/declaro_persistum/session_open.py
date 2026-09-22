"""`session_open`: the one door a caller opens a database through.

    boundary_in fn session_open : (target: str, shutdown: Shutdown)
        -> Database | Fault awaited
        side_effect reads "caller"
        side_effect reads "env:DECLARO_CREW_SIZE"
        side_effect reads "env:DECLARO_CONNECTIONS"
        invokes session_connect

A DOOR IS THIN, AND THIS IS THE WHOLE OF IT. It takes what a caller said, reads
the number the deployment set, builds the record `orchestrator fn
session_connect` carries, calls it, and hands back the handle that orchestrator
built. Everything else about opening a database happens behind it, in the
twenty-eight functions that orchestrator sequences. Anything more here would be
a second place where opening is decided.

ONE DOOR, AND THE TARGET SAYS WHICH ENGINE. There were three, `open_sqlite`,
`open_postgresql` and `open_turso`, and each stamped a constant, so a caller
decided the engine before writing a line and that decision then lived in their
code, in the one place it could go stale against the target beside it. There is
one now. `fn engine_of` reads the target, once, behind this door, and this
module names no engine at all.

`shutdown` HAS NO DEFAULT AND MUST NOT GAIN ONE. The usual deployment has a
disk that does not survive the process, so whether the data is still there
tomorrow rests on the push at exit. A caller who did not say what should happen
and a caller who chose to exit immediately want different things, and a default
makes them the same caller. So the argument is required, and a caller who says
nothing about it has not made the call.

A SETTING IS CARRIED, NOT READ. `busy_timeout_s` means nothing to PostgreSQL and
`primary` means nothing to SQLite, and one door has to accept the union of what
three engines need. Every keyword a caller adds travels in `settings` to
`fn refuse_unusable_settings`, which refuses one the detected engine cannot use
rather than dropping it on the floor. Nothing about them is decided here.

IT IS AWAITED, BECAUSE `session_connect` IS. aiosqlite, asyncpg and pyturso are
all async-only, and nothing in pyturso is natively synchronous. Calling a
coroutine without awaiting it raises nothing and does nothing, because a
coroutine is truthy, so the one call below is awaited.

A FAILURE COMES BACK IN THE RETURN, IN THE WORDS IT ARRIVED IN. Any of the
twenty-eight functions behind this door can hand back a `Fault`, and it travels
out of here untouched. A refusal reworded twice is a refusal whose author
nobody can find.

TWO VARIABLES, AND EACH HAS ITS OWN FIELD SINCE 2026-09-22. `type Connecting`
carried one number for both until then, so `orchestrator fn session_connect`
handed it to `fn permitted_connections` and to `fn crew_size` alike. Measured by
opening a database with the crew set to 2 and the connections to 4: four writers
opened, and the crew size a deployment had asked for was gone without a word.
`crew_asked` is what the deployment said and `crew` is what was settled, the way
`connections` and `permitted` already sat beside each other.

A VARIABLE NOBODY SET IS ABSENCE AND NOT A NUMBER. Both come through as
`NOTHING`, and `fn permitted_connections` and `fn crew_size` each take that as a
case of its own. Inventing a number here would make a deployment that chose one
and a deployment that forgot look the same.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, cast

from declaro_persistum.session_connect import session_connect
from declaro_persistum.types import NOTHING, Database, Fault, Shutdown, Value

# =============================================================================
# telling a failure from a handle, once
# =============================================================================
#
# A `Fault` IS THE THREE FIELDS AND NOTHING ELSE, so what came back is read by
# its shape rather than by a flag somebody remembered to set.

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_a_fault(answered: object) -> bool:
    """Whether what the orchestrator handed back is a failure on its way out."""
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _the_fault_unchanged(answered: Any) -> Database | Fault:
    """It failed, so it leaves in the words the function that found it wrote."""
    return cast(Fault, answered)


def _the_handle_it_built(answered: Any) -> Database | Fault:
    """It worked, so the caller gets the handle out of the record."""
    return cast(Database, answered["database"])


# Whether the open failed. Both keys are here because a bool has two.
WHAT_THE_ORCHESTRATOR_ANSWERED: dict[bool, Callable[[Any], Database | Fault]] = {
    True: _the_fault_unchanged,
    False: _the_handle_it_built,
}


# =============================================================================
# the number the deployment stated
# =============================================================================
#
# AN UNSET VARIABLE IS ABSENCE AND NOT A NUMBER. `fn permitted_connections` and
# `fn crew_size` each take absence as a case of its own and answer it their own
# way, so nothing here stands in for it. `env DECLARO_CONNECTIONS : int |
# Absent` is the declaration's own word for the same thing.

WHAT_THE_DEPLOYMENT_STATED_THE_CONNECTIONS_IN = "DECLARO_CONNECTIONS"
WHAT_THE_DEPLOYMENT_STATED_THE_CREW_IN = "DECLARO_CREW_SIZE"


def _nothing_was_stated(_said: str | None) -> int | None:
    return NOTHING


def _the_number_it_names(said: str | None) -> int | None:
    """Reached only under the key that says the variable is set, so the cast
    states what the key already decided rather than testing it again."""
    return int(said)  # type: ignore[arg-type]


WHETHER_THE_DEPLOYMENT_STATED_IT: dict[bool, Callable[[str | None], int | None]] = {
    True: _nothing_was_stated,
    False: _the_number_it_names,
}


def _a_number_the_deployment_stated(variable: str) -> int | None:
    said = os.environ.get(variable)
    return WHETHER_THE_DEPLOYMENT_STATED_IT[said is NOTHING](said)


# =============================================================================
# the door
# =============================================================================


async def session_open(target: str, shutdown: Shutdown, **settings: Value) -> Database | Fault:
    """Open a database and hand back the handle, or say why it could not be opened.

    `target` says where the database is, and it is the only thing that says
    which engine is behind it: a bare path or `sqlite://` is SQLite,
    `postgresql://` or `postgres://` is PostgreSQL, and `libsql://` is Turso.
    `shutdown` says what happens to unreplicated writes when the process ends,
    and it has no default. Any further keyword is a setting for whichever engine
    the target names, and one that engine cannot use comes back refused.
    """
    what_the_caller_said: Any = {
        "target": target,
        "shutdown": shutdown,
        "connections": _a_number_the_deployment_stated(
            WHAT_THE_DEPLOYMENT_STATED_THE_CONNECTIONS_IN
        ),
        "crew_asked": _a_number_the_deployment_stated(WHAT_THE_DEPLOYMENT_STATED_THE_CREW_IN),
        "settings": settings,
    }
    answered = await session_connect(what_the_caller_said)
    return WHAT_THE_ORCHESTRATOR_ANSWERED[_is_a_fault(answered)](answered)
