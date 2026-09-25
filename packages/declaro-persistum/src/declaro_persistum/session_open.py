"""The one door a caller opens a database at.

    boundary_in fn session_open : (target: str, shutdown: Shutdown, primary: str | NOTHING, token: str | NOTHING) -> Database | Fault awaited
    side_effect reads "caller"
    side_effect reads "env:DECLARO_CREW_SIZE"
    side_effect reads "env:DECLARO_CONNECTIONS"

THE TARGET SAYS WHICH ENGINE, SO NOBODY NAMES ONE. `open_sqlite`,
`open_postgresql` and `open_turso` each stamped a constant, so a caller decided
the engine before writing a line and that decision then lived in their code, in
the one place it could go stale against the target beside it. One door reading
one string means a file can open all three in any order and name none of them.

FOUR ARGUMENTS, AND NOT ONE OF THEM HAS A DEFAULT. `shutdown` says what happens
to local commits when the process is asked to stop, and getting it wrong on disk
that does not survive the process costs the data rather than a feature. A
default would let a caller who never thought about it get whichever answer
somebody typed here. So Python's own missing-argument error is the refusal, and
a caller who says nothing hears about it before anything opens.

THE TWO NUMBERS COME OFF THE ENVIRONMENT AND NOT OFF THE CALL. How many
connections a database may hold and how many drainers write for it are
properties of the machine it runs on, not of the line that opens it. The same
program on a laptop and on a container with half a core wants different numbers
and identical source. So the deployment sets DECLARO_CONNECTIONS and
DECLARO_CREW_SIZE, this reads them, and `fn permitted_connections` and
`fn crew_size` refuse a deployment that set neither.

THEY ARE TWO NUMBERS AND NOT ONE. This record held one for both until
2026-09-22, and opening with the crew set to 2 and the connections to 4 opened
four writers.

WHAT COMES BACK IS THE HANDLE, NOT THE RECORD. `orchestrator fn session_connect`
answers with everything it worked out, which is what a reader of that
orchestrator wants and more than a caller needs. A caller gets the `Database`,
which is what every later call takes. Anything that refused the open comes back
as a `Fault` instead, in the return value, never raised.
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any, cast

from declaro_persistum.session_connect import session_connect
from declaro_persistum.types import NOTHING, Connecting, Database, Fault, Shutdown

# WHAT THE DEPLOYMENT SETS, AND WHICH FIELD EACH ONE FILLS. Adding a number the
# deployment states is one more row, and the name a deployment writes is beside
# the field it lands in rather than two lines apart.
WHAT_THE_DEPLOYMENT_STATES: dict[str, str] = {
    "DECLARO_CONNECTIONS": "connections_asked",
    "DECLARO_CREW_SIZE": "crew_asked",
}


def _not_a_number(name: str, written: str) -> Fault:
    """A variable that is set to something that is not a count."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"{name} is set to {written!r}, which is not a whole number. It says "
            f"how many, so it has to be one."
        ),
        what_to_do_instead=(
            f"Set {name} to a whole number, or unset it and hear which number "
            f"this deployment has to state."
        ),
    )


def _nothing_was_set(_name: str, _written: str | None) -> int | None:
    """The deployment said nothing, and that absence is the answer.

    IT HANDS BACK `NOTHING`, WHICH IS THIS PACKAGE'S WORD FOR IT. Written as a
    bare `None` it reads as a function somebody left half finished, and
    `types.py:123` says why the word exists: Python has one empty value and a
    reader has to see which of the two meanings is meant.

    The absence travels on rather than being filled in here.
    `fn permitted_connections` and `fn crew_size` each refuse it by name and
    each says what that number is for, and choosing one here would be the
    silent default those two exist to stop.
    """
    return NOTHING


def _whatever_it_was_set_to(name: str, written: str | None) -> int | Fault:
    """Read as a count, and refused when it is not one."""
    return _WHETHER_IT_IS_A_COUNT[str(written).lstrip("-").isdigit()](name, str(written))


def _the_count_it_is(_name: str, written: str) -> int:
    return int(written)


_WHETHER_IT_IS_A_COUNT: dict[bool, Callable[[str, str], int | Fault]] = {
    True: _the_count_it_is,
    False: _not_a_number,
}

_WHETHER_THE_DEPLOYMENT_SET_IT: dict[bool, Callable[[str, str | None], Any]] = {
    True: _whatever_it_was_set_to,
    False: _nothing_was_set,
}


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and frozenset(piece) == frozenset(
        {"kind", "what_went_wrong", "what_to_do_instead"}
    )


def _what_the_machine_says() -> dict[str, Any]:
    """Both numbers, each read by name, each still absent when nothing set it."""
    return {
        fills: _WHETHER_THE_DEPLOYMENT_SET_IT[os.environ.get(name) is not None](
            name, os.environ.get(name)
        )
        for name, fills in WHAT_THE_DEPLOYMENT_STATES.items()
    }


def _nothing_opened_yet(
    target: str,
    shutdown: Shutdown,
    primary: str | None,
    token: str | None,
    machine: dict[str, Any],
) -> Connecting:
    """What the caller said and what the machine said, and nothing worked out yet.

    Every other field is the empty value of its own type. `session_connect`
    fills each one in turn, so a reader of what comes back can see how far the
    open got.
    """
    return cast(
        "Connecting",
        {
            "target": target,
            "shutdown": shutdown,
            "primary": primary,
            "token": token,
            **machine,
            # NO `engine` KEY, AND THAT IS THE POINT OF THIS DOOR. Every other
            # field below is the empty value of its own type, and `Engine` has
            # no empty member: it is one of three words, and this door knows
            # none of them. `session_connect` reads the target and writes the
            # word in as its first step. A placeholder here would be this
            # module naming an engine, which is the one thing it exists not to
            # do, and `tests/unit/test_only_the_renderer_knows_an_engine.py`
            # said so on 2026-09-25.
            "settings": {},
            "permitted": 0,
            "crew_size": 0,
            "writers": [],
            "connections": [],
            "channel": NOTHING,
            "declared": {
                "models": [],
                "views": [],
                "dropping_tables": [],
                "dropping_views": [],
            },
            "live": {"tables": [], "views": []},
            "wanted_schema": {"tables": [], "views": []},
            "needs_migration": False,
            "migration": {"plan": {"operations": [], "destroys": []}, "applied": 0},
            "database": NOTHING,
        },
    )


def _whatever_refused_it(opened: object) -> Fault:
    return cast("Fault", opened)


def _the_handle_in_it(opened: object) -> Database:
    """The caller gets the `Database`, which is what every later call takes."""
    return cast("Database", dict(opened)["database"])  # type: ignore[call-overload]


_WHETHER_THE_OPEN_WAS_REFUSED: dict[bool, Callable[[object], Database | Fault]] = {
    True: _whatever_refused_it,
    False: _the_handle_in_it,
}


async def session_open(
    target: str, shutdown: Shutdown, primary: str | None, token: str | None
) -> Database | Fault:
    """Open the database the target names, and hand back what every later call reads.

    `target` says where it is, and the engine is read off it: a bare path is
    SQLite, `postgresql://` or `postgres://` is PostgreSQL, `libsql://` is Turso.
    `shutdown` says what happens to local commits when the process is asked to
    stop. `primary` and `token` are where a replica pulls from and what that
    primary accepts, and a database with no primary passes nothing for both.

    Anything that refused the open comes back as a `Fault` in the return value,
    naming what went wrong and what to do instead. Nothing here raises.
    """
    machine = _what_the_machine_says()
    refused = [one for one in machine.values() if _is_fault(one)]
    return await _WHETHER_THE_MACHINE_MADE_SENSE[refused != []](
        target, shutdown, primary, token, machine, refused
    )


async def _the_machine_is_set_wrong(
    _target: str,
    _shutdown: Shutdown,
    _primary: str | None,
    _token: str | None,
    _machine: dict[str, Any],
    refused: list[Any],
) -> Database | Fault:
    """A variable set to something that is not a count, said before anything opens."""
    return cast("Fault", refused[0])


async def _go_and_open_it(
    target: str,
    shutdown: Shutdown,
    primary: str | None,
    token: str | None,
    machine: dict[str, Any],
    _refused: list[Any],
) -> Database | Fault:
    opened = await session_connect(_nothing_opened_yet(target, shutdown, primary, token, machine))
    return _WHETHER_THE_OPEN_WAS_REFUSED[_is_fault(opened)](opened)


_WHETHER_THE_MACHINE_MADE_SENSE: dict[bool, Callable[..., Awaitable[Database | Fault]]] = {
    True: _the_machine_is_set_wrong,
    False: _go_and_open_it,
}
