"""How many connections this database may hold.

    fn permitted_connections : (asked: int | NOTHING) -> int | Fault

The deployment states the number, through `DECLARO_CONNECTIONS`, and nothing
overrides it.

COPYING TO THE CLOUD USED TO OVERRIDE IT AND NO LONGER DOES. The rule was that a
replicated database holds one connection, from a measurement on 2026-08-12 that
a second one answers `database tape error: database is busy`. That measurement
was about the one channel to the primary, which is the thing there is exactly
one of. Statements run on connections to the local file and the engine permits
many of those, which `docs/architecture.md:195` states outright.

AN UNSET VARIABLE IS A CASE AND NOT A DEFAULT, for the same reason as the crew.

ZERO CONNECTIONS IS A DATABASE NOTHING CAN BE ASKED OF.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Fault


def _what_was_asked_for(asked: int | None) -> int | Fault:
    """A number the deployment chose, used as it stands."""
    return asked  # type: ignore[return-value]


def _nobody_said(_asked: int | None) -> int | Fault:
    """The variable is unset, which is an answer rather than a gap to fill."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            "DECLARO_CONNECTIONS is not set, so nothing says how many "
            "connections this database may hold. Every connection is an OS "
            "thread, so the number is a property of the machine."
        ),
        what_to_do_instead=("Set DECLARO_CONNECTIONS to the number this deployment wants."),
    )


def _too_few_to_open(asked: int | None) -> int | Fault:
    """No connection at all, so nothing can be asked of the database."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"DECLARO_CONNECTIONS is {asked}, so no connection would open and "
            f"nothing could be asked of this database."
        ),
        what_to_do_instead="Set DECLARO_CONNECTIONS to at least 1.",
    )


# WHETHER SOMETHING WAS SAID, AND WHETHER IT CAN BE OPENED.
_THE_ANSWER: dict[tuple[bool, bool], Callable[[int | None], int | Fault]] = {
    (False, False): _nobody_said,
    (False, True): _nobody_said,
    (True, False): _too_few_to_open,
    (True, True): _what_was_asked_for,
}


def permitted_connections(asked: int | None) -> int | Fault:
    """Settle how many connections this database opens."""
    said = asked is not None
    return _THE_ANSWER[(said, said and asked >= 1)](asked)  # type: ignore[operator]
