"""How many drainers this database runs.

    fn crew_size : (asked: int | NOTHING) -> int | Fault

The deployment states the number, through `DECLARO_CREW_SIZE`, and nothing
overrides it.

COPYING TO THE CLOUD USED TO OVERRIDE IT AND NO LONGER DOES. The rule was that a
replicated database runs one drainer, from a measurement that a second
connection to a replica is refused. That measurement was about the one channel
to the primary. A drainer writes on a connection to the local file, never on the
channel, so the number of drainers has nothing to do with it.
`docs/architecture.md:195` says so: whether a crew works over a replicated file
has never been measured, which is unverified rather than unreachable.

AN UNSET VARIABLE IS A CASE AND NOT A DEFAULT. `architecture.md:122`: a default
cannot tell a caller who chose from a caller who forgot, and the path nobody
chose is the path nobody tested. The right crew is a property of the machine,
measured at 32 on one Render tier and 64 on another, so a number picked here
would be one nobody chose applied to a machine nobody measured.

ZERO DRAINERS IS A DATABASE THAT NEVER WRITES. A deposit would take a ticket,
nothing would ever run it, and the caller would wait on an outcome nobody can
produce.
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
            "DECLARO_CREW_SIZE is not set, so nothing says how many drainers "
            "this database runs. The right number is a property of the machine: "
            "it was measured at 32 on one Render tier and 64 on another."
        ),
        what_to_do_instead=(
            "Set DECLARO_CREW_SIZE to the number of drainers this deployment "
            "wants. Size it to the CPUs you actually have; a 0.5 CPU instance "
            "does worse at 16 than at 1."
        ),
    )


def _too_few_to_write(asked: int | None) -> int | Fault:
    """A crew of none, which takes tickets and runs nothing."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"DECLARO_CREW_SIZE is {asked}, so no drainer would run. Every "
            f"write would take a ticket and wait on an outcome nobody can "
            f"produce."
        ),
        what_to_do_instead="Set DECLARO_CREW_SIZE to at least 1.",
    )


# WHETHER SOMETHING WAS SAID, AND WHETHER IT CAN BE RUN. Both questions are
# answered before either is asked, so every pair is a row and no answer is
# reached through a fallback.
_THE_ANSWER: dict[tuple[bool, bool], Callable[[int | None], int | Fault]] = {
    (False, False): _nobody_said,
    (False, True): _nobody_said,
    (True, False): _too_few_to_write,
    (True, True): _what_was_asked_for,
}


def crew_size(asked: int | None) -> int | Fault:
    """Settle how many drainers run, from what the deployment asked for."""
    said = asked is not None
    return _THE_ANSWER[(said, said and asked >= 1)](asked)  # type: ignore[operator]
