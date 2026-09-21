"""How many connections this database may hold.

`docs/design/persistum.hd` line 684:
`fn permitted_connections : (asked: int | NOTHING, replicated: bool) -> int | Fault`

THE DEPLOYMENT STATES THE NUMBER, AND ONE MEASURED RULE OVERRIDES IT. A
replicated database holds one sync connection. Measured 2026-08-12 against a
real primary with pyturso 0.7.2 and recorded in `docs/architecture.md` line 179:
a second connection answers `database tape error: database is busy`, 3 of 4 runs
outright and once after 12 retries over 30 seconds on an idle database. In the
run where eight opened, 5 writes landed locally and 0 reached the primary.

THAT NUMBER BOUNDS CONNECTIONS AND NOTHING ELSE. It is not a write limit and
not a concurrency limit. Concurrent writers come from `BEGIN CONCURRENT` on one
connection, so a replica running MVCC writes concurrently while still permitting
one sync connection. No sentence about queueing follows from the 1 below.

AN UNSET NUMBER IS A CASE, NOT A DEFAULT, on a local database. The right number
is a property of the machine, so inventing one here would hand every deployment
a number nobody chose and nobody could tell apart from a number somebody did.
The refusal comes back as a `Fault` in the return value, never raised.

ON A REPLICA AN UNSET NUMBER COSTS NOTHING, because the deployment's number is
overridden anyway. `permitted_connections.feature` decides this: "a replicated
database" states no number and then asks for exactly one connection.

IT READS NOTHING. No clock, no environment, no file, no connection. The number
the deployment set arrives as `asked`, already read by whoever opened the
database, so this function is the same function on every machine.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from declaro_persistum.types import NOTHING, Fault


def _the_measured_one(_asked: int | None) -> int | Fault:
    """A replicated database holds one sync connection, whatever was asked for.

    The underscore is the point: this answer does not depend on what the
    deployment stated, because the engine has already settled it.
    """
    return 1


def _what_the_deployment_set(asked: int | None) -> int | Fault:
    """A local database holds the number the deployment stated."""
    # Reached only under the key that says `asked` is not NOTHING, so the cast
    # states what the key already decided rather than testing it a second time.
    return cast(int, asked)


def _nobody_set_a_number(_asked: int | None) -> int | Fault:
    """Nothing was stated and nothing here may stand in for it."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            "No number of connections was set, and this database is not replicated, "
            "so there is no measured number to fall back on."
        ),
        what_to_do_instead=(
            "State the number where the database is opened. The right number is a "
            "property of the machine, so nothing in this library can invent it."
        ),
    )


# KEYED BY THE TWO FACTS, so all four combinations are written down and a reader
# can see which one is which. The first element says whether the deployment
# stated a number; the second is whether the database is replicated.
_ANSWER: dict[tuple[bool, bool], Callable[[int | None], int | Fault]] = {
    (False, True): _the_measured_one,
    (False, False): _what_the_deployment_set,
    (True, True): _the_measured_one,
    (True, False): _nobody_set_a_number,
}


def permitted_connections(asked: int | None, replicated: bool) -> int | Fault:
    """Settle how many connections this engine actually permits.

    `asked` is what the deployment stated, or NOTHING when it stated nothing.
    """
    return _ANSWER[(asked is NOTHING, replicated)](asked)
