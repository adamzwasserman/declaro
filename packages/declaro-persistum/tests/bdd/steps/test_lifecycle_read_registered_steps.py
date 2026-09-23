"""Bind `tests/bdd/features/lifecycle/lifecycle_read_registered.feature`.

WHAT THE FEATURE ASKS. Every database that asked to replicate at exit comes
back, and nothing registered is an answer rather than a failure.

THE LIST IS FILLED BY REGISTERING, NOT BY WRITING IT OUT. `boundary_out fn
lifecycle_register` is what a database calls as it opens, and reaching into the
list here would let this file agree with a shape nothing else has.

THE PROCESS IS LEFT AS IT WAS FOUND. One process has one list, so a scenario
that added to it and walked away would put databases into every test that ran
afterwards.
"""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_read_registered import lifecycle_read_registered
from declaro_persistum.lifecycle_register import REGISTERED, lifecycle_register
from declaro_persistum.types import Database

scenarios("../features/lifecycle/lifecycle_read_registered.feature")


def _a_database(target: str, shutdown: str) -> Database:
    """One handle, every field said out loud."""
    return Database(
        target=target,
        engine="turso",
        connections=[],
        writers=[],
        channel=object(),
        crew=crew_make(),
        shutdown=shutdown,  # type: ignore[typeddict-item]
    )


@pytest.fixture(autouse=True)
def _the_list_is_left_as_it_was_found():
    """One process, one list. A scenario that grew it would grow every test."""
    was = list(REGISTERED)
    yield
    REGISTERED[:] = was


# ONE FUNCTION OVER A TABLE. The two openings differ in how many databases asked
# and in nothing else.
HOW_MANY_ASKED = {
    "three databases opened asking to replicate at exit": 3,
    "no database asking to replicate": 0,
}


@given(
    parsers.re(
        r"^(?P<opening>three databases opened asking to replicate at exit"
        r"|no database asking to replicate)$"
    ),
    target_fixture="opened",
)
def _databases_opened(opening: str) -> list[Database]:
    REGISTERED.clear()
    opened = [
        _a_database(f"libsql://notes-{n}.turso.io", "replicate_then_exit")
        for n in range(HOW_MANY_ASKED[opening])
    ]
    for db in opened:
        lifecycle_register(db)

    # AND ONE THAT DID NOT ASK, so an empty answer means nobody asked rather
    # than nobody opened. A database that only exits is not on the list.
    lifecycle_register(_a_database("/tmp/local.db", "exit_immediately"))
    return opened


@when("the list is read", target_fixture="read_back")
def _the_list_is_read() -> object:
    return lifecycle_read_registered()


@then("all three come back")
def _all_three(read_back: object, opened: list[Database]) -> None:
    assert read_back == opened, (
        f"the list read back {read_back!r} and three databases asked to "
        f"replicate. One missing is one whose local commits never reach the "
        f"primary, on a machine whose disk does not survive the process."
    )


@then("it is empty, and that is not a failure")
def _empty(read_back: object) -> None:
    assert read_back == [], (
        f"nobody asked to replicate and the list read back {read_back!r}, so a "
        f"shutdown would wait on a database that never wanted pushing"
    )
    assert isinstance(read_back, list), (
        f"an empty list came back as {read_back!r}, so a process that opened no "
        f"replicating database looks like one whose shutdown failed"
    )
