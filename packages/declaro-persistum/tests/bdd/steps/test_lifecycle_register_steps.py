"""Bind `tests/bdd/features/lifecycle/lifecycle_register.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `lifecycle_register(db: Database) -> NOTHING | Fault`
puts one database on the list shutdown works through, or leaves it off. Two
sentences, and they differ in one field of the handle: a caller who asked to
replicate at exit is on the list, and a caller who asked only to exit is not.

WHY THE WHEN SAYS "OPENED" AND THIS FILE CALLS ONE FUNCTION. `orchestrator fn
session_connect` opens a database and invokes `lifecycle_register` while it does.
Nobody else calls it, so the only thing an open does about the shutdown list is
that one call, and that call is what each When makes. A step that opened a real
database would be a test of `orchestrator fn session_connect`, which is not
written and is not this card.

WHY THE TWO WHENS ARE ONE FUNCTION. They differ in the shutdown the caller asked
for and in nothing else, so the word is a table and the body is written once.

THE LIST IS REAL AND IT IS PUT BACK. `REGISTERED` is a module-level list, which
is what the declaration means by the shutdown list, and the steps write to it and
read it back rather than standing anything in for it. An autouse fixture records
what was on it and puts exactly that back, so a database this file opened is
never on the list a later test reads, and `reader fn lifecycle_read_registered`
never sees one of ours.

THE CONNECTION IS A HANDLE. The `Database` built below carries a plain object in
its `connection` field. Nothing in this card opens anything or asks a connection
a question, so a real one would only be slower.
"""

from __future__ import annotations

import pytest
from pytest_bdd import scenarios, then, when

from declaro_persistum.lifecycle_register import REGISTERED, lifecycle_register
from declaro_persistum.types import Database, Shutdown

scenarios("../features/lifecycle/lifecycle_register.feature")


@pytest.fixture(autouse=True)
def the_shutdown_list_is_put_back(scenario: dict[str, object]):
    """Record what was on the list, and give exactly that back afterwards.

    The list is process-wide. A database this file left on it would be a handle
    `orchestrator fn lifecycle_shut_down` tries to push at exit, and the
    connection in it is not a connection.
    """
    was_on_it = list(REGISTERED)
    scenario["what was on the list"] = was_on_it
    yield
    REGISTERED[:] = was_on_it


# --- the opens -------------------------------------------------------------
#
# One function over a table. The two sentences differ in the shutdown the caller
# asked for and in nothing else.

WHAT_THE_CALLER_ASKED_FOR: dict[str, Shutdown] = {
    "a database is opened asking to replicate at exit": "replicate_then_exit",
    "a database is opened asking only to exit": "exit_immediately",
}


def opening_a_database(asked_for: Shutdown):
    """Build the When that opens a database asking for that shutdown."""

    def step(scenario: dict[str, object]) -> None:
        db: Database = {
            "engine": "sqlite",
            "connection": object(),
            "writers": [],
            "room": {"arrivals": [], "waiting": []},
            "shutdown": asked_for,
            "primary": "libsql://notes.example.invalid",
        }
        scenario["the database"] = db
        scenario["what came back"] = lifecycle_register(db)

    return step


for sentence, shutdown in WHAT_THE_CALLER_ASKED_FOR.items():
    when(sentence)(opening_a_database(shutdown))


# --- what has to be true afterwards ----------------------------------------


@then("shutdown will work through it")
def _it_is_on_the_list(scenario: dict[str, object]) -> None:
    db = scenario["the database"]
    came_back = scenario["what came back"]

    assert came_back is None, f"registering a database came back with a refusal: {came_back}"

    on_the_list = [held for held in REGISTERED if held is db]
    assert len(on_the_list) == 1, (
        f"the database this caller opened is on the shutdown list "
        f"{len(on_the_list)} times, and shutdown pushes what is on that list"
    )

    # THE HANDLE ITSELF, NOT A COPY OF IT. Shutdown pushes through the connection
    # in the handle, so a copy would be a database with no live connection in it
    # and the push would have nothing to push with.
    assert REGISTERED[-1] is db, (
        "the list holds something other than the handle the caller was given"
    )

    was_on_it = scenario["what was on the list"]
    assert REGISTERED[: len(was_on_it)] == was_on_it, (
        "registering one database disturbed the databases already on the list"
    )


@then("shutdown has nothing to push for it")
def _it_is_not_on_the_list(scenario: dict[str, object]) -> None:
    db = scenario["the database"]
    came_back = scenario["what came back"]

    # NOT A FAILURE. A caller who asked only to exit asked for something this
    # library does, so a refusal here would be the library objecting to a
    # perfectly good answer.
    assert came_back is None, (
        f"a caller who asked only to exit was refused rather than left off: {came_back}"
    )

    assert not [held for held in REGISTERED if held is db], (
        "a database whose caller asked only to exit is on the shutdown list, so "
        "exit will block pushing a database nobody asked to replicate"
    )

    was_on_it = scenario["what was on the list"]
    assert was_on_it == REGISTERED, (
        "opening a database that asked only to exit changed the shutdown list: "
        f"{was_on_it} became {REGISTERED}"
    )
