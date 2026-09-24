"""Bind `tests/bdd/features/core/replicates.feature`, and nothing else.

WHAT THE FEATURE ASKS. A database holding a channel replicates and one without
does not, and nothing else anywhere records the answer.

NOTHING HERE TOUCHES A DATABASE. The call is handed a `Database` and reads one
field of it. The channel in these scenarios is a marker, because this function
never asks a channel anything: that is the whole of why it can be pure.

HOW "NOTHING ELSE RECORDS IT" IS ASKED. By reading `Database` itself rather than
by reading this module's text. A second field carrying the same fact is a field
that can disagree with the first, and the way that shows up is two callers
getting two answers about one database. So the scenario walks the record's own
fields and says which of them could hold it.
"""

from __future__ import annotations

import typing

import pytest
from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.replicates import replicates
from declaro_persistum.types import NOTHING, Crew, Database

scenarios("../features/core/replicates.feature")


# WHAT A CHANNEL IS HERE. A marker, because nothing in this function opens one
# or asks it anything: it is either there or it is the named absence.
A_CHANNEL_TO_A_PRIMARY = "a channel"

# The fields a second answer could hide in. Walked off `Database` itself, so a
# field added to the record is a field this scenario asks about.
WHAT_A_DATABASE_CARRIES = frozenset(typing.get_type_hints(Database))


@pytest.fixture
def scenario() -> dict:
    return {}


def _a_database(channel: object) -> Database:
    return Database(
        target="sqlite:///app.db",
        engine="sqlite",
        connections=[],
        writers=[],
        channel=channel,  # type: ignore[typeddict-item]
        crew=typing.cast(Crew, {}),
        shutdown="exit_immediately",
    )


# The two sentences, and what each puts in the channel's place.
_WHAT_EACH_GIVEN_HOLDS: dict[str, object] = {
    "a database opened with a channel to a primary": A_CHANNEL_TO_A_PRIMARY,
    "a database opened with no channel": NOTHING,
}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(_WHAT_EACH_GIVEN_HOLDS, key=len, reverse=True)))))
def _a_database_was_opened(scenario: dict, sentence: str) -> None:
    scenario["db"] = _a_database(_WHAT_EACH_GIVEN_HOLDS[sentence])


@then(parsers.re(r"^it (?P<claim>replicates|does not replicate)$"))
def _whether_it_replicates(scenario: dict, claim: str) -> None:
    wanted = claim == "replicates"
    answered = replicates(scenario["db"])

    assert answered is wanted, (
        f"a database whose channel is {scenario['db']['channel']!r} answered "
        f"{answered}, and the scenario says it {claim}. Getting this wrong "
        f"either skips the sync on a database that needed it, losing every local "
        f"commit at shutdown, or tries to sync one that has nowhere to sync to."
    )


@then("no field anywhere says it, beside the channel itself")
def _nothing_else_records_it() -> None:
    """A second field carrying this fact is a field that can disagree with it.

    READ OFF `Database` RATHER THAN OFF THIS MODULE. A gate over the source would
    say this function reads one field, which is a different claim: what matters
    is that there is nowhere else the answer could have been written down.
    """
    could_hold_it = sorted(
        name for name in WHAT_A_DATABASE_CARRIES if "replicat" in name or "sync" in name
    )

    assert could_hold_it == [], (
        f"`Database` carries {could_hold_it} beside its channel, and two fields "
        f"holding one fact are two callers getting two answers about one database"
    )
