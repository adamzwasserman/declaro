"""Bind `tests/bdd/features/core/replicates.feature`.

WHAT THE FEATURE ASKS FOR. Two scenarios ask the question and read the answer.
The third asks something a normal test cannot: that nothing else in the package
records whether a database replicates. That one is answered two ways, because
either alone would pass while the design was broken.

First it reads the shapes. Every TypedDict in `declaro_persistum.types` is
searched for a field whose NAME claims replication, and there must be none. A
`replicating: bool` beside `primary` is exactly the second answer the feature
forbids, and only a scan of the declared fields can see it.

Then it reads the behaviour. `primary` is moved and the answer has to move with
it; every other field of a `Database` is moved and the answer has to stay put.
A function that read `shutdown` would pass the field scan and fail here, since
`shutdown` says `replicate_then_exit` without being a record of replication.
"""

import typing

import pytest
from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum import types as declared_types

scenarios("../features/core/replicates.feature")


# Words a field name would use if it were keeping its own answer. `primary` is
# not among them on purpose: it is the one the answer is derived FROM.
A_CLAIM_OF_REPLICATION = ("replicat", "replica", "syncing", "synced", "mirror", "follower")

# What the feature's two openings mean, as the name of the fixture that builds
# each one. Two step functions differing only in which fixture they named were
# one decision written twice, which is the shape this package deletes.
A_DATABASE_OPENED = {
    "against a primary": "a_replicated_database",
    "with no primary": "a_database",
}

# What the feature's two readings assert. The verdict is a value, so one step
# reads it rather than two steps each pasting one in.
THE_VERDICT = {"replicates": True, "does not replicate": False}


@pytest.fixture
def answer() -> dict:
    """Where a Given leaves the database and a Then reads the answer."""
    return {}


@given(parsers.re(r"a database opened (?P<opening>against a primary|with no primary)"))
def _a_database_opened(answer: dict, request: pytest.FixtureRequest, opening: str) -> None:
    answer["db"] = request.getfixturevalue(A_DATABASE_OPENED[opening])


@then(parsers.re(r"it (?P<verdict>replicates|does not replicate)"))
def _it_replicates_or_not(answer: dict, verdict: str) -> None:
    from declaro_persistum.replicates import replicates

    assert replicates(answer["db"]) is THE_VERDICT[verdict]


@then("no field anywhere says it, beside the primary itself")
def _no_field_says_it(a_database: dict) -> None:
    from declaro_persistum.replicates import replicates

    offenders = [
        f"{shape.__name__}.{field}"
        for shape in vars(declared_types).values()
        if isinstance(shape, type) and typing.is_typeddict(shape)
        for field in typing.get_type_hints(shape)
        if any(word in field.lower() for word in A_CLAIM_OF_REPLICATION)
    ]
    assert offenders == [], f"a second place records replication: {offenders}"

    # The answer moves with `primary`, and with nothing else.
    assert replicates({**a_database, "primary": "libsql://team-app.turso.io"}) is True
    assert replicates({**a_database, "primary": None}) is False

    everything_else = {
        "engine": ["sqlite", "postgresql", "turso"],
        "connection": [object(), None],
        "writers": [[], [object()]],
        "room": [{"arrivals": [], "waiting": []}, {"arrivals": [object()], "waiting": ["t1"]}],
        "shutdown": ["exit_immediately", "replicate_then_exit"],
    }
    unmoved = [
        (field, value, primary, replicates({**a_database, "primary": primary, field: value}))
        for field, values in everything_else.items()
        for value in values
        for primary in [None, "libsql://team-app.turso.io"]
    ]
    wrong = [seen for seen in unmoved if seen[3] is not (seen[2] is not None)]
    assert wrong == [], f"the answer moved with something other than the primary: {wrong}"
