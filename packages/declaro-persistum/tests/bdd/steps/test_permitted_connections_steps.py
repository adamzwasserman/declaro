"""Bind `tests/bdd/features/core/permitted_connections.feature`, and nothing else.

WHAT THE FEATURE ASKS. The deployment's number is the answer, an unset variable
is a refusal, and no connection at all is refused.

THE SECOND SCENARIO IS WHY THIS FUNCTION CHANGED. It used to cut a replicated
database to one, from a measurement on 2026-08-12 that a second connection to a
replica answers `database tape error: database is busy`. That measurement was
about the one channel to the primary. Statements run on connections to the local
file and the engine permits many of those, which `docs/architecture.md:195`
states outright. Reasoning about concurrency by counting connections has
produced a wrong answer in this package every time.
"""

from __future__ import annotations

import inspect

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.permitted_connections import permitted_connections

scenarios("../features/core/permitted_connections.feature")

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

WHAT_THE_DEPLOYMENT_SAID = {
    "a deployment asking for eight": 8,
    "no number set": None,
    "a deployment asking for none at all": 0,
}


@given(
    parsers.re(
        r"^(?P<opening>a deployment asking for eight|no number set"
        r"|a deployment asking for none at all)$"
    ),
    target_fixture="answered",
)
def _what_was_asked(opening: str) -> object:
    return permitted_connections(WHAT_THE_DEPLOYMENT_SAID[opening])


@then("eight are permitted")
def _eight(answered: object) -> None:
    assert answered == 8, (
        f"the deployment asked for eight and got {answered!r}. Every connection "
        f"is an OS thread, so the number is one a deployment sizes to its own "
        f"machine and this is not it."
    )


@then("it does not matter whether the database copies to the cloud")
def _replication_is_not_consulted() -> None:
    """Asked of the signature, because that is what makes it unaskable."""
    taken = list(inspect.signature(permitted_connections).parameters)
    assert taken == ["asked"], (
        f"permitted_connections takes {taken}. Anything beyond what the "
        f"deployment asked for is a fact it could cut the count by, and the one "
        f"that used to be here was replication, which bounds the one channel to "
        f"the primary and nothing else."
    )


@then("the answer says so")
def _unset_is_refused(answered: object) -> None:
    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"an unset variable answered {answered!r}, so a number nobody chose is "
        f"opening threads on a machine nobody measured"
    )
    assert "DECLARO_CONNECTIONS" in answered["what_to_do_instead"], (
        "the refusal does not say which variable to set"
    )


@then("the answer says so, and names what it was asked for")
def _none_is_refused(answered: object) -> None:
    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"no connection at all answered {answered!r}, so nothing could be asked "
        f"of this database and the open would look like it worked"
    )
    assert "0" in answered["what_went_wrong"], (
        f"the refusal does not say what it was asked for: {answered['what_went_wrong']!r}"
    )
