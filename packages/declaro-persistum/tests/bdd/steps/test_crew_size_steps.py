"""Bind `tests/bdd/features/core/crew_size.feature`, and nothing else.

WHAT THE FEATURE ASKS. The deployment's number is the answer, an unset variable
is a refusal rather than a number nobody chose, and a crew of none is refused.

THE SECOND SCENARIO IS THE ONE THAT MATTERS. Until 2026-09-23 a database that
copies to the cloud was cut to one drainer, from a measurement about the one
channel to the primary. A drainer writes on a connection to the local file and
never touches the channel. The scenario asks that the answer does not depend on
replication at all, which is the only way to keep that belief from coming back.

THE SIGNATURE IS WHAT ENFORCES IT. `crew_size` takes what was asked and nothing
else, so there is no way for it to consult replication even by accident.
"""

from __future__ import annotations

import inspect

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.crew_size import crew_size

scenarios("../features/core/crew_size.feature")

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# ONE FUNCTION OVER A TABLE. The three openings differ in what the deployment
# said and in nothing else.
WHAT_THE_DEPLOYMENT_SAID = {
    "a deployment asking for eight": 8,
    "no crew size set": None,
    "a deployment asking for none at all": 0,
}


@given(
    parsers.re(
        r"^(?P<opening>a deployment asking for eight|no crew size set"
        r"|a deployment asking for none at all)$"
    ),
    target_fixture="answered",
)
def _what_was_asked(opening: str) -> object:
    return crew_size(WHAT_THE_DEPLOYMENT_SAID[opening])


@then("eight drainers run")
def _eight(answered: object) -> None:
    assert answered == 8, (
        f"the deployment asked for eight and got {answered!r}, so the number a "
        f"deployment tuned to its own machine is not the number that runs"
    )


@then("it does not matter whether the database copies to the cloud")
def _replication_is_not_consulted() -> None:
    """Asked of the signature, because that is what makes it unaskable.

    A scenario passing the same database twice would prove only that today's
    body ignores it. One argument means replication cannot be reached from here
    at all.
    """
    taken = list(inspect.signature(crew_size).parameters)
    assert taken == ["asked"], (
        f"crew_size takes {taken}. Anything beyond what the deployment asked "
        f"for is a fact it could cut the crew by, and the one that used to be "
        f"here was replication, which bounds the channel and not the "
        f"connections a drainer writes on."
    )


@then("the answer says so rather than picking a number quietly")
def _unset_is_refused(answered: object) -> None:
    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"an unset variable answered {answered!r}, so a number nobody chose is "
        f"running on a machine nobody measured"
    )
    assert "DECLARO_CREW_SIZE" in answered["what_to_do_instead"], (
        "the refusal does not say which variable to set"
    )


@then("the answer says so, and names what it was asked for")
def _none_is_refused(answered: object) -> None:
    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a crew of none answered {answered!r}, so every write would take a "
        f"ticket and wait on an outcome nobody can produce"
    )
    assert "0" in answered["what_went_wrong"], (
        f"the refusal does not say what it was asked for: {answered['what_went_wrong']!r}"
    )
