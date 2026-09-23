"""Bind `tests/bdd/features/core/writer_waiting.feature`, and nothing else.

WHAT THE FEATURE ASKS. Three readings, one question. A crew holding an untaken
write says a sync pass would be dropped; a crew holding nothing says a pass may
run; and a write whose outcome has come back is not a writer waiting.

THE THIRD SCENARIO IS THE ONE THAT BITES. Without it, a crew that had ever held
anything would block every sync pass for the life of the process, and the other
two scenarios would both pass.

THE CREW IS BUILT BY HANDING WRITES TO IT. A crew joins a ticket to its write
and its waiter, and hand-building one is how the shape changed under eight test
files in a day without one of them noticing.

WHAT A DROPPED PASS COSTS, which is why the verdict is read with `is` rather
than truthiness. A pass dropped by mistake costs one skipped sync and the next
free turn takes it. A pass RUN by mistake takes the connection out from under a
caller waiting on it. The two are not the same size.
"""

from __future__ import annotations

import asyncio

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.types import Crew, Statement
from declaro_persistum.writer_waiting import writer_waiting

scenarios("../features/core/writer_waiting.feature")

A_WRITE: Statement = {
    "text": "INSERT INTO orders (id, total) VALUES (:id, :total)",
    "parameters": {"id": 1, "total": 42},
}


def _a_crew_holding_one_write() -> Crew:
    """One write handed over, the way a caller hands one over."""

    async def _made() -> Crew:
        crew = crew_make()
        ticket = crew_deposit(crew, A_WRITE)
        assert isinstance(ticket, str), f"the write was refused: {ticket!r}"
        return crew

    return asyncio.run(_made())


def _a_crew_whose_write_was_answered() -> Crew:
    """The same, with the write taken and its outcome handed back.

    Taking a write out of the arrival order is a drainer's work, and a drainer
    is not built yet, so this does what one does: it takes the ticket out. What
    it must not do is leave it there, because then the third scenario would be
    the second wearing a different name.
    """
    crew = _a_crew_holding_one_write()
    crew["arrivals"].clear()
    return crew


# WHAT EACH OPENING MEANS, as the crew it builds.
A_CREW = {
    "one write handed to the crew": _a_crew_holding_one_write,
    "nothing handed to the crew": crew_make,
    "one write handed to the crew and its outcome handed back": (_a_crew_whose_write_was_answered),
}

# WHAT EACH READING ASSERTS. A writer waiting drops the pass, so the verdict a
# scenario states maps straight onto the bool the function returns.
THE_VERDICT = {"would be dropped": True, "may run": False}


@given(
    parsers.re(
        r"^(?P<opening>one write handed to the crew and its outcome handed back"
        r"|one write handed to the crew|nothing handed to the crew)$"
    ),
    target_fixture="crew",
)
def _the_crew_holds(opening: str) -> Crew:
    return A_CREW[opening]()


@then(parsers.re(r"^a sync pass (?P<verdict>would be dropped|may run)$"))
def _a_sync_pass(crew: Crew, verdict: str) -> None:
    wanted = THE_VERDICT[verdict]
    answered = writer_waiting(crew)
    assert answered is wanted, (
        f"the crew holds {crew['arrivals']} and the scenario says the pass "
        f"{verdict}, so writer_waiting has to answer {wanted} and it answered "
        f"{answered!r}"
    )
