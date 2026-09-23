"""Bind `tests/bdd/features/crew/crew_post_receipt.feature`, and nothing else.

WHAT THE FEATURE ASKS. A drainer posts what a write did and the caller parked on
that ticket is given it. A ticket nobody is holding any more has its outcome
written to the log, and the crew keeps nothing for it.

NOTHING HERE TOUCHES A DATABASE. The call is handed a crew, a ticket and a
receipt. The write is over by the time anything gets here; what is being tested
is where its outcome goes.

THE LOG IS READ THROUGH `caplog`, WHICH IS THE REAL LOGGER. A stand-in that
recorded calls would pass while `fn log_write` wrote nothing an operator can
read, and "it is written to the log" is the whole of what the second scenario
asks. So the line is read out of the logging module, where an operator would
find it.

WHY THE SECOND SCENARIO CHECKS THE CREW TOO. A receipt reaching the log and the
crew still holding the write is the shape that grows for the life of the
process: every dropped request leaves an arrival nobody will ever drain.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_post_receipt import crew_post_receipt
from declaro_persistum.receipt_for import receipt_for
from declaro_persistum.types import Crew, Receipt, Statement, Ticket

scenarios("../features/crew/crew_post_receipt.feature")


A_WRITE: Statement = {
    "text": "INSERT INTO tasks (title) VALUES (:title)",
    "parameters": {"title": "wash the dog"},
}

# What the drainer says the write did, in the words `fn receipt_for` builds.
ROWS_IT_TOUCHED = 3
ATTEMPTS_IT_TOOK = 1


@pytest.fixture
def scenario(caplog: pytest.LogCaptureFixture) -> dict:
    """One crew per scenario, with the log being read from the start.

    `caplog` IS SET UP BEFORE THE CALL, NOT AFTER. A level raised afterwards
    catches nothing that already happened, which reads exactly like a function
    that logged nothing.
    """
    caplog.set_level(logging.INFO)
    return {"crew": crew_make(), "log": caplog}


def _a_deposited_ticket(crew: Crew) -> Ticket:
    """One write handed over, which is the only way a waiter comes to exist."""
    ticket = crew_deposit(crew, A_WRITE)
    assert isinstance(ticket, str), f"the deposit was refused: {ticket}"
    return ticket


def _an_outcome(ticket: Ticket) -> Receipt:
    """What a drainer would hand back for a write that landed."""
    return receipt_for(ticket, ROWS_IT_TOUCHED, None, ATTEMPTS_IT_TOOK)


# WHETHER THE CALLER IS STILL THERE WHEN THE DRAINER POSTS. A caller that left
# is one whose waiter is gone, which is what `fn crew_release_ticket` leaves
# behind when a caller is cancelled while parked, and the only trace of the
# departure the crew ever has.
_WHETHER_THE_CALLER_STAYED: dict[bool, Callable[[Crew, Ticket], None]] = {
    True: lambda _crew, _ticket: None,
    False: lambda crew, ticket: crew["waiting"].pop(ticket),
}


# The two sentences the feature writes, and whether each leaves a caller parked.
_WHETHER_THE_SENTENCE_LEAVES_A_CALLER: dict[str, bool] = {
    "": True,
    " whose caller has gone": False,
}


@when(parsers.re(r"a drainer posts an outcome for a ticket(?P<gone> whose caller has gone)?$"))
def _a_drainer_posts_an_outcome(scenario: dict, gone: str | None) -> None:
    """Deposit a write, then post its outcome, inside one event loop.

    ONE LOOP FOR BOTH, because a waiter belongs to the loop that made it. A
    deposit on one loop and a post on another would be posting to a waiter
    nothing is running.
    """

    async def _posted() -> dict:
        crew = scenario["crew"]
        ticket = _a_deposited_ticket(crew)
        _WHETHER_THE_CALLER_STAYED[_WHETHER_THE_SENTENCE_LEAVES_A_CALLER[gone or ""]](crew, ticket)
        receipt = _an_outcome(ticket)
        return {
            "ticket": ticket,
            "receipt": receipt,
            "answer": crew_post_receipt(crew, ticket, receipt),
        }

    scenario.update(asyncio.run(_posted()))


@then("the caller waiting on that ticket is given it")
def _the_caller_is_given_it(scenario: dict) -> None:
    """The waiter holds this write's own outcome, not some other write's."""
    waiter = scenario["crew"]["waiting"][scenario["ticket"]]

    assert waiter.done(), (
        f"the caller parked on {scenario['ticket']!r} is still waiting, so the "
        f"outcome went somewhere else"
    )
    assert waiter.result() == scenario["receipt"], (
        f"the caller was given {waiter.result()}, and the drainer posted {scenario['receipt']}"
    )


@then("it is written to the log")
def _it_is_written_to_the_log(scenario: dict) -> None:
    """An operator reading afterwards can see the write happened.

    WHAT HAS TO BE IN THE LINE is what nothing else records: the ticket, so it
    can be matched against the caller's own logs, and whether the write landed.
    """
    written = scenario["log"].text

    assert scenario["ticket"] in written, (
        f"the log never names ticket {scenario['ticket']!r}, so nothing joins "
        f"this line to the request that made the write: {written!r}"
    )
    assert str(ROWS_IT_TOUCHED) in written, (
        f"the log does not say how many rows the write touched: {written!r}"
    )


@then("nothing else is kept")
def _nothing_else_is_kept(scenario: dict) -> None:
    """A crew holding a write nobody will drain grows for the life of the process."""
    crew = scenario["crew"]
    ticket = scenario["ticket"]
    held = {
        "arrivals": [one for one in crew["arrivals"] if one == ticket],
        "writes": [one for one in crew["writes"] if one == ticket],
        "waiting": [one for one in crew["waiting"] if one == ticket],
    }

    assert held == {"arrivals": [], "writes": [], "waiting": []}, (
        f"the crew still holds {held} for a write that is over and a caller who has gone"
    )
