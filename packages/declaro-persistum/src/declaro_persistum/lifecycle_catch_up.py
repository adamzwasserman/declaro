"""What one drainer spends a free turn on.

    orchestrator fn lifecycle_catch_up : (catching_up: CatchingUp) -> CatchingUp | Fault

TWO USES OF ONE SPARE MOMENT, AND THEY ARE NOT RELATED TO EACH OTHER. Run what
the crew is holding, and if nobody is waiting to write, push what is local up
the channel and bring down what came the other way. The second only happens
because the first found nothing to do.

WHY THE SYNC IS DROPPED RATHER THAN QUEUED. A turn that finds a writer waiting
does the writing and never reaches the sync, and nothing anywhere records the
pass that did not happen. So ten busy turns leave nothing behind them and the
next free turn runs one pass rather than ten. Recording them would be a backlog
of round trips that all want the same channel, which is the thing there is
exactly one of.

WHY A CONFLICT IS RETRIED HERE AND NOT BY THE CALLER. The caller handed over a
write and holds a ticket; it is not in a loop and cannot be. A conflict lost a
race and the next run is a different race, so this runs it again. A refusal is a
statement about the write itself and will not change, so it goes back to its
caller at once. `fn worth_retrying` reads the fault's kind and nothing else.

EACH ATTEMPT WAITS LONGER THAN THE ONE BEFORE, AND TWO WRITES DO NOT WAIT IN
STEP. `fn next_wait` doubles the wait and spreads it by the ticket, so two
writes that conflicted in the same instant come back at different instants.
Without the spread they would collide again, and again, until both budgets went.

A SPENT BUDGET IS AN ANSWER, NOT A REFUSAL. A write that only ever conflicted
was never wrong; it never won a race. Its caller is told how many attempts it
took, and `fn refusal_reaches_caller` says that kind comes back as a receipt
rather than being raised.

EVERY OUTCOME REACHES ITS OWN CALLER. The drainer never speaks to a caller:
`fn crew_post_receipt` puts the outcome where that ticket's caller is parked.
A write whose caller has gone is logged rather than lost, which that card owns.

THE ORCHESTRATOR DECIDES AND THE DOORS DO. Nothing here opens a connection,
writes SQL or names an engine. It reads the crew, asks the pure functions what
follows, and hands the doors what they take.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from declaro_persistum.budget_spent import budget_spent
from declaro_persistum.channel_pull import channel_pull
from declaro_persistum.channel_push import channel_push
from declaro_persistum.clock_read import clock_read
from declaro_persistum.crew_post_receipt import crew_post_receipt
from declaro_persistum.crew_run_write import crew_run_write
from declaro_persistum.crew_wait_for_work import crew_wait_for_work
from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.log_write import log_write
from declaro_persistum.next_in_order import next_in_order
from declaro_persistum.next_wait import next_wait
from declaro_persistum.receipt_for import receipt_for
from declaro_persistum.replicates import replicates
from declaro_persistum.types import NOTHING, CatchingUp, Crew, Draining, Fault, Ticket
from declaro_persistum.worth_retrying import worth_retrying
from declaro_persistum.writer_waiting import writer_waiting

# HOW MANY ATTEMPTS ONE WRITE GETS BEFORE ITS BUDGET IS SPENT. It is here rather
# than on the database because no caller has ever asked for a different number,
# and a setting nobody sets is a setting nobody has tested. The day one asks, it
# moves to the declaration that opened the database.
HOW_MANY_ATTEMPTS_A_WRITE_GETS = 5

# HOW LONG A TURN SLEEPS BEFORE IT GIVES UP WAITING FOR WORK. A write rings the
# bell and wakes the sleeper at once, whatever this is, so it decides how often
# an idle database takes a free turn to sync rather than how fast a write is
# picked up.
HOW_LONG_A_DRAINER_SLEEPS_WITH_NOTHING_TO_DO = 1.0


async def _run_one_write(
    draining: Draining, ticket: Ticket, statement: Any
) -> tuple[int, Fault | None, int]:
    """Run one write, retrying while a conflict says it is worth it.

    What comes back is how many rows it touched, what stopped it if anything,
    and how many attempts it took. All three are what a receipt carries.
    """
    engine = draining["db"]["engine"]
    transacting = how_to_write(engine)
    attempts = 0
    stopped: Fault | None = None
    answer: Any = 0
    # THE LOOP ENDS WHEN THE WRITE IS OVER, and what "over" means is one pure
    # question asked of the fault and the count: it landed, or it was refused,
    # or its budget went. `_try_again` is that question and the loop reads it.
    while attempts == 0 or _try_again(stopped, attempts):
        await _WHETHER_TO_WAIT_FIRST[attempts > 0](attempts, ticket)
        attempts = attempts + 1
        answer = await crew_run_write(draining["writer"], engine, statement, transacting)
        stopped = _WHAT_STOPPED_IT[isinstance(answer, dict)](answer)
    return _WHAT_IT_TOUCHED[stopped is None](answer), stopped, attempts


def _try_again(stopped: Fault | None, attempts: int) -> bool:
    """Whether this write gets another attempt: it lost a race and has budget."""
    return (
        stopped is not None
        and worth_retrying(stopped)
        and not budget_spent(attempts, HOW_MANY_ATTEMPTS_A_WRITE_GETS)
    )


async def _wait_first(attempts: int, ticket: Ticket) -> float:
    """Sleep the doubling, spread wait before another attempt, and say how long.

    THE SPREAD IS WHAT KEEPS TWO WRITES APART. `fn next_wait` draws it from the
    ticket, so two writes that conflicted in the same instant come back at
    different instants rather than colliding again until both budgets go.
    """
    waited = next_wait(attempts - 1, clock_read(), ticket)
    await asyncio.sleep(waited)
    return waited


async def _go_straight_in(_attempts: int, _ticket: Ticket) -> float:
    """The first attempt waits for nothing, and that is a real answer."""
    return 0.0


_WHETHER_TO_WAIT_FIRST: dict[bool, Callable[[int, Ticket], Awaitable[float]]] = {
    True: _wait_first,
    False: _go_straight_in,
}

_WHAT_STOPPED_IT: dict[bool, Callable[[Any], Fault | None]] = {
    True: lambda answer: answer,
    False: lambda _answer: None,
}

_WHAT_IT_TOUCHED: dict[bool, Callable[[Any], int]] = {
    True: lambda answer: int(answer),
    False: lambda _answer: 0,
}


def _the_budget_was_spent(stopped: Fault | None, attempts: int) -> Fault | None:
    """A write that only ever conflicted was never wrong; it never won a race.

    ITS KIND CHANGES SO ITS CALLER CAN TELL. `fn refusal_reaches_caller` reads
    `budget_spent` as a receipt and `refused` as something raised, which is the
    difference between "the engine will never take this" and "nobody ever got a
    turn".
    """
    return Fault(
        kind="budget_spent",
        what_went_wrong=(
            f"A write lost a race {attempts} times and its allowance is "
            f"{HOW_MANY_ATTEMPTS_A_WRITE_GETS} attempts, so it stopped being "
            f"retried. The last thing the engine said: "
            f"{_WHAT_IT_LAST_SAID[stopped is None](stopped)}"
        ),
        what_to_do_instead=(
            "Declare the write again. It was never refused: every attempt lost a "
            "race to another writer, and the next one is a different race."
        ),
    )


# What the engine said last, or nothing when nothing stopped it. Both rows are
# here so the message is built from a value rather than from a lookup that finds
# nothing.
_WHAT_IT_LAST_SAID: dict[bool, Callable[[Any], str]] = {
    True: lambda _stopped: "",
    False: lambda stopped: str(stopped["what_went_wrong"]),
}


def _whatever_stopped_it(stopped: Fault | None, _attempts: int) -> Fault | None:
    return stopped


_WHAT_THE_CALLER_IS_TOLD: dict[bool, Callable[[Fault | None, int], Fault | None]] = {
    True: _the_budget_was_spent,
    False: _whatever_stopped_it,
}


def _ran_out_of_attempts(stopped: Fault | None, attempts: int) -> bool:
    return (
        stopped is not None
        and worth_retrying(stopped)
        and budget_spent(attempts, HOW_MANY_ATTEMPTS_A_WRITE_GETS)
    )


async def _drain_one(draining: Draining, ticket: Ticket) -> None:
    """One write, run and reported, and taken off the crew either way."""
    crew = draining["db"]["crew"]
    statement = crew["writes"][ticket]
    touched, stopped, attempts = await _run_one_write(draining, ticket, statement)
    told = _WHAT_THE_CALLER_IS_TOLD[_ran_out_of_attempts(stopped, attempts)](stopped, attempts)
    crew_post_receipt(crew, ticket, receipt_for(ticket, touched, told, attempts))


async def _drain_everything(draining: Draining) -> None:
    """Every write the crew is holding, one at a time, in arrival order.

    `fn next_in_order` IS PURE AND TAKES NOTHING OFF THE CREW. It says which
    ticket is next; `fn crew_post_receipt` is what removes it once the write is
    over, which is why this loop ends.
    """
    crew: Crew = draining["db"]["crew"]
    while writer_waiting(crew):
        ticket = next_in_order(crew)
        await _drain_one(draining, ticket)  # type: ignore[arg-type]


async def _sync(draining: Draining) -> Fault | None:
    """Push what is local, then bring down what is not.

    THE ORDER IS NOT A PREFERENCE. What is here has to go up before what is
    there comes down, or a local commit made since the last pass is overwritten
    by an older version of the same rows arriving from the primary.
    """
    channel = draining["db"]["channel"]
    sent = await channel_push(channel)
    return await _THEN_PULL[sent is None](channel, sent)


async def _pull_as_well(channel: Any, _sent: Fault | None) -> Fault | None:
    return await channel_pull(channel)


async def _the_push_failed(_channel: Any, sent: Fault | None) -> Fault | None:
    """Nothing is brought down over a channel that would not take what we sent."""
    return sent


_THEN_PULL: dict[bool, Callable[..., Awaitable[Fault | None]]] = {
    True: _pull_as_well,
    False: _the_push_failed,
}


# BOTH HALVES SAY WHETHER A PASS RAN AND WHAT STOPPED IT, so neither is a call
# made for its effect. A quiet half returning nothing at all reads the same as a
# half nobody wrote, and the caller could not tell a dropped pass from one that
# ran and went well.
async def _a_pass(draining: Draining) -> tuple[bool, Fault | None]:
    return True, await _sync(draining)


async def _no_pass(_draining: Draining) -> tuple[bool, Fault | None]:
    """A writer is waiting, or there is no channel. The pass is dropped and
    nothing records it, which is why ten dropped passes do not pile up."""
    return False, None


async def _sync_if_it_is_free(draining: Draining) -> tuple[bool, Fault | None]:
    """The second use of the turn, which only happens because the first found
    nothing: a database that replicates and a crew holding nothing."""
    crew = draining["db"]["crew"]
    free = replicates(draining["db"]) and not writer_waiting(crew)
    return await _WHETHER_THE_TURN_IS_FREE[free](draining)


_WHETHER_THE_TURN_IS_FREE: dict[
    bool, Callable[[Draining], Awaitable[tuple[bool, Fault | None]]]
] = {
    True: _a_pass,
    False: _no_pass,
}


async def lifecycle_catch_up(catching_up: CatchingUp) -> CatchingUp | Fault:
    """Spend one free turn: run what the crew holds, then sync if nobody writes.

    What comes back is what arrived, with `writer_waiting` and `attempts` saying
    what the turn found and what it took. A sync the channel refused comes back
    as a `Fault`, because local commits that did not leave are what shutdown has
    to hear about.
    """
    draining = Draining(
        db=catching_up["db"],
        writer=catching_up["writer"],
        stop=catching_up["db"]["crew"]["stop"],
        idle=HOW_LONG_A_DRAINER_SLEEPS_WITH_NOTHING_TO_DO,
    )
    await crew_wait_for_work(draining)
    await _drain_everything(draining)
    ran, stopped = await _sync_if_it_is_free(draining)
    log_write({"event": "turn_finished", "synced": ran})
    return _WHAT_THE_TURN_ANSWERS[stopped is None](catching_up, draining, stopped)


def _the_turn_that_ran(
    catching_up: CatchingUp, draining: Draining, _stopped: Fault | None
) -> CatchingUp:
    return CatchingUp(
        db=catching_up["db"],
        writer=catching_up["writer"],
        statement=NOTHING,
        writer_waiting=writer_waiting(draining["db"]["crew"]),
        attempts=catching_up["attempts"],
    )


def _what_the_channel_said(
    _catching_up: CatchingUp, _draining: Draining, stopped: Fault | None
) -> Fault:
    log_write({"event": "sync_refused", "level": "error", "why": stopped["what_went_wrong"]})  # type: ignore[index]
    return stopped  # type: ignore[return-value]


_WHAT_THE_TURN_ANSWERS: dict[bool, Callable[..., CatchingUp | Fault]] = {
    True: _the_turn_that_ran,
    False: _what_the_channel_said,
}
