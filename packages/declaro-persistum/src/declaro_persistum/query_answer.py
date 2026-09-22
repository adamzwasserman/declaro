"""`query_answer`: everything between a caller's question and its answer.

    orchestrator fn query_answer : (asking: Answering) -> Answering | Fault

WHAT AN ORCHESTRATOR DOES HERE. It sequences the functions the declaration lists
and decides which path runs. It does no work of its own that a pure function
could do, and it reaches outside the process only through the boundaries it
calls. What it keeps is what each step returned, in the record the declaration
gives it.

A QUESTION AND A WRITE TRAVEL DIFFERENTLY, AND THAT IS THE ONE DECISION HERE.
A read runs now and answers now: compile it, put it to the engine, shape the
rows. A write of one row is put in the waiting room and answered by its ticket
at once, and the row reaches the engine later, on a spare moment. Several rows
handed to one call are neither, because the caller asked for all of them or
none, and the room cannot promise that: two deposits are two independent writes
and the second failing does not undo the first. So a group holds a connection
and runs as one transaction. The caller gets its answer without waiting, on
every one of the three.

THE FORK IS TWO QUESTIONS, ASKED IN THIS ORDER. Do these writes have to land
together, which `fn travels_together` answers; and, when they do not, is this a
write at all. Each is a table of two rows and both rows of each are reachable,
which is what an eighteen-row table keyed on the asking word would not have
been: `set writings` in the declaration is that column, written once, and this
reads it rather than spelling the words again.

A GRAMMAR IS A LOOKUP AND NOT A DETECTION, because the key arrived with the
call. Four builders, one row each, and a word with no row comes back a `Fault`
naming what arrived and listing the four. There is no fallback: reaching for a
default builder would answer a question in a grammar the caller did not write.

A FAILURE COMES BACK IN THE RETURN AND TRAVELS OUT UNCHANGED. Any of the
functions below can hand back a `Fault`: a grammar builder, the refusal of a
column no model declares, the renderer, the engine, or the room. The chain stops
at the first one and carries it out in the words whoever found it wrote. This
function never rewrites one, because a refusal reworded twice is a refusal whose
author nobody can find.

NOT ONE BRANCH, AND NOT ONE DEFAULT. Every decision is a lookup in a table keyed
by a value somebody already decided: whether the last step failed, whether the
grammar has a row, whether the writes travel together, whether this is a write.
Each table holds every key its question can produce, and no lookup carries a
fallback.

IT KNOWS NO ENGINE BY NAME. It reads the engine off the handle the open decided
and hands that word to `fn compile_statement`, which is the one function that
turns a declaration into an engine's own characters. There is no engine spelled
anywhere in this module and no SQL in any string.

`fn join_between` IS ON THIS CARD AND IS NOT REACHED HERE. It is declared over
two models and `type Answering` carries one, so a question across models arrives
with no far model for a join to be worked out from. Saying so here is the
alternative to inventing a shape for it, which would be inventing one in `type
Declaration` too.

IT IS ASYNCHRONOUS BECAUSE THE BOUNDARIES IT CALLS ARE. aiosqlite, asyncpg and
pyturso are all async-only, and nothing in pyturso is natively synchronous.
Calling one of them without awaiting raises nothing and does nothing, because a
coroutine is truthy, so every outward call below is awaited.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast, get_args

from declaro_persistum.answer_from_rows import answer_from_rows
from declaro_persistum.answer_from_ticket import answer_from_ticket
from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.declaration_from_django import declaration_from_django
from declaro_persistum.declaration_from_prisma import declaration_from_prisma
from declaro_persistum.declaration_from_pseudo_sql import declaration_from_pseudo_sql
from declaro_persistum.declaration_from_sqlalchemy import declaration_from_sqlalchemy
from declaro_persistum.log_write import log_write
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.query_run_together import query_run_together
from declaro_persistum.refuse_undeclared_columns import refuse_undeclared_columns
from declaro_persistum.room_deposit import room_deposit
from declaro_persistum.travels_together import travels_together
from declaro_persistum.types import (
    Answering,
    Asked,
    Declaration,
    Fault,
    Grammar,
    Rows,
    Writing,
)

# =============================================================================
# telling a failure from an answer, once
# =============================================================================
#
# A `Fault` IS THE THREE FIELDS AND NOTHING ELSE, so what came back is read by
# its shape rather than by a flag somebody remembered to set. Every other record
# this chain produces carries different names, so no answer here can read as one.

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_fault(answer: object) -> bool:
    """Whether what came back is a failure on its way to the caller."""
    return isinstance(answer, dict) and frozenset(answer) == THE_FIELDS_OF_A_FAULT


def _the_fault(answer: Any, _keep: Any) -> Any:
    """It failed, so it leaves in the words it arrived in."""
    return answer


def _kept(answer: Any, keep: Callable[[Any], Any]) -> Any:
    """It worked, so the next step is handed what it produced."""
    return keep(answer)


# Whether the last call failed. Both keys are here because a bool has two.
WHETHER_IT_FAILED: dict[bool, Callable[[Any, Any], Any]] = {True: _the_fault, False: _kept}


def _or_fault(answer: Any, keep: Callable[[Any], Any]) -> Any:
    """Hand `answer` on to `keep`, unless it is a failure, which travels out."""
    return WHETHER_IT_FAILED[_is_fault(answer)](answer, keep)


# =============================================================================
# the chain, and how it stops
# =============================================================================
#
# EVERY STEP TAKES THE RECORD AND HANDS BACK THE RECORD. That is what lets the
# order be a tuple a reader can follow top to bottom, and what lets the stop be
# one table read once per step instead of a check written five times.

type Step = Callable[[Answering], Awaitable[Any]]


async def _skipped(_step: Step, stopped: Any) -> Any:
    """Something already failed, so this step does not run and does no I/O."""
    return stopped


async def _taken(step: Step, carried: Any) -> Any:
    return await step(carried)


WHETHER_TO_TAKE_IT: dict[bool, Callable[[Step, Any], Awaitable[Any]]] = {
    True: _skipped,
    False: _taken,
}


async def _through(steps: tuple[Step, ...], carried: Any) -> Any:
    """Run the steps in order, and stop carrying anything once one fails."""
    for step in steps:
        carried = await WHETHER_TO_TAKE_IT[_is_fault(carried)](step, carried)
    return carried


# =============================================================================
# what the caller said, in whichever of the four ways they wrote it
# =============================================================================
#
# `dispatch by_grammar`. One row per spelling, and the key arrived with the call
# because the entry point a caller reached for is the shape. Nothing here
# inspects what they wrote to work it out.

BY_GRAMMAR: dict[Grammar, Callable[[Asked], Declaration | Fault]] = {
    "pseudo_sql": declaration_from_pseudo_sql,
    "django": declaration_from_django,
    "prisma": declaration_from_prisma,
    "sqlalchemy": declaration_from_sqlalchemy,
}


def _no_grammar_is_called_that(grammar: Any, _asked: Asked) -> Declaration | Fault:
    """A word with no row. It names what arrived and the four that exist."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The call arrived in a grammar called {grammar!r}, and this library "
            f"reads no grammar by that name, so there is nothing to build the "
            f"question with."
        ),
        what_to_do_instead=(
            "Ask through one of the four spellings there are: "
            + ", ".join(sorted(BY_GRAMMAR))
            + "."
        ),
    )


def _built_by_its_grammar(grammar: Any, asked: Asked) -> Declaration | Fault:
    """Reached only under the key that says the grammar has a row."""
    return BY_GRAMMAR[grammar](asked)


# NO FALLBACK, AND BOTH ROWS WRITTEN OUT. A grammar with no builder is a refusal
# naming it, never the nearest builder to hand.
WHETHER_THE_GRAMMAR_HAS_A_ROW: dict[bool, Callable[[Any, Asked], Declaration | Fault]] = {
    True: _built_by_its_grammar,
    False: _no_grammar_is_called_that,
}


async def _the_declaration(carried: Answering) -> Any:
    """What the caller wrote, in the one shape every engine is served from."""
    return _or_fault(
        WHETHER_THE_GRAMMAR_HAS_A_ROW[carried["grammar"] in BY_GRAMMAR](
            carried["grammar"], carried["asked"]
        ),
        lambda built: {**carried, "declaration": built},
    )


async def _no_column_the_model_does_not_declare(carried: Answering) -> Any:
    """Refused here, in the caller's own process, before anything is sent.

    `fn refuse_undeclared_columns` checks and produces nothing, so what comes
    back on the clear path is absence and the record travels on untouched.
    """
    return _or_fault(
        refuse_undeclared_columns(carried["model"], carried["declaration"]),
        lambda _clear: carried,
    )


async def _the_statement(carried: Answering) -> Any:
    """The one place this question becomes an engine's own characters.

    The engine is the one the open decided, carried on the handle, so nothing
    here works it out a second time. The caller's values never reach the text:
    they come back beside it for the driver to bind.
    """
    return _or_fault(
        compile_statement(carried["declaration"], carried["db"]["engine"]),
        lambda written: {**carried, "statement": written},
    )


# =============================================================================
# the fork: run it now, deposit it, or hold a connection for the group
# =============================================================================
#
# THE SIX WRITING WORDS ARE READ OFF `Writing`, not typed again here. Adding a
# word to the declaration's vocabulary must not need an edit in this file too,
# because the copy would be the one that went stale.

A_WRITING_WORD = frozenset(get_args(Writing))

# NO ROWS CAME BACK, WHICH IS A VALUE. A write answers with a number or a
# ticket, and the field that holds what the driver handed over is empty rather
# than absent, so nothing downstream has to look for it and find nothing.
NOTHING_CAME_BACK: Rows = []


async def _as_one_transaction(carried: Answering) -> Any:
    """Several rows the caller asked for together, on one held connection."""
    return _or_fault(
        await query_run_together(carried["db"], [carried["statement"]]),
        lambda touched: {**carried, "rows": NOTHING_CAME_BACK, "answer": touched},
    )


async def _deposited(carried: Answering) -> Any:
    """One write, left in the room, answered by its ticket at once.

    Nothing runs here. A drainer takes it later, and the caller comes back for
    the outcome with the ticket it is holding.
    """
    return _or_fault(
        room_deposit(carried["db"]["room"], carried["statement"]),
        lambda ticket: {
            **carried,
            "rows": NOTHING_CAME_BACK,
            "answer": answer_from_ticket(ticket),
        },
    )


async def _run_now(carried: Answering) -> Any:
    """A question. It goes to the engine and the rows come back shaped."""
    return _or_fault(
        await query_run_statement(carried["db"], carried["statement"]),
        lambda answered: _or_fault(
            answer_from_rows(carried["model"], cast(Rows, answered), carried["declaration"]),
            lambda shaped: {**carried, "rows": cast(Rows, answered), "answer": shaped},
        ),
    )


# Whether this is one of the six writing words. Both rows are reachable: a
# writing word that got here carries one row, and everything else is a question.
WHETHER_IT_IS_A_WRITE: dict[bool, Callable[[Answering], Awaitable[Any]]] = {
    True: _deposited,
    False: _run_now,
}


async def _on_its_own(carried: Answering) -> Any:
    """Nothing has to land with it, so it is a deposit or a question."""
    return await WHETHER_IT_IS_A_WRITE[carried["declaration"]["wanted"] in A_WRITING_WORD](carried)


WHETHER_THEY_TRAVEL_TOGETHER: dict[bool, Callable[[Answering], Awaitable[Any]]] = {
    True: _as_one_transaction,
    False: _on_its_own,
}


async def _answered(carried: Answering) -> Any:
    """Which of the three ways this question or write travels."""
    return await WHETHER_THEY_TRAVEL_TOGETHER[travels_together(carried["declaration"])](carried)


# =============================================================================
# what an operator reads afterwards
# =============================================================================


async def _said_out_loud(carried: Answering) -> Any:
    """One line naming what was asked of what, and how much came back.

    It runs where the question was answered; a failure says what happened in the
    fault it hands the caller, and `docs/architecture.md` keeps failures out of
    the log for that reason.
    """
    log_write(
        {
            "asked": carried["declaration"]["model"],
            "wanted": carried["declaration"]["wanted"],
            "rows": len(carried["rows"]),
        }
    )
    return carried


# =============================================================================
# the order, top to bottom
# =============================================================================
#
# THE RECORD FILLS IN THIS ORDER, and the order is the declaration's: the four
# things the caller said arrive filled, then the declaration, the statement, the
# rows and the answer, which is what leaves on the right.

THE_CHAIN: tuple[Step, ...] = (
    _the_declaration,
    _no_column_the_model_does_not_declare,
    _the_statement,
    _answered,
    _said_out_loud,
)


async def query_answer(asking: Answering) -> Answering | Fault:
    """Turn a caller's question into rows, a number, or a ticket.

    `asking` arrives carrying what the caller said, which is the database, the
    model, the grammar the call came in through and the question itself.
    Everything else in the record is filled here, in the order above, and the
    record that comes back carries the caller's answer in its last field.

    A failure at any step comes back instead, in the words of the function that
    found it.
    """
    return await _through(THE_CHAIN, asking)
