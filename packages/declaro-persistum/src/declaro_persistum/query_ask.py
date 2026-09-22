"""`query_ask`: the one door a caller asks a question or declares a write through.

    boundary_in fn query_ask : (db: Database, model: Model, grammar: Grammar,
                                asked: Asked)
        -> Answer | Fault
        awaited
        side_effect reads "caller"
        invokes query_answer

A DOOR IS THIN, AND THIS IS THE WHOLE OF IT. It takes what a caller said,
builds the record `orchestrator fn query_answer` carries, calls it, and hands
back the answer out of that record. Everything about answering a question
happens behind it, in the eleven functions that orchestrator sequences.
Anything more here would be a second place where a question is decided.

ONE DOOR AND FOUR SPELLINGS. Four doors would say a caller who writes Django
and a caller who writes Prisma entered four different buildings. One door
carrying the name of the entry point they reached for says the four are
spellings of one question, and `dispatch by_grammar`, behind the orchestrator,
is where that name is spent. Nothing here detects a grammar and nothing here
names one: the name arrives as a value because the shape IS the entry point,
and a call that would have to be inspected to find it out is a call that threw
the answer away.

ONE ANSWER FOR BOTH HALVES OF THE ASKING VOCABULARY. A read answers with what
was found: rows, a number, true or false, or the named absence. A write answers
with its ticket at once and the row reaches the engine later, on a spare
moment. `type Answer` is the union of both, which is what lets one door serve a
caller who is asking and a caller who is declaring.

IT IS AWAITED, BECAUSE `query_answer` IS. aiosqlite, asyncpg and pyturso are
all async-only, and nothing in pyturso is natively synchronous. Calling a
coroutine without awaiting it raises nothing and does nothing, because a
coroutine is truthy, so the one call below is awaited.

A FAILURE COMES BACK IN THE RETURN, IN THE WORDS IT ARRIVED IN. A grammar
builder, the refusal of a column no model declares, the renderer, the engine or
the room can each hand back a `Fault`, and it travels out of here untouched. A
refusal reworded twice is a refusal whose author nobody can find.

IT KNOWS NO ENGINE. The handle a caller opened carries the engine the open
decided, and `fn compile_statement` is the one function that turns a
declaration into an engine's own characters. There is no engine spelled
anywhere in this module and no SQL in any string.

NOTHING HERE TAKES A DEFAULT. All four arguments are required. A caller who
named no model, or no grammar, or wrote no question has not made the call, and
filling one in here would make a caller who chose and a caller who forgot the
same caller.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from declaro_persistum.query_answer import query_answer
from declaro_persistum.types import Answer, Asked, Database, Fault, Grammar, Model

# =============================================================================
# telling a failure from an answer, once
# =============================================================================
#
# A `Fault` IS THE THREE FIELDS AND NOTHING ELSE, so what came back is read by
# its shape rather than by a flag somebody remembered to set. What the
# orchestrator hands back when it worked is an `Answering`, which carries eight.

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_a_fault(answered: object) -> bool:
    """Whether what the orchestrator handed back is a failure on its way out."""
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _the_fault_unchanged(answered: Any) -> Answer | Fault:
    """It failed, so it leaves in the words the function that found it wrote."""
    return cast(Fault, answered)


def _the_answer_it_reached(answered: Any) -> Answer | Fault:
    """It worked, so the caller gets the last field of the record."""
    return cast(Answer, answered["answer"])


# Whether answering the question failed. Both keys are here because a bool has
# two, and neither is a fallback for the other.
WHAT_THE_ORCHESTRATOR_ANSWERED: dict[bool, Callable[[Any], Answer | Fault]] = {
    True: _the_fault_unchanged,
    False: _the_answer_it_reached,
}


# =============================================================================
# the door
# =============================================================================


async def query_ask(db: Database, model: Model, grammar: Grammar, asked: Asked) -> Answer | Fault:
    """Answer a caller's question, or take their write, or say why it was refused.

    `db` is the database they opened, `model` is what they are asking about,
    `grammar` is the name of the entry point they reached for, and `asked` is
    the question itself, written in the words that grammar gave them.

    A read comes back as rows, a number, true or false, or the named absence. A
    write comes back as the ticket it was deposited under, and the row reaches
    the engine later. Anything that could not be done comes back as a `Fault`
    in the words of whoever refused it.
    """
    what_the_caller_said: Any = {
        "db": db,
        "model": model,
        "grammar": grammar,
        "asked": asked,
    }
    answered = await query_answer(what_the_caller_said)
    return WHAT_THE_ORCHESTRATOR_ANSWERED[_is_a_fault(answered)](answered)
