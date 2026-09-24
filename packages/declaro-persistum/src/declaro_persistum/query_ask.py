"""The door a caller asks a question or declares a write at.

    boundary_in fn query_ask : (db: Database, model: Model, grammar: Grammar, asked: Asked) -> Answer | Fault

ONE DOOR, FOUR SPELLINGS. Which grammar a call arrived in comes with the call,
because the entry point a caller reached for is the shape. Nothing here inspects
a question to work out which phrasing it is written in: two grammars can spell
one question the same way, and a guess would send it to the wrong builder.

IT TAKES THE MODEL, NOT A NAME. A caller holds the class they wrote, and the
class knows both its own name and the table its rows live in.

WHAT COMES BACK IS WHAT THE CALLER ASKED FOR, AND NOTHING ELSE. Rows for a
question, a count for a change, a refusal for either. A `Ticket` used to be one
of the answers and is not: a caller holding a ticket has to come back for the
outcome, and every caller did the same thing with it, so the waiting moved into
`orchestrator fn query_answer` and the caller gets the outcome.

A DOOR NEVER RAISES. A refusal comes back as a `Fault` in the return, whether
the model refused the question or the engine refused the statement. A caller
reading one gets the same shape either way, and `fn refusal_reaches_caller`
decides which of those a caller is handed rather than told to read.

IT DECIDES NOTHING. `orchestrator fn query_answer` owns the sequence. This
builds the record that orchestrator takes, and unwraps the answer out of what
comes back, so a caller gets rows rather than a record with rows inside it.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from declaro_persistum.log_write import log_write
from declaro_persistum.query_answer import query_answer
from declaro_persistum.types import (
    Answer,
    Answering,
    Asked,
    Database,
    Fault,
    Grammar,
    Model,
)


def _what_went_wrong(model: Model, raised: BaseException) -> Fault:
    """Something inside raised, which a caller asking a question must not see."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A question about {model['name']} raised rather than answering: "
            f"{type(raised).__name__}: {raised}"
        ),
        what_to_do_instead=(
            "Nothing you did causes this. A question that raises is a defect in "
            "this package, and the database is whatever the question left it."
        ),
    )


def _the_answer_inside(answered: Any) -> Answer | Fault:
    """The caller asked a question, so the caller gets the answer to it.

    `orchestrator fn query_answer` hands back the record it filled in, which is
    how a reader sees where a question got to. A caller wants the rows.
    """
    return cast("Answer | Fault", answered["answer"])


def _whatever_refused_it(answered: Any) -> Answer | Fault:
    return cast(Fault, answered)


_WHAT_CAME_BACK: dict[bool, Callable[[Any], Answer | Fault]] = {
    False: _the_answer_inside,
    True: _whatever_refused_it,
}


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


async def query_ask(db: Database, model: Model, grammar: Grammar, asked: Asked) -> Answer | Fault:
    """Ask one question, or declare one write, and get the answer.

    Rows for a question, a count for a change, and a `Fault` for either when
    something refused it. Nothing is raised: a caller reading the answer gets
    the same shape whichever happened.
    """
    try:
        answered = await query_answer(
            Answering(
                db=db,
                model=model,
                grammar=grammar,
                asked=asked,
                declaration=cast("Any", None),
                statement=cast("Any", None),
                ticket="",
                receipt=cast("Any", None),
                rows=[],
                answer=None,
            )
        )
        return _WHAT_CAME_BACK[_is_fault(answered)](answered)
    except Exception as raised:  # noqa: BLE001
        stopped = _what_went_wrong(model, raised)
        log_write({"event": "question_raised", "level": "error", "why": stopped["what_went_wrong"]})
        return stopped
