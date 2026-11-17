"""A caller's question, turned into an answer.

    orchestrator fn query_answer : (asking: Answering) -> Answering | Fault

ONE SEQUENCE, AND EVERY STEP IS SOMEBODY ELSE'S CARD. Build the declaration from
whichever grammar the call arrived in, refuse what the model does not declare,
compile the statement once for this engine, and then either run it or hand it to
the crew. Nothing here writes SQL, opens a connection or names an engine.

A READ RUNS NOW AND A WRITE WAITS FOR A DRAINER. A read is one statement on the
connection this database holds, and the caller has the rows when it comes back.
A write goes to the crew, and the caller holds a ticket until a drainer has run
it. That is the whole difference between the two paths here.

ROWS THAT TRAVEL TOGETHER SKIP THE CREW. `fn travels_together` reads the
declaration and answers whether its rows have to land together. A group is one
transaction on one connection, which is the only rollback there is, so it cannot
be split across drainers and never takes a ticket.

THE CLASS'S NAME AND THE TABLE'S NAME ARE DIFFERENT WORDS. A caller writes
`Person` because that is the only name they have; the rows live under `people`,
which is the only name an engine answers to. The model carries both and the
declaration is compiled under the table's. Until 2026-09-22 nothing traded one
for the other and every question written the way a caller writes one came back
`no such table`.

A GRAMMAR WITH NO BUILDER IS REFUSED BY NAME. `dispatch by_grammar` has four
rows and no fallback, so a word nobody declared comes back naming what arrived
and the four spellings there are, rather than being answered by whichever
builder happened to be nearest.

WHAT COMES BACK IS THE RECORD IT WAS HANDED, FILLED IN. Each step writes its own
field, so a reader can see where a question got to. A refusal at any step is the
answer, and `fn refusal_reaches_caller` is what decides whether the caller is
handed it or told to read it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast, get_args

from declaro_persistum.answer_from_receipt import answer_from_receipt
from declaro_persistum.answer_from_rows import answer_from_rows
from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.crew_deposit import crew_deposit
from declaro_persistum.crew_release_ticket import crew_release_ticket
from declaro_persistum.declaration_from_asked import declaration_from_asked
from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.log_write import log_write
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.query_run_together import query_run_together
from declaro_persistum.refuse_some_columns_as_an_object import refuse_some_columns_as_an_object
from declaro_persistum.refuse_undeclared_columns import refuse_undeclared_columns
from declaro_persistum.travels_together import travels_together
from declaro_persistum.types import Answering, Asked, Declaration, Fault, Grammar, Writing

# WHICH SPELLINGS REACH A BUILDER. Every one of them, and all four reach the same
# one. The four modules that used to fill this table were written before the
# declaration and no card named them, so they were deleted on 2026-11-16; the
# declaration replaces all four with `fn declaration_from_asked`, reached through
# `fn grammar_of`, `fn methods_of` and `fn asked_from_written`.
#
# IT IS STILL A TABLE, AND STILL KEYED BY SPELLING, because a word nobody
# declared must still be refused by name rather than read as one of the four.
# What changed is that the rows no longer differ: there is one builder, so a
# fifth spelling is one more row here and no new module.
BY_GRAMMAR: dict[str, Callable[[Asked], Declaration | Fault]] = dict.fromkeys(
    get_args(Grammar), declaration_from_asked
)


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


def _no_such_grammar(grammar: str) -> Fault:
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A question arrived written in {grammar!r}, and this package builds "
            f"a declaration out of {', '.join(sorted(BY_GRAMMAR))}."
        ),
        what_to_do_instead=(
            "Ask again naming one of those four spellings. A call in a grammar "
            "nobody built for is a call whose meaning nothing here knows, and "
            "guessing at it would run SQL nobody wrote."
        ),
    )


def _the_declaration(asking: Answering) -> Declaration | Fault:
    """The caller's question, in the words this package works in.

    THE TABLE'S NAME GOES IN, NOT THE CLASS'S. A caller wrote the class's name
    because that is the only name they have, and an engine answers to the table.
    """
    grammar = str(asking["grammar"])
    built = _WHETHER_ANYTHING_BUILDS_IT[grammar in BY_GRAMMAR](grammar, asking["asked"])
    return _UNDER_THE_TABLES_NAME[_is_fault(built)](built, asking)


def _built_by_its_grammar(_grammar: str, asked: Asked) -> Declaration | Fault:
    """One card builds the declaration now, whichever spelling the question came in.

    IT TAKES NO SPELLING, WHICH IS THE WHOLE CHANGE. Until 2026-11-16 this
    dispatched to one of four builders by a word the caller passed in. The
    declaration replaced all four with `fn declaration_from_asked`, which reads
    parts already under this library's own words, because `fn asked_from_written`
    put them there. Nothing downstream of here can tell which spelling was used.
    """
    return declaration_from_asked(cast("dict[str, Any]", asked))


def _built_by_nothing(grammar: str, _asked: Asked) -> Fault:
    return _no_such_grammar(grammar)


_WHETHER_ANYTHING_BUILDS_IT: dict[bool, Callable[..., Declaration | Fault]] = {
    True: _built_by_its_grammar,
    False: _built_by_nothing,
}


def _under_the_tables_name(built: Any, asking: Answering) -> Declaration:
    return cast("Declaration", {**built, "model": asking["model"]["table"]})


def _whatever_refused_it(built: Any, _asking: Answering) -> Fault:
    return cast(Fault, built)


_UNDER_THE_TABLES_NAME: dict[bool, Callable[..., Declaration | Fault]] = {
    False: _under_the_tables_name,
    True: _whatever_refused_it,
}


# --- running it, or handing it to the crew -----------------------------------


async def _run_it_now(asking: Answering, statement: Any) -> Any:
    """A read. One statement on the connection this database holds."""
    return await query_run_statement(
        asking["db"]["connections"][0], asking["db"]["engine"], statement
    )


async def _run_them_together(asking: Answering, statement: Any) -> Any:
    """Rows that have to land together, which is one transaction on one writer.

    IT CANNOT BE SPLIT ACROSS DRAINERS, so it never takes a ticket. Holding one
    connection for the whole group is the only rollback there is.
    """
    return await query_run_together(
        asking["db"]["writers"][0],
        asking["db"]["engine"],
        [statement],
        how_to_write(asking["db"]["engine"]),
    )


async def _hand_it_to_the_crew(asking: Answering, statement: Any) -> Any:
    """A write. The caller holds a ticket until a drainer has run it."""
    ticket = crew_deposit(asking["db"]["crew"], statement)
    return await _WHETHER_THE_CREW_TOOK_IT[_is_fault(ticket)](asking, ticket)


async def _wait_for_the_drainer(asking: Answering, ticket: Any) -> Any:
    return await crew_release_ticket(asking["db"]["crew"], ticket)


async def _the_crew_refused_it(_asking: Answering, ticket: Any) -> Any:
    return ticket


_WHETHER_THE_CREW_TOOK_IT: dict[bool, Callable[..., Awaitable[Any]]] = {
    False: _wait_for_the_drainer,
    True: _the_crew_refused_it,
}


# WHAT A DECLARATION'S SHAPE DECIDES. A read runs now. A write goes to the crew.
# Rows that travel together run as one transaction and skip the crew, because a
# group split across drainers has no rollback.
_HOW_IT_RUNS: dict[tuple[bool, bool], Callable[..., Awaitable[Any]]] = {
    (False, False): _run_it_now,
    (False, True): _run_it_now,
    (True, False): _hand_it_to_the_crew,
    (True, True): _run_them_together,
}

# WHICH WORDS ASK FOR A CHANGE. Walked off `Writing` itself, so a word added to
# that vocabulary is a word this reads as a write without anything changing here.
WHAT_CHANGES_THE_DATABASE = frozenset(get_args(Writing))


def _the_answer_a_caller_reads(asking: Answering, ran: Any) -> Any:
    """Three things can come back and each reads differently.

    Rows take the model's own shape. A receipt is what a drainer wrote down
    about somebody's ticket, and `fn answer_from_receipt` reads it. A bare
    number is a change that ran here and already says how many rows it touched.
    """
    return _WHAT_CAME_BACK[_which_came_back(ran)](asking, ran)


def _which_came_back(ran: Any) -> str:
    """Rows, a receipt, or a count. A receipt is the only dict of the three."""
    return {True: "rows", False: {True: "a receipt", False: "a count"}[isinstance(ran, dict)]}[
        isinstance(ran, list)
    ]


def _rows_shaped_as_the_model(asking: Answering, ran: Any) -> Any:
    return answer_from_rows(asking["model"], ran, asking["declaration"])


def _what_the_receipt_says(_asking: Answering, ran: Any) -> Any:
    return answer_from_receipt(ran)


def _the_count_it_already_is(_asking: Answering, ran: Any) -> Any:
    """A change that ran here, which answers with how many rows it touched."""
    return ran


_WHAT_CAME_BACK: dict[str, Callable[..., Any]] = {
    "rows": _rows_shaped_as_the_model,
    "a receipt": _what_the_receipt_says,
    "a count": _the_count_it_already_is,
}


async def query_answer(asking: Answering) -> Answering | Fault:
    """Turn one caller's question into rows, a number, or a refusal.

    What comes back is the record this was handed with each step's own field
    filled in, so a reader can see where the question got to. A refusal at any
    step is the answer instead.
    """
    declaration = _the_declaration(asking)
    refusals: list[Any] = _WHAT_STILL_HAS_TO_BE_CHECKED[_is_fault(declaration)](asking, declaration)
    stopped = [one for one in refusals if _is_fault(one)]
    return await _WHETHER_ANYTHING_REFUSED_IT[stopped != []](asking, declaration, stopped)


def _nothing_left_to_check(_asking: Answering, declaration: Any) -> list[Any]:
    """The grammar already refused it, so there is nothing to ask the model."""
    return [declaration]


def _what_the_model_says(asking: Answering, declaration: Any) -> list[Any]:
    """Two questions the model answers about a declaration built from it."""
    return [
        refuse_undeclared_columns(asking["model"], cast("Declaration", declaration)),
        refuse_some_columns_as_an_object(cast("Declaration", declaration)),
    ]


_WHAT_STILL_HAS_TO_BE_CHECKED: dict[bool, Callable[..., list[Any]]] = {
    True: _nothing_left_to_check,
    False: _what_the_model_says,
}


async def _the_first_refusal(_asking: Answering, _declaration: Any, stopped: list[Any]) -> Fault:
    log_write({"event": "question_refused", "why": str(stopped[0]["what_went_wrong"])})
    return cast(Fault, stopped[0])


async def _compile_and_run(
    asking: Answering, declaration: Any, _stopped: list[Any]
) -> Answering | Fault:
    statement = compile_statement(declaration, asking["db"]["engine"])
    return await _WHETHER_IT_COMPILED[_is_fault(statement)](asking, declaration, statement)


_WHETHER_ANYTHING_REFUSED_IT: dict[bool, Callable[..., Awaitable[Answering | Fault]]] = {
    True: _the_first_refusal,
    False: _compile_and_run,
}


async def _it_would_not_compile(_asking: Answering, _declaration: Any, statement: Any) -> Fault:
    return cast(Fault, statement)


async def _it_compiled(asking: Answering, declaration: Any, statement: Any) -> Answering | Fault:
    changes = str(declaration["wanted"]) in WHAT_CHANGES_THE_DATABASE
    ran = await _HOW_IT_RUNS[(changes, travels_together(declaration))](asking, statement)
    return _WHAT_THE_QUESTION_ANSWERED[_is_fault(ran)](asking, declaration, statement, ran)


_WHETHER_IT_COMPILED: dict[bool, Callable[..., Awaitable[Answering | Fault]]] = {
    True: _it_would_not_compile,
    False: _it_compiled,
}


def _the_engine_refused_it(
    _asking: Answering, _declaration: Any, _statement: Any, ran: Any
) -> Fault:
    return cast(Fault, ran)


def _the_question_answered(
    asking: Answering, declaration: Any, statement: Any, ran: Any
) -> Answering:
    filled = {**asking, "declaration": declaration, "statement": statement}
    return cast(
        Answering,
        {**filled, "answer": _the_answer_a_caller_reads(cast(Answering, filled), ran)},
    )


_WHAT_THE_QUESTION_ANSWERED: dict[bool, Callable[..., Answering | Fault]] = {
    True: _the_engine_refused_it,
    False: _the_question_answered,
}
