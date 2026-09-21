"""`spell_view`: a declared view and an engine in, the SELECT it stands for out.

A VIEW HOLDS A DECLARATION, NEVER A STRING. `HOW-IT-SHOULD-WORK.md` puts it in
one line: "A view is a query with a name. You declare it against your models,
the way you declare a table, and the library writes the SELECT." That sentence
only means something if what is stored is the same `Declaration` a caller asks
with, and writing the SELECT is this function.

SO IT IS THE SAME COMPILER THE DATA HALF USES. A second renderer for views
would be a second place where a value could reach the statement text, a second
place where a placeholder style is decided, and a second thing to change when
an engine changes. `compile_statement` already takes a declaration and an
engine and hands back the text with the values beside it, and a view carries a
declaration, so there is nothing here to write twice.

THE VALUES COME BACK BOUND, EXACTLY AS THEY DO FOR A QUESTION. A view declared
over the paid orders carries "paid", and "paid" comes back in `parameters`
rather than in the text. What a caller does with a view's SELECT is
`spell_operation`'s business, not this function's, and handing it a statement
whose value was already pasted in would take that decision away from it.

WHY THIS ONE IS HANDED AN ENGINE. The declaration hands an `engine: Engine` to
a handful of functions and to no others, and that is the permission to name
PostgreSQL, SQLite or Turso. Everything upstream is engine-free: a model
declares what exists and a view declares what it stands for, and neither
carries a word belonging to one engine.

WHAT IT REFUSES, RATHER THAN GUESSES, and every refusal comes back in the
return, never raised and never logged. Three of them. An engine nobody
declared. A view standing for something that is not a question, since a view is
a stored question and `add` is not one. And a declaration the compiler itself
refuses, which comes back carrying the compiler's own reason with the view's
name in front of it: two voices saying it differently would leave the reader
working out whether they are the same refusal.

WHAT IT DOES NOT DECIDE. Freshness. A view may ask to be materialized, and
PostgreSQL has a real materialized view where SQLite does not, but the SELECT
that fills one is the same SELECT either way. `capability_of` says which engine
has it and `mechanism_for` picks what builds it where it does not, so neither
question is asked here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import get_args

from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.types import Engine, Fault, Reading, Statement, View

THE_THREE_ENGINES = get_args(Engine)

# WHAT A VIEW MAY STAND FOR ON EACH ENGINE. A view is a stored question, so the
# words are the twelve readings and none of the six writings.
#
# ONE ROW PER ENGINE, AND THE THREE ROWS AGREE TODAY. All three take a SELECT
# after CREATE VIEW and none of them takes an INSERT, so there is nothing to
# differ about yet. They still have a row each, for the reason the renderer's
# own engine table gives: the first engine that cannot hold one of these words
# in a view loses that word from its row, which is an edit to one row rather
# than a new shape for this function.
A_VIEW_MAY_STAND_FOR: dict[Engine, frozenset[str]] = {
    "postgresql": frozenset(get_args(Reading)),
    "sqlite": frozenset(get_args(Reading)),
    "turso": frozenset(get_args(Reading)),
}


# --- the three refusals ------------------------------------------------------


def _no_such_engine(view: View, engine: object) -> Fault:
    """The engine is not one of the three, so there is no way to spell anything."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The view {view['name']!r} was handed to be spelled for {engine!r}, and this "
            f"package writes for {', '.join(THE_THREE_ENGINES)} and for nothing else."
        ),
        what_to_do_instead=(
            "Open the database with a target naming one of "
            f"{', '.join(THE_THREE_ENGINES)}; the engine a view is spelled for is the one "
            "the open decided."
        ),
    )


def _not_a_question(view: View, engine: Engine) -> Fault:
    """A view stands for a question, and this declaration asks to change rows."""
    words = ", ".join(sorted(A_VIEW_MAY_STAND_FOR[engine]))
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The view {view['name']!r} stands for a declaration that asks to "
            f"{view['declaration']['wanted']}, and {engine} holds a view as a stored question. "
            "There is no SELECT here to write."
        ),
        what_to_do_instead=(
            f"Declare the view as something you read: {words}. A change to the rows is a "
            "write, and a write is asked for when you want it to happen rather than stored "
            "under a name."
        ),
    )


def _named(view: View, engine: Engine, answer: Fault) -> Fault:
    """The compiler's refusal, with the view it came from in front of it.

    The reason is the compiler's own words, because it is the one that knows
    which word was short of what. What this adds is the name, since a caller
    holding a dozen declared views cannot act on a refusal that names none.
    """
    return Fault(
        kind=answer["kind"],
        what_went_wrong=(
            f"The view {view['name']!r} cannot be spelled for {engine}. {answer['what_went_wrong']}"
        ),
        what_to_do_instead=answer["what_to_do_instead"],
    )


def _kept(_view: View, _engine: Engine, answer: Statement) -> Statement:
    """The compiler wrote the SELECT, and it comes back untouched."""
    return answer


# --- the three questions, each one a lookup ----------------------------------

_WHAT_CAME_BACK: dict[bool, Callable[..., Statement | Fault]] = {
    True: _kept,
    False: _named,
}


def _compiled(view: View, engine: Engine) -> Statement | Fault:
    """The declaration the view holds, compiled for this engine."""
    answer = compile_statement(view["declaration"], engine)
    return _WHAT_CAME_BACK["text" in answer](view, engine, answer)


_WHEN_IT_STANDS_FOR_A_QUESTION: dict[bool, Callable[[View, Engine], Statement | Fault]] = {
    True: _compiled,
    False: _not_a_question,
}


def _with_a_declared_engine(view: View, engine: Engine) -> Statement | Fault:
    stands_for = view["declaration"]["wanted"] in A_VIEW_MAY_STAND_FOR[engine]
    return _WHEN_IT_STANDS_FOR_A_QUESTION[stands_for](view, engine)


_WHEN_THE_ENGINE_IS_DECLARED: dict[bool, Callable[..., Statement | Fault]] = {
    True: _with_a_declared_engine,
    False: _no_such_engine,
}


def spell_view(view: View, engine: Engine) -> Statement | Fault:
    """Write the SELECT a declared view stands for, on one engine.

    It opens nothing, reads no clock, no random source and no environment
    variable, and asks no database what it supports. Handed the same view and
    the same engine it writes the same characters, on a machine with no
    database on it.
    """
    return _WHEN_THE_ENGINE_IS_DECLARED[engine in THE_THREE_ENGINES](view, engine)
