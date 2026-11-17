"""Which of the four spellings a question was written in, read off its shape.

NOTHING IN THE CALL SAYS WHICH SPELLING IT IS, AND THAT IS DELIBERATE. Until
2026-10-05 the caller passed a word naming the spelling beside the question, and
`boundary_in fn query_ask` took it as an argument. That kept one fact in two
places where they can disagree, and it was never needed: the four look nothing
alike. `docs/design/persistum.hd` took the word off the door and gave the job to
this card.

THE SHAPE DECIDES, NEVER A METHOD NAME. Two spellings can share a method. Django
has `filter` and so does SQLAlchemy 1.x, and any library may take the word
tomorrow, so a guess from keywords gets weaker with every spelling added. What
cannot collide is the attribute a chain hangs off: `.objects.` is Django and
nothing else, `.query.` is SQLAlchemy, `find_many(` is Prisma's one call
carrying named parts, and a method straight off the model is this library's own.

THE MARKS ARE TRIED IN THE ORDER THEY ARE WRITTEN, which matters because the
library's own spelling is recognised by what it lacks rather than by what it
has. `people.select(...)` has no marking attribute at all, so it is what is left
once the three marked spellings have each had their turn. Putting it anywhere
but last would claim every question.

A QUESTION CARRYING NONE OF THE MARKS IS REFUSED AND THE REFUSAL NAMES ALL FOUR.
A caller who wrote raw SQL, or who meant to write Django and mistyped the
attribute, needs to know which spellings exist. A refusal saying only
"unrecognised" sends them to this source to find out, and `set grammars` in the
declaration is where the four words come from rather than a list typed here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import get_args

from declaro_persistum.types import Fault, Grammar, Written

# EVERY SPELLING THE PACKAGE READS, from the type rather than from a list. A
# fifth word added to `Grammar` and not to `WHAT_MARKS_EACH_SPELLING` below is
# caught by this package's own gate rather than by a caller.
THE_FOUR_SPELLINGS: tuple[Grammar, ...] = get_args(Grammar)

# WHAT MARKS EACH SPELLING, AND THE ORDER IS PART OF THE ANSWER. Each row is the
# text a question in that spelling carries and no question in another spelling
# does. `pseudo_sql` has no row because it is marked by carrying none of these.
WHAT_MARKS_EACH_SPELLING: dict[Grammar, str] = {
    "django": ".objects.",
    "sqlalchemy": ".query.",
    "prisma": "find_many(",
}

# WHAT THE LIBRARY'S OWN SPELLING LOOKS LIKE: a method called straight off the
# model, so there is a dot and a call and no marking attribute between them.
A_METHOD_ON_THE_MODEL = "."
A_CALL = "("


def _refused(written: Written) -> Fault:
    """The refusal a question in no spelling this reads earns."""
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"This question is not written in any spelling this package reads: "
            f"{written!r}. Nothing in it marks a spelling, so there is no way to "
            f"tell which parts are the model, the narrowing and the columns."
        ),
        "what_to_do_instead": (
            f"Write the question in one of {', '.join(sorted(THE_FOUR_SPELLINGS))}. "
            f"django hangs its chain off .objects., sqlalchemy off .query., prisma "
            f"is one find_many( call carrying named parts, and pseudo_sql calls a "
            f"method straight off the model."
        ),
    }


def _the_spelling_it_marks(written: Written) -> Grammar | Fault:
    """The marked spelling this question carries, in the order the table names."""
    marked = [spelling for spelling, mark in WHAT_MARKS_EACH_SPELLING.items() if mark in written]
    return _WHETHER_ANY_MARK_IS_THERE[marked != []](marked, written)


def _the_first_mark(marked: list[Grammar], _written: Written) -> Grammar:
    return marked[0]


def _no_mark_at_all(_marked: list[Grammar], written: Written) -> Grammar | Fault:
    """Nothing marked it, so it is this library's own spelling or it is refused.

    The library's own is a method called on the model: a dot and a call, with no
    marking attribute between them. Raw SQL has neither, which is what separates
    the two cases here.
    """
    return _WHETHER_A_METHOD_IS_CALLED_ON_A_MODEL[
        A_METHOD_ON_THE_MODEL in written and A_CALL in written
    ](written)


_WHETHER_ANY_MARK_IS_THERE: dict[bool, Callable[[list[Grammar], Written], Grammar | Fault]] = {
    True: _the_first_mark,
    False: _no_mark_at_all,
}

_WHETHER_A_METHOD_IS_CALLED_ON_A_MODEL: dict[bool, Callable[[Written], Grammar | Fault]] = {
    True: lambda _written: "pseudo_sql",
    False: _refused,
}


def grammar_of(written: Written) -> Grammar | Fault:
    """Which way of writing this is, or the refusal it earns.

    Reads nothing but the text it was handed, so the same text always answers
    the same.
    """
    return _the_spelling_it_marks(written)
