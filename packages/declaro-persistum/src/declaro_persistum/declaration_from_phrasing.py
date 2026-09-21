"""One reader, and a grammar is only its words.

    fn declaration_from_phrasing : (asked: Asked, phrasing: Phrasing) -> Declaration | Fault

A caller's question arrives in the words their grammar gave them, beside a table
saying which word fills which field of a declaration. This reads the one through
the other and builds the declaration all four grammars produce.

WHY THE WORDS ARRIVE AS DATA. Django says `filter`, Prisma and the library's own
phrasing say `where`, SQLAlchemy says `filter_by`, and all four mean `condition`.
That difference is a table, so each grammar is a table and this is the reader
they share. Measured 2026-09-21: the four builders were written separately, each
from its own feature, by four people who could not see each other's work. They
came to 1,330 lines, all four had invented the same reading, and three of them
read the model under `model` while one read it under `from`.

WHERE A GRAMMAR HAS NO WORD, THE LIBRARY SUPPLIES ONE. `THE_LIBRARY_WORDS` below
is the whole of the library's own phrasing, and a grammar's table names only the
fields it spells differently. So a caller who picked a spelling has not picked a
smaller library: every grammar reaches all ten fields, including the four that
`declaration_from_sqlalchemy` refused until 2026-09-21 because the ORM it
imitates has no phrasing this surface copies.

A GRAMMAR'S OWN WORD REPLACES THE LIBRARY'S, IT DOES NOT SIT BESIDE IT. One word
reaches each field and the reader can say which, so a part the caller wrote that
no word points at is named in a refusal rather than dropped.

THE NARROWING IS NOT READ HERE. `condition_from_narrowing` turns what a caller
wrote into the condition this library holds, and `condition` and `having` are
both handed to it. Nothing of that reading is repeated here.

WHAT ABSENCE MEANS IS DECIDED BY THE TYPE, FIELD BY FIELD. `columns`, `order`
and `grouping` have an empty value meaning the caller named none, and `paging`,
`rows` and `across` are written `| None` in the declaration, so absence is
already representable. `model` and `wanted` are not: a table name and one of
eighteen words, with no member meaning absence. A caller who named neither would
otherwise get a table and a verb nobody chose and nobody can tell from a choice,
so both come back refused.

NO TABLE HERE TAKES A FALLBACK. A `.get(key, something)` answers for a key
nobody wrote a row for, which is the silent default this package refuses. Every
table below is total: a field with a row is built by that row, and a part with
no row is named in a Fault saying what was written and what to write instead.

NO `if` ANYWHERE, AND NOT BY CONTORTION. Every decision here is "which of these
is this", which is a key, so every one is a dict lookup: by the field, by
whether the caller said anything under its word, and by whether anything came
back refused.

IT KNOWS NO ENGINE AND NO GRAMMAR. The grammar arrives as data and leaves no
mark on what is built, which is what lets four callers asking one question in
four spellings compare their declarations and find them equal.

Pure: two arguments in, one dict out. No clock, no random source, no
environment, no file and no connection. A Fault comes back in the return, never
raised and never logged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args, get_type_hints

from declaro_persistum.condition_from_narrowing import condition_from_narrowing
from declaro_persistum.types import (
    NOTHING,
    Asked,
    Declaration,
    Fault,
    Phrasing,
    Reading,
    Writing,
)

# The eighteen asking words, read off the two halves of the vocabulary rather
# than written out again. A list here would be a second place the same fact
# lives, and it would drift the first time a nineteenth was declared.
WORDS: tuple[str, ...] = get_args(Reading) + get_args(Writing)

_FAULT_FIELDS = frozenset(get_type_hints(Fault))

# THE LIBRARY'S OWN PHRASING, WHICH IS ALSO THE BACKFILL. Ten fields, ten words,
# read in SQL's word order without SQL's text. `model` and `wanted` are nobody's
# phrasing and every grammar carries them under these names.
THE_LIBRARY_WORDS: Phrasing = {
    "model": "model",
    "wanted": "wanted",
    "columns": "select",
    "condition": "where",
    "order": "ordered_by",
    "grouping": "grouped_by",
    "having": "having",
    "paging": "page",
    "rows": "rows",
    "across": "across",
}


def _is_fault(built: object) -> bool:
    """Whether a step handed back a refusal rather than what it was asked for.

    A Fault carries three named fields and nothing else this module builds
    carries those three: a Condition carries `each`, `any_of` and `none_of`.
    """
    return isinstance(built, dict) and frozenset(built) == _FAULT_FIELDS


# =============================================================================
# The ten fields, one builder each
# =============================================================================


_A_MODEL_NAME: dict[bool, Callable[[Any], Any]] = {
    True: lambda written: written,
    False: lambda written: {
        "kind": "refused",
        "what_went_wrong": (
            f"a declaration names one table and this question names no model: "
            f"{written!r} was written where the table's name goes."
        ),
        "what_to_do_instead": (
            "Name the model under the word your grammar carries it in, which is "
            '"model" in all four: people.select("id", "name") puts "people" there.'
        ),
    },
}


def _a_model_name(_field: str, written: Any) -> Any:
    """The table this question is about, as the caller named it."""
    return _A_MODEL_NAME[isinstance(written, str) and written != ""](written)


_AN_ASKING_WORD: dict[bool, Callable[[Any], Any]] = {
    True: lambda written: written,
    False: lambda written: {
        "kind": "refused",
        "what_went_wrong": (
            f"a declaration says which one of the eighteen words was asked for, and "
            f"this question gives {written!r} as its wanted."
        ),
        "what_to_do_instead": (
            "Ask for one of: " + ", ".join(WORDS) + ". Each one means one thing, "
            "and the table in HOW-IT-SHOULD-WORK.md says which."
        ),
    },
}


def _an_asking_word(_field: str, written: Any) -> Any:
    """Which of the eighteen words the caller asked for."""
    return _AN_ASKING_WORD[isinstance(written, str) and written in WORDS](written)


def _a_narrowing(_field: str, written: Any) -> Any:
    """`condition` and `having` are both narrowings, and one function reads both.

    A caller who wrote neither hands NOTHING down, which comes back as the
    condition that narrows nothing rather than as an absence, because every
    reader of a Declaration reads this field by name.
    """
    return condition_from_narrowing(written)


# WHAT EACH FIELD IS WHEN THE CALLER NAMED NONE. Built fresh on every call, so
# no two declarations share a list and nothing done to one reaches the next.
NAMED_NONE: dict[str, Callable[[], Any]] = {
    "columns": list,
    "order": dict,
    "grouping": list,
}

_EMPTY_WHEN_UNSAID: dict[bool, Callable[[str, Any], Any]] = {
    True: lambda field, _written: NAMED_NONE[field](),
    False: lambda _field, written: written,
}


def _named_or_none(field: str, written: Any) -> Any:
    """The columns, the order or the grouping the caller named, or none of them.

    Carried as written. A column is checked against the model by
    `fn refuse_undeclared_columns`, so nothing here cuts a string apart to guess
    at it and nothing here looks a name up.
    """
    return _EMPTY_WHEN_UNSAID[written is NOTHING](field, written)


def _as_written(_field: str, written: Any) -> Any:
    """Paging, rows and joins. Each is written `| None` in the declaration, so
    absence is already one of the things it can be and needs nothing standing in
    for it."""
    return written


# THE TEN FIELDS AND HOW EACH IS BUILT. The word that fills each one is the
# grammar's business and arrives in the `Phrasing`; what to do with what the
# caller wrote under it is the same in every grammar, which is why it is here.
_HOW_EACH_FIELD_IS_BUILT: dict[str, Callable[[str, Any], Any]] = {
    "model": _a_model_name,
    "wanted": _an_asking_word,
    "columns": _named_or_none,
    "condition": _a_narrowing,
    "order": _named_or_none,
    "grouping": _named_or_none,
    "having": _a_narrowing,
    "paging": _as_written,
    "rows": _as_written,
    "across": _as_written,
}


# =============================================================================
# What the caller said, and what no word points at
# =============================================================================


# A WORD THE CALLER LEFT OUT IS NOTHING, SAID ON PURPOSE. `asked.get(word)` would
# have produced `None` as a side effect of a lookup missing, which reads the same
# and means something else.
_WHAT_THE_CALLER_SAID: dict[bool, Callable[[Asked, str], Any]] = {
    True: lambda asked, word: asked[word],
    False: lambda _asked, _word: NOTHING,
}


def _no_word_points_at(unread: list[str], words: Phrasing) -> Fault:
    """A part the caller named that no word in this grammar's table points at.

    Dropping it would answer a different question and say nothing, which is the
    silent success this library exists to delete.
    """
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"this grammar reads no part called {', '.join(sorted(unread))}, and a part "
            f"nobody reads would change nothing about the answer."
        ),
        "what_to_do_instead": (
            "Write the question again in the words this grammar reads: "
            + ", ".join(sorted(set(words.values())))
            + "."
        ),
    }


_THE_PARTS_NOBODY_READS: dict[bool, Callable[[list[str], Phrasing], list[Fault]]] = {
    True: lambda unread, words: [_no_word_points_at(unread, words)],
    False: lambda _unread, _words: [],
}


_ANSWER: dict[bool, Callable[[dict[str, Any], list[Any]], Declaration | Fault]] = {
    False: lambda built, _refusals: cast(Declaration, built),
    True: lambda _built, refusals: cast(Fault, refusals[0]),
}


# =============================================================================
# The one door
# =============================================================================


def declaration_from_phrasing(asked: Asked, phrasing: Phrasing) -> Declaration | Fault:
    """Read one caller's question through one grammar's words.

    `phrasing` names a word for each field the grammar spells its own way. A
    field it leaves out is reached by the library's own word, so all ten are
    reached whichever grammar was picked.
    """
    words: Phrasing = {
        field: {**THE_LIBRARY_WORDS, **phrasing}[field] for field in THE_LIBRARY_WORDS
    }
    unread = [part for part in asked if part not in set(words.values())]
    built = {
        field: _HOW_EACH_FIELD_IS_BUILT[field](
            field, _WHAT_THE_CALLER_SAID[word in asked](asked, word)
        )
        for field, word in words.items()
    }
    refusals = _THE_PARTS_NOBODY_READS[bool(unread)](unread, words) + [
        one for one in built.values() if _is_fault(one)
    ]
    return _ANSWER[bool(refusals)](built, refusals)
