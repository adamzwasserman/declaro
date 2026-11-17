"""The parts a reader lifted become the one declaration every later step reads.

FOUR SPELLINGS, FOUR READINGS, ONE DECLARATION. `fn asked_from_written` reads and
nothing else. This takes what it lifted and builds what the renderer, the differ
and the engines all read, and it is the one place a part a declaration needs and
nobody wrote is told no.

WRITTEN 2026-11-16, out of `declaration_from_phrasing` and the shared half of the
four grammar features. The old card took a question and a word-list and did two
jobs: renaming keys, and checking the result. The renaming is gone with the
word-lists, because `fn asked_from_written` now hands over parts already under
this library's own words. The checking is real, it is the same for all four
spellings, and it is here.

A DECLARATION CARRIES EVERY FIELD, HOWEVER FEW THE CALLER NAMED. Eleven fields,
and the ones nobody named are empty rather than missing, so nothing downstream
reads a field that may not be there. That is why `WHAT_EMPTY_MEANS` has a row per
field instead of one `None` for all of them: a list is `[]`, a condition is a
condition holding nothing, and paging is the named absence. A reader handed
`None` where it expected a condition would ask the absence for its comparisons.

ABSENCE IS A REFUSAL ONLY WHERE THE SHAPE CANNOT HOLD IT. `paging` is typed
`dict[str, int] | NOTHING`, so a caller may leave it out and the declaration says
so. `model` and `wanted` have nowhere to put an absence, and a declaration
missing either names no table or asks for nothing, so those two are refused.

THE DEFAULT FOR SHAPE IS A READING OF SILENCE, NOT A GUESS. A caller who says
nothing about shape gets dictionaries, because that is what this library hands
back unless asked for objects. It is written in `WHAT_EMPTY_MEANS` beside the
other ten so a reader sees all eleven answers in one place.

NAMING NO COLUMNS AND NAMING ALL OF THEM ARE DIFFERENT ASKS, and this keeps them
apart. A caller who called no method for columns gets `[]`, and the renderer
reads that as "every column this model declares". Turning `[]` into the model's
column list here would spend a read on a question nobody asked.

NOTHING IN THE ANSWER REMEMBERS WHICH SPELLING BUILT IT. The four arrive as the
same parts and leave as the same declaration, which is what keeps four surfaces
from becoming four sets of rules.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.condition_from_narrowing import condition_from_narrowing
from declaro_persistum.types import (
    NOTHING,
    Condition,
    Declaration,
    Fault,
    Reading,
    Writing,
)

# WHICH PART OF A QUESTION FILLS WHICH FIELD OF A DECLARATION. A caller calls
# methods and a declaration has fields, and the two use different words for the
# same thing: `select` is the method and `columns` is the field.
WHICH_FIELD_EACH_PART_FILLS: dict[str, str] = {
    "select": "columns",
    "where": "condition",
    "ordered_by": "order",
    "grouped_by": "grouping",
    "having": "having",
    "page": "paging",
    "across": "across",
    "rows": "rows",
    "model": "model",
    "wanted": "wanted",
    "shaping": "shaping",
}

# WHAT EACH FIELD HOLDS WHEN THE CALLER NAMED NOTHING, one row per field, because
# empty means a different value for each shape. `shaping` is the one row that is
# a reading of silence rather than an emptiness: a caller who said nothing wants
# dictionaries.
WHAT_EMPTY_MEANS: dict[str, Callable[[], Any]] = {
    "columns": lambda: [],
    "grouping": lambda: [],
    "condition": lambda: Condition(each={}, any_of=[], none_of=[]),
    "having": lambda: Condition(each={}, any_of=[], none_of=[]),
    "order": lambda: {},
    "paging": lambda: NOTHING,
    "rows": lambda: NOTHING,
    "across": lambda: NOTHING,
    "shaping": lambda: "dictionaries",
}

# THE TWO FIELDS WITH NOWHERE TO PUT AN ABSENCE. A declaration missing a model
# names no table, and one missing an asking word asks for nothing.
WHAT_A_DECLARATION_CANNOT_DO_WITHOUT = ("model", "wanted")

# EVERY ASKING WORD THIS LIBRARY READS, from the vocabularies rather than a list
# here, so a word added to either is read without a second edit.
EVERY_ASKING_WORD: tuple[str, ...] = (*get_args(Reading), *get_args(Writing))


def _refused(went_wrong: str, do_instead: str) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": went_wrong,
        "what_to_do_instead": do_instead,
    }


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


def _the_first_refusal_or(read: list[Any], build: Callable[[], Any]) -> Any:
    refused = [piece for piece in read if _is_fault(piece)]
    return _WHETHER_ANYTHING_WAS_REFUSED[refused != []](refused, build)


_WHETHER_ANYTHING_WAS_REFUSED: dict[bool, Callable[[list[Any], Callable[[], Any]], Any]] = {
    True: lambda refused, _build: refused[0],
    False: lambda _refused, build: build(),
}


# --- what the caller must have named -----------------------------------------


def _nothing_names_it(part: str) -> Fault:
    return _refused(
        f"This question names no {part}, and a declaration cannot be built without one.",
        f"Name the {part}. A question is a model with an asking word on it, as in "
        'people.select("id").all.',
    )


def _whatever_is_missing(asked: dict[str, Any]) -> list[Any]:
    """A refusal for each field the caller had to name and did not."""
    return [
        _nothing_names_it(part)
        for part in WHAT_A_DECLARATION_CANNOT_DO_WITHOUT
        if asked.get(part) is NOTHING
    ]


# --- the asking word ---------------------------------------------------------


def _no_such_asking_word(word: object) -> Fault:
    return _refused(
        f"This question asks for {word!r}, and that is not a word this package reads.",
        f"Ask for one of {', '.join(EVERY_ASKING_WORD)}.",
    )


def _whatever_the_word_is(asked: dict[str, Any]) -> Any:
    """The asking word, or the refusal a word nobody declared earns."""
    word = asked.get("wanted")
    return _WHETHER_THE_WORD_IS_ONE_WE_READ[word in EVERY_ASKING_WORD](word)


_WHETHER_THE_WORD_IS_ONE_WE_READ: dict[bool, Callable[[Any], Any]] = {
    True: lambda word: word,
    False: _no_such_asking_word,
}


# --- one field ---------------------------------------------------------------


def _the_condition_in(narrowing: Any) -> Any:
    """A narrowing becomes a condition, which is `fn condition_from_narrowing`'s job."""
    return condition_from_narrowing(narrowing)


# WHICH FIELDS NEED TURNING RATHER THAN COPYING. A narrowing a caller wrote is
# wider than a condition this library holds, so both condition fields go through
# the one card that turns one into the other.
HOW_A_FIELD_IS_TURNED: dict[str, Callable[[Any], Any]] = {
    "condition": _the_condition_in,
    "having": _the_condition_in,
}


def _as_it_arrived(said: Any) -> Any:
    return said


def _turned(field: str, said: Any) -> Any:
    """What the caller wrote, turned into what the field holds."""
    return _WHETHER_THE_FIELD_NEEDS_TURNING[field in HOW_A_FIELD_IS_TURNED](field, said)


_WHETHER_THE_FIELD_NEEDS_TURNING: dict[bool, Callable[[str, Any], Any]] = {
    True: lambda field, said: HOW_A_FIELD_IS_TURNED[field](said),
    False: lambda _field, said: _as_it_arrived(said),
}


def _what_the_caller_said(field: str, asked: dict[str, Any]) -> Any:
    """The part filling this field, turned, or what empty means for it."""
    parts = {
        WHICH_FIELD_EACH_PART_FILLS[part]: said
        for part, said in asked.items()
        if part in WHICH_FIELD_EACH_PART_FILLS
    }
    return _WHETHER_THE_CALLER_NAMED_IT[field in parts](field, parts)


def _the_part_they_named(field: str, parts: dict[str, Any]) -> Any:
    return _turned(field, parts[field])


def _what_empty_means(field: str, _parts: dict[str, Any]) -> Any:
    return WHAT_EMPTY_MEANS[field]()


_WHETHER_THE_CALLER_NAMED_IT: dict[bool, Callable[[str, dict[str, Any]], Any]] = {
    True: _the_part_they_named,
    False: _what_empty_means,
}


# --- the whole declaration ---------------------------------------------------

# EVERY FIELD A DECLARATION CARRIES, in the order the type declares them. Read
# from the TypedDict rather than listed, so a twelfth field is carried without a
# second edit here.
EVERY_FIELD: tuple[str, ...] = tuple(Declaration.__annotations__)

# The two the caller must name are read by their own steps above, so they are not
# filled from the empty table.
THE_FIELDS_FILLED_FROM_THE_PARTS: tuple[str, ...] = tuple(
    field for field in EVERY_FIELD if field not in WHAT_A_DECLARATION_CANNOT_DO_WITHOUT
)


def _built(asked: dict[str, Any], word: Any) -> Any:
    """Every field, filled from the parts or from what empty means for it."""
    filled = {
        field: _what_the_caller_said(field, asked) for field in THE_FIELDS_FILLED_FROM_THE_PARTS
    }
    return _the_first_refusal_or(
        list(filled.values()),
        lambda: cast("Declaration", {"model": asked["model"], "wanted": word, **filled}),
    )


def declaration_from_asked(asked: dict[str, Any]) -> Declaration | Fault:
    """The one declaration these parts build, or the refusal they earn.

    Reads nothing but the parts it was handed, so the same parts always build the
    same declaration.
    """
    word = _whatever_the_word_is(asked)
    return cast(
        "Declaration | Fault",
        _the_first_refusal_or([*_whatever_is_missing(asked), word], lambda: _built(asked, word)),
    )
