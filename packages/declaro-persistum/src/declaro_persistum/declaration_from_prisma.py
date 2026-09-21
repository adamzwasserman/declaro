"""Prisma's phrasing, turned into the declaration all four grammars build.

    fn declaration_from_prisma : (asked: Asked) -> Declaration | Fault

Prisma phrases a question as one call carrying named parts, so
`Person.find_many(where={"name": "bob"}, select=["id", "name"])` arrives here as
the model it was made on, the method the caller reached for, and each named
argument under its own name:

    {"model": "Person", "wanted": "find_many",
     "where": {"name": "bob"}, "select": ["id", "name"]}

THE VERB IS PRISMA'S AND THE DECLARATION'S IS NOT. A caller writes `find_many`
and the declaration says `all`, and `_THE_ASKING_WORD` is where the two meet.
Every asking word prisma spells the way the vocabulary spells it maps to itself,
and `find_many` is the one `HOW-IT-SHOULD-WORK.md` writes differently. A second
row joins that table the day a second prisma spelling is written down, and until
then a verb in neither column is refused by name rather than guessed at.

PRISMA READS TWO NAMED PARTS HERE, `where` and `select`, which are the two that
document writes. A third, `order_by` among them, is refused naming the two that
exist. That is an absence a caller can see and act on; reading an unknown part
as the nearest field it resembles would be the guess this package exists to
prevent.

A BARE VALUE MEANS `eq`, AND THE SHAPE FORCES IT. A `Comparison` carries an
operator, a value and whether case is ignored, so `{"name": "bob"}` cannot
survive as a bare string: it becomes an `eq` comparison on the way in. Case is
not ignored unless the caller says so, which is what `HOW-IT-SHOULD-WORK.md`
means by text matching being case-sensitive unless you say otherwise, so an
absent `ignore_case` is that stated meaning rather than a default standing in
for an answer nobody gave.

NOTHING IS EVER PASTED. Every key the caller wrote arrives as a key, to be
looked up against the model by `refuse_undeclared_columns`, and every value
arrives under `value`, to be bound by `compile_statement`. A `where` that is not
data -- a string of SQL, most of all -- is refused here rather than carried.

A FAILURE COMES BACK. Every refusal is a `Fault` in the return, and the first
one found wins; nothing here raises, logs, or carries on.

Pure: one dict in, one dict out. No clock, no random source, no environment, no
file and no connection.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, get_args

from declaro_persistum.types import (
    Asked,
    Comparison,
    Condition,
    Declaration,
    Fault,
    Operator,
    Reading,
    Wanted,
    Writing,
)

# The fifteen operators a condition may name, read off the declaration rather
# than typed again, so a sixteenth is a change in one file.
_AN_OPERATOR: frozenset[str] = frozenset(get_args(Operator))

# Prisma's spelling of each asking word. Everything it spells the vocabulary's
# way maps to itself; `find_many` is the one HOW-IT-SHOULD-WORK.md spells
# differently.
#
# THE VOCABULARY IS READ OFF ITS TWO HALVES, not off `Wanted`. `Wanted` is the
# union of `Reading` and `Writing`, so asking it for its arguments hands back
# two types rather than eighteen words.
_THE_ASKING_WORD: dict[str, Wanted] = {
    **{word: word for word in [*get_args(Reading), *get_args(Writing)]},
    "find_many": "all",
}

# Which field of the declaration each of prisma's named parts fills.
_THE_FIELD_IT_FILLS: dict[str, str] = {"where": "condition", "select": "columns"}

# The two keys that carry the call itself rather than one of its named parts.
_THE_CALL_ITSELF: frozenset[str] = frozenset({"model", "wanted"})

# The two keys that carry a condition of their own instead of a column.
_THE_SLOT_A_COMBINER_FILLS: dict[str, str] = {"or": "any_of", "not": "none_of"}

# How to read what each combiner carries. `or` carries a list of conditions and
# `not` carries one, and both come back as a list so the walk below is one walk.
_THE_CONDITIONS_UNDER: dict[str, Callable[[dict], list]] = {
    "or": lambda stated: list(stated.get("or", [])),
    "not": lambda stated: [one for one in [stated.get("not", None)] if one is not None],
}

# What a Fault is, by its fields, so one can be told from a comparison or a
# condition without anything carrying a flag saying which it is.
_THE_FIELDS_OF_A_FAULT: frozenset[str] = frozenset(
    {"kind", "what_went_wrong", "what_to_do_instead"}
)


def _is_a_fault(value: object) -> bool:
    """Whether this is a refusal rather than the thing that was asked for."""
    return isinstance(value, dict) and frozenset(value) == _THE_FIELDS_OF_A_FAULT


def _the_first_fault(values: list[Any]) -> Fault | None:
    """The first refusal among these, or NOTHING when they all worked."""
    return ([value for value in values if _is_a_fault(value)] + [None])[0]


def _refused(what_went_wrong: str, what_to_do_instead: str) -> Fault:
    """A refusal that says what happened and what to write instead."""
    return {
        "kind": "refused",
        "what_went_wrong": what_went_wrong,
        "what_to_do_instead": what_to_do_instead,
    }


# --- reading one comparison -------------------------------------------------


def _a_bare_value_means_eq(stated: Any) -> Comparison | Fault:
    """`{"name": "bob"}` and `{"name": {"eq": "bob"}}` are the same question."""
    return {"operator": "eq", "value": stated, "ignore_case": False}


def _the_named_operator(stated: dict, named: list[str]) -> Comparison | Fault:
    """The operator the caller named, with the value it carries."""
    return {
        "operator": named[0],
        "value": stated[named[0]],
        "ignore_case": bool(stated.get("ignore_case", False)),
    }


def _no_operator_was_named(stated: dict, named: list[str]) -> Comparison | Fault:
    """A dict stood beside a column and named no operator, or named several."""
    return _refused(
        f"A value standing beside a column names exactly one operator, and {stated} names {named}.",
        "Write one operator: " + ", ".join(sorted(_AN_OPERATOR)) + ".",
    )


_THE_COMPARISON_A_DICT_MAKES: dict[bool, Callable[[dict, list[str]], Comparison | Fault]] = {
    True: _the_named_operator,
    False: _no_operator_was_named,
}


def _an_operator_was_named(stated: Any) -> Comparison | Fault:
    """Read the one operator out of a dict standing beside a column."""
    named = [key for key in stated if key != "ignore_case"]
    exactly_one = len(named) == 1 and named[0] in _AN_OPERATOR
    return _THE_COMPARISON_A_DICT_MAKES[exactly_one](stated, named)


# --- reading one condition --------------------------------------------------


def _a_condition(stated: dict) -> Condition | Fault:
    """Column keys become comparisons; `or` and `not` carry conditions."""
    each = {
        column: _read(value, "a comparison")
        for column, value in stated.items()
        if column not in _THE_SLOT_A_COMBINER_FILLS
    }
    combined = {
        _THE_SLOT_A_COMBINER_FILLS[key]: [_read(one, "a condition") for one in read(stated)]
        for key, read in _THE_CONDITIONS_UNDER.items()
    }
    built: dict[str, Any] = {"each": each, **combined}
    fault = _the_first_fault([*each.values(), *combined["any_of"], *combined["none_of"]])
    return _OR_THE_FAULT[fault is None](built, fault)


def _not_a_condition(stated: Any) -> Condition | Fault:
    """Anything that is not data, a string of SQL first among them."""
    return _refused(
        f"A condition is data, and this one is a {type(stated).__name__}: {stated!r}. "
        f"A string is neither a name this library can look up nor a value it can "
        f"bind, so the only thing left to do with it would be to paste it into a "
        f"statement.",
        'Write the condition as data: where={"name": "bob"}, or where={"age": {"gt": 18}}.',
    )


# --- the one reading, in the two places a caller writes data ----------------
#
# A COMPARISON AND A CONDITION ARE READ THE SAME WAY. Both ask one question of
# what the caller wrote -- is this a dict? -- and hand it to one of two
# functions. They were two functions saying that twice, alike in every line once
# the names were covered up, so the two words they differed by are the keys of
# this table and the reading below is written once.
_WHAT_A_DICT_MEANS: dict[str, dict[bool, Callable[[Any], Any]]] = {
    "a comparison": {True: _an_operator_was_named, False: _a_bare_value_means_eq},
    "a condition": {True: _a_condition, False: _not_a_condition},
}


def _read(stated: Any, as_what: str) -> Any:
    """Read what the caller wrote as a comparison, or as a condition."""
    return _WHAT_A_DICT_MEANS[as_what][isinstance(stated, dict)](stated)


# --- reading the columns ----------------------------------------------------


def _not_column_names(stated: Any) -> list[str] | Fault:
    """`select` carries the names of columns and carries nothing else."""
    return _refused(
        f"select carries the names of columns, and this one carries {stated!r}.",
        'Write select as a list of names: select=["id", "name"].',
    )


_THE_COLUMNS: dict[bool, Callable[[Any], list[str] | Fault]] = {
    True: list,
    False: _not_column_names,
}


def _the_columns(stated: Any) -> list[str] | Fault:
    """The columns a caller named, or the refusal something else earns."""
    names = isinstance(stated, list) and all(isinstance(name, str) for name in stated)
    return _THE_COLUMNS[names](stated)


_HOW_TO_READ: dict[str, Callable[[Any], Any]] = {
    "where": lambda stated: _read(stated, "a condition"),
    "select": _the_columns,
}


# --- reading the call itself ------------------------------------------------


def _no_model_was_named() -> Fault:
    """A question with no model on it is not a question about anything."""
    return _refused(
        "The call names no model, so there is nothing for it to be a question about.",
        "Make the call on a model: Person.find_many(...).",
    )


def _no_such_asking_word(spelled: Any) -> Fault:
    """A verb this grammar does not carry, named rather than guessed at."""
    return _refused(
        f"Prisma has no {spelled!r} to reach for.",
        "Reach for one of: " + ", ".join(sorted(_THE_ASKING_WORD)) + ".",
    )


def _nothing_was_reached_for() -> Fault:
    """A call that named no method asked for nothing, and guessing which of the
    eighteen asking words was meant is the one thing this grammar may not do."""
    return _refused(
        "The call names no method, so it says nothing about what is wanted.",
        "Reach for one of: " + ", ".join(sorted(_THE_ASKING_WORD)) + ".",
    )


# WHETHER A METHOD WAS REACHED FOR AT ALL. An absent verb and a verb nobody has
# is not the same mistake and does not send a caller to the same fix, so the two
# are settled apart rather than by one message covering both.
_THE_ASKING_WORD_REACHED_FOR: dict[bool, Callable[[Asked], Any]] = {
    True: lambda asked: _THE_ASKING_WORD.get(
        asked["wanted"], _no_such_asking_word(asked["wanted"])
    ),
    False: lambda _asked: _nothing_was_reached_for(),
}


# WHETHER ANY NAMED PART WAS ONE PRISMA DOES NOT READ. One refusal naming all of
# them, rather than one refusal per part, because a caller fixing the call wants
# to see the whole list at once.
_THE_UNKNOWN_PARTS: dict[bool, Callable[[list[str]], list[Fault]]] = {
    True: lambda named: [_no_such_named_part(named)],
    False: lambda _named: [],
}


def _no_such_named_part(named: list[str]) -> Fault:
    """A named argument prisma does not read, refused rather than dropped."""
    return _refused(
        f"find_many does not read {', '.join(named)}.",
        "It reads " + " and ".join(sorted(_THE_FIELD_IT_FILLS)) + ".",
    )


def _nothing_asked_for() -> dict[str, Any]:
    """The eight fields a call that asked for nothing else leaves empty.

    Built fresh on every call, so no two declarations share a condition and
    nothing a caller does to one reaches the next.
    """
    return {
        "columns": [],
        "condition": {"each": {}, "any_of": [], "none_of": []},
        "order": {},
        "grouping": [],
        "having": {"each": {}, "any_of": [], "none_of": []},
        "paging": None,
        "rows": None,
        "across": None,
    }


_OR_THE_FAULT: dict[bool, Callable[[Any, Any], Any]] = {
    True: lambda built, _fault: built,
    False: lambda _built, fault: fault,
}


def declaration_from_prisma(asked: Asked) -> Declaration | Fault:
    """Turn one prisma call into the declaration all four grammars build.

    Reads the argument and nothing else. It opens no connection, reads no clock
    and touches no file, so the same call always builds the same declaration.
    """
    unknown = [
        key for key in asked if key not in _THE_FIELD_IT_FILLS and key not in _THE_CALL_ITSELF
    ]
    parts = {
        _THE_FIELD_IT_FILLS[key]: _HOW_TO_READ[key](value)
        for key, value in asked.items()
        if key in _THE_FIELD_IT_FILLS
    }
    model = asked.get("model", _no_model_was_named())
    wanted = _THE_ASKING_WORD_REACHED_FOR["wanted" in asked](asked)

    built: dict[str, Any] = {
        **_nothing_asked_for(),
        "model": model,
        "wanted": wanted,
        **parts,
    }
    fault = _the_first_fault(
        [*_THE_UNKNOWN_PARTS[bool(unknown)](unknown), model, wanted, *parts.values()]
    )
    return _OR_THE_FAULT[fault is None](built, fault)
