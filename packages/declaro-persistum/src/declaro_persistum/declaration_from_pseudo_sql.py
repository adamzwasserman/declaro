"""The library's own phrasing, turned into the one declaration all four produce.

    fn declaration_from_pseudo_sql : (asked: Asked) -> Declaration | Fault

`people.select("id", "name").where({"name": "bob"})` reads in SQL's word order
without SQL's text, so the words this reads are SQL's: `select`, `where`,
`ordered_by`, `grouped_by`, `having` and `page`. Beside them sit the two words
every grammar carries, because neither is anybody's phrasing: `model`, the table
the call was written against, and `wanted`, one of the vocabulary words from
HOW-IT-SHOULD-WORK.md line 108. It is one of four builders and the only one that
speaks the first list; the Declaration it hands back is the same shape Django,
Prisma and SQLAlchemy build, which is what keeps four surfaces from becoming
four sets of rules.

A BARE VALUE BECOMES AN EQ COMPARISON HERE, BEFORE THE DECLARATION EXISTS.
`{"name": "bob"}` and `{"name": {"eq": "bob"}}` are the same question written two
ways, and every reader downstream would otherwise have to know both. One of them
is the declaration and the other is a spelling, and this is where the spelling
ends.

THE MODEL ARRIVES AS A NAME AND IS NOT LOOKED UP HERE. `query_ask` holds the
`Model` and hands this function only what the caller said, so no column can be
checked against a model from inside here. What this does is put every key where
a name gets looked up -- as a key of `each`, or an entry in `columns` -- and
every value where a parameter gets bound, under `value`. The lookup is
`refuse_undeclared_columns`, declared at `docs/design/persistum.hd` line 711, and
the binding is `compile_statement`. A condition that can be neither, which is a
condition given as a string, is refused here rather than carried.

EVERY MISSING WORD IS EITHER AN EMPTY VALUE OR A REFUSAL, AND THE TYPE DECIDES
WHICH. `columns`, `order` and `grouping` have an empty value that means the
caller named none, and `paging`, `rows` and `across` are written `| None` in the
declaration, so absence is representable. `model` and `wanted` are not: a str
and one of eighteen words, with no member meaning absence. So a caller who named
neither gets a Fault, because anything else would be a table name and a
vocabulary word that nobody chose and nobody can tell from a choice.

NO TABLE HERE TAKES A FALLBACK, AND THAT IS THE SAME RULE ONE LEVEL DOWN. A
`.get(key, something)` answers for a key nobody wrote a row for, which is the
silent default this package refuses: hand a `set`, a `bytes` or a `Decimal` to a
table whose fallback reads "a bare value" and the caller's mistake travels to the
engine before anything notices. So every table below is total. A shape with a
row is built by that row; a shape with no row is named in a Fault that says what
was written and what to write instead.

NO `if` ANYWHERE, AND NOT BY CONTORTION. Every decision here is "which of these
shapes is this", which is a key, so every one of them is a dict lookup: by the
word the caller used, by the type of what they wrote, and by whether the
operator they named is one of the fifteen.

Pure: one argument in, one dict out. No clock, no random source, no environment,
no file and no connection. A Fault comes back in the return, never raised and
never logged.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args, get_type_hints

from declaro_persistum.types import (
    NOTHING,
    Comparison,
    Condition,
    Declaration,
    Fault,
    Operator,
    Reading,
    Writing,
)

# The fifteen operators and the eighteen vocabulary words, read off the types
# rather than written out again. A list here would be a second place the same
# fact lives, and it would drift the first time a sixteenth was declared.
OPERATORS: frozenset[str] = frozenset(get_args(Operator))
WORDS: tuple[str, ...] = get_args(Reading) + get_args(Writing)

_FAULT_FIELDS = frozenset(get_type_hints(Fault))

# `ignore_case` is a companion of the operator, not an operator, so it is the
# one key inside a comparison that names no operator.
COMPANION = "ignore_case"


def _is_fault(built: object) -> bool:
    """Whether a step handed back a refusal rather than what it was asked for.

    A Fault carries three named fields and nothing else this module builds
    carries those three, so the shape answers it. A Comparison carries
    `operator`, `value` and `ignore_case`; a Condition carries `each`, `any_of`
    and `none_of`.
    """
    return isinstance(built, dict) and frozenset(built) == _FAULT_FIELDS


# =============================================================================
# One comparison: what a caller wrote against one column
# =============================================================================


def _a_bare_value(written: Any) -> Comparison | Fault:
    """`{"name": "bob"}`. A bare value means eq, so this is where it says so."""
    return {"operator": "eq", "value": written, "ignore_case": False}


def _the_comparison(written: dict[str, Any], named: list[str]) -> Comparison | Fault:
    """`{"gt": 18}`, or `{"like": "bo%", "ignore_case": True}`."""
    operator = named[0]
    return {
        "operator": cast(Operator, operator),
        "value": written[operator],
        "ignore_case": _IGNORE_CASE[COMPANION in written](written),
    }


def _an_operator_nobody_declared(written: dict[str, Any], named: list[str]) -> Comparison | Fault:
    """A key inside a comparison that is not one of the fifteen.

    There is nothing to do with such a key but paste it into a statement, which
    is the one thing this library exists to prevent, so it stops here.
    """
    invented = [word for word in named if word not in OPERATORS]
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"{invented} is not an operator this library declares, and a comparison "
            f"was written as {written}."
        ),
        "what_to_do_instead": (
            "Name one of the fifteen operators: " + ", ".join(sorted(OPERATORS)) + ". "
            'A bare value means eq, so {"age": 18} says the same as {"age": {"eq": 18}}.'
        ),
    }


def _not_exactly_one_operator(_written: dict[str, Any], named: list[str]) -> Comparison | Fault:
    """`{"gt": 18, "lt": 30}`, or a comparison naming no operator at all.

    A comparison holds one operator and one value, so a second operator has
    nowhere to go. Two keys already mean both, one level up.
    """
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"a comparison names one operator and one value, and this one names "
            f"{len(named)}: {named}."
        ),
        "what_to_do_instead": (
            "Write one comparison per operator and let two keys mean both: "
            '{"age": {"gt": 18}, "name": "bob"} narrows by each of them.'
        ),
    }


# THE COMPANION IS NOT A DEFAULT. HOW-IT-SHOULD-WORK.md line 143: text matching
# is case-sensitive unless you say otherwise, so a comparison that does not
# carry `ignore_case` is declared case-sensitive rather than left to one.
_IGNORE_CASE: dict[bool, Callable[[dict[str, Any]], bool]] = {
    True: lambda written: bool(written[COMPANION]),
    False: lambda _written: False,
}

# KEYED BY THE TWO FACTS ABOUT THE KEYS INSIDE A COMPARISON: whether exactly one
# of them names an operator, and whether every one of them is declared. All four
# combinations are written down, so a reader can see which is which.
_ONE_DECLARED_OPERATOR: dict[
    tuple[bool, bool], Callable[[dict[str, Any], list[str]], Comparison | Fault]
] = {
    (True, True): _the_comparison,
    (True, False): _an_operator_nobody_declared,
    (False, True): _not_exactly_one_operator,
    (False, False): _an_operator_nobody_declared,
}


def _a_named_operator(written: Any) -> Comparison | Fault:
    """A comparison written as a dict, which is the caller naming an operator."""
    named = [key for key in written if key != COMPANION]
    return _ONE_DECLARED_OPERATOR[(len(named) == 1, set(named) <= OPERATORS)](written, named)


# =============================================================================
# One condition: every key a caller narrowed by
# =============================================================================


_LIST_ANSWER: dict[bool, Callable[[list[Any], list[Any]], Any]] = {
    False: lambda built, _refusals: built,
    True: lambda _built, refusals: refusals[0],
}


def _each_one_of(written: list[Any]) -> list[Condition] | Fault:
    """Every condition hung under `or` or under `not`, or the first refusal."""
    built = [_built_from("condition", one) for one in written]
    refusals = [one for one in built if _is_fault(one)]
    return cast("list[Condition] | Fault", _LIST_ANSWER[bool(refusals)](built, refusals))


# `{"or": [a, b]}` carries a list and `{"not": c}` carries one condition, and
# both land on a list, because `any_of` and `none_of` are both lists. A caller
# who wrote neither shape is refused by `_combined` below rather than wrapped.
_MANY_CONDITIONS: dict[type, Callable[[Any], list[Any]]] = {
    list: lambda written: written,
    dict: lambda written: [written],
}

_COMBINED: dict[bool, Callable[[Any], Any]] = {
    True: lambda written: _each_one_of(_MANY_CONDITIONS[type(written)](written)),
    False: lambda written: _nothing_to_do_with("condition", written),
}


def _conditions_under(written: Any) -> list[Condition] | Fault:
    """What a caller hung under `or` or under `not`."""
    return cast("list[Condition] | Fault", _COMBINED[type(written) in _MANY_CONDITIONS](written))


# THE TWO KEYS THAT ARE NOT COLUMNS. `or` and `not` are still what a caller
# writes, and a Condition holds them as `any_of` and `none_of`.
_NOT_A_COLUMN: dict[str, Callable[[Any], Any]] = {
    "or": _conditions_under,
    "not": _conditions_under,
}


def _assembled(built: dict[str, Any], _refusals: list[Any]) -> Condition | Fault:
    """The three parts of a Condition, out of what each key built.

    `built` is read by key and never with a fallback, because `_each_key_of`
    starts it with the two combining keys already present and empty.
    """
    return {
        "each": {key: one for key, one in built.items() if key not in _NOT_A_COLUMN},
        "any_of": built["or"],
        "none_of": built["not"],
    }


def _the_first_refusal(_built: dict[str, Any], refusals: list[Any]) -> Condition | Fault:
    """A condition with a refusal in it is a refusal. The caller gets the first."""
    return cast(Fault, refusals[0])


_CONDITION_ANSWER: dict[bool, Callable[[dict[str, Any], list[Any]], Condition | Fault]] = {
    False: _assembled,
    True: _the_first_refusal,
}


# A KEY IS EITHER ONE OF THE TWO THAT COMBINE OR IT IS A COLUMN. There is no
# third answer and no fallback: the two are named in `_NOT_A_COLUMN` and a key
# outside it is a column name, which is a name this library cannot enumerate.
_A_COMBINING_KEY: dict[bool, Callable[[str, Any], Any]] = {
    True: lambda key, value: _NOT_A_COLUMN[key](value),
    False: lambda _key, value: _a_comparison_for(value),
}


def _each_key_of(written: Any) -> Condition | Fault:
    """A condition written as data, which is the only way it may be written.

    It starts with `or` and `not` present and empty so that `_assembled` can
    read both by name. A caller who wrote either replaces the empty list; a
    caller who wrote neither leaves it.
    """
    built: dict[str, Any] = {"or": [], "not": []}
    built.update(
        {key: _A_COMBINING_KEY[key in _NOT_A_COLUMN](key, value) for key, value in written.items()}
    )
    refusals = [one for one in built.values() if _is_fault(one)]
    return _CONDITION_ANSWER[bool(refusals)](built, refusals)


def _a_comparison_for(written: Any) -> Comparison | Fault:
    """A column's comparison. Named so `_NOT_A_COLUMN` can be read past it."""
    return cast("Comparison | Fault", _built_from("comparison", written))


def _narrowed_by_nothing(_written: Any) -> Condition | Fault:
    """A caller who narrowed by nothing. Empty, and never absent, because every
    reader of a Declaration reads this field by name."""
    return {"each": {}, "any_of": [], "none_of": []}


# =============================================================================
# One table for both shapes a caller writes inside a condition
# =============================================================================
#
# `_a_comparison` AND `_condition_from` WERE ONE FUNCTION WEARING TWO NAMES.
# Erase the words and both said: look the type of what was written up in a
# table, and refuse a type with no row. So the words moved into the rows and
# the function below is the one that was written twice.
#
# EVERY ROW IS NAMED AND NOTHING FALLS THROUGH. The five value types are the
# five `Value` names in `types.py` line 90, written out because `bool` is a
# subclass of `int` and an exact-type key is the only lookup that tells the two
# apart. A type absent from a row is a shape this library does not understand,
# and saying so is the whole point of the table being total.

_SHAPES: dict[str, dict[type, Callable[[Any], Any]]] = {
    "comparison": {
        dict: _a_named_operator,
        str: _a_bare_value,
        int: _a_bare_value,
        float: _a_bare_value,
        bool: _a_bare_value,
        type(NOTHING): _a_bare_value,
    },
    "condition": {
        dict: _each_key_of,
        type(NOTHING): _narrowed_by_nothing,
    },
}

_WHAT_IT_IS: dict[str, str] = {
    "comparison": "a comparison is a value, or a dict naming one of the fifteen operators",
    "condition": "a condition is data, written as a dict, or left out entirely",
}

_WRITE_INSTEAD: dict[str, str] = {
    "comparison": (
        "Write a value this library can bind -- a string, a number, true, false or "
        'nothing -- or name an operator: {"age": {"gt": 18}}, {"id": {"in": [1, 2]}}.'
    ),
    "condition": (
        'Write the condition as data: {"name": "bob"} for an equality, '
        '{"age": {"gt": 18}} for an operator, and {"or": [...]} or {"not": {...}} '
        "to combine them."
    ),
}


def _nothing_to_do_with(what: str, written: Any) -> Fault:
    """A shape with no row. HOW-IT-SHOULD-WORK.md line 212: it is neither a name
    this library can look up nor a value it can bind, so the only thing left to
    do with it would be to paste it into a statement."""
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"{_WHAT_IT_IS[what]}, and this one arrived as {type(written).__name__}: {written!r}."
        ),
        "what_to_do_instead": _WRITE_INSTEAD[what],
    }


_A_SHAPE_WITH_A_ROW: dict[bool, Callable[[str, Any], Any]] = {
    True: lambda what, written: _SHAPES[what][type(written)](written),
    False: lambda what, written: _nothing_to_do_with(what, written),
}


def _built_from(what: str, written: Any) -> Any:
    """Build a comparison or a condition out of what the caller wrote."""
    return _A_SHAPE_WITH_A_ROW[type(written) in _SHAPES[what]](what, written)


# =============================================================================
# The ten fields of a declaration, out of the words a caller used
# =============================================================================


_A_MODEL_NAME: dict[bool, Callable[[Any], Any]] = {
    True: lambda written: written,
    False: lambda written: {
        "kind": "refused",
        "what_went_wrong": (
            f"no model was named, and a declaration names one table: {written!r} was written "
            f"where the table's name goes."
        ),
        "what_to_do_instead": (
            'Name the model under "model", the word every grammar carries it under: '
            'people.select("id", "name") puts "people" there.'
        ),
    },
}


def _a_model_name(_word: str, written: Any) -> Any:
    """The table this question is about, as the caller named it."""
    return _A_MODEL_NAME[isinstance(written, str) and written != ""](written)


_A_VOCABULARY_WORD: dict[bool, Callable[[Any], Any]] = {
    True: lambda written: written,
    False: lambda written: {
        "kind": "refused",
        "what_went_wrong": (
            f"{written!r} is not one of the words this library answers to, and a "
            f"declaration says which one was asked for."
        ),
        "what_to_do_instead": (
            "Ask for one of: " + ", ".join(WORDS) + ". Each one means one thing, and "
            "the table in HOW-IT-SHOULD-WORK.md says which."
        ),
    },
}


def _a_vocabulary_word(_word: str, written: Any) -> Any:
    """Which of the eighteen words the caller asked for."""
    return _A_VOCABULARY_WORD[isinstance(written, str) and written in WORDS](written)


def _a_condition(_word: str, written: Any) -> Any:
    """`where` and `having` are both conditions, built the same way."""
    return _built_from("condition", written)


# WHAT EACH WORD MEANS WHEN THE CALLER LEFT IT OUT. `_named_columns` and
# `_a_grouping` were one function twice, differing only in the empty value they
# named, so the empty value moved into a row and the function below is the one
# they both were. `ordered_by` joins them, because it is the same function again
# with `{}` in place of `[]`.
UNSAID: dict[str, Any] = {"select": [], "grouped_by": [], "ordered_by": {}}

_EMPTY_WHEN_UNSAID: dict[bool, Callable[[str, Any], Any]] = {
    True: lambda word, _written: UNSAID[word],
    False: lambda _word, written: written,
}


def _named_or_none(word: str, written: Any) -> Any:
    """The columns, the grouping or the order the caller named, or none of them.

    Carried as written. A column is checked against the model by
    `refuse_undeclared_columns`, which is where "id, name" passed as one string
    is refused by name, so nothing here cuts a string apart to guess at it.
    """
    return _EMPTY_WHEN_UNSAID[written is NOTHING](word, written)


def _as_written(_word: str, written: Any) -> Any:
    """Paging, rows and joins. Each is written `| None` in the declaration, so
    absence is already one of the things it can be and needs no standing in."""
    return written


# THE TABLE THAT IS THE GRAMMAR. The left column is a field of a Declaration,
# the middle is the word this grammar spells it with, and the right builds one
# out of the other. Reading down the middle column is reading the surface.
_FIELDS: dict[str, tuple[str, Callable[[str, Any], Any]]] = {
    "model": ("model", _a_model_name),
    "wanted": ("wanted", _a_vocabulary_word),
    "columns": ("select", _named_or_none),
    "condition": ("where", _a_condition),
    "order": ("ordered_by", _named_or_none),
    "grouping": ("grouped_by", _named_or_none),
    "having": ("having", _a_condition),
    "paging": ("page", _as_written),
    "rows": ("rows", _as_written),
    "across": ("across", _as_written),
}


# A WORD THE CALLER LEFT OUT IS NOTHING, SAID ON PURPOSE. `asked.get(word)`
# would have produced `None` as a side effect of a lookup missing, which reads
# the same and means something else.
_WHAT_THE_CALLER_SAID: dict[bool, Callable[[dict[str, Any], str], Any]] = {
    True: lambda asked, word: asked[word],
    False: lambda _asked, _word: NOTHING,
}


_DECLARATION_ANSWER: dict[bool, Callable[[dict[str, Any], list[Any]], Declaration | Fault]] = {
    False: lambda built, _refusals: cast(Declaration, built),
    True: lambda _built, refusals: cast(Fault, refusals[0]),
}


def declaration_from_pseudo_sql(asked: dict[str, Any]) -> Declaration | Fault:
    """Turn the library's own phrasing into a declaration.

    `asked` carries the words this grammar uses: `model`, `wanted`, `select`,
    `where`, `ordered_by`, `grouped_by`, `having`, `page`, `rows` and `across`.
    A word the caller did not use is NOTHING, and each field above says what
    that means for it.
    """
    built = {
        field: build(word, _WHAT_THE_CALLER_SAID[word in asked](asked, word))
        for field, (word, build) in _FIELDS.items()
    }
    refusals = [one for one in built.values() if _is_fault(one)]
    return _DECLARATION_ANSWER[bool(refusals)](built, refusals)
