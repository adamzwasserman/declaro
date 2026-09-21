"""What a caller wrote becomes what this library holds.

    fn condition_from_narrowing : (narrowing: AskedValue) -> Condition | Fault

A caller narrows a question in the words their grammar gave them. This library
holds one shape: every comparison spelled out, with `any_of` and `none_of`
present whether or not anybody used them. This is the one place the first
becomes the second.

WHY IT IS ONE FUNCTION AND NOT FOUR. Measured 2026-09-21, after four grammar
builders were written separately from their own features by four people who
could not see each other's work: 1,330 lines, and all four had invented this
same work. Reading the caller's operator against the fifteen. The `ignore_case`
companion. A bare value meaning eq. `or` and `not` becoming `any_of` and
`none_of`. The refusal when a comparison names two operators or none. Two of the
four had already drifted from the other two on questions nobody had decided: one
read `not` as a single narrowing and two read it as a list, and two fabricated an
`eq` comparison for a caller who named no operator and then threw it away. A
condition is data, and data composes the same way whatever spelled it.

A BARE VALUE BECOMES AN EQ COMPARISON HERE. `{"name": "bob"}` and
`{"name": {"eq": "bob"}}` are the same question written two ways, and every
reader downstream would otherwise have to know both. One of them is the
condition and the other is a spelling, and this is where the spelling ends.

THE COMPANION IS NOT A DEFAULT, AND THE DIFFERENCE IS THE WHOLE RULE. A
comparison arriving without `ignore_case` is declared case-sensitive here, once,
rather than left for three engines to decide differently further down. The same
`like` matched a name on one engine and nothing on another, silently, and
making the engines agree is this library's job rather than the caller's.

IT KNOWS NO GRAMMAR AND NO ENGINE. A grammar is a set of words and nothing
else, and none of those words reaches this function: a `Narrowing` is what is
left after the word has been read off. No engine is named here either, and a
gate over `src/` says so every run.

NO TABLE HERE TAKES A FALLBACK. A `.get(key, something)` answers for a key
nobody wrote a row for, which is the silent default this package refuses: hand a
`set`, a `bytes` or a `Decimal` to a table whose fallback reads "a bare value"
and the caller's mistake travels to the engine before anything notices. So every
table below is total. A shape with a row is built by that row; a shape with no
row is named in a Fault saying what was written and what to write instead.

NO `if` ANYWHERE, AND NOT BY CONTORTION. Every decision here is "which of these
shapes is this", which is a key, so every one is a dict lookup: by the type of
what was written, by whether the key combines or names a column, and by whether
the operator named is one of the fifteen.

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
    Fault,
    Operator,
)

# The fifteen operators, read off the type rather than written out again. A list
# here would be a second place the same fact lives, and it would drift the first
# time a sixteenth was declared.
OPERATORS: frozenset[str] = frozenset(get_args(Operator))

_FAULT_FIELDS = frozenset(get_type_hints(Fault))

# `ignore_case` is a companion of the operator, not an operator, so it is the
# one key inside a comparison that names no operator.
COMPANION = "ignore_case"

# THE TWO KEYS THAT ARE NOT COLUMNS. `or` and `not` are still what a caller
# writes -- they are the words in HOW-IT-SHOULD-WORK.md and they do not change
# -- and a Condition holds them as `any_of` and `none_of`, so a reader further
# down has no word left to read again.
ANY_OF = "or"
NONE_OF = "not"


def _is_fault(built: object) -> bool:
    """Whether a step handed back a refusal rather than what it was asked for.

    A Fault carries three named fields and nothing else this module builds
    carries those three: a Comparison carries `operator`, `value` and
    `ignore_case`, and a Condition carries `each`, `any_of` and `none_of`.
    """
    return isinstance(built, dict) and frozenset(built) == _FAULT_FIELDS


# =============================================================================
# One comparison: what a caller wrote against one column
# =============================================================================


def _a_bare_value(written: Any) -> Comparison | Fault:
    """`{"name": "bob"}`. A bare value means eq, so this is where it says so."""
    return {"operator": "eq", "value": written, "ignore_case": False}


# THE COMPANION IS DECIDED HERE, NOT LEFT TO AN ENGINE. A comparison that does
# not carry `ignore_case` is declared case-sensitive rather than left to one.
_IGNORE_CASE: dict[bool, Callable[[dict[str, Any]], bool]] = {
    True: lambda written: bool(written[COMPANION]),
    False: lambda _written: False,
}


def _the_comparison(written: dict[str, Any], named: list[str]) -> Comparison | Fault:
    """`{"gt": 18}`, `{"between": [18, 30]}`, or `{"like": "bo%",
    "ignore_case": True}`.

    `between` takes two values and `in` takes a list, so the value is carried
    exactly as written. A list is still bound by the driver, one parameter per
    member, and still never pasted into the text.
    """
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
    nowhere to go and a zeroth leaves the value with nothing to be compared by.
    Two keys already mean both, one level up.
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
# both land on a list, because `any_of` and `none_of` are both lists. THIS IS
# WHERE TWO OF THE FOUR COPIES DISAGREED: one read `not` as a single narrowing
# and the other two read it only as a list, so `{"not": {"name": "bob"}}` built
# an exclusion in one builder and nothing at all in the others. Both shapes are
# read here, and a caller who wrote neither is refused rather than wrapped.
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


_NOT_A_COLUMN: dict[str, Callable[[Any], Any]] = {
    ANY_OF: _conditions_under,
    NONE_OF: _conditions_under,
}


def _assembled(built: dict[str, Any], _refusals: list[Any]) -> Condition | Fault:
    """The three parts of a Condition, out of what each key built.

    `built` is read by key and never with a fallback, because `_each_key_of`
    starts it with the two combining keys already present and empty.
    """
    return {
        "each": {key: one for key, one in built.items() if key not in _NOT_A_COLUMN},
        "any_of": built[ANY_OF],
        "none_of": built[NONE_OF],
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
    """A narrowing written as data, which is the only way it may be written.

    It starts with `or` and `not` present and empty so that `_assembled` can
    read both by name. A caller who wrote either replaces the empty list; a
    caller who wrote neither leaves it, which is also what makes a caller who
    narrowed nothing come back with a condition rather than with absence.
    """
    built: dict[str, Any] = {ANY_OF: [], NONE_OF: []}
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
# One table for both shapes a caller writes inside a narrowing
# =============================================================================
#
# EVERY ROW IS NAMED AND NOTHING FALLS THROUGH. The five value types are the
# five `Value` names in `types.py`, written out because `bool` is a subclass of
# `int` and an exact-type key is the only lookup that tells the two apart. A
# type absent from a row is a shape this library does not understand, and saying
# so is the whole point of the table being total.

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
        'Write the narrowing as data: {"name": "bob"} for an equality, '
        '{"age": {"gt": 18}} for an operator, and {"or": [...]} or {"not": {...}} '
        "to combine them."
    ),
}


def _nothing_to_do_with(what: str, written: Any) -> Fault:
    """A shape with no row. It is neither a name this library can look up nor a
    value it can bind, so the only thing left to do with it would be to paste it
    into a statement."""
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
# The one door
# =============================================================================


def condition_from_narrowing(narrowing: Any) -> Condition | Fault:
    """Turn what a caller wrote into the condition this library holds.

    A bare value means eq. `or` and `not` become `any_of` and `none_of`, and
    what hangs under either is a narrowing of the same shape, read the same way.
    A narrowing this library has no rule for comes back as a Fault naming the
    shape that was written and saying what to write instead.
    """
    return cast("Condition | Fault", _built_from("condition", narrowing))
