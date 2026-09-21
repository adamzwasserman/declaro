"""Django's phrasing, turned into the declaration all four grammars produce.

    fn declaration_from_django : (asked: Asked) -> Declaration | Fault

A team that already writes Django writes what it already knows:

    Person.objects.filter(name="bob").values("id", "name")

IT BORROWS THE PHRASING AND NOTHING ELSE. No session holding state, no manager
attached to a class, and no second place where a column can be named. What
arrives is one `Asked`: the call already taken apart, under the four words this
grammar spells a question with. What leaves is the same `Declaration` the other
three grammars produce, so a caller writing the same question in any of the four
gets the same rows.

THE FOUR WORDS ARE KEYS, NOT A FLATTENED CALL. `model` and `wanted` are the
declaration's own, carried by every grammar. `filter` is Django's word for
narrowing and `values` is its word for naming columns. Flattening
`filter(name="bob")` into the top level would read better until a model declared
a column called `model`, and then a column name and a grammar word would be the
same key.

A WORD THIS GRAMMAR DOES NOT HAVE IS REFUSED BY NAME. An `order_by` it cannot
yet phrase is a question the caller asked, and passing over it answers a
different question while saying nothing. Nothing is picked on the caller's
behalf either: a call naming no model, or no asking word, is refused rather than
given `all`, because a builder that assumed `all` could not tell a caller who
chose it from one who forgot to ask for anything.

A BARE VALUE MEANS `eq`. `{"name": "bob"}` and `{"name": {"eq": "bob"}}` are the
same thing, so both build one comparison and neither is a special case. `and` is
what two keys already mean; `or` and `not` carry conditions rather than columns,
so they recur and the declaration holds neither word. An operator nobody declared
is refused naming it and listing the fifteen.

A WRITING WORD IS REFUSED, NOT HALF BUILT. `add` and `change` carry rows, this
phrasing carries none, and a write that lands nothing while reporting success is
the silent failure this library exists to delete. Django's spelling for the rows
of a write is written down nowhere, so nothing here invents one.

WHAT IT CHECKS AND WHAT IT TRUSTS. The four words arrive at the top level from a
caller, so their shapes are checked here. Inside a narrowing it reads what
`Asked` says is there and re-checks nothing the type already states.

WHAT IT DOES NOT DO. It never looks at a model, so a column no model declares
passes straight through: `refuse_undeclared_columns` answers that, and it is
handed the declaration this builds.

Pure: one dict in, one dict out. No clock, no random source, no environment, no
file and no connection.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.types import (
    Asked,
    Comparison,
    Condition,
    Declaration,
    Fault,
    Operator,
    Reading,
    Wanted,
)

AN_OPERATOR = frozenset(get_args(Operator))
AN_ASKING_WORD = frozenset(get_args(Reading))

# The two words a caller writes, and the two fields they become. A declaration
# holds no `or` and no `not`, so this is the only place they exist.
ANY_OF = "or"
NONE_OF = "not"

# The companion every text operator takes, sitting beside the operator in the
# same mapping: `{"like": "bo%", "ignore_case": True}`.
IGNORE_CASE = "ignore_case"

A_DJANGO_WORD: dict[str, tuple[type, str]] = {
    "model": (str, "the name of the model the call was written against"),
    "wanted": (str, "one asking word"),
    "filter": (dict, "a mapping of column to what it is narrowed by"),
    "values": (list, "a list of column names"),
}
"""Each word this grammar has, and the shape Django's phrasing carries beside it."""

SAID_BY_EVERY_CALL = ("model", "wanted")
"""The two a declaration cannot be built without. Neither takes a default."""


def _said(given: dict[str, Any], word: str, otherwise: Any) -> Any:
    """What the caller said under `word`, or `otherwise` when they said nothing.

    The one visible step where an absence becomes a value, which is what keeps
    it out of every signature in this module.
    """
    return {word: otherwise, **given}[word]


def _nothing_narrowed() -> Condition:
    """A condition that narrows nothing. Fresh each call, so no two declarations
    share one and a later edit to either moves both."""
    return {"each": {}, "any_of": [], "none_of": []}


# --- one comparison --------------------------------------------------------


def _a_bare_value(narrowing: Any) -> tuple[Comparison, list[str]]:
    """`{"name": "bob"}`. Expanded here, so that by the time a declaration
    exists every comparison names its operator."""
    return {"operator": "eq", "value": narrowing, "ignore_case": False}, []


def _a_named_operator(narrowing: Any) -> tuple[Comparison, list[str]]:
    """`{"age": {"gt": 18}}`, and `{"name": {"like": "bo%", "ignore_case": True}}`."""
    named = sorted(set(narrowing) - {IGNORE_CASE})
    wrong = [
        f"`{name}` is not one of the fifteen operators: {', '.join(sorted(AN_OPERATOR))}"
        for name in sorted(set(named) - AN_OPERATOR)
    ] + [
        f"a comparison names one operator and this one names {len(named)}: {named}"
        for _ in range(len(named) != 1)
    ]

    # THE FIRST NAMED OPERATOR, OR `eq` WHEN THE CALLER NAMED NONE. The second
    # case always arrives with a complaint beside it, so the comparison built
    # from it is thrown away; it is built anyway so this function has one shape.
    operator = [*named, "eq"][0]
    comparison: Comparison = {
        "operator": cast(Operator, operator),
        "value": _said(narrowing, operator, None),
        "ignore_case": bool(_said(narrowing, IGNORE_CASE, False)),
    }
    return comparison, wrong


# Which of the two a narrowing is. A mapping names its operator; anything else
# is the value itself.
HOW_IT_IS_SPELLED: dict[bool, Callable[[Any], tuple[Comparison, list[str]]]] = {
    True: _a_named_operator,
    False: _a_bare_value,
}


def _compared(narrowing: Any) -> tuple[Comparison, list[str]]:
    return HOW_IT_IS_SPELLED[isinstance(narrowing, dict)](narrowing)


# --- the whole narrowing ---------------------------------------------------


def _narrowed_by(narrowing: dict[str, Any]) -> tuple[Condition, list[str]]:
    """What `filter` was given, as a condition, and what it got wrong.

    `and` is what two keys already mean, so `each` needs no word of its own.
    `or` and `not` carry conditions rather than columns, and they recur.
    """
    columns = sorted(set(narrowing) - {ANY_OF, NONE_OF})
    compared = {column: _compared(narrowing[column]) for column in columns}

    any_of = [_narrowed_by(one) for one in _said(narrowing, ANY_OF, [])]
    none_of = [_narrowed_by(one) for one in _said(narrowing, NONE_OF, [])]

    condition: Condition = {
        "each": {column: comparison for column, (comparison, _) in compared.items()},
        "any_of": [nested for nested, _ in any_of],
        "none_of": [nested for nested, _ in none_of],
    }
    wrong = [
        f"`{column}`: {complaint}" for column in columns for complaint in compared[column][1]
    ] + [complaint for _, complaints in any_of + none_of for complaint in complaints]
    return condition, wrong


def _the_narrowing(asked: Asked) -> dict[str, Any]:
    """What `filter` was given, when it was given the shape Django carries.

    A `filter` of another shape is a complaint of its own, and reading it as a
    narrowing on top of that would say the same thing twice in worse words.
    """
    return {
        column: narrowed_by
        for word in ("filter",)
        if isinstance(_said(dict(asked), word, None), dict)
        for column, narrowed_by in cast(dict, asked[word]).items()
    }


# --- what is wrong with the call, all of it at once -------------------------


def _complaints(asked: Asked, narrowing_wrong: list[str]) -> list[str]:
    """Everything wrong with this call, so one refusal can say all of it."""
    return (
        [
            f"`{word}` is not a word this grammar has. Django's phrasing arrives "
            f"here as {', '.join(sorted(A_DJANGO_WORD))}"
            for word in asked
            if word not in A_DJANGO_WORD
        ]
        + [
            f"`{word}` is missing, and nothing here stands in for it"
            for word in SAID_BY_EVERY_CALL
            if word not in asked
        ]
        + [
            f"`{word}` arrived as a {type(asked[word]).__name__}, and Django's "
            f"phrasing carries {reads} there"
            for word, (shape, reads) in A_DJANGO_WORD.items()
            if word in asked and not isinstance(asked[word], shape)
        ]
        + [
            f"`{word}` is not one of the twelve words a question can want: "
            f"{', '.join(sorted(AN_ASKING_WORD))}. A write carries rows, this "
            f"phrasing carries none, and a write carrying no rows lands nothing"
            for word in [asked[key] for key in ("wanted",) if key in asked]
            if word not in AN_ASKING_WORD
        ]
        + narrowing_wrong
    )


# --- the two answers --------------------------------------------------------


def _build(asked: Asked, condition: Condition, _complaints: list[str]) -> Declaration | Fault:
    """Reached only with nothing to complain about, so every word this grammar
    needs is there and each one is the shape Django carries."""
    return {
        "model": cast(str, asked["model"]),
        "wanted": cast(Wanted, asked["wanted"]),
        "columns": list(_said(dict(asked), "values", [])),
        "condition": condition,
        "order": {},
        "grouping": [],
        "having": _nothing_narrowed(),
        "paging": None,
        "rows": None,
        "across": None,
    }


def _refuse(_asked: Asked, _condition: Condition, complaints: list[str]) -> Declaration | Fault:
    """Say every one of them, in the caller's own process, before anything runs."""
    return Fault(
        kind="refused",
        what_went_wrong="This call cannot be read as Django's phrasing: " + "; ".join(complaints),
        what_to_do_instead=(
            "Write the call again in the four words this grammar has: a model, an "
            "asking word, `filter` carrying a mapping of column to what it is "
            "narrowed by, and `values` carrying the column names you want."
        ),
    )


_ANSWER: dict[bool, Callable[[Asked, Condition, list[str]], Declaration | Fault]] = {
    False: _build,
    True: _refuse,
}


def declaration_from_django(asked: Asked) -> Declaration | Fault:
    """Turn one call written in Django's phrasing into a declaration."""
    condition, narrowing_wrong = _narrowed_by(_the_narrowing(asked))
    complaints = _complaints(asked, narrowing_wrong)
    return _ANSWER[bool(complaints)](asked, condition, complaints)
