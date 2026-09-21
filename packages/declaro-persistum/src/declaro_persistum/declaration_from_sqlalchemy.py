"""SQLAlchemy's phrasing, turned into a declaration.

WHAT THIS IS. One of four builders. A caller who thinks in a query object
writes `Person.query.filter_by(name="bob").with_only("id", "name")`, and this
turns that call into the same `Declaration` the other three produce for the
same question. It borrows the phrasing and nothing else: no session, no unit of
work, and no way back to SQL.

THE FOUR WORDS IT READS, AND WHY THEY ARE THE METHOD NAMES. `Asked` is the call
as the caller phrased it, so the keys here are SQLAlchemy's own words: `model`,
`wanted`, `filter_by`, `with_only`. Flattening `filter_by(name="bob")` into the
top level would read better until a model declared a column called `model`, and
then a column name and a grammar word would be the same key.

IT REFUSES A WORD IT DOES NOT HAVE, rather than passing over it. An `order_by`
this grammar cannot yet phrase is a question the caller asked; dropping it
answers a different question and says nothing. The phrasing for an order, a
page, a group and a write is not built, so each of those arrives as a refusal
naming itself.

NOTHING IS PICKED ON THE CALLER'S BEHALF. A question that names no model, or no
asking word, is refused. `all` is not assumed for a caller who did not write
it, because a builder that assumed it could not tell a caller who chose `all`
from one who forgot to ask for anything.

WHAT IT TRUSTS. `Asked` says a narrowing is a mapping and a column list is a
list of names. This reads them as the type says they are and checks nothing the
signature already states; a value of the wrong shape is the boundary's to
refuse, not this function's to re-check.
"""

from typing import Any, get_args

from declaro_persistum.types import (
    Asked,
    Comparison,
    Condition,
    Declaration,
    Fault,
    Operator,
    Reading,
    Writing,
)

AN_OPERATOR = frozenset(get_args(Operator))
A_READING_WORD = frozenset(get_args(Reading))
A_WRITING_WORD = frozenset(get_args(Writing))

# The two words a caller writes, and the two fields they become. A declaration
# holds no `or` and no `not`, so this is the only place they exist.
ANY_OF = "or"
NONE_OF = "not"

# The companion every text operator takes, sitting beside the operator in the
# same mapping: `{"like": "bo%", "ignore_case": True}`.
IGNORE_CASE = "ignore_case"


def _resolved(given: dict[str, Any], word: str, otherwise: Any) -> Any:
    """What the caller said under `word`, or `otherwise` when they said nothing.

    The one visible step where an absence becomes a value, which is what keeps
    it out of every signature in this module.
    """
    return {word: otherwise, **given}[word]


def _nothing_narrowed() -> Condition:
    """A condition that narrows nothing. Fresh each call, so no two
    declarations share one and a later edit to either moves both."""
    return {"each": {}, "any_of": [], "none_of": []}


# --- one comparison --------------------------------------------------------


def _a_bare_value(value: Any) -> tuple[Comparison, list[str]]:
    """`{"name": "bob"}`. A bare value means eq, expanded here so that by the
    time a declaration exists every comparison names its operator."""
    return {"operator": "eq", "value": value, "ignore_case": False}, []


def _a_named_operator(spelled: dict[str, Any]) -> tuple[Comparison, list[str]]:
    """`{"age": {"gt": 18}}`, and `{"name": {"like": "bo%", "ignore_case": True}}`."""
    named = sorted(set(spelled) - {IGNORE_CASE})
    unknown = sorted(set(named) - AN_OPERATOR)

    wrong = [
        f"{name!r} is not one of the fifteen operators: {', '.join(sorted(AN_OPERATOR))}"
        for name in unknown
    ] + [f"a comparison names one operator and this one names {len(named)}: {named}"] * (
        len(named) != 1
    )

    # THE FIRST NAMED OPERATOR, OR `eq` WHEN THE CALLER NAMED NONE. The second
    # case always arrives with a complaint beside it, so the comparison built
    # from it is thrown away; it is built anyway so this function has one shape.
    operator = [*named, "eq"][0]
    comparison: Comparison = {
        "operator": operator,
        "value": _resolved(spelled, operator, None),
        "ignore_case": _resolved(spelled, IGNORE_CASE, False),
    }
    return comparison, wrong


# Which of the two a value is. A mapping spells its operator; anything else is
# the value itself.
HOW_IT_IS_SPELLED = {True: _a_named_operator, False: _a_bare_value}


def _compared(value: Any) -> tuple[Comparison, list[str]]:
    return HOW_IT_IS_SPELLED[isinstance(value, dict)](value)


# --- the whole narrowing ---------------------------------------------------


def _narrowed_by(narrowing: Any) -> tuple[Condition, list[str]]:
    """What `filter_by` was given, as a condition, and what it got wrong.

    `and` is what two keys already mean, so `each` needs no word of its own.
    `or` and `not` carry conditions rather than columns, and they recur.
    """
    columns = sorted(set(narrowing) - {ANY_OF, NONE_OF})
    compared = {column: _compared(narrowing[column]) for column in columns}

    any_of = [_narrowed_by(one) for one in _resolved(narrowing, ANY_OF, [])]
    none_of = [_narrowed_by(one) for one in _resolved(narrowing, NONE_OF, [])]

    condition: Condition = {
        "each": {column: comparison for column, (comparison, _) in compared.items()},
        "any_of": [nested for nested, _ in any_of],
        "none_of": [nested for nested, _ in none_of],
    }
    wrong = (
        [f"{column}: {complaint}" for column in columns for complaint in compared[column][1]]
        + [complaint for _, complaints in any_of for complaint in complaints]
        + [complaint for _, complaints in none_of for complaint in complaints]
    )
    return condition, wrong


def _columns_named(columns: Any) -> tuple[list[str], list[str]]:
    """What `with_only` was given. A caller who named none asked for none."""
    return list(columns), []


# --- the four words this grammar has ---------------------------------------

# The phrased word, the declaration field it fills, and how it is read.
PHRASED = {
    "filter_by": ("condition", _narrowed_by),
    "with_only": ("columns", _columns_named),
}
REQUIRED = frozenset({"model", "wanted"})
KNOWN = REQUIRED | frozenset(PHRASED)


def _read(asked: Asked) -> tuple[dict[str, Any], list[str]]:
    """Every field this grammar can fill, and every complaint about the call."""
    spoken = PHRASED.keys() & asked.keys()
    built = {word: PHRASED[word][1](asked[word]) for word in spoken}

    named: dict[str, Any] = {
        "columns": [],
        "condition": _nothing_narrowed(),
        **{word: asked[word] for word in REQUIRED & asked.keys()},
        **{PHRASED[word][0]: built[word][0] for word in spoken},
    }

    # The asking word, as a set, so the three questions about it are set
    # arithmetic and not three tests of whether it is there.
    asking = {asked[word] for word in {"wanted"} & asked.keys()}

    wrong = (
        [
            f"this grammar has no word {word!r}; it phrases a question with "
            f"{', '.join(sorted(KNOWN))}"
            for word in sorted(asked.keys() - KNOWN)
        ]
        + [
            f"a declaration names a {word}, and this question names none"
            for word in sorted(REQUIRED - asked.keys())
        ]
        + [
            f"{word!r} is not one of the words a question may ask for"
            for word in sorted(asking - A_READING_WORD - A_WRITING_WORD)
        ]
        + [
            f"the SQLAlchemy phrasing carries no rows, so it cannot ask for {word!r}"
            for word in sorted(asking & A_WRITING_WORD)
        ]
        + [complaint for word in sorted(spoken) for complaint in built[word][1]]
    )
    return named, wrong


def _a_declaration(named: dict[str, Any], _wrong: list[str]) -> Declaration:
    """The six fields below are what this grammar has no phrasing for yet. They
    are empty rather than absent, because a field a reader has to check for is
    a field every reader has to check for."""
    return {
        "model": named["model"],
        "wanted": named["wanted"],
        "columns": named["columns"],
        "condition": named["condition"],
        "order": {},
        "grouping": [],
        "having": _nothing_narrowed(),
        "paging": None,
        "rows": None,
        "across": None,
    }


def _a_refusal(_named: dict[str, Any], wrong: list[str]) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": "; ".join(wrong),
        "what_to_do_instead": (
            "a question in this grammar names a model and one of "
            f"{', '.join(sorted(A_READING_WORD))}, narrows with filter_by and "
            "picks columns with with_only"
        ),
    }


WHAT_COMES_BACK = {False: _a_declaration, True: _a_refusal}


def declaration_from_sqlalchemy(asked: Asked) -> Declaration | Fault:
    """A query object's phrasing, as the declaration all four grammars build."""
    named, wrong = _read(asked)
    return WHAT_COMES_BACK[bool(wrong)](named, wrong)
