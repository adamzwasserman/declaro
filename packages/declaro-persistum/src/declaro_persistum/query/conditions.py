"""A WHERE clause is data. This renders it.

    {"age": {"gt": 18}, "status": "active"}
    -> ("age > :w_age AND status = :w_status", {"w_age": 18, "w_status": "active"})

WHY THIS IS A DICT AND NOT A PYTHON EXPRESSION.

The old query layer spelled the same thing as `users.age > 18`. Making that
work took four classes and eight dunder methods:

    TableProxy.__getattr__                          so `users.age` resolved
    ColumnProxy.__eq__ __ne__ __lt__ __le__ __gt__ __ge__
    Condition.__and__ __or__                        so `a & b` composed

Every one of them existed to borrow Python's expression grammar, so that the
interpreter would build our AST for us. A dict is already that AST. There is
no evaluation step to arrange, because the condition was never anything but
data.

Two defects left with them. `ColumnProxy.__eq__` returned a `Condition`
instead of a bool — so `col == col` was not True, and `col in [a, b]` did the
wrong thing silently, since `in` is defined in terms of `__eq__`. And a class
that overrides `__eq__` without `__hash__` is UNHASHABLE, so a column could
not be put in a set or used as a dict key.

WHAT DATA BUYS BEYOND HONESTY. A condition can be printed, diffed, stored in a
column, sent over a wire, built by a loop, and compared in a test with `==`
that means equality. None of that is true of an object graph assembled by
operator overloading.

SHAPE

    {"col": value}                  equality, the common case
    {"col": {"op": operand}}        any operator in OPERATORS
    {"and": [cond, cond, ...]}      explicit conjunction
    {"or":  [cond, cond, ...]}      disjunction
    several keys in one dict        conjunction, so `and` is rarely needed

PARAMETER NAMES COME FROM THE PATH, never a counter. Two renderings of the
same condition produce byte-identical SQL, so a database's plan cache can hit
it (declaro-8a3, where a process-global counter made every query textually
novel).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from declaro_persistum.types import NOTHING, Nothing, resolved, written_as

__all__ = ["OPERATORS", "COMBINATORS", "Rendered", "Renderer", "render"]

# WHAT EVERY OPERATOR IN THE TABLE IS. Fifteen entries built by four factories,
# and none of them declared a signature: `render_binary(column, operand, path,
# dialect)` said only that it takes four things. An audit read that as an
# undeclared domain and was right.
#
# DECLARING IT EXPOSED A FOURTH ARGUMENT NOBODY READS. Every one of the five
# leaf renderers took `dialect` and loaded it zero times; `render` and
# `_render_group` only passed it down. A condition renders the same on every
# engine, so there was nothing for it to decide. It was removed rather than
# typed: a parameter no implementation reads is not a contract, and giving it
# a precise type would only have made the claim precise.
#
# It also hid a defect for as long as it existed. `builder.py` called
# `render(where, "sql", path)`, passing the literal "sql" as the dialect,
# which is not one. Nothing noticed, because nothing read it.
#
# It is also the shape the rest of this package already uses for an injected
# callable: `Connect`, `WriteOne`, `Transaction`, `ForDDL`. One more of the
# same is consistent; a Protocol per operator would not be.
Rendered = tuple[str, dict[str, Any]]
Renderer = Callable[[str, Any, str], Rendered]

# Characters that cannot appear in a generated parameter name. A column called
# `user.id` or `order-total` would otherwise produce `:w_user.id`, which no
# driver accepts.
_SAFE = str.maketrans({".": "_", "-": "_", " ": "_", '"': "", "'": ""})


def _name(path: str, column: str, suffix: str | Nothing) -> str:
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    suffix = resolved(suffix, '')
    return f"{path}_{column.translate(_SAFE)}{suffix}"


def _binary(op: str) -> Renderer:
    """Build a renderer for the `col OP :param` shape.

    Ten of the fifteen operators differ only in the SQL token, so they are
    generated rather than written out ten times — the difference IS the token,
    and a table of tokens says that where ten near-identical functions would
    hide it.
    """

    def render_binary(column: str, operand: Any, path: str) -> Rendered:
        name = _name(path, column, suffix=NOTHING)
        return f"{column} {op} :{name}", {name: operand}

    return render_binary


def _like(pattern: str) -> Renderer:
    """LIKE with the wildcards supplied by the operator, not by the caller.

    `startswith` means the caller said "a", not "a%". Putting the wildcard in
    the operator keeps the caller's value literal, which also means a value
    containing `%` cannot silently become a wildcard.
    """

    def render_like(column: str, operand: Any, path: str) -> Rendered:
        name = _name(path, column, suffix=NOTHING)
        return f"{column} LIKE :{name}", {name: pattern.format(operand)}

    return render_like


# WHAT AN EMPTY SET COMPARES TO. `IN ()` is a syntax error in every dialect.
# An empty set matches nothing, and its negation matches everything; saying so
# in SQL is clearer than raising, because an empty list is a legitimate thing
# for a caller's loop to produce.
_EMPTY_SET: dict[bool, str] = {False: "1 = 0", True: "1 = 1"}


def _in(negated: bool) -> Renderer:
    def _no_values(_column: str, _values: list[Any], _path: str) -> Rendered:
        """Nothing to list, so the comparison is written as a constant."""
        return _EMPTY_SET[negated], {}

    def _some_values(column: str, values: list[Any], path: str) -> Rendered:
        """One placeholder per value, named by position under this path."""
        names = [_name(path, column, f"_{i}") for i in range(len(values))]
        placeholders = ", ".join(f":{n}" for n in names)
        keyword = "NOT IN" if negated else "IN"
        # strict: `names` is built from `len(values)` two lines up, so a length
        # mismatch is impossible unless that changes -- at which point this
        # raises instead of silently dropping the tail of the IN list.
        return f"{column} {keyword} ({placeholders})", dict(
            zip(names, values, strict=True)
        )

    _WHEN_VALUES: dict[bool, Callable[[str, list[Any], str], Rendered]] = {
        True: _some_values,
        False: _no_values,
    }

    def render_in(column: str, operand: Sequence[Any], path: str) -> Rendered:
        values = list(operand)
        return _WHEN_VALUES[bool(values)](column, values, path)

    return render_in


def _between(column: str, operand: tuple[Any, Any], path: str) -> Rendered:
    low_name, high_name = _name(path, column, "_lo"), _name(path, column, "_hi")
    low, high = operand
    return (
        f"{column} BETWEEN :{low_name} AND :{high_name}",
        {low_name: low, high_name: high},
    )


def _null(sql: str) -> Renderer:
    def render_null(column: str, _operand: Any, _path: str) -> Rendered:
        # The operand is ignored on purpose: `{"col": {"is_null": True}}` and
        # `{"col": {"is_null": False}}` would otherwise mean opposite things
        # while looking alike. Use `is_not_null` for the negation.
        return f"{column} {sql}", {}

    return render_null


OPERATORS: dict[str, Renderer] = {
    "eq": _binary("="),
    "ne": _binary("!="),
    "gt": _binary(">"),
    "gte": _binary(">="),
    "lt": _binary("<"),
    "lte": _binary("<="),
    "like": _binary("LIKE"),
    "startswith": _like("{}%"),
    "endswith": _like("%{}"),
    "contains": _like("%{}%"),
    "in": _in(negated=False),
    "not_in": _in(negated=True),
    "between": _between,
    "is_null": _null("IS NULL"),
    "is_not_null": _null("IS NOT NULL"),
}
"""The bounded operator vocabulary. A dict lookup, so an unknown operator
fails at the lookup rather than falling through an if/elif chain to whatever
the last branch happened to be (Rule 1)."""

COMBINATORS: dict[str, str] = {"and": " AND ", "or": " OR "}


def _render_group(
    key: str, members: list[dict], path: str
) -> tuple[str, dict[str, Any]]:
    # A MEMBER THAT RENDERS TO NOTHING CONTRIBUTES NOTHING: neither an empty
    # pair of parentheses nor its parameters. An empty condition dict is the
    # member that does this.
    written = [
        rendered
        for i, member in enumerate(members)
        for rendered in [render(member, f"{path}_{i}")]
        if rendered[0]
    ]
    parts = [f"({sql})" for sql, _ in written]
    params = {
        name: value
        for _, member_params in written
        for name, value in member_params.items()
    }
    return COMBINATORS[key].join(parts), params


def _render_combinator(key: str, value: Any, here: str) -> list[Rendered]:
    """`and` or `or`: one group, dropped when it renders to nothing."""
    return [rendered for rendered in [_render_group(key, value, here)] if rendered[0]]


def _render_equality(column: str, value: Any, here: str) -> list[Rendered]:
    """`{"status": "active"}` — a bare value means equality, because that is
    what a caller writing a plain value means."""
    return [OPERATORS["eq"](column, value, here)]


def _refuse_unknown_operators(column: str, unknown: list[str]) -> list[str]:
    """An operator no entry answers for, named so the author sees the typo."""
    raise ValueError(
        f"Unknown operator {unknown[0]!r} on column {column!r}. "
        f"Known operators: {', '.join(sorted(OPERATORS))}"
    )


def _the_known_operators(_column: str, unknown: list[str]) -> list[str]:
    """Every operator named is in the table, so nothing is unknown."""
    return unknown


_WHEN_UNKNOWN_OPERATOR: dict[bool, Callable[[str, list[str]], list[str]]] = {
    True: _the_known_operators,
    False: _refuse_unknown_operators,
}


def _render_operators(column: str, value: dict[str, Any], here: str) -> list[Rendered]:
    """A mapping names operators, and every one of them must be in the table."""
    unknown = [op for op in value if op not in OPERATORS]
    _WHEN_UNKNOWN_OPERATOR[not unknown](column, unknown)
    return [OPERATORS[op](column, operand, here) for op, operand in value.items()]


# A MAPPING NAMES OPERATORS; ANYTHING ELSE IS THE VALUE ITSELF.
_RENDER_COLUMN: dict[bool, Callable[[str, Any, str], list[Rendered]]] = {
    True: _render_operators,
    False: _render_equality,
}


def _render_column(key: str, value: Any, here: str) -> list[Rendered]:
    """A key that is not a combinator is a column name."""
    return _RENDER_COLUMN[written_as(dict, value)](key, value, here)


# WHAT A KEY IN A CONDITION DICT IS: either one of the two combinators, or a
# column. Read with [], so the two are answered by the same lookup.
_RENDER_ENTRY: dict[bool, Callable[[str, Any, str], list[Rendered]]] = {
    True: _render_combinator,
    False: _render_column,
}


def render(
    condition: dict[str, Any], path: str
) -> tuple[str, dict[str, Any]]:
    """Render a condition to (sql, params). Pure.

    `path` is required and identifies this node's position in the statement.
    It is what keeps two conditions on the same column apart, and what makes
    the output depend only on the input rather than on how many queries the
    process has built (Rule 14: no default, because two nodes silently sharing
    a path collide their parameter names).
    """
    rendered = [
        pair
        for i, (key, value) in enumerate(condition.items())
        for pair in _RENDER_ENTRY[key in COMBINATORS](key, value, f"{path}_{i}")
    ]
    parts = [sql for sql, _ in rendered]
    params = {
        name: value
        for _, part_params in rendered
        for name, value in part_params.items()
    }
    return " AND ".join(parts), params
