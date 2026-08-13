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

from typing import Any

__all__ = ["OPERATORS", "COMBINATORS", "render"]

# Characters that cannot appear in a generated parameter name. A column called
# `user.id` or `order-total` would otherwise produce `:w_user.id`, which no
# driver accepts.
_SAFE = str.maketrans({".": "_", "-": "_", " ": "_", '"': "", "'": ""})


def _name(path: str, column: str, suffix: str = "") -> str:
    return f"{path}_{column.translate(_SAFE)}{suffix}"


def _binary(op: str):
    """Build a renderer for the `col OP :param` shape.

    Ten of the fifteen operators differ only in the SQL token, so they are
    generated rather than written out ten times — the difference IS the token,
    and a table of tokens says that where ten near-identical functions would
    hide it.
    """

    def render_binary(column, operand, path, dialect):
        name = _name(path, column)
        return f"{column} {op} :{name}", {name: operand}

    return render_binary


def _like(pattern):
    """LIKE with the wildcards supplied by the operator, not by the caller.

    `startswith` means the caller said "a", not "a%". Putting the wildcard in
    the operator keeps the caller's value literal, which also means a value
    containing `%` cannot silently become a wildcard.
    """

    def render_like(column, operand, path, dialect):
        name = _name(path, column)
        return f"{column} LIKE :{name}", {name: pattern.format(operand)}

    return render_like


def _in(negated: bool):
    def render_in(column, operand, path, dialect):
        values = list(operand)
        if not values:
            # `IN ()` is a syntax error in every dialect. An empty set matches
            # nothing, and its negation matches everything; saying so in SQL
            # is clearer than raising, because an empty list is a legitimate
            # thing for a caller's loop to produce.
            return ("1 = 0", {}) if not negated else ("1 = 1", {})
        names = [_name(path, column, f"_{i}") for i in range(len(values))]
        placeholders = ", ".join(f":{n}" for n in names)
        keyword = "NOT IN" if negated else "IN"
        return f"{column} {keyword} ({placeholders})", dict(zip(names, values))

    return render_in


def _between(column, operand, path, dialect):
    low_name, high_name = _name(path, column, "_lo"), _name(path, column, "_hi")
    low, high = operand
    return (
        f"{column} BETWEEN :{low_name} AND :{high_name}",
        {low_name: low, high_name: high},
    )


def _null(sql: str):
    def render_null(column, operand, path, dialect):
        # The operand is ignored on purpose: `{"col": {"is_null": True}}` and
        # `{"col": {"is_null": False}}` would otherwise mean opposite things
        # while looking alike. Use `is_not_null` for the negation.
        return f"{column} {sql}", {}

    return render_null


OPERATORS = {
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

COMBINATORS = {"and": " AND ", "or": " OR "}


def _render_group(
    key: str, members: list[dict], dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    parts: list[str] = []
    params: dict[str, Any] = {}
    for i, member in enumerate(members):
        sql, member_params = render(member, dialect, f"{path}_{i}")
        if sql:
            parts.append(f"({sql})")
            params.update(member_params)
    return COMBINATORS[key].join(parts), params


def render(
    condition: dict[str, Any], dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    """Render a condition to (sql, params). Pure.

    `path` is required and identifies this node's position in the statement.
    It is what keeps two conditions on the same column apart, and what makes
    the output depend only on the input rather than on how many queries the
    process has built (Rule 14: no default, because two nodes silently sharing
    a path collide their parameter names).
    """
    parts: list[str] = []
    params: dict[str, Any] = {}

    for i, (key, value) in enumerate(condition.items()):
        here = f"{path}_{i}"

        if key in COMBINATORS:
            sql, group_params = _render_group(key, value, dialect, here)
            if sql:
                parts.append(sql)
                params.update(group_params)
            continue

        column = key
        if not isinstance(value, dict):
            # `{"status": "active"}` — a bare value means equality, because
            # that is what a caller writing a plain value means.
            sql, p = OPERATORS["eq"](column, value, here, dialect)
            parts.append(sql)
            params.update(p)
            continue

        for op, operand in value.items():
            if op not in OPERATORS:
                raise ValueError(
                    f"Unknown operator {op!r} on column {column!r}. "
                    f"Known operators: {', '.join(sorted(OPERATORS))}"
                )
            sql, p = OPERATORS[op](column, operand, here, dialect)
            parts.append(sql)
            params.update(p)

    return " AND ".join(parts), params
