"""SQL functions and CASE expressions, as data.

`count_("*")` used to return an `SQLFunction` OBJECT carrying an `.as_()`
method. It now returns a dict:

    count_("*", alias="n")  ->  {"kind": "function", "name": "COUNT",
                                 "args": ["*"], "alias": "n"}

`.as_()` is gone and nothing is lost: every factory already took `alias=` as a
parameter, so the method was a second spelling of an argument that shipped
beside it. `case_` gains the same parameter, which is the only shape that
lacked one.

`TableProxy` and `ColumnProxy` are gone entirely. They existed so that
`users.age > 18` would resolve and compare — eight dunder methods borrowing
Python's expression grammar so the interpreter would build our AST. A
condition is a dict now (`query/conditions.py`), so a column is just its name
and there is nothing left for a proxy to do.

Two defects left with them: `ColumnProxy.__eq__` returned a `Condition`
instead of a bool, so `col == col` was not True and `col in [a, b]` silently
misbehaved; and overriding `__eq__` without `__hash__` made the class
unhashable, so a column could not go in a set.

`table(name, schema)` survives, because checking a table name against the
schema at build time is real work. It returns the schema's own table
definition rather than a proxy.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, TypedDict

from declaro_persistum.boundary import helper
from declaro_persistum.exceptions import ValidationError
from declaro_persistum.query.expressions import (  # noqa: F401
    ALIASED,
    CaseExpression,
    CaseOrderBy,
    JoinClause,
    OrderBy,
    SubqueryExpr,
    is_subquery,
    kind_of,
    render_case,
    render_order_term,
    render_subquery,
)
from declaro_persistum.types import NOTHING, Dialect, Nothing, Schema, Table, resolved

__all__ = [
    "SQLFunction",
    "CaseExpression",
    "table",
    "count_",
    "sum_",
    "avg_",
    "min_",
    "max_",
    "now_",
    "case_",
    "subquery",
    "render_function",
    "render_case",
]


class SQLFunction(TypedDict):
    """A SQL function call. Data, so it can be printed, diffed and stored."""

    kind: Literal["function"]
    name: str
    args: list[Any]
    alias: str | None


@helper
def _refuse_unknown_table(name: str, schema: Schema) -> Table:
    """A name the schema does not hold, named beside the ones it does."""
    raise ValueError(
        f"Table {name!r} not found in schema. Available: {sorted(schema)}"
    )


@helper
def _the_known_table(name: str, schema: Schema) -> Table:
    """The schema holds this name, so its own definition is the answer."""
    return schema[name]


_WHEN_UNKNOWN_TABLE: dict[bool, Callable[[str, Schema], Table]] = {
    True: _the_known_table,
    False: _refuse_unknown_table,
}


@helper
def table(name: str, schema: Schema) -> Table:
    """Look a table up in the schema, failing loudly if it is absent.

    This returned a `TableProxy` whose `__getattr__` manufactured a
    `ColumnProxy` per attribute. It returns the schema's own table definition
    now — what a caller actually wants to consult — and a column is referred
    to by name, because that is all a column ever was.
    """
    return _WHEN_UNKNOWN_TABLE[name in schema](name, schema)


@helper
def _function(name: str, *args: Any, alias: str | None) -> SQLFunction:
    return {"kind": "function", "name": name, "args": list(args), "alias": alias}


@helper
def count_(column: str, alias: str | Nothing) -> SQLFunction:
    """COUNT of a column, or of everything if the caller writes `"*"`.

    THE STAR IS WRITTEN DOWN. This defaulted `column` to `"*"`, so a call
    that meant "count every row" and a call that forgot to say what to count
    produced the same SQL and nothing could tell them apart.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    the_alias = resolved(alias, None)
    return _function("COUNT", column, alias=the_alias)


# THE FIVE AGGREGATES, which differ by one SQL word and nothing else. They
# were five copies of the same three lines, including the same comment about
# the alias, so the word is the table and the function is written once.
#
# The public names survive because they are what a caller writes:
# `sum_("total")` reads as SQL does, and `aggregate("SUM", "total")` would not.
_AGGREGATES = ("SUM", "AVG", "MIN", "MAX")


@helper
def _aggregate(word: str) -> Callable[[str, str | Nothing], SQLFunction]:
    """One aggregate function, named for the SQL word it writes.

    `alias` has no default here for the same reason it has none anywhere in
    this file: a caller who omits it and a caller who chose `None` would be
    indistinguishable, and the non-default region would never be exercised.
    """

    @helper
    def of(column: str, alias: str | Nothing) -> SQLFunction:
        return _function(word, column, alias=resolved(alias, None))

    return of


sum_, avg_, min_, max_ = (_aggregate(word) for word in _AGGREGATES)


@helper
def now_(alias: str | Nothing) -> SQLFunction:
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    the_alias = resolved(alias, None)
    return _function("NOW", alias=the_alias)


@helper
def case_(
    *whens: tuple[dict[str, Any], Any],
    else_: Any | Nothing,
    alias: str | Nothing,
) -> CaseExpression:
    """A CASE expression.

    Each `when` is (condition, result), where the condition is a condition
    DICT — the same shape `where` takes:

        case_(({"age": {"gt": 18}}, "adult"), else_="minor", alias="band")

    `alias` is new here. Every other factory took one; CaseExpression was the
    only shape that needed `.as_()` instead, which is why that method outlived
    its usefulness.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    else_ = resolved(else_, None)
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    else_ = resolved(else_, None)
    the_alias = resolved(alias, None)
    return {"kind": "case", "whens": list(whens), "else_": else_, "alias": the_alias}


@helper
def subquery(query: Any) -> SubqueryExpr:
    return {"kind": "subquery", "query": query}



@helper
def _render_case_argument(
    arg: Any, path: str, _dialect: Dialect
) -> tuple[str, dict[str, Any]]:
    """A CASE used as a function argument carries no alias of its own."""
    return render_case(arg, path, with_alias=False)


@helper
def _render_subquery_argument(
    arg: Any, _path: str, dialect: Dialect
) -> tuple[str, dict[str, Any]]:
    """A subquery argument is wrapped, because it is one value here."""
    sql, params = render_subquery(arg, dialect)
    return f"({sql})", params


@helper
def _render_literal(
    arg: Any, _path: str, _dialect: Dialect
) -> tuple[str, dict[str, Any]]:
    """Anything carrying no tag is written as it stands."""
    return str(arg), {}


# WHAT EACH TAGGED ARGUMENT RENDERS AS. This was an if/elif chain asking
# `isinstance(arg, dict) and arg.get("kind") == "..."` twice, which is the tag
# read once per branch with the answer hard-coded into each. `kind_of` reads
# it; this says what each tag means.
#
# EVERY ENTRY TAKES THE SAME THREE THINGS whether it reads them or not, which
# is what lets the dispatch be a lookup. An unread one carries the underscore.
_RENDER_ARGUMENT: dict[str | None, Any] = {
    "case": _render_case_argument,
    "subquery": _render_subquery_argument,
    # THE UNTAGGED CASE IS A MEMBER, NOT A FALLBACK. This was the second
    # argument of a `.get`, so the two tags the table declares and every tag
    # it does not were the same question. An argument tagged `{"kind":
    # "windo"}` rendered as `UPPER({'kind': 'windo', 'over': 'x'})`: a Python
    # dict repr inside a SQL statement, unbound and unescaped, with the
    # engine the first thing to see the mistake.
    None: _render_literal,
}


@helper
def _refuse_unknown_kind(kind: str | None) -> str | None:
    """A tag no renderer answers for, named so the author sees the typo."""
    raise ValidationError(
        f"a query argument is tagged {kind!r}, which nothing renders. The "
        f"tags that render are {sorted(k for k in _RENDER_ARGUMENT if k)}, "
        f"and an argument carrying no tag is written as it stands.",
        table=NOTHING,
        column=NOTHING,
        reference=NOTHING,
    )


@helper
def _the_known_kind(kind: str | None) -> str | None:
    """The tag is one the table answers for, handed back to look it up with."""
    return kind


_WHEN_UNKNOWN_KIND: dict[bool, Any] = {
    True: _the_known_kind,
    False: _refuse_unknown_kind,
}


@helper
def render_function(
    fn: SQLFunction, dialect: Dialect, path: str
) -> tuple[str, dict[str, Any]]:
    """Render a function call, and any expression nested inside it.

    A nested argument gets its own sub-path, so a CASE inside an aggregate
    cannot collide with one in the SELECT list.
    """
    parts: list[str] = []
    params: dict[str, Any] = {}

    for i, arg in enumerate(fn["args"]):
        kind = kind_of(arg)
        known = _WHEN_UNKNOWN_KIND[kind in _RENDER_ARGUMENT](kind)
        sql, rendered_params = _RENDER_ARGUMENT[known](arg, f"{path}_a{i}", dialect)
        parts.append(sql)
        params.update(rendered_params)

    sql = f"{fn['name']}({', '.join(parts)})"
    # The same table `render_case` reads, so "does this carry an alias" is
    # answered in one place rather than once per renderer.
    return ALIASED[bool(fn["alias"])](sql, fn["alias"]), params
