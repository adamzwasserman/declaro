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

from typing import Any, Literal, TypedDict

from declaro_persistum.query.expressions import (  # noqa: F401
    CaseOrderBy,
    JoinClause,
    OrderBy,
    SubqueryExpr,
    is_subquery,
    render_order_term,
    render_subquery,
)
from declaro_persistum.types import Dialect, Schema

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


class CaseExpression(TypedDict):
    """A CASE expression. `whens` pairs a condition dict with a result."""

    kind: Literal["case"]
    whens: list[tuple[dict[str, Any], Any]]
    else_: Any
    alias: str | None


def table(name: str, schema: Schema) -> dict[str, Any]:
    """Look a table up in the schema, failing loudly if it is absent.

    This returned a `TableProxy` whose `__getattr__` manufactured a
    `ColumnProxy` per attribute. It returns the schema's own table definition
    now — what a caller actually wants to consult — and a column is referred
    to by name, because that is all a column ever was.
    """
    if name not in schema:
        raise ValueError(
            f"Table {name!r} not found in schema. Available: {sorted(schema)}"
        )
    return schema[name]


def _function(name: str, *args: Any, alias: str | None) -> SQLFunction:
    return {"kind": "function", "name": name, "args": list(args), "alias": alias}


def count_(column: str = "*", alias: str | None = None) -> SQLFunction:
    return _function("COUNT", column, alias=alias)


def sum_(column: str, alias: str | None = None) -> SQLFunction:
    return _function("SUM", column, alias=alias)


def avg_(column: str, alias: str | None = None) -> SQLFunction:
    return _function("AVG", column, alias=alias)


def min_(column: str, alias: str | None = None) -> SQLFunction:
    return _function("MIN", column, alias=alias)


def max_(column: str, alias: str | None = None) -> SQLFunction:
    return _function("MAX", column, alias=alias)


def now_(alias: str | None = None) -> SQLFunction:
    return _function("NOW", alias=alias)


def case_(
    *whens: tuple[dict[str, Any], Any],
    else_: Any = None,
    alias: str | None = None,
) -> CaseExpression:
    """A CASE expression.

    Each `when` is (condition, result), where the condition is a condition
    DICT — the same shape `where` takes:

        case_(({"age": {"gt": 18}}, "adult"), else_="minor", alias="band")

    `alias` is new here. Every other factory took one; CaseExpression was the
    only shape that needed `.as_()` instead, which is why that method outlived
    its usefulness.
    """
    return {"kind": "case", "whens": list(whens), "else_": else_, "alias": alias}


def subquery(query: Any) -> SubqueryExpr:
    return {"kind": "subquery", "query": query}


def render_case(
    expr: CaseExpression, path: str, *, with_alias: bool
) -> tuple[str, dict[str, Any]]:
    """Render CASE WHEN ... THEN ... ELSE ... END.

    NO DIALECT. It took one and read it zero times. The argument existed only
    to be handed to `render_condition`, and that function no longer takes one
    either, for the same reason: a condition renders the same on every engine.
    Threading a value nobody reads is how `builder.py` came to pass the
    literal "sql" as a dialect without anything noticing.

    `with_alias` is REQUIRED, not defaulted, because the two callers want
    opposite things and neither is the obvious one: a SELECT column wants the
    alias, and an ORDER BY term must not have it — `... END AS band ASC` is
    not valid SQL.
    """
    from declaro_persistum.query.conditions import render as render_condition

    params: dict[str, Any] = {}
    parts = ["CASE"]

    for i, (condition, result) in enumerate(expr["whens"]):
        cond_sql, cond_params = render_condition(condition, f"{path}_w{i}")
        params.update(cond_params)
        name = f"{path}_t{i}"
        params[name] = result
        parts.append(f"WHEN {cond_sql} THEN :{name}")

    if expr["else_"] is not None:
        name = f"{path}_else"
        params[name] = expr["else_"]
        parts.append(f"ELSE :{name}")

    parts.append("END")
    sql = " ".join(parts)
    if with_alias and expr["alias"]:
        sql = f"{sql} AS {expr['alias']}"
    return sql, params


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
        if isinstance(arg, dict) and arg.get("kind") == "case":
            sql, p = render_case(arg, f"{path}_a{i}", with_alias=False)
            parts.append(sql)
            params.update(p)
        elif isinstance(arg, dict) and arg.get("kind") == "subquery":
            sql, p = render_subquery(arg, dialect)
            parts.append(f"({sql})")
            params.update(p)
        else:
            parts.append(str(arg))

    sql = f"{fn['name']}({', '.join(parts)})"
    if fn["alias"]:
        sql = f"{sql} AS {fn['alias']}"
    return sql, params
