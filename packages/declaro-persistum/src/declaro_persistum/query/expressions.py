"""Query expression DATA and the pure functions that render it.

Split out of `table.py` on 2026-08-12, when that file crossed 1000 lines and
the Slop Audit's god-file indicator went from 0.0% to 1.41% — a regression
caused by adding these very functions to it.

The split follows a real seam, not a line count. This module holds the parts
of a query that are DATA, plus the pure functions that turn them into SQL.
`table.py` keeps the fluent OBJECT surface — TableProxy, ColumnProxy,
Condition and the rest — which exists because callers chain methods and write
`a & b`, and which a TypedDict cannot provide.

Nothing here imports `table.py`, so the dependency runs one way. The renderers
reach the objects they need through the values passed to them
(`term["expr"]`, `expr["query"]`), never through an import, and that is what
keeps it acyclic.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol, TypedDict

from declaro_persistum.boundary import helper, recognizer
from declaro_persistum.types import Dialect


class RendersCondition(Protocol):
    """What a JOIN's `on` must be able to do. Structural, so this module
    names no class from `table.py` and the dependency stays one-way."""

    @helper
    def to_sql(self, dialect: Dialect, path: str) -> tuple[str, dict[str, Any]]:
        raise NotImplementedError("CODE NOT WRITTEN")


# `RendersBareFragment` stood here, a Protocol declaring `_bare_sql_fragment`.
# NOTHING HAD THAT METHOD. A CASE expression is a dict, and
# `_render_case_order_by` called the method on it, so ordering by a CASE
# raised `'dict' object has no attribute '_bare_sql_fragment'` on every call.
# Measured 2026-08-17, by the first test ever written for that renderer.
#
# It is a leftover from the object API: the de-classing turned the VALUES into
# dicts and left two renderers calling methods on them. The other is
# `render_subquery`, which is a missing feature rather than a one-line repair
# and is pinned as broken in
# `test_the_query_expression_layer_renders.py`.


class CaseExpression(TypedDict):
    """A CASE expression. `whens` pairs a condition dict with a result."""

    kind: Literal["case"]
    whens: list[tuple[dict[str, Any], Any]]
    else_: Any
    alias: str | None



@helper
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



class OrderBy(TypedDict):
    """An ORDER BY term over a plain column. Data, not an object.

    `kind` is load-bearing. Rendering used to pick a branch with
    `hasattr(o, "to_sql_fragment")`, which classifies by shape and silently
    reclassifies anything that gains or loses a method. Terms now dispatch
    through ORDER_TERM_RENDERERS on this tag (Rule 1), which fails loudly on
    an unknown one.
    """

    kind: Literal["order_by"]
    column: str
    direction: str



class CaseOrderBy(TypedDict):
    """An ORDER BY term over a CASE expression. The other bounded member."""

    kind: Literal["case_order_by"]
    expr: CaseExpression
    direction: str


@helper
def _render_order_by(
    term: OrderBy, _dialect: str, _path: str
) -> tuple[str, dict[str, Any]]:
    """A plain column term takes no parameters."""
    return f"{term['column']} {term['direction']}", {}


@helper
def _render_case_order_by(
    term: CaseOrderBy, _dialect: Dialect, path: str
) -> tuple[str, dict[str, Any]]:
    """A CASE term emits the BARE expression — an alias is not valid here."""
    sql, params = render_case(term["expr"], path, with_alias=False)
    return f"{sql} {term['direction']}", params


ORDER_TERM_RENDERERS: dict[str, Any] = {
    "order_by": _render_order_by,
    "case_order_by": _render_case_order_by,
}
"""The bounded ORDER BY vocabulary. Two members.

An earlier version of this line said "both rendered by tests". Neither was:
of 22 public callables in this module and `table.py`, 21 were never called by
any test, and the first one written for `case_order_by` found it raising
AttributeError on every call.

Replaces `if hasattr(o, "to_sql_fragment"): ... else: ...` in select.py. A
term carrying an unrecognised tag raises KeyError instead of falling through
to whichever branch happened to be last.
"""


@helper
def render_order_term(
    term: OrderBy | CaseOrderBy, dialect: Dialect, path: str
) -> tuple[str, dict[str, Any]]:
    """Render one ORDER BY term. Pure: term in, (sql, params) out."""
    return ORDER_TERM_RENDERERS[term["kind"]](term, dialect, path)


class SubqueryExpr(TypedDict):
    """
    Subquery expression for use in IN/NOT IN.

    Example:
        admin_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "admin")
        )
        rows = await users.select(users.id).where(users.id.in_(admin_ids)).execute()
    """

    kind: Literal["subquery"]
    query: Any


@helper
def is_subquery(value: Any) -> bool:
    """True when a value is a subquery expression.

    `isinstance` cannot be used on a TypedDict, and the operand it guards is
    caller-supplied — a list, a scalar, or a subquery — so the check is on
    the tag, not the type. A plain dict passed as a value has no `kind` and
    is correctly not a subquery.
    """
    return kind_of(value) == "subquery"


@recognizer
def kind_of(value: Any) -> str | None:
    """The tag a query expression carries, or None if it carries none.

    A `case` and a `subquery` are TypedDicts, and `isinstance` cannot read
    one, so what distinguishes them is the `kind` they declare. Three places
    asked `isinstance(x, dict) and x.get("kind") == "..."`, which is this
    question written three times with the answer hard-coded into each.

    A plain dict passed as a value carries no tag and is correctly nothing.
    """
    return value.get("kind") if isinstance(value, dict) else None


@helper
def render_subquery(expr: SubqueryExpr, dialect: Dialect) -> tuple[str, dict[str, Any]]:
    """Render the inner SELECT."""
    return expr["query"].to_sql(dialect)


class JoinClause(TypedDict):
    """A JOIN clause. Data, not an object — there is no behaviour here.

    This was a class whose whole body was an `__init__` assigning three
    fields. `query/builder.py` already rendered joins from plain dicts
    (`join["table"]`, `join.get("type")`), so one concept had two
    representations and only one of them needed a class.

    `join_type` also carried an implicit default of "inner", which made a
    caller who omitted it indistinguishable from one who chose it. A
    TypedDict has no defaults, so the omission is now a construction error
    instead of a silent decision (Rule 14). `SelectQuery.join` keeps its own
    documented default at the CONSUMER boundary, where a default is visible
    in the signature the caller reads.

    First slice of the strangler on this module: smallest possible unit,
    converted end to end rather than wrapped, because a facade that still
    reads `self` moves nothing.
    """

    table: str
    on: RendersCondition
    type: str
