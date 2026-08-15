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

from declaro_persistum.types import Dialect


class RendersCondition(Protocol):
    """What a JOIN's `on` must be able to do. Structural, so this module
    names no class from `table.py` and the dependency stays one-way."""

    def to_sql(self, dialect: Dialect, path: str) -> tuple[str, dict[str, Any]]: ...


class RendersBareFragment(Protocol):
    """What a CASE ORDER BY term's expression must be able to do.

    `_bare_sql_fragment` rather than `to_sql_fragment`: an ORDER BY term must
    emit the CASE without its alias, because `... END AS band ASC` is not
    valid SQL.
    """

    def _bare_sql_fragment(
        self, dialect: Dialect, path: str
    ) -> tuple[str, dict[str, Any]]: ...


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
    expr: RendersBareFragment
    direction: str


def _render_order_by(
    term: OrderBy, _dialect: str, _path: str
) -> tuple[str, dict[str, Any]]:
    """A plain column term takes no parameters."""
    return f"{term['column']} {term['direction']}", {}


def _render_case_order_by(
    term: CaseOrderBy, dialect: Dialect, path: str
) -> tuple[str, dict[str, Any]]:
    """A CASE term emits the BARE expression — an alias is not valid here."""
    sql, params = term["expr"]._bare_sql_fragment(dialect, path)
    return f"{sql} {term['direction']}", params


ORDER_TERM_RENDERERS: dict[str, Any] = {
    "order_by": _render_order_by,
    "case_order_by": _render_case_order_by,
}
"""The bounded ORDER BY vocabulary. Two members, both rendered by tests.

Replaces `if hasattr(o, "to_sql_fragment"): ... else: ...` in select.py. A
term carrying an unrecognised tag raises KeyError instead of falling through
to whichever branch happened to be last.
"""


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


def is_subquery(value: Any) -> bool:
    """True when a value is a subquery expression.

    `isinstance` cannot be used on a TypedDict, and the operand it guards is
    caller-supplied — a list, a scalar, or a subquery — so the check is on
    the tag, not the type. A plain dict passed as a value has no `kind` and
    is correctly not a subquery.
    """
    return isinstance(value, dict) and value.get("kind") == "subquery"


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
