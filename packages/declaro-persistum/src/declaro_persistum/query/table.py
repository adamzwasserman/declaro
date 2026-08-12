"""
Schema-validated table and column proxies.

Provides dot-notation access to tables and columns that validates
against the loaded schema at query-build time, not execution time.

-------------------------------------------------------------------------------
STOP. THIS MODULE CONTAINS INTERNAL IMPLEMENTATION DETAILS.
-------------------------------------------------------------------------------

``Condition``, ``ConditionGroup``, ``CaseExpression``, ``CaseOrderBy``,
``SubqueryExpr``, and ``SQLFunction`` are **internal classes**. Their
``.to_sql()`` and ``.to_sql_fragment()`` methods are called by ``SelectQuery``
as part of query assembly. They are not part of the public API. They carry no
stability guarantee. Their signatures may change without notice.

If you are calling ``.to_sql()`` on a ``Condition`` or any other internal class
directly, **stop doing that immediately**. You are bypassing the query layer and
will be broken by the next refactor with no sympathy.

The public API is ``SelectQuery``:

    # Build a query using the table proxy
    rows = await (
        users.select(users.id, users.email)
        .where(users.status == "active")
        .order_by(users.created_at.desc())
        .execute()
    )

    # Or get the SQL string + params if you need to inspect them
    sql, params = q.to_sql()           # defaults to postgresql dialect
    sql, params = q.to_sql("sqlite")   # explicit dialect

``SelectQuery.to_sql(dialect)`` is the one place the dialect is exposed.
It propagates to every internal component automatically. You do not touch
``Condition``, ``ConditionGroup``, or anything else in this module directly.
-------------------------------------------------------------------------------
"""

from typing import TYPE_CHECKING, Any

# The expression data types and their renderers live in `expressions.py` and
# are re-exported here because `table.py` is the documented home of the query
# API. The dependency runs one way: expressions.py imports nothing from here.
from declaro_persistum.query.expressions import (  # noqa: F401
    ORDER_TERM_RENDERERS,
    CaseOrderBy,
    JoinClause,
    OrderBy,
    SubqueryExpr,
    is_subquery,
    render_order_term,
    render_subquery,
)
from declaro_persistum.types import Column, Schema

if TYPE_CHECKING:
    from declaro_persistum.query.delete import DeleteQuery
    from declaro_persistum.query.django_style import QuerySet
    from declaro_persistum.query.hooks import PostHook, PreHook
    from declaro_persistum.query.insert import InsertQuery
    from declaro_persistum.query.prisma_style import PrismaQueryBuilder
    from declaro_persistum.query.select import SelectQuery
    from declaro_persistum.query.update import UpdateQuery


def table(
    name: str,
    schema: Schema,
    pool: Any = None,
    *,
    pre: "PreHook | None" = None,
    post: "PostHook | None" = None,
) -> "TableProxy":
    """
    Create a schema-validated table proxy.

    Args:
        name: Table name (must exist in schema)
        schema: Schema dict
        pool: Connection pool with acquire() context manager
        pre: Optional pre-hook — runs before SQL is built, transforms the query object.
        post: Optional post-hook — runs after DB returns, transforms rows.

    Returns:
        TableProxy for building queries

    Raises:
        ValueError: If table not found in schema
    """
    if name not in schema:
        raise ValueError(f"Table '{name}' not found in schema. Available: {list(schema.keys())}")
    return TableProxy(name, schema, pool, pre=pre, post=post)








def render_condition(
    column: str, operator: str, value: Any, dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    """Render one condition. Pure: fields in, (sql, params) out.

    Every parameter name is derived from `path`, so the result depends only
    on the arguments — no counter, no process history. Two renderings of the
    same condition at the same path are byte-identical.
    """
    if value is None and operator == "IS":
        return f"{column} IS NULL", {}
    if value is None and operator == "IS NOT":
        return f"{column} IS NOT NULL", {}

    if operator in ("IN", "NOT IN"):
        if is_subquery(value):
            sub_sql, sub_params = render_subquery(value, dialect)
            return f"{column} {operator} ({sub_sql})", sub_params
        placeholders = ", ".join(f":_in_{path}_{i}" for i in range(len(value)))
        params = {f"_in_{path}_{i}": v for i, v in enumerate(value)}
        return f"{column} {operator} ({placeholders})", params

    if operator == "BETWEEN":
        low, high = f"_between_{path}_low", f"_between_{path}_high"
        return (
            f"{column} BETWEEN :{low} AND :{high}",
            {low: value[0], high: value[1]},
        )

    if operator == "ILIKE" and dialect != "postgresql":
        # SQLite has no ILIKE; LOWER() on both sides is the portable form.
        name = f"_like_{path}"
        return f"LOWER({column}) LIKE LOWER(:{name})", {name: value}

    # Column-to-column comparison, as in a JOIN ON clause. Binds nothing.
    if isinstance(value, ColumnProxy):
        return f"{column} {operator} {value._full_name}", {}

    # A string already written as :name is a caller-supplied placeholder.
    if isinstance(value, str) and value.startswith(":"):
        return f"{column} {operator} {value}", {}

    name = f"_p_{path}"
    return f"{column} {operator} :{name}", {name: value}








# Function factories
def count_(column: str | ColumnProxy = "*", alias: str | None = None) -> SQLFunction:
    """COUNT aggregate function."""
    return SQLFunction("COUNT", column, alias=alias)


def sum_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """SUM aggregate function."""
    return SQLFunction("SUM", column, alias=alias)


def avg_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """AVG aggregate function."""
    return SQLFunction("AVG", column, alias=alias)


def min_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """MIN aggregate function."""
    return SQLFunction("MIN", column, alias=alias)


def max_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """MAX aggregate function."""
    return SQLFunction("MAX", column, alias=alias)


def now_() -> SQLFunction:
    """Current timestamp function (dialect-aware)."""
    return SQLFunction("NOW")


def case_(*whens: tuple[Any, Any], else_: Any = None) -> CaseExpression:
    """
    Build a CASE WHEN ... THEN ... ELSE ... END expression.

    Each positional argument is a (condition, value) tuple.
    The optional else_ keyword sets the ELSE value.

    Example:
        priority = case_(
            (tickets.severity == "critical", 0),
            (tickets.severity == "high", 1),
            else_=2,
        ).as_("priority")

        # Use in SELECT + ORDER BY
        rows = await (
            tickets.select(tickets.id, priority)
            .order_by(priority.asc())
            .execute()
        )

        # Use inside an aggregate
        total = sum_(case_(
            (orders.status == "paid", orders.amount),
            else_=0,
        )).as_("paid_total")
    """
    return CaseExpression(*whens, else_=else_)


def subquery(query: Any) -> SubqueryExpr:
    """
    Wrap a SelectQuery for use in IN / NOT IN.

    Example:
        admin_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "admin")
        )
        rows = await (
            users.select(users.id, users.email)
            .where(users.id.in_(admin_ids))
            .execute()
        )
    """
    return SubqueryExpr(kind="subquery", query=query)
