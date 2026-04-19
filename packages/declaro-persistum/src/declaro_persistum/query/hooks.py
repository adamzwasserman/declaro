"""
Query hooks — inject pre/post transformation functions into the execute flow.

Hooks are passed as function arguments (via table_factory or .execute() kwargs),
not registered via decorators. This keeps behavior explicit at the call site
and avoids module-level state.

    pre_hook:  (query_object) -> query_object      # runs before SQL is built
    post_hook: (rows, QueryMeta) -> rows           # runs after DB returns

See docs/hooks.md for usage examples (RLS, audit logging, soft delete).
"""

from typing import TYPE_CHECKING, Any, Callable, Literal, TypedDict

from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.delete import DeleteQuery
    from declaro_persistum.query.insert import InsertQuery
    from declaro_persistum.query.select import SelectQuery
    from declaro_persistum.query.table import TableProxy
    from declaro_persistum.query.update import UpdateQuery

    Query = "SelectQuery | InsertQuery | UpdateQuery | DeleteQuery"
else:
    Query = Any


class QueryMeta(TypedDict):
    """Metadata passed to post-hook. Reflects the SQL actually executed (post-pre-hook)."""

    operation: Literal["select", "insert", "update", "delete"]
    table: str
    sql: str
    params: dict[str, Any]
    dialect: str


PreHook = Callable[[Any], Any]
PostHook = Callable[[list[dict[str, Any]], QueryMeta], list[dict[str, Any]]]


def table_factory(
    schema: Schema,
    pool: Any,
    *,
    pre: PreHook | None = None,
    post: PostHook | None = None,
) -> Callable[[str], "TableProxy"]:
    """
    Return a closure that builds TableProxy instances with hooks pre-wired.

    Usage:
        get_table = table_factory(schema, pool, pre=apply_rls, post=log_audit)
        items = get_table("items")
        await items.select(...).where(...).execute()   # hooks applied
    """
    from declaro_persistum.query.table import table

    def make_table(name: str) -> "TableProxy":
        return table(name, schema, pool, pre=pre, post=post)

    return make_table


async def _execute_with_hooks(
    query: Any,
    pre: PreHook | None,
    post: PostHook | None,
    without_hooks: bool,
    mode: str,
) -> Any:
    """
    Shared hook-aware execution. Pure-function style — query passed in explicitly.

    Flow:
        1. If without_hooks=True → call query._run_raw(mode) and return.
        2. Resolve effective pre/post: execute-level kwargs replace factory-level.
        3. Run pre-hook; validate non-None; use returned query object (may be a
           different query type, e.g. DELETE → UPDATE).
        4. If no post-hook: call q._run_raw(mode) with native mode (efficient).
        5. If post-hook: always fetch full list[dict], apply post-hook, then
           reduce to mode ("one" → first or None; "scalar" → first-col or None).

    Post-hook always sees list[dict] regardless of caller's execute mode —
    mode reduction happens AFTER the post-hook runs.
    """
    if without_hooks:
        return await query._run_raw(mode=mode)

    effective_pre = pre if pre is not None else getattr(query, "_pre", None)
    effective_post = post if post is not None else getattr(query, "_post", None)

    q = effective_pre(query) if effective_pre else query
    if q is None:
        raise TypeError(
            "pre-hook returned None; must return a query object "
            "(SelectQuery, InsertQuery, UpdateQuery, or DeleteQuery)"
        )

    if effective_post is None:
        return await q._run_raw(mode=mode)

    rows = await q._run_raw(mode="all")

    dialect = getattr(q._pool, "dialect", None) or "postgresql"
    sql, params = q.to_sql(dialect)
    meta: QueryMeta = {
        "operation": q._OPERATION,
        "table": q._table,
        "sql": sql,
        "params": params,
        "dialect": dialect,
    }
    transformed = effective_post(rows, meta)

    if mode == "all":
        return transformed
    if mode == "one":
        return transformed[0] if transformed else None
    if mode == "scalar":
        if not transformed:
            return None
        first = transformed[0]
        return next(iter(first.values())) if first else None
    raise ValueError(f"Unknown execute mode: {mode}")
