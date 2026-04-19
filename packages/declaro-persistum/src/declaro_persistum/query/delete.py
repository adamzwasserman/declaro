"""
DELETE query builder.

Provides an immutable, fluent API for building DELETE queries.
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
)
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook


class DeleteQuery:
    """Immutable DELETE query builder."""

    _OPERATION = "delete"

    __slots__ = (
        "_table",
        "_schema",
        "_where",
        "_returning",
        "_params",
        "_pool",
        "_pre",
        "_post",
    )

    def __init__(
        self,
        table: str,
        schema: Schema,
        where: Condition | ConditionGroup | None = None,
        returning: list[str] | None = None,
        params: dict[str, Any] | None = None,
        pool: Any = None,
        *,
        pre: "PreHook | None" = None,
        post: "PostHook | None" = None,
    ):
        self._table = table
        self._schema = schema
        self._where = where
        self._returning = returning
        self._params = params or {}
        self._pool = pool
        self._pre = pre
        self._post = post

    def where(self, condition: Condition | ConditionGroup) -> "DeleteQuery":
        """Add WHERE clause (returns new query)."""
        return DeleteQuery(
            self._table,
            self._schema,
            where=condition,
            returning=self._returning,
            params=self._params,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def returning(self, *columns: ColumnProxy | str) -> "DeleteQuery":
        """Add RETURNING clause (returns new query)."""
        ret_cols = []
        for col in columns:
            if isinstance(col, ColumnProxy):
                ret_cols.append(col._col_name)
            else:
                ret_cols.append(col)
        return DeleteQuery(
            self._table,
            self._schema,
            where=self._where,
            returning=ret_cols,
            params=self._params,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def params(self, **kwargs: Any) -> "DeleteQuery":
        """Add query parameters (returns new query)."""
        return DeleteQuery(
            self._table,
            self._schema,
            where=self._where,
            returning=self._returning,
            params={**self._params, **kwargs},
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        sql = f"DELETE FROM {self._table}"
        all_params = dict(self._params)

        # WHERE
        if self._where:
            where_sql, where_params = self._where.to_sql(dialect)
            sql += f" WHERE {where_sql}"
            all_params.update(where_params)

        # RETURNING
        if self._returning:
            ret_cols = ", ".join(self._returning)
            sql += f" RETURNING {ret_cols}"

        return sql, all_params

    def to_query(self, dialect: str = "postgresql") -> Query:
        """Convert to Query dict for executor."""
        sql, params = self.to_sql(dialect)
        return {"sql": sql, "params": params, "dialect": dialect}

    async def _run_raw(self, mode: str = "all") -> Any:
        """Internal seam — execute without hook logic. API may change."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode=mode)

    async def execute(
        self,
        *,
        pre: "PreHook | None" = None,
        post: "PostHook | None" = None,
        without_hooks: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute delete and return results (if RETURNING specified). Runs hooks if configured."""
        from declaro_persistum.query.hooks import _execute_with_hooks

        return await _execute_with_hooks(self, pre, post, without_hooks, "all")

    async def execute_one(
        self,
        *,
        pre: "PreHook | None" = None,
        post: "PostHook | None" = None,
        without_hooks: bool = False,
    ) -> dict[str, Any] | None:
        """Execute delete and return single result (if RETURNING specified). Runs hooks if configured."""
        from declaro_persistum.query.hooks import _execute_with_hooks

        return await _execute_with_hooks(self, pre, post, without_hooks, "one")
