"""
SELECT query builder.

Provides an immutable, fluent API for building SELECT queries.
"""

from typing import TYPE_CHECKING, Any, Literal

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
    JoinClause,
    OrderBy,
    SQLFunction,
)
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.table import TableProxy


class SelectQuery:
    """Immutable SELECT query builder."""

    __slots__ = (
        "_table",
        "_schema",
        "_columns",
        "_where",
        "_order_by",
        "_limit_val",
        "_offset_val",
        "_joins",
        "_group_by",
        "_having",
        "_params",
        "_pool",
    )

    def __init__(
        self,
        table: str,
        schema: Schema,
        columns: tuple["ColumnProxy | SQLFunction", ...],
        where: Condition | ConditionGroup | None = None,
        order_by: list[OrderBy] | None = None,
        limit_val: int | None = None,
        offset_val: int | None = None,
        joins: list[JoinClause] | None = None,
        group_by: list[ColumnProxy] | None = None,
        having: Condition | ConditionGroup | None = None,
        params: dict[str, Any] | None = None,
        pool: Any = None,
    ):
        self._table = table
        self._schema = schema
        self._columns = columns
        self._where = where
        self._order_by = order_by or []
        self._limit_val = limit_val
        self._offset_val = offset_val
        self._joins = joins or []
        self._group_by = group_by or []
        self._having = having
        self._params = params or {}
        self._pool = pool

    def where(self, condition: Condition | ConditionGroup) -> "SelectQuery":
        """Add WHERE clause (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=condition,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def order_by(self, *orders: OrderBy | ColumnProxy) -> "SelectQuery":
        """Add ORDER BY clause (returns new query)."""
        # Convert ColumnProxy to default ASC ordering
        normalized: list[OrderBy] = []
        for order in orders:
            if isinstance(order, ColumnProxy):
                normalized.append(OrderBy(order._full_name, "ASC"))
            else:
                normalized.append(order)

        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=normalized,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def limit(self, n: int) -> "SelectQuery":
        """Add LIMIT clause (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=n,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def offset(self, n: int) -> "SelectQuery":
        """Add OFFSET clause (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=n,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def join(
        self,
        other: "TableProxy",
        on: Condition,
        type: Literal["inner", "left", "right", "full"] = "inner",
    ) -> "SelectQuery":
        """Add JOIN clause (returns new query)."""
        new_joins = list(self._joins) + [JoinClause(other._table_name, on, type)]
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=new_joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def group_by(self, *columns: ColumnProxy) -> "SelectQuery":
        """Add GROUP BY clause (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=list(columns),
            having=self._having,
            params=self._params,
            pool=self._pool,
        )

    def having(self, condition: Condition | ConditionGroup) -> "SelectQuery":
        """Add HAVING clause (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=self._group_by,
            having=condition,
            params=self._params,
            pool=self._pool,
        )

    def params(self, **kwargs: Any) -> "SelectQuery":
        """Add query parameters (returns new query)."""
        return SelectQuery(
            self._table,
            self._schema,
            self._columns,
            where=self._where,
            order_by=self._order_by,
            limit_val=self._limit_val,
            offset_val=self._offset_val,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params={**self._params, **kwargs},
            pool=self._pool,
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        # SELECT clause
        cols = ", ".join(c._full_name for c in self._columns) if self._columns else "*"
        sql = f"SELECT {cols} FROM {self._table}"
        params = dict(self._params)

        # JOINs
        for join in self._joins:
            join_sql, join_params = join.on.to_sql(dialect)
            join_type = join.type.upper()
            sql += f" {join_type} JOIN {join.table} ON {join_sql}"
            params.update(join_params)

        # WHERE
        if self._where:
            where_sql, where_params = self._where.to_sql(dialect)
            sql += f" WHERE {where_sql}"
            params.update(where_params)

        # GROUP BY
        if self._group_by:
            group_cols = ", ".join(c._full_name for c in self._group_by)
            sql += f" GROUP BY {group_cols}"

        # HAVING
        if self._having:
            having_sql, having_params = self._having.to_sql(dialect)
            sql += f" HAVING {having_sql}"
            params.update(having_params)

        # ORDER BY
        if self._order_by:
            orders = ", ".join(o.to_sql() for o in self._order_by)
            sql += f" ORDER BY {orders}"

        # LIMIT/OFFSET
        if self._limit_val is not None:
            sql += f" LIMIT {self._limit_val}"
        if self._offset_val is not None:
            sql += f" OFFSET {self._offset_val}"

        return sql, params

    def to_query(self, dialect: str = "postgresql") -> Query:
        """Convert to Query dict for executor."""
        sql, params = self.to_sql(dialect)
        return {"sql": sql, "params": params, "dialect": dialect}

    async def execute(self) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode="all")

    async def execute_one(self) -> dict[str, Any] | None:
        """Execute query and return single row or None."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode="one")

    async def execute_scalar(self) -> Any:
        """Execute query and return scalar value."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode="scalar")
