"""
UPDATE query builder.

Provides an immutable, fluent API for building UPDATE queries.
"""

from typing import Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
    SQLFunction,
)
from declaro_persistum.types import Schema


def _translate_function(func: SQLFunction, dialect: str) -> str:
    """Translate SQL function to dialect-specific SQL."""
    name = func.name.upper()

    if name == "NOW":
        if dialect == "postgresql":
            return "now()"
        else:
            return "datetime('now')"

    if name == "GEN_RANDOM_UUID":
        if dialect == "postgresql":
            return "gen_random_uuid()"
        else:
            return (
                "lower(hex(randomblob(4))) || '-' || "
                "lower(hex(randomblob(2))) || '-' || "
                "'4' || substr(lower(hex(randomblob(2))), 2) || '-' || "
                "substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' || "
                "lower(hex(randomblob(6)))"
            )

    # Default: return as-is
    args_str = ", ".join(
        str(a._full_name) if isinstance(a, ColumnProxy) else str(a) for a in func.args
    )
    return f"{func.name}({args_str})"


class UpdateQuery:
    """Immutable UPDATE query builder."""

    __slots__ = ("_table", "_schema", "_values", "_columns_def", "_where", "_returning", "_params", "_pool")

    def __init__(
        self,
        table: str,
        schema: Schema,
        values: dict[str, Any],
        columns_def: dict[str, ColumnProxy],
        where: Condition | ConditionGroup | None = None,
        returning: list[str] | None = None,
        params: dict[str, Any] | None = None,
        pool: Any = None,
    ):
        self._table = table
        self._schema = schema
        self._values = values
        self._columns_def = columns_def
        self._where = where
        self._returning = returning
        self._params = params or {}
        self._pool = pool

        # Validate column names against schema
        for col_name in values:
            if col_name not in columns_def:
                available = ", ".join(sorted(columns_def.keys()))
                raise AttributeError(
                    f"Table '{table}' has no column '{col_name}'.\nAvailable columns: {available}"
                )

    def where(self, condition: Condition | ConditionGroup) -> "UpdateQuery":
        """Add WHERE clause (returns new query)."""
        return UpdateQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            where=condition,
            returning=self._returning,
            params=self._params,
            pool=self._pool,
        )

    def returning(self, *columns: ColumnProxy | str) -> "UpdateQuery":
        """Add RETURNING clause (returns new query)."""
        ret_cols = []
        for col in columns:
            if isinstance(col, ColumnProxy):
                ret_cols.append(col._col_name)
            else:
                ret_cols.append(col)
        return UpdateQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            where=self._where,
            returning=ret_cols,
            params=self._params,
            pool=self._pool,
        )

    def params(self, **kwargs: Any) -> "UpdateQuery":
        """Add query parameters (returns new query)."""
        return UpdateQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            where=self._where,
            returning=self._returning,
            params={**self._params, **kwargs},
            pool=self._pool,
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        if not self._values:
            raise ValueError("UPDATE requires at least one column to set")

        # Build SET clause
        set_parts = []
        all_params = dict(self._params)

        for col, value in self._values.items():
            if isinstance(value, SQLFunction):
                set_parts.append(f"{col} = {_translate_function(value, dialect)}")
            elif isinstance(value, str) and value.startswith(":"):
                set_parts.append(f"{col} = {value}")
            else:
                param_name = f"upd_{col}"
                set_parts.append(f"{col} = :{param_name}")
                all_params[param_name] = value

        set_sql = ", ".join(set_parts)
        sql = f"UPDATE {self._table} SET {set_sql}"

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

    async def execute(self) -> list[dict[str, Any]]:
        """Execute update and return results (if RETURNING specified)."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode="all")

    async def execute_one(self) -> dict[str, Any] | None:
        """Execute update and return single result (if RETURNING specified)."""
        from declaro_persistum.query.executor import execute_with_pool

        return await execute_with_pool(self._pool, self.to_query, mode="one")
