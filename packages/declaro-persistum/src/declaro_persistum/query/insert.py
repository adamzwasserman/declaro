"""
INSERT query builder.

Provides an immutable, fluent API for building INSERT queries.
"""

from typing import Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import ColumnProxy, SQLFunction
from declaro_persistum.types import Schema


def _detect_dialect(connection: Any) -> str:
    """Detect database dialect from connection type."""
    conn_type = type(connection).__module__
    if "asyncpg" in conn_type:
        return "postgresql"
    elif "aiosqlite" in conn_type:
        return "sqlite"
    elif "libsql" in conn_type:
        return "turso"
    return "postgresql"


class InsertQuery:
    """Immutable INSERT query builder."""

    __slots__ = (
        "_table",
        "_schema",
        "_values",
        "_columns_def",
        "_returning",
        "_on_conflict",
        "_params",
    )

    def __init__(
        self,
        table: str,
        schema: Schema,
        values: dict[str, Any],
        columns_def: dict[str, ColumnProxy],
        returning: list[str] | None = None,
        on_conflict: str | None = None,
        params: dict[str, Any] | None = None,
    ):
        self._table = table
        self._schema = schema
        self._values = values
        self._columns_def = columns_def
        self._returning = returning
        self._on_conflict = on_conflict
        self._params = params or {}

        # Validate column names against schema
        for col_name in values:
            if col_name not in columns_def:
                available = ", ".join(sorted(columns_def.keys()))
                raise AttributeError(
                    f"Table '{table}' has no column '{col_name}'.\nAvailable columns: {available}"
                )

    def returning(self, *columns: ColumnProxy | str) -> "InsertQuery":
        """Add RETURNING clause (returns new query)."""
        ret_cols = []
        for col in columns:
            if isinstance(col, ColumnProxy):
                ret_cols.append(col._col_name)
            else:
                ret_cols.append(col)
        return InsertQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            returning=ret_cols,
            on_conflict=self._on_conflict,
            params=self._params,
        )

    def on_conflict(self, clause: str) -> "InsertQuery":
        """Add ON CONFLICT clause (returns new query)."""
        return InsertQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            returning=self._returning,
            on_conflict=clause,
            params=self._params,
        )

    def params(self, **kwargs: Any) -> "InsertQuery":
        """Add query parameters (returns new query)."""
        return InsertQuery(
            self._table,
            self._schema,
            self._values,
            self._columns_def,
            returning=self._returning,
            on_conflict=self._on_conflict,
            params={**self._params, **kwargs},
        )

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        columns = list(self._values.keys())
        cols_sql = ", ".join(columns)

        # Build VALUES clause
        all_params = dict(self._params)
        placeholders = []

        for col in columns:
            value = self._values[col]

            # Handle SQLFunction (like now_())
            if isinstance(value, SQLFunction):
                # Translate function for dialect
                placeholders.append(_translate_function(value, dialect))
            elif isinstance(value, str) and value.startswith(":"):
                # Parameter reference
                placeholders.append(value)
            else:
                # Direct value - add as param
                param_name = f"ins_{col}"
                placeholders.append(f":{param_name}")
                all_params[param_name] = value

        values_sql = ", ".join(placeholders)
        sql = f"INSERT INTO {self._table} ({cols_sql}) VALUES ({values_sql})"

        # ON CONFLICT
        if self._on_conflict:
            sql += f" ON CONFLICT {self._on_conflict}"

        # RETURNING
        if self._returning:
            ret_cols = ", ".join(self._returning)
            sql += f" RETURNING {ret_cols}"

        return sql, all_params

    def to_query(self, dialect: str = "postgresql") -> Query:
        """Convert to Query dict for executor."""
        sql, params = self.to_sql(dialect)
        return {"sql": sql, "params": params, "dialect": dialect}

    async def execute(self, connection: Any) -> list[dict[str, Any]]:
        """Execute insert and return results (if RETURNING specified)."""
        from declaro_persistum.query.executor import execute

        dialect = _detect_dialect(connection)
        return await execute(self.to_query(dialect), connection)

    async def execute_one(self, connection: Any) -> dict[str, Any] | None:
        """Execute insert and return single result (if RETURNING specified)."""
        from declaro_persistum.query.executor import execute_one

        dialect = _detect_dialect(connection)
        return await execute_one(self.to_query(dialect), connection)


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
            # SQLite/Turso UUID generation
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
