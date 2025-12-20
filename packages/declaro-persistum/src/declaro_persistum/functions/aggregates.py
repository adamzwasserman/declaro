"""
Aggregate function wrappers for SQL generation.

Provides type-safe, dialect-aware aggregate functions.
"""

from .translations import translate_function


class SQLFunction:
    """Base class for SQL functions."""

    def __init__(self, alias: str | None = None):
        self._alias = alias

    @property
    def alias(self) -> str | None:
        """Get the alias for this function."""
        return self._alias

    def to_sql(self, dialect: str) -> str:
        """Generate SQL for this function."""
        raise NotImplementedError

    def _with_alias(self, sql: str) -> str:
        """Append AS alias if set."""
        if self._alias:
            return f"{sql} AS {self._alias}"
        return sql


class SumFunction(SQLFunction):
    """SUM aggregate function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"SUM({self._column})"
        return self._with_alias(sql)


class CountFunction(SQLFunction):
    """COUNT aggregate function."""

    def __init__(self, column: str = "*", alias: str | None = None, distinct: bool = False):
        super().__init__(alias)
        self._column = column
        self._distinct = distinct

    def to_sql(self, _dialect: str) -> str:
        sql = f"COUNT(DISTINCT {self._column})" if self._distinct else f"COUNT({self._column})"
        return self._with_alias(sql)


class AvgFunction(SQLFunction):
    """AVG aggregate function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"AVG({self._column})"
        return self._with_alias(sql)


class MinFunction(SQLFunction):
    """MIN aggregate function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"MIN({self._column})"
        return self._with_alias(sql)


class MaxFunction(SQLFunction):
    """MAX aggregate function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"MAX({self._column})"
        return self._with_alias(sql)


class StringAggFunction(SQLFunction):
    """STRING_AGG / GROUP_CONCAT aggregate function."""

    def __init__(
        self,
        column: str,
        separator: str,
        alias: str | None = None,
        order_by: str | None = None,
    ):
        super().__init__(alias)
        self._column = column
        self._separator = separator
        self._order_by = order_by

    def to_sql(self, dialect: str) -> str:
        sql = translate_function(
            "string_agg",
            dialect,
            column=self._column,
            separator=self._separator,
        )

        if self._order_by and dialect == "postgresql":
            # PostgreSQL supports ORDER BY within STRING_AGG
            sql = sql.rstrip(")")
            sql += f" ORDER BY {self._order_by})"

        return self._with_alias(sql)


class ArrayAggFunction(SQLFunction):
    """ARRAY_AGG / JSON_GROUP_ARRAY aggregate function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("array_agg", dialect, column=self._column)
        return self._with_alias(sql)


# Factory functions for cleaner API


def sum_(column: str, alias: str | None = None) -> SumFunction:
    """
    Create SUM aggregate.

    Args:
        column: Column to sum
        alias: Optional alias for the result

    Returns:
        SumFunction instance.
    """
    return SumFunction(column, alias)


def count_(column: str = "*", alias: str | None = None, distinct: bool = False) -> CountFunction:
    """
    Create COUNT aggregate.

    Args:
        column: Column to count (default: "*")
        alias: Optional alias for the result
        distinct: Whether to count distinct values

    Returns:
        CountFunction instance.
    """
    return CountFunction(column, alias, distinct)


def avg_(column: str, alias: str | None = None) -> AvgFunction:
    """
    Create AVG aggregate.

    Args:
        column: Column to average
        alias: Optional alias for the result

    Returns:
        AvgFunction instance.
    """
    return AvgFunction(column, alias)


def min_(column: str, alias: str | None = None) -> MinFunction:
    """
    Create MIN aggregate.

    Args:
        column: Column to find minimum
        alias: Optional alias for the result

    Returns:
        MinFunction instance.
    """
    return MinFunction(column, alias)


def max_(column: str, alias: str | None = None) -> MaxFunction:
    """
    Create MAX aggregate.

    Args:
        column: Column to find maximum
        alias: Optional alias for the result

    Returns:
        MaxFunction instance.
    """
    return MaxFunction(column, alias)


def string_agg_(
    column: str,
    separator: str,
    alias: str | None = None,
    order_by: str | None = None,
) -> StringAggFunction:
    """
    Create STRING_AGG / GROUP_CONCAT aggregate.

    Args:
        column: Column to concatenate
        separator: Separator between values
        alias: Optional alias for the result
        order_by: Optional ORDER BY clause (PostgreSQL only)

    Returns:
        StringAggFunction instance.
    """
    return StringAggFunction(column, separator, alias, order_by)


def array_agg_(column: str, alias: str | None = None) -> ArrayAggFunction:
    """
    Create ARRAY_AGG / JSON_GROUP_ARRAY aggregate.

    Args:
        column: Column to aggregate into array
        alias: Optional alias for the result

    Returns:
        ArrayAggFunction instance.
    """
    return ArrayAggFunction(column, alias)
