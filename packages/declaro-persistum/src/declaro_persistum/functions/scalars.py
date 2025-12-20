"""
Scalar function wrappers for SQL generation.

Provides type-safe, dialect-aware scalar functions.
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


class LowerFunction(SQLFunction):
    """LOWER function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"LOWER({self._column})"
        return self._with_alias(sql)


class UpperFunction(SQLFunction):
    """UPPER function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"UPPER({self._column})"
        return self._with_alias(sql)


class CoalesceFunction(SQLFunction):
    """COALESCE function."""

    def __init__(self, *columns: str, alias: str | None = None):
        super().__init__(alias)
        self._columns = columns

    def to_sql(self, _dialect: str) -> str:
        cols = ", ".join(self._columns)
        sql = f"COALESCE({cols})"
        return self._with_alias(sql)


class LengthFunction(SQLFunction):
    """LENGTH function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"LENGTH({self._column})"
        return self._with_alias(sql)


class TrimFunction(SQLFunction):
    """TRIM function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"TRIM({self._column})"
        return self._with_alias(sql)


class LTrimFunction(SQLFunction):
    """LTRIM function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"LTRIM({self._column})"
        return self._with_alias(sql)


class RTrimFunction(SQLFunction):
    """RTRIM function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"RTRIM({self._column})"
        return self._with_alias(sql)


class NowFunction(SQLFunction):
    """NOW / datetime('now') function."""

    def __init__(self, alias: str | None = None):
        super().__init__(alias)

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("now", dialect)
        return self._with_alias(sql)


class GenRandomUuidFunction(SQLFunction):
    """gen_random_uuid() function."""

    def __init__(self, alias: str | None = None):
        super().__init__(alias)

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("gen_random_uuid", dialect)
        return self._with_alias(sql)


class ExtractYearFunction(SQLFunction):
    """EXTRACT(YEAR) function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("extract_year", dialect, column=self._column)
        return self._with_alias(sql)


class ExtractMonthFunction(SQLFunction):
    """EXTRACT(MONTH) function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("extract_month", dialect, column=self._column)
        return self._with_alias(sql)


class ExtractDayFunction(SQLFunction):
    """EXTRACT(DAY) function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("extract_day", dialect, column=self._column)
        return self._with_alias(sql)


class DateAddDaysFunction(SQLFunction):
    """Date addition function."""

    def __init__(self, column: str, days: int, alias: str | None = None):
        super().__init__(alias)
        self._column = column
        self._days = days

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("date_add", dialect, column=self._column, days=self._days)
        return self._with_alias(sql)


class CastFunction(SQLFunction):
    """CAST function."""

    def __init__(self, column: str, target_type: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column
        self._target_type = target_type.upper()

    def to_sql(self, _dialect: str) -> str:
        sql = f"CAST({self._column} AS {self._target_type})"
        return self._with_alias(sql)


class ConcatFunction(SQLFunction):
    """CONCAT / || function."""

    def __init__(self, *args: str, alias: str | None = None):
        super().__init__(alias)
        self._args = args

    def to_sql(self, dialect: str) -> str:
        sql = translate_function("concat", dialect, args=list(self._args))
        return self._with_alias(sql)


class SubstringFunction(SQLFunction):
    """SUBSTRING / SUBSTR function."""

    def __init__(self, column: str, start: int, length: int, alias: str | None = None):
        super().__init__(alias)
        self._column = column
        self._start = start
        self._length = length

    def to_sql(self, dialect: str) -> str:
        if dialect == "sqlite":
            sql = f"SUBSTR({self._column}, {self._start}, {self._length})"
        else:
            sql = f"SUBSTRING({self._column} FROM {self._start} FOR {self._length})"
        return self._with_alias(sql)


class NullIfFunction(SQLFunction):
    """NULLIF function."""

    def __init__(self, column: str, value: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column
        self._value = value

    def to_sql(self, _dialect: str) -> str:
        sql = f"NULLIF({self._column}, {self._value})"
        return self._with_alias(sql)


class AbsFunction(SQLFunction):
    """ABS function."""

    def __init__(self, column: str, alias: str | None = None):
        super().__init__(alias)
        self._column = column

    def to_sql(self, _dialect: str) -> str:
        sql = f"ABS({self._column})"
        return self._with_alias(sql)


class RoundFunction(SQLFunction):
    """ROUND function."""

    def __init__(self, column: str, decimals: int | None = None, alias: str | None = None):
        super().__init__(alias)
        self._column = column
        self._decimals = decimals

    def to_sql(self, _dialect: str) -> str:
        if self._decimals is not None:
            sql = f"ROUND({self._column}, {self._decimals})"
        else:
            sql = f"ROUND({self._column})"
        return self._with_alias(sql)


# Factory functions for cleaner API


def lower_(column: str, alias: str | None = None) -> LowerFunction:
    """Create LOWER function."""
    return LowerFunction(column, alias)


def upper_(column: str, alias: str | None = None) -> UpperFunction:
    """Create UPPER function."""
    return UpperFunction(column, alias)


def coalesce_(*columns: str, alias: str | None = None) -> CoalesceFunction:
    """Create COALESCE function."""
    return CoalesceFunction(*columns, alias=alias)


def length_(column: str, alias: str | None = None) -> LengthFunction:
    """Create LENGTH function."""
    return LengthFunction(column, alias)


def trim_(column: str, alias: str | None = None) -> TrimFunction:
    """Create TRIM function."""
    return TrimFunction(column, alias)


def ltrim_(column: str, alias: str | None = None) -> LTrimFunction:
    """Create LTRIM function."""
    return LTrimFunction(column, alias)


def rtrim_(column: str, alias: str | None = None) -> RTrimFunction:
    """Create RTRIM function."""
    return RTrimFunction(column, alias)


def now_(alias: str | None = None) -> NowFunction:
    """Create NOW function."""
    return NowFunction(alias)


def gen_random_uuid_(alias: str | None = None) -> GenRandomUuidFunction:
    """Create gen_random_uuid function."""
    return GenRandomUuidFunction(alias)


def extract_year_(column: str, alias: str | None = None) -> ExtractYearFunction:
    """Create EXTRACT(YEAR) function."""
    return ExtractYearFunction(column, alias)


def extract_month_(column: str, alias: str | None = None) -> ExtractMonthFunction:
    """Create EXTRACT(MONTH) function."""
    return ExtractMonthFunction(column, alias)


def extract_day_(column: str, alias: str | None = None) -> ExtractDayFunction:
    """Create EXTRACT(DAY) function."""
    return ExtractDayFunction(column, alias)


def date_add_days_(column: str, days: int, alias: str | None = None) -> DateAddDaysFunction:
    """Create date addition function."""
    return DateAddDaysFunction(column, days, alias)


def cast_(column: str, target_type: str, alias: str | None = None) -> CastFunction:
    """Create CAST function."""
    return CastFunction(column, target_type, alias)


def concat_(*args: str, alias: str | None = None) -> ConcatFunction:
    """Create CONCAT function."""
    return ConcatFunction(*args, alias=alias)


def substring_(column: str, start: int, length: int, alias: str | None = None) -> SubstringFunction:
    """Create SUBSTRING function."""
    return SubstringFunction(column, start, length, alias)


def nullif_(column: str, value: str, alias: str | None = None) -> NullIfFunction:
    """Create NULLIF function."""
    return NullIfFunction(column, value, alias)


def abs_(column: str, alias: str | None = None) -> AbsFunction:
    """Create ABS function."""
    return AbsFunction(column, alias)


def round_(column: str, decimals: int | None = None, alias: str | None = None) -> RoundFunction:
    """Create ROUND function."""
    return RoundFunction(column, decimals, alias)
