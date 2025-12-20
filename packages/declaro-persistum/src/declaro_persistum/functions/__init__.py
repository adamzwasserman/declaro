"""
SQL function wrappers with dialect-aware translation.

This module provides type-safe, portable SQL functions that translate
to the appropriate syntax for PostgreSQL and SQLite.
"""

from .aggregates import (
    SQLFunction,
    array_agg_,
    avg_,
    count_,
    max_,
    min_,
    string_agg_,
    sum_,
)
from .scalars import (
    abs_,
    cast_,
    coalesce_,
    concat_,
    date_add_days_,
    extract_day_,
    extract_month_,
    extract_year_,
    gen_random_uuid_,
    length_,
    lower_,
    ltrim_,
    now_,
    nullif_,
    round_,
    rtrim_,
    substring_,
    trim_,
    upper_,
)
from .translations import (
    FUNCTION_TRANSLATIONS,
    translate_function,
)

__all__ = [
    # Aggregates
    "sum_",
    "count_",
    "avg_",
    "min_",
    "max_",
    "string_agg_",
    "array_agg_",
    "SQLFunction",
    # Scalars
    "lower_",
    "upper_",
    "coalesce_",
    "length_",
    "trim_",
    "ltrim_",
    "rtrim_",
    "now_",
    "gen_random_uuid_",
    "extract_year_",
    "extract_month_",
    "extract_day_",
    "date_add_days_",
    "cast_",
    "concat_",
    "substring_",
    "nullif_",
    "abs_",
    "round_",
    # Translations
    "FUNCTION_TRANSLATIONS",
    "translate_function",
]
