"""
Dialect-aware SQL function translation.

Maps function names to their dialect-specific SQL implementations.
"""

from collections.abc import Callable
from typing import Any

# SQLite UUID generation expression
# Creates a UUID v4 using randomblob
SQLITE_UUID_EXPR = (
    "lower(hex(randomblob(4))) || '-' || "
    "lower(hex(randomblob(2))) || '-' || "
    "'4' || substr(lower(hex(randomblob(2))), 2) || '-' || "
    "substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' || "
    "lower(hex(randomblob(6)))"
)


# Function translations: function_name -> dialect -> SQL template or callable
FUNCTION_TRANSLATIONS: dict[str, dict[str, str | Callable[..., str]]] = {
    # Date/Time functions
    "now": {
        "postgresql": "NOW()",
        "sqlite": "datetime('now')",
    },
    "current_date": {
        "postgresql": "CURRENT_DATE",
        "sqlite": "date('now')",
    },
    "current_timestamp": {
        "postgresql": "CURRENT_TIMESTAMP",
        "sqlite": "datetime('now')",
    },
    "gen_random_uuid": {
        "postgresql": "gen_random_uuid()",
        "sqlite": SQLITE_UUID_EXPR,
    },
    # String aggregation
    "string_agg": {
        "postgresql": lambda column, separator, **_: f"STRING_AGG({column}, '{separator}')",
        "sqlite": lambda column, separator, **_: f"GROUP_CONCAT({column}, '{separator}')",
    },
    # Array aggregation
    "array_agg": {
        "postgresql": lambda column, **_: f"ARRAY_AGG({column})",
        "sqlite": lambda column, **_: f"JSON_GROUP_ARRAY({column})",
    },
    # Date extraction
    "extract_year": {
        "postgresql": lambda column, **_: f"EXTRACT(YEAR FROM {column})",
        "sqlite": lambda column, **_: f"CAST(strftime('%Y', {column}) AS INTEGER)",
    },
    "extract_month": {
        "postgresql": lambda column, **_: f"EXTRACT(MONTH FROM {column})",
        "sqlite": lambda column, **_: f"CAST(strftime('%m', {column}) AS INTEGER)",
    },
    "extract_day": {
        "postgresql": lambda column, **_: f"EXTRACT(DAY FROM {column})",
        "sqlite": lambda column, **_: f"CAST(strftime('%d', {column}) AS INTEGER)",
    },
    "extract_hour": {
        "postgresql": lambda column, **_: f"EXTRACT(HOUR FROM {column})",
        "sqlite": lambda column, **_: f"CAST(strftime('%H', {column}) AS INTEGER)",
    },
    "extract_minute": {
        "postgresql": lambda column, **_: f"EXTRACT(MINUTE FROM {column})",
        "sqlite": lambda column, **_: f"CAST(strftime('%M', {column}) AS INTEGER)",
    },
    # Date arithmetic
    "date_add": {
        "postgresql": lambda column, days, **_: f"({column} + INTERVAL '{days} days')",
        "sqlite": lambda column, days, **_: f"datetime({column}, '+{days} days')",
    },
    # String concatenation
    "concat": {
        "postgresql": lambda args, **_: f"CONCAT({', '.join(args)})",
        "sqlite": lambda args, **_: " || ".join(args),
    },
    # Case-insensitive LIKE
    "ilike": {
        "postgresql": lambda column, pattern, **_: f"{column} ILIKE {pattern}",
        "sqlite": lambda column, pattern, **_: f"LOWER({column}) LIKE LOWER({pattern})",
    },
    # JSON extraction
    "json_extract": {
        "postgresql": lambda column, path, **_: f"{column}->>{path[2:]!r}"
        if path.startswith("$.")
        else f"{column}->>'{path}'",
        "sqlite": lambda column, path, **_: f"json_extract({column}, '{path}')",
    },
    # Boolean literals
    "bool_true": {
        "postgresql": "TRUE",
        "sqlite": "1",
    },
    "bool_false": {
        "postgresql": "FALSE",
        "sqlite": "0",
    },
}


def translate_function(name: str, dialect: str, **kwargs: Any) -> str:
    """
    Translate a function to dialect-specific SQL.

    Args:
        name: Function name (e.g., "now", "gen_random_uuid")
        dialect: Database dialect ("postgresql" or "sqlite")
        **kwargs: Function-specific parameters

    Returns:
        SQL string for the function.

    Raises:
        KeyError: If function or dialect not found.
    """
    if name not in FUNCTION_TRANSLATIONS:
        raise KeyError(f"Unknown function: {name}")

    func_dialects = FUNCTION_TRANSLATIONS[name]
    if dialect not in func_dialects:
        raise KeyError(f"Unknown dialect '{dialect}' for function '{name}'")

    translation = func_dialects[dialect]

    if callable(translation):
        return translation(**kwargs)
    return translation
