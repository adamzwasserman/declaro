"""
Range abstraction using start/end columns with CHECK constraints.

This provides a portable way to store ranges (time periods, numeric ranges)
that works across PostgreSQL and SQLite without relying on native range types.

NULL values represent unbounded endpoints:
- NULL start = unbounded lower bound (-infinity)
- NULL end = unbounded upper bound (+infinity)
"""

import re
from typing import Any


def parse_range_type(type_str: str) -> str | None:
    """
    Parse range<element_type> type declaration.

    Returns the element type if it's a range type, None otherwise.

    Examples:
        parse_range_type("range<timestamptz>") -> "timestamptz"
        parse_range_type("range<date>") -> "date"
        parse_range_type("range<integer>") -> "integer"
        parse_range_type("timestamptz") -> None
    """
    match = re.match(r"^range<(.+)>$", type_str.strip())
    if match:
        return match.group(1).strip()
    return None


def generate_range_columns(
    column_name: str,
    element_type: str,
) -> dict[str, Any]:
    """
    Generate start/end column definitions for a range.

    Args:
        column_name: Name of the range column (e.g., "valid_period")
        element_type: Type of range bounds (e.g., "timestamptz", "date", "integer")

    Returns:
        Dict with column definitions for {column_name}_start and {column_name}_end.
    """
    return {
        f"{column_name}_start": {
            "type": element_type,
            "nullable": True,  # NULL = unbounded
        },
        f"{column_name}_end": {
            "type": element_type,
            "nullable": True,  # NULL = unbounded
            "check": f"({column_name}_start IS NULL OR {column_name}_end IS NULL OR {column_name}_start <= {column_name}_end)",
        },
    }


def range_overlaps_sql(column_name: str) -> str:
    """
    Generate SQL for range overlap check.

    Two ranges overlap if:
    - Both have some intersection
    - Handles NULL bounds as unbounded

    Args:
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        SQL WHERE clause fragment for overlap check.
    """
    start = f"{column_name}_start"
    end = f"{column_name}_end"

    # Overlap: (a_start <= b_end OR a_start IS NULL OR b_end IS NULL)
    #      AND (a_end >= b_start OR a_end IS NULL OR b_start IS NULL)
    return f"""(
    ({start} IS NULL OR :range_end IS NULL OR {start} <= :range_end)
    AND ({end} IS NULL OR :range_start IS NULL OR {end} >= :range_start)
)"""


def range_contains_point_sql(column_name: str) -> str:
    """
    Generate SQL for point containment check.

    A point is within a range if:
    - start <= point (or start is NULL)
    - end >= point (or end is NULL)

    Args:
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        SQL WHERE clause fragment for containment check.
    """
    start = f"{column_name}_start"
    end = f"{column_name}_end"

    return f"""(
    ({start} IS NULL OR {start} <= :point)
    AND ({end} IS NULL OR {end} >= :point)
)"""


def range_contains_range_sql(column_name: str) -> str:
    """
    Generate SQL for range containment check.

    Outer range contains inner range if:
    - outer_start <= inner_start (or outer_start is NULL)
    - outer_end >= inner_end (or outer_end is NULL)

    Args:
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        SQL WHERE clause fragment for containment check.
    """
    start = f"{column_name}_start"
    end = f"{column_name}_end"

    return f"""(
    ({start} IS NULL OR :inner_start IS NOT NULL AND {start} <= :inner_start)
    AND ({end} IS NULL OR :inner_end IS NOT NULL AND {end} >= :inner_end)
)"""


def range_adjacent_sql(column_name: str) -> str:
    """
    Generate SQL for adjacency check.

    Two ranges are adjacent if:
    - a_end = b_start (left adjacent)
    - OR a_start = b_end (right adjacent)

    Args:
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        SQL WHERE clause fragment for adjacency check.
    """
    start = f"{column_name}_start"
    end = f"{column_name}_end"

    return f"""(
    ({end} IS NOT NULL AND :other_start IS NOT NULL AND {end} = :other_start)
    OR ({start} IS NOT NULL AND :other_end IS NOT NULL AND {start} = :other_end)
)"""


def range_to_dict(
    start: Any,
    end: Any,
    column_name: str,
) -> dict[str, Any]:
    """
    Convert range bounds to dict for database insertion.

    Args:
        start: Start bound value (or None for unbounded)
        end: End bound value (or None for unbounded)
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        Dict with {column_name}_start and {column_name}_end keys.
    """
    return {
        f"{column_name}_start": start,
        f"{column_name}_end": end,
    }


def range_from_dict(
    data: dict[str, Any],
    column_name: str,
) -> tuple[Any, Any]:
    """
    Extract range bounds from dict.

    Args:
        data: Dict containing range columns
        column_name: Name of the range column (without _start/_end suffix)

    Returns:
        Tuple of (start, end) values.
    """
    return (
        data.get(f"{column_name}_start"),
        data.get(f"{column_name}_end"),
    )
