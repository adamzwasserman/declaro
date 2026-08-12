"""
Prisma-style query API.

Provides a dict-based interface familiar to Prisma users:
    users = await db.users.find_many(
        where={"status": "active"},
        order={"created_at": "desc"},
        take=10
    )
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import ColumnProxy, Condition, ConditionGroup, OrderBy
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook


def _find_pk_column(schema: Schema, table_name: str) -> str:
    """Return the primary key column name for a table, or '' if not found."""
    table_def = schema.get(table_name, {})
    for col_name, col_def in table_def.get("columns", {}).items():
        if col_def.get("primary_key"):
            return col_name
    return ""


def compose_update_values(
    data: dict[str, Any] | None,
    increment: dict[str, int | float] | None,
) -> dict[str, Any]:
    """Combine literal ``data`` with ``increment`` markers into the values dict
    used by ``UpdateQuery``.

    Pure function — independent of any table proxy / pool / schema. Lives at
    module scope so it is callable and testable without a PrismaQueryBuilder
    instance.

    Raises ValueError if both inputs are empty (no-op update) or if any column
    appears in both inputs (ambiguous intent — set or increment, not both).
    """
    from declaro_persistum.query.update import Increment

    if not data and not increment:
        raise ValueError(
            "update / update_many requires data= or increment= "
            "(or both); both were empty/None"
        )

    values: dict[str, Any] = dict(data) if data else {}
    if increment:
        for col, delta in increment.items():
            if col in values:
                raise ValueError(
                    f"Column '{col}' appears in both data and increment — "
                    "use one or the other for a given column"
                )
            values[col] = Increment(delta)
    return values


