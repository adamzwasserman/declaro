"""
INSERT query builder.

Provides an immutable, fluent API for building INSERT queries.
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import ColumnProxy, SQLFunction
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook




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
