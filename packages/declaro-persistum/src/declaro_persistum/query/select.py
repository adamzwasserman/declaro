"""
SELECT query builder.

Provides an immutable, fluent API for building SELECT queries.
"""

from typing import TYPE_CHECKING, Any, Literal

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import (
    CaseOrderBy,
    ColumnProxy,
    Condition,
    ConditionGroup,
    JoinClause,
    OrderBy,
    SQLFunction,
    render_order_term,
)
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook
    from declaro_persistum.query.table import TableProxy


