"""
Django ORM-style query API.

Provides a familiar interface for Django developers:
    users.filter(status="active").order("-created_at")[:10]
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
    OrderBy,
    render_order_term,
)
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook
    from declaro_persistum.query.select import SelectQuery




class DoesNotExist(Exception):
    """Raised when .get() finds no matching object."""

    pass


class MultipleObjectsReturned(Exception):
    """Raised when .get() finds more than one object."""

    pass
