"""Domain models for declaro-tablix.

This module contains all the core domain models for table and filter configuration.
"""

from declaro_tablix.domain.filter_layout import (
    FilterControlConfig,
    FilterControlType,
    FilterLayoutConfig,
    FilterOption,
    FilterState,
)
from declaro_tablix.domain.models import (
    ColumnAlias,
    ColumnDefinition,
    ColumnType,
    FilterDefinition,
    FilterOperator,
    HeaderStat,
    PaginationSettings,
    SortDefinition,
    SortDirection,
    TableConfig,
    TableData,
    TableHeaderConfig,
    TableState,
    UserColumnRename,
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
)

__all__ = [
    # Enums
    "ColumnType",
    "FilterControlType",
    "FilterOperator",
    "SortDirection",
    # Core table models
    "ColumnDefinition",
    "FilterDefinition",
    "HeaderStat",
    "PaginationSettings",
    "SortDefinition",
    "TableConfig",
    "TableData",
    "TableHeaderConfig",
    "TableState",
    # Filter layout models
    "FilterControlConfig",
    "FilterLayoutConfig",
    "FilterOption",
    "FilterState",
    # User customization models
    "ColumnAlias",
    "UserColumnRename",
    "UserDefaultPreferences",
    "UserFilterSet",
    "UserSavedLayout",
]
