"""declaro-tablix: Declarative table and filter system with CSS Grid layouts.

This package provides:
- Domain models for table configuration, filtering, sorting, and pagination
- CSS Grid-based filter layout system with viewport scaling
- Database-driven configuration via declaro-persistum integration
- Jinja2 templates for HTMX-powered filter controls
- Financial data integration (tableV2 compatible)
"""

from declaro_tablix.api_integration import (
    create_financial_table_response,
    fetch_financial_table_data,
    get_nasdaq_table_data,
    get_nyse_table_data,
    get_tsx_table_data,
)
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
    PaginationSettings,
    SortDefinition,
    SortDirection,
    TableConfig,
    TableData,
    TableState,
    UserColumnRename,
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)
from declaro_tablix.financial_integration import (
    ensure_financial_integration_initialized,
    get_financial_table_configuration,
    initialize_financial_integration,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    # Enums
    "ColumnType",
    "FilterControlType",
    "FilterOperator",
    "SortDirection",
    # Core table models
    "ColumnDefinition",
    "FilterDefinition",
    "PaginationSettings",
    "SortDefinition",
    "TableConfig",
    "TableData",
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
    "UserSavedSearch",
    # API integration
    "fetch_financial_table_data",
    "create_financial_table_response",
    "get_tsx_table_data",
    "get_nyse_table_data",
    "get_nasdaq_table_data",
    # Financial integration
    "ensure_financial_integration_initialized",
    "initialize_financial_integration",
    "get_financial_table_configuration",
]
