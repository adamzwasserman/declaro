"""Domain models for declaro-tablix.

Core Pydantic models for table configuration, filtering, sorting, and pagination.
Adapted from tableV2 with clean architecture principles.
"""

from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


class SortDirection(str, Enum):
    """Sort direction enumeration."""

    ASC = "asc"
    DESC = "desc"


class FilterOperator(str, Enum):
    """Filter operator enumeration."""

    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    GREATER_EQUAL = "greater_equal"
    LESS_EQUAL = "less_equal"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"
    NOT_BETWEEN = "not_between"


class ColumnType(str, Enum):
    """Column data type enumeration."""

    TEXT = "text"
    NUMBER = "number"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    JSON = "json"
    IMAGE = "image"


class TableCssConfig(BaseModel):
    """CSS class configuration for table elements.

    Allows passthrough of CSS classes from React source to tablix templates,
    enabling pixel-perfect styling preservation.
    """

    table: list[str] = Field(default_factory=list, description="CSS classes for table element")
    thead: list[str] = Field(default_factory=list, description="CSS classes for thead element")
    tbody: list[str] = Field(default_factory=list, description="CSS classes for tbody element")
    row: list[str] = Field(default_factory=list, description="CSS classes for tr elements")
    row_alt: list[str] = Field(
        default_factory=list, description="CSS classes for alternating row styling"
    )


class ColumnDefinition(BaseModel):
    """Column definition model."""

    id: str = Field(..., description="Unique column identifier")
    name: str = Field(..., description="Column display name")
    type: ColumnType = Field(..., description="Column data type")
    width: int | None = Field(None, ge=50, le=2000, description="Column width in pixels")
    sortable: bool = Field(True, description="Whether column is sortable")
    filterable: bool = Field(True, description="Whether column is filterable")
    visible: bool = Field(True, description="Whether column is visible")
    required: bool = Field(False, description="Whether column is required")
    unique: bool = Field(False, description="Whether column values must be unique")
    default_value: Any | None = Field(None, description="Default value for column")
    format_options: dict[str, Any] | None = Field(None, description="Column formatting options")
    validation_rules: dict[str, Any] | None = Field(None, description="Column validation rules")
    aggregation_functions: list[str] | None = Field(
        None, description="Available aggregation functions"
    )
    system_alias: str | None = Field(None, description="System-level column alias")
    css_class: str | None = Field(None, description="CSS class(es) for header cell (th)")
    cell_css_class: str | None = Field(None, description="CSS class(es) for body cells (td)")
    cell_id_template: str | None = Field(
        None, description="ID template for table cells (e.g., 'cell-{row_idx}')"
    )

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError("Column ID cannot be empty")
        return v.strip()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError("Column name cannot be empty")
        return v.strip()

    @field_validator("format_options")
    @classmethod
    def validate_format_options(
        cls, v: dict[str, Any] | None, info: Any
    ) -> dict[str, Any] | None:
        if v is None:
            return v

        column_type = info.data.get("type")
        if column_type == ColumnType.CURRENCY and "currency_symbol" not in v:
            raise ValueError("Currency columns must have currency_symbol in format_options")

        return v


class FilterDefinition(BaseModel):
    """Filter definition model."""

    column_id: str = Field(..., description="Column identifier to filter")
    operator: FilterOperator = Field(..., description="Filter operator")
    value: Any | None = Field(None, description="Filter value")
    values: list[Any] | None = Field(None, description="Filter values for IN/NOT_IN operators")

    @model_validator(mode="after")
    def validate_filter_definition(self) -> "FilterDefinition":
        operator = self.operator
        value = self.value
        vals = self.values

        if operator in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            if value is not None:
                raise ValueError(f"Operator {operator} should not have a value")
            self.value = None
            return self

        if operator in [FilterOperator.IN, FilterOperator.NOT_IN]:
            if not vals or len(vals) == 0:
                raise ValueError("IN/NOT_IN operators require values list")
            self.value = None
            return self

        if value is None and operator not in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
            raise ValueError(f"Operator {operator} requires a value")

        return self


class SortDefinition(BaseModel):
    """Sort definition model."""

    column_id: str = Field(..., description="Column identifier to sort")
    direction: SortDirection = Field(..., description="Sort direction")
    priority: int = Field(0, description="Sort priority (0 = highest)")

    @field_validator("column_id")
    @classmethod
    def validate_column_id(cls, v: str) -> str:
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError("Column ID cannot be empty")
        return v.strip()


class PaginationSettings(BaseModel):
    """Pagination settings model."""

    page: int = Field(1, ge=1, description="Current page number")
    page_size: int = Field(25, ge=1, le=1000, description="Number of items per page")
    total_count: int | None = Field(None, ge=0, description="Total number of items")

    @property
    def total_pages(self) -> int | None:
        """Calculate total number of pages."""
        if self.total_count is None:
            return None
        return (self.total_count + self.page_size - 1) // self.page_size

    @property
    def offset(self) -> int:
        """Calculate offset for database queries."""
        return (self.page - 1) * self.page_size


class HeaderStat(BaseModel):
    """A stat item displayed in the table header.

    Used for showing summary metrics like totals, counts, or key values
    in the header area above the table.
    """

    label: str = Field(..., description="Stat label (e.g., 'Securities', 'AUM')")
    value: str = Field(..., description="Stat value (e.g., '408', '$96M')")
    badge: str | None = Field(None, description="Optional badge text (e.g., '26%')")
    badge_css: str | None = Field(None, description="CSS classes for the badge")
    css: str | None = Field(None, description="CSS classes for this stat item")


class TableHeaderConfig(BaseModel):
    """Configuration for the table header/branding section.

    Allows client apps to add a logo, title, subtitle, and summary stats
    above the table controls. Supports custom CSS for all elements.
    """

    logo_url: str | None = Field(None, description="URL or path to logo/avatar image")
    logo_alt: str | None = Field(None, description="Alt text for logo image")
    logo_css: str | None = Field(None, description="CSS classes for the logo container")
    title: str | None = Field(None, description="Main title (e.g., user name)")
    title_css: str | None = Field(None, description="CSS classes for the title")
    subtitle: str | None = Field(None, description="Subtitle (e.g., 'Advisor Code: 85F4722')")
    subtitle_css: str | None = Field(None, description="CSS classes for the subtitle")
    stats: list[HeaderStat] = Field(default_factory=list, description="List of stat items to display")
    container_css: str | None = Field(None, description="CSS classes for the header container")
    show_divider: bool = Field(True, description="Show divider line below header")


class TableConfig(BaseModel):
    """Table configuration model."""

    table_name: str = Field(..., description="Table name")
    columns: list[ColumnDefinition] = Field(..., description="Column definitions")
    filters: list[FilterDefinition] = Field(default_factory=list, description="Active filters")
    sorts: list[SortDefinition] = Field(default_factory=list, description="Sort definitions")
    pagination: PaginationSettings = Field(
        default_factory=PaginationSettings, description="Pagination settings"
    )
    search_term: str | None = Field(None, description="Global search term")
    group_by: str | None = Field(None, description="Group by column")
    css: TableCssConfig = Field(
        default_factory=TableCssConfig, description="CSS class configuration for table elements"
    )
    header: TableHeaderConfig | None = Field(
        None, description="Optional header/branding configuration with logo, title, and stats"
    )
    row_id_template: str | None = Field(
        None, description="ID template for table rows (e.g., 'row-{row_idx}')"
    )
    table_id_override: str | None = Field(
        None, description="Override the default table ID (default: 'tablix-{table_name}')"
    )

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError("Table name cannot be empty")
        return v.strip()

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, v: list[ColumnDefinition]) -> list[ColumnDefinition]:
        if not v:
            raise ValueError("Table must have at least one column")

        # Check for duplicate column IDs
        column_ids = [col.id for col in v]
        if len(column_ids) != len(set(column_ids)):
            raise ValueError("Column IDs must be unique")

        return v

    @field_validator("sorts")
    @classmethod
    def validate_sorts(
        cls, v: list[SortDefinition], info: Any
    ) -> list[SortDefinition]:
        if not v:
            return v

        columns = info.data.get("columns", [])
        column_ids = {col.id for col in columns}

        for sort in v:
            if sort.column_id not in column_ids:
                raise ValueError(f"Sort column {sort.column_id} not found in table columns")

        return v


class TableData(BaseModel):
    """Table data model."""

    rows: list[dict[str, Any]] = Field(..., description="Table rows")
    total_count: int = Field(..., ge=0, description="Total number of rows")
    metadata: dict[str, Any] | None = Field(None, description="Additional metadata")

    @field_validator("rows")
    @classmethod
    def validate_rows(cls, v: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not isinstance(v, list):
            raise ValueError("Rows must be a list")
        return v

    @field_validator("total_count")
    @classmethod
    def validate_total_count(cls, v: int, info: Any) -> int:
        rows = info.data.get("rows", [])
        if v < len(rows):
            raise ValueError("Total count cannot be less than number of rows")
        return v


class TableState(BaseModel):
    """Complete table state model."""

    config: TableConfig = Field(..., description="Table configuration")
    data: TableData = Field(..., description="Table data")
    user_id: str | None = Field(None, description="User ID for personalization")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")


# Column Customization Models


class ColumnAlias(BaseModel):
    """System-level column alias model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique alias ID")
    table_id: str = Field(..., description="Table identifier")
    column_id: str = Field(..., description="Column identifier")
    alias: str = Field(..., description="Alias name")
    created_at: str | None = Field(None, description="Creation timestamp")

    @field_validator("alias")
    @classmethod
    def validate_alias(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Alias cannot be empty")
        if len(v.strip()) > 100:
            raise ValueError("Alias cannot exceed 100 characters")
        return v.strip()


class UserColumnRename(BaseModel):
    """User-specific column rename model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique rename ID")
    user_id: str = Field(..., description="User identifier")
    table_id: str = Field(..., description="Table identifier")
    column_id: str = Field(..., description="Column identifier")
    custom_name: str = Field(..., description="Custom column name")
    is_visible: bool = Field(default=True, description="Whether column is visible")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")

    @field_validator("custom_name")
    @classmethod
    def validate_custom_name(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Custom name cannot be empty")
        return v.strip()


class ChildColumnDefinition(BaseModel):
    """Child column definition with formula support."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique child column ID")
    table_id: str = Field(..., description="Table identifier")
    column_id: str = Field(..., description="Child column identifier")
    parent_column_id: str = Field(..., description="Parent column identifier")
    formula: str = Field(..., description="Excel-like formula")
    display_name: str = Field(..., description="Display name")
    value_translations: dict[str, str] | None = Field(None, description="Value translation mapping")
    created_at: str | None = Field(None, description="Creation timestamp")

    @field_validator("formula")
    @classmethod
    def validate_formula(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Formula cannot be empty")
        if len(v.strip()) > 1000:
            raise ValueError("Formula cannot exceed 1000 characters")
        return v.strip()


class UserSavedSearch(BaseModel):
    """User saved search model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique search ID")
    user_id: str = Field(..., description="User identifier")
    table_id: str = Field(..., description="Table identifier")
    name: str = Field(..., description="Search name")
    search_term: str = Field(..., description="Search term")
    fuzzy_search: bool = Field(False, description="Enable fuzzy search")
    is_default: bool = Field(False, description="Is default search")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")

    @field_validator("name")
    @classmethod
    def validate_search_name(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Search name cannot be empty")
        return v.strip()


class UserFilterSet(BaseModel):
    """User saved filter set model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique filter set ID")
    user_id: str = Field(..., description="User identifier")
    table_id: str = Field(..., description="Table identifier")
    name: str = Field(..., description="Filter set name")
    description: str | None = Field(None, description="Filter set description")
    filters: list[FilterDefinition] = Field(..., description="Filter definitions")
    is_default: bool = Field(False, description="Is default filter set")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")

    @field_validator("name")
    @classmethod
    def validate_filter_set_name(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Filter set name cannot be empty")
        return v.strip()

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, v: list[FilterDefinition]) -> list[FilterDefinition]:
        if not v:
            raise ValueError("Filter set must have at least one filter")
        return v


class UserSavedLayout(BaseModel):
    """Complete saved layout model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique layout ID")
    user_id: str = Field(..., description="User identifier")
    table_id: str = Field(..., description="Table identifier")
    name: str = Field(..., description="Layout name")
    description: str | None = Field(None, description="Layout description")
    column_configuration: dict[str, Any] = Field(..., description="Column configuration")
    filter_configuration: dict[str, Any] = Field(..., description="Filter configuration")
    search_configuration: dict[str, Any] = Field(..., description="Search configuration")
    sort_configuration: dict[str, Any] = Field(..., description="Sort configuration")
    pagination_configuration: dict[str, Any] = Field(..., description="Pagination configuration")
    customization_configuration: dict[str, Any] = Field(
        ..., description="Customization configuration"
    )
    is_default: bool = Field(False, description="Is default layout")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")

    @field_validator("name")
    @classmethod
    def validate_layout_name(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Layout name cannot be empty")
        return v.strip()


class UserDefaultPreferences(BaseModel):
    """User default preferences aggregate model."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique preferences ID")
    user_id: str = Field(..., description="User identifier")
    table_id: str = Field(..., description="Table identifier")
    auto_apply_defaults: bool = Field(True, description="Auto-apply defaults on load")
    show_default_indicators: bool = Field(True, description="Show default indicators")
    default_search_id: str | None = Field(None, description="Default search ID")
    default_filter_set_id: str | None = Field(None, description="Default filter set ID")
    default_layout_id: str | None = Field(None, description="Default layout ID")
    # Full objects for convenience when loading defaults
    default_search: "UserSavedSearch | None" = Field(None, description="Default search object")
    default_filter_set: "UserFilterSet | None" = Field(None, description="Default filter set object")
    default_layout: "UserSavedLayout | None" = Field(None, description="Default layout object")
    created_at: str | None = Field(None, description="Creation timestamp")
    updated_at: str | None = Field(None, description="Last update timestamp")
