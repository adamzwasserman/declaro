"""Persistum models for declaro-tablix.

Database models decorated with @table for use with declaro-persistum.
These models define the schema for table and filter configuration storage.
"""

from typing import Literal

from pydantic import BaseModel, Field


def table(name: str):
    """Decorator to mark a Pydantic model as a database table.

    This decorator sets __tablename__ for declaro-persistum to discover.
    """

    def decorator(cls: type) -> type:
        cls.__tablename__ = name  # type: ignore[attr-defined]
        return cls

    return decorator


def field(
    *,
    primary: bool = False,
    unique: bool = False,
    references: str | None = None,
    on_delete: str | None = None,
    nullable: bool | None = None,
    db_type: str | None = None,
    **kwargs,
) -> dict:
    """Create field metadata for declaro-persistum.

    Args:
        primary: Whether this is a primary key column
        unique: Whether values must be unique
        references: Foreign key reference (e.g., "users.id")
        on_delete: ON DELETE action (CASCADE, SET NULL, etc.)
        nullable: Whether column allows NULL
        db_type: Override SQL type

    Returns:
        Dictionary of metadata for json_schema_extra
    """
    meta = {}
    if primary:
        meta["primary"] = True
    if unique:
        meta["unique"] = True
    if references:
        meta["references"] = references
    if on_delete:
        meta["on_delete"] = on_delete
    if nullable is not None:
        meta["nullable"] = nullable
    if db_type:
        meta["db_type"] = db_type
    meta.update(kwargs)
    return meta


@table("table_configs")
class TableConfigModel(BaseModel):
    """Database model for storing table configurations.

    Stores the serialized JSON configuration for tables and their layouts.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    table_name: str = Field(..., json_schema_extra=field(unique=True))
    display_name: str | None = None
    description: str | None = None
    config_json: str = Field(..., description="Serialized TableConfig JSON")
    filter_layout_json: str | None = Field(
        None, description="Serialized FilterLayoutConfig JSON"
    )
    is_active: bool = Field(True)
    created_at: str | None = None
    updated_at: str | None = None


@table("filter_enum_values")
class FilterEnumValue(BaseModel):
    """Database model for storing filter dropdown/checkbox options.

    Provides database-driven options for filter controls instead of
    hardcoding values in the UI or configuration files.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    source_name: str = Field(
        ...,
        description="Source identifier (e.g., 'security_types', 'sectors')",
    )
    value: str = Field(..., description="The actual value stored/filtered")
    label: str = Field(..., description="Display label in the UI")
    sort_order: int = Field(0, description="Sort order for display")
    is_active: bool = Field(True, description="Whether this option is active")
    parent_value: str | None = Field(None, description="Parent value for hierarchical options")
    metadata_json: str | None = Field(None, description="Additional JSON metadata")
    created_at: str | None = None
    updated_at: str | None = None

    class Meta:
        """Index definitions for performance."""

        indexes = [
            {"name": "idx_source_name", "columns": ["source_name"]},
            {"name": "idx_source_order", "columns": ["source_name", "sort_order"]},
        ]


@table("user_filter_preferences")
class UserFilterPreference(BaseModel):
    """Database model for storing user-specific filter defaults.

    Allows users to save their preferred filter configurations
    for quick access.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    user_id: str = Field(..., json_schema_extra=field(references="users.id"))
    table_id: str = Field(..., json_schema_extra=field(references="table_configs.id"))
    preference_name: str = Field(..., description="Name for this saved preference")
    filter_state_json: str = Field(..., description="Serialized FilterState JSON")
    is_default: bool = Field(False, description="Whether this is the default for this table")
    created_at: str | None = None
    updated_at: str | None = None

    class Meta:
        """Index definitions for performance."""

        indexes = [
            {"name": "idx_user_table", "columns": ["user_id", "table_id"]},
        ]


@table("column_configs")
class ColumnConfigModel(BaseModel):
    """Database model for storing column configurations.

    Stores individual column settings for a table configuration.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    table_config_id: str = Field(
        ..., json_schema_extra=field(references="table_configs.id", on_delete="CASCADE")
    )
    column_id: str = Field(..., description="Column identifier")
    column_name: str = Field(..., description="Display name")
    column_type: Literal["text", "number", "date", "datetime", "boolean", "currency", "percentage"]
    width: int | None = Field(None, ge=50, le=2000)
    sortable: bool = Field(True)
    filterable: bool = Field(True)
    visible: bool = Field(True)
    sort_order: int = Field(0)
    format_options_json: str | None = None
    created_at: str | None = None
    updated_at: str | None = None

    class Meta:
        """Index and constraint definitions."""

        indexes = [
            {"name": "idx_table_column", "columns": ["table_config_id", "column_id"]},
        ]


@table("filter_control_configs")
class FilterControlConfigModel(BaseModel):
    """Database model for storing filter control configurations.

    Stores the configuration for individual filter controls within a layout.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    layout_id: str = Field(
        ..., json_schema_extra=field(references="filter_layout_configs.id", on_delete="CASCADE")
    )
    control_type: Literal[
        "search_input", "checkbox_group", "multi_select", "number_range", "date_range", "single_select"
    ]
    column_id: str = Field(..., description="Database column this filter applies to")
    label: str | None = None
    placeholder: str | None = None
    grid_column: str | None = None
    grid_row: str | None = None
    options_source: str | None = Field(
        None, description="Source name in filter_enum_values"
    )
    config_json: str | None = Field(None, description="Additional config as JSON")
    sort_order: int = Field(0)
    created_at: str | None = None
    updated_at: str | None = None

    class Meta:
        """Index definitions."""

        indexes = [
            {"name": "idx_layout_order", "columns": ["layout_id", "sort_order"]},
        ]


@table("filter_layout_configs")
class FilterLayoutConfigModel(BaseModel):
    """Database model for storing filter layout configurations.

    Stores the CSS Grid layout settings for filter sections.
    """

    id: str = Field(..., json_schema_extra=field(primary=True))
    table_config_id: str = Field(
        ..., json_schema_extra=field(references="table_configs.id", on_delete="CASCADE")
    )
    layout_name: str | None = None
    grid_template_columns: str = Field("auto 1fr 1fr auto")
    grid_template_rows: str | None = None
    grid_gap: str = Field("1rem")
    grid_template_areas: str | None = None
    container_class: str = Field("filter-layout")
    container_style: str | None = None
    collapsible: bool = Field(False)
    default_collapsed: bool = Field(False)
    show_clear_all: bool = Field(True)
    show_apply_button: bool = Field(False)
    created_at: str | None = None
    updated_at: str | None = None

    class Meta:
        """Index definitions."""

        indexes = [
            {"name": "idx_table_config", "columns": ["table_config_id"]},
        ]
