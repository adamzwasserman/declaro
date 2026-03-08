"""Filter layout domain models for declaro-tablix.

Provides declarative configuration for CSS Grid-based filter layouts.
Supports multiple filter control types with database-driven options.
"""

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class FilterControlType(str, Enum):
    """Filter control type enumeration.

    Defines the UI control used to render each filter.
    """

    SEARCH_INPUT = "search_input"
    CHECKBOX_GROUP = "checkbox_group"
    MULTI_SELECT = "multi_select"
    NUMBER_RANGE = "number_range"
    DATE_RANGE = "date_range"
    SINGLE_SELECT = "single_select"
    TAB_FILTER = "tab_filter"  # Horizontal tabs with counts (e.g., "All (100) | To Review (33)")
    STATIC_TEXT = "static_text"
    STATIC_IMAGE = "static_image"
    TOTAL_ABSOLUTE = "total_absolute"
    TOTAL_VISIBLE = "total_visible"
    CALCULATED_FIELD = "calculated_field"
    ACTION_BUTTON = "action_button"  # Clear Search, Reset Filters, etc.


class FilterPosition(str, Enum):
    """Position in 3-column filter layout.

    Controls horizontal alignment within the grid cell.
    """

    LEFT = "left"      # justify-content: flex-start
    MIDDLE = "middle"  # justify-content: center
    RIGHT = "right"    # justify-content: flex-end


class FilterControlConfig(BaseModel):
    """Configuration for a single filter control.

    Defines how a filter control is rendered and what data it operates on.
    """

    id: str = Field(..., description="Unique identifier for the filter control")
    control_type: FilterControlType = Field(..., description="Type of UI control to render")
    column_id: str = Field(..., description="Database column this filter applies to")
    label: str | None = Field(None, description="Display label for the filter")
    placeholder: str | None = Field(None, description="Placeholder text for input controls")

    # Grid positioning
    position: FilterPosition | None = Field(
        None, description="Position in 3-column layout: left, middle, right"
    )
    grid_column: str | None = Field(None, description="CSS grid-column value (overrides position)")
    grid_row: str | None = Field(None, description="CSS grid-row value")

    def get_cell_style(self) -> str:
        """Generate flex styles for the grid cell based on position."""
        if not self.position:
            return ""

        justify = {
            FilterPosition.LEFT: "flex-start",
            FilterPosition.MIDDLE: "center",
            FilterPosition.RIGHT: "flex-end",
        }.get(self.position, "flex-start")

        return f"display: flex; flex-direction: row; align-items: flex-end; justify-content: {justify};"

    def get_grid_column(self) -> str | None:
        """Get grid-column value from explicit setting or position."""
        if self.grid_column:
            return self.grid_column
        if self.position:
            return {
                FilterPosition.LEFT: "1",
                FilterPosition.MIDDLE: "2",
                FilterPosition.RIGHT: "3",
            }.get(self.position)
        return None

    # Options for select/checkbox controls
    options_source: str | None = Field(
        None, description="Database source for options (e.g., 'security_types')"
    )
    options_static: list[str] | None = Field(
        None, description="Static list of options (when not using database)"
    )

    # Range filter settings
    min_value: float | None = Field(None, description="Minimum value for range filters")
    max_value: float | None = Field(None, description="Maximum value for range filters")
    step: float | None = Field(None, description="Step value for number inputs")

    # Date range settings
    date_format: str = Field("YYYY-MM-DD", description="Date format string")
    min_date: str | None = Field(None, description="Minimum date for date range filters")
    max_date: str | None = Field(None, description="Maximum date for date range filters")

    # HTMX attributes
    hx_get: str | None = Field(None, description="HTMX hx-get URL for auto-submit")
    hx_trigger: str = Field("change", description="HTMX trigger event")
    hx_target: str | None = Field(None, description="HTMX target selector")
    hx_swap: str = Field("innerHTML", description="HTMX swap strategy")

    # Styling
    css_class: str | None = Field(None, description="Additional CSS classes")
    width: str | None = Field(None, description="CSS width value")

    # Behavior
    debounce_ms: int = Field(300, ge=0, description="Debounce time for search inputs")
    auto_submit: bool = Field(True, description="Whether to auto-submit on change")
    searchable: bool = Field(False, description="Whether multiselect has search input")

    # Static display controls
    text_content: str | None = Field(None, description="Text content for STATIC_TEXT controls")
    image_url: str | None = Field(None, description="Image URL for STATIC_IMAGE controls")
    image_alt: str | None = Field(None, description="Alt text for STATIC_IMAGE controls")

    # Totals and calculated fields
    source_field: str | None = Field(None, description="Source field for TOTAL_* controls")
    formula: str | None = Field(None, description="Formula for CALCULATED_FIELD controls")
    format: str | None = Field(None, description="Format type: currency, number, percentage")
    badge_thresholds: list[dict[str, Any]] | None = Field(
        None, description="Badge thresholds for styling calculated values"
    )

    # ID override for accessibility and testing
    input_id_override: str | None = Field(
        None, description="Override the default input ID (default: 'filter-{id}')"
    )
    button_id_override: str | None = Field(
        None, description="Override the dropdown trigger button ID (for multi_select)"
    )
    label_id_override: str | None = Field(
        None, description="Override the label element ID"
    )
    wrapper_id_override: str | None = Field(
        None, description="Override the input wrapper div ID (e.g., 'search-input-wrapper')"
    )

    # Action button fields
    action_type: str | None = Field(
        None, description="Action type for ACTION_BUTTON (e.g., 'clear-search', 'reset-filters')"
    )
    icon_id_override: str | None = Field(
        None, description="ID for the button's SVG icon element"
    )
    text_id_override: str | None = Field(
        None, description="ID for the button's text span element"
    )
    icon_svg: str | None = Field(
        None, description="SVG markup for the button icon"
    )

    # Dropdown-specific ID overrides for div-based dropdowns (React pattern)
    container_id_override: str | None = Field(
        None, description="Override for the dropdown container div ID (e.g., 'sector-filter')"
    )
    button_text_id_override: str | None = Field(
        None, description="ID for the trigger button's text span (e.g., 'sector-button-text')"
    )
    button_icon_id_override: str | None = Field(
        None, description="ID for the trigger button's chevron icon (e.g., 'sector-button-icon')"
    )
    dropdown_menu_id_override: str | None = Field(
        None, description="ID for the dropdown menu container (e.g., 'sector-dropdown')"
    )
    options_container_id_override: str | None = Field(
        None, description="ID for the options list container (e.g., 'sector-options-container')"
    )
    clear_button_id_override: str | None = Field(
        None, description="ID for the dropdown's internal clear button (e.g., 'sector-clear-button')"
    )

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Filter control ID cannot be empty")
        return v.strip()

    @field_validator("column_id")
    @classmethod
    def validate_column_id(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Column ID cannot be empty")
        return v.strip()


class FilterControlGroup(BaseModel):
    """Group multiple controls in a single grid cell.

    Controls within a group flow horizontally (flex-direction: row).
    """

    id: str = Field(..., description="Unique identifier for the group")
    controls: list[FilterControlConfig] = Field(
        ..., description="Controls to render in this group"
    )

    # Grid positioning (same as FilterControlConfig)
    position: FilterPosition | None = Field(
        None, description="Position in 3-column layout: left, middle, right"
    )
    grid_column: str | None = Field(None, description="CSS grid-column value (overrides position)")
    grid_row: str | None = Field(None, description="CSS grid-row value")

    # Styling
    css_class: str | None = Field(None, description="Additional CSS classes for the group container")
    gap: str = Field("0.5rem", description="Gap between controls in the group")

    # ID override
    container_id_override: str | None = Field(
        None, description="Override the default group container ID"
    )

    def get_cell_style(self) -> str:
        """Generate flex styles for the grid cell based on position."""
        styles = [f"gap: {self.gap}"]

        if self.position:
            justify = {
                FilterPosition.LEFT: "flex-start",
                FilterPosition.MIDDLE: "center",
                FilterPosition.RIGHT: "flex-end",
            }.get(self.position, "flex-start")
            styles.extend([
                "display: flex",
                "flex-direction: row",
                "align-items: flex-end",
                f"justify-content: {justify}",
            ])
        else:
            # Default flex row without justify
            styles.extend([
                "display: flex",
                "flex-direction: row",
                "align-items: flex-end",
            ])

        return "; ".join(styles) + ";"

    def get_grid_column(self) -> str | None:
        """Get grid-column value from explicit setting or position."""
        if self.grid_column:
            return self.grid_column
        if self.position:
            return {
                FilterPosition.LEFT: "1",
                FilterPosition.MIDDLE: "2",
                FilterPosition.RIGHT: "3",
            }.get(self.position)
        return None

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Filter control group ID cannot be empty")
        return v.strip()

    @field_validator("controls")
    @classmethod
    def validate_controls(cls, v: list[FilterControlConfig]) -> list[FilterControlConfig]:
        if not v:
            raise ValueError("Filter control group must have at least one control")
        return v


class FilterLayoutConfig(BaseModel):
    """Configuration for a complete filter layout.

    Defines the CSS Grid layout and all filter controls within it.
    """

    id: str = Field(..., description="Unique identifier for the filter layout")
    table_id: str = Field(..., description="Associated table identifier")
    name: str | None = Field(None, description="Human-readable name for the layout")

    # CSS Grid configuration (3-column layout: left, middle, right)
    grid_template_columns: str = Field(
        "1fr 1fr 1fr", description="CSS grid-template-columns value (default: 3 equal columns)"
    )
    grid_template_rows: str | None = Field(None, description="CSS grid-template-rows value")
    grid_gap: str = Field("1rem", description="CSS gap between grid items")
    grid_template_areas: str | None = Field(
        None, description="CSS grid-template-areas for named areas"
    )

    # Container styling
    container_class: str = Field("filter-layout", description="CSS class for container")
    container_style: str | None = Field(None, description="Inline style for container")
    background_color: str | None = Field(None, description="Background color")
    padding: str = Field("1rem", description="Container padding")
    border_radius: str | None = Field(None, description="Border radius")

    # Filter controls (can be individual controls or groups)
    controls: list[FilterControlConfig | FilterControlGroup] = Field(
        default_factory=list, description="Filter controls and groups in this layout"
    )

    # Layout behavior
    collapsible: bool = Field(False, description="Whether the filter layout can be collapsed")
    default_collapsed: bool = Field(False, description="Whether collapsed by default")
    show_clear_all: bool = Field(True, description="Whether to show a clear all button")
    show_apply_button: bool = Field(
        False, description="Whether to show an explicit apply button"
    )
    use_form_wrapper: bool = Field(
        True, description="Whether to wrap controls in a form element (set False for React parity)"
    )
    use_controls_wrapper: bool = Field(
        True, description="Whether to wrap controls in a controls div (set False for React parity)"
    )

    # HTMX configuration for the entire layout
    form_hx_get: str | None = Field(None, description="HTMX hx-get URL for form submission")
    form_hx_trigger: str = Field("submit", description="HTMX trigger for form")
    form_hx_target: str | None = Field(None, description="HTMX target for form submission")

    # ID overrides for accessibility and testing
    container_id_override: str | None = Field(
        None, description="Override the default container ID (default: 'filter-layout-{id}')"
    )
    form_id_override: str | None = Field(
        None, description="Override the default form ID (default: 'filter-form-{id}')"
    )
    controls_id_override: str | None = Field(
        None, description="Override the default controls container ID (default: 'filter-controls-{id}')"
    )

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Filter layout ID cannot be empty")
        return v.strip()

    @field_validator("table_id")
    @classmethod
    def validate_table_id(cls, v: str) -> str:
        if not v or len(v.strip()) == 0:
            raise ValueError("Table ID cannot be empty")
        return v.strip()

    @field_validator("controls")
    @classmethod
    def validate_controls(cls, v: list[FilterControlConfig]) -> list[FilterControlConfig]:
        # Check for duplicate control IDs
        ids = [c.id for c in v]
        if len(ids) != len(set(ids)):
            raise ValueError("Filter control IDs must be unique within a layout")
        return v

    def get_control(self, control_id: str) -> FilterControlConfig | None:
        """Get a filter control by its ID."""
        for control in self.controls:
            if control.id == control_id:
                return control
        return None

    def get_controls_by_type(
        self, control_type: FilterControlType
    ) -> list[FilterControlConfig]:
        """Get all controls of a specific type."""
        return [c for c in self.controls if c.control_type == control_type]

    def to_css_grid_style(self) -> str:
        """Generate CSS grid styles for the container."""
        styles = [
            "display: grid",
            f"grid-template-columns: {self.grid_template_columns}",
            f"gap: {self.grid_gap}",
            f"padding: {self.padding}",
        ]

        if self.grid_template_rows:
            styles.append(f"grid-template-rows: {self.grid_template_rows}")

        if self.grid_template_areas:
            styles.append(f"grid-template-areas: {self.grid_template_areas}")

        if self.background_color:
            styles.append(f"background-color: {self.background_color}")

        if self.border_radius:
            styles.append(f"border-radius: {self.border_radius}")

        return "; ".join(styles)


class FilterOption(BaseModel):
    """Single filter option for select/checkbox controls."""

    value: str = Field(..., description="Option value")
    label: str = Field(..., description="Option display label")
    selected: bool = Field(False, description="Whether option is selected")
    disabled: bool = Field(False, description="Whether option is disabled")
    count: int | None = Field(None, description="Optional count of matching items")
    sort_order: int = Field(0, description="Sort order for display")
    id_override: str | None = Field(None, description="Override the checkbox/option ID")


class FilterState(BaseModel):
    """Current state of filters for a table.

    Used to track active filter values and pass them between requests.
    """

    layout_id: str = Field(..., description="Filter layout identifier")
    values: dict[str, Any] = Field(default_factory=dict, description="Current filter values")

    def get_value(self, control_id: str) -> Any | None:
        """Get the current value for a filter control."""
        return self.values.get(control_id)

    def set_value(self, control_id: str, value: Any) -> None:
        """Set the value for a filter control."""
        self.values[control_id] = value

    def clear(self) -> None:
        """Clear all filter values."""
        self.values.clear()

    def clear_control(self, control_id: str) -> None:
        """Clear the value for a specific control."""
        self.values.pop(control_id, None)

    def to_query_params(self) -> dict[str, str]:
        """Convert filter state to URL query parameters."""
        params: dict[str, str] = {}
        for key, value in self.values.items():
            if value is None:
                continue
            if isinstance(value, list):
                params[key] = ",".join(str(v) for v in value)
            else:
                params[key] = str(value)
        return params

    @classmethod
    def from_query_params(cls, layout_id: str, params: dict[str, str]) -> "FilterState":
        """Create filter state from URL query parameters."""
        values: dict[str, Any] = {}
        for key, value in params.items():
            if "," in value:
                values[key] = value.split(",")
            else:
                values[key] = value
        return cls(layout_id=layout_id, values=values)
