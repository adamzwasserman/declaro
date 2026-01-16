"""Jinja2 environment configuration for Table Module V2.

This module sets up the Jinja2 environment with custom filters and functions
for table rendering using function-based architecture.
"""

import html
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from declaro_tablix.domain.models import ColumnDefinition, SortDefinition


def _get_package_templates_path() -> str:
    """Get the path to the bundled templates directory.

    Returns:
        Path to the templates directory bundled with the package
    """
    # Use importlib.resources to find package templates
    try:
        # Python 3.9+ with files() API
        with resources.as_file(resources.files("declaro_tablix.templates")) as templates_dir:
            return str(templates_dir)
    except (TypeError, AttributeError):
        # Fallback: use the module's file location
        return str(Path(__file__).parent)


def get_jinja_environment(template_path: Optional[str] = None) -> Environment:
    """Set up Jinja2 environment with custom filters and functions.

    Args:
        template_path: Path to template directory. If None, uses bundled package templates.

    Returns:
        Configured Jinja2 environment
    """
    if template_path is None:
        template_path = _get_package_templates_path()

    env = Environment(
        loader=FileSystemLoader(template_path),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
        enable_async=True,
    )

    # Register custom filters
    env.filters["format_currency"] = filter_format_currency
    env.filters["format_percentage"] = filter_format_percentage
    env.filters["format_number"] = filter_format_number
    env.filters["format_date"] = filter_format_date
    env.filters["format_boolean"] = filter_format_boolean
    env.filters["safe_string"] = filter_safe_string
    env.filters["truncate_text"] = filter_truncate_text
    env.filters["column_class"] = filter_column_class
    env.filters["sort_icon"] = filter_sort_icon
    env.filters["operator_text"] = filter_operator_text
    env.filters["default_indicator"] = filter_default_indicator
    env.filters["customization_indicator"] = filter_customization_indicator

    # Register global functions
    env.globals["generate_table_id"] = generate_table_id
    env.globals["get_column_width"] = get_column_width
    env.globals["get_cell_style"] = get_cell_style
    env.globals["is_sortable_column"] = is_sortable_column
    env.globals["is_filterable_column"] = is_filterable_column
    env.globals["get_pagination_range"] = get_pagination_range
    env.globals["format_row_number"] = format_row_number
    env.globals["get_column_type_icon"] = get_column_type_icon
    env.globals["calculate_table_stats"] = calculate_table_stats
    env.globals["validate_template_context"] = validate_template_context
    env.globals["get_filter_operators_for_column"] = get_filter_operators_for_column

    return env


async def render_template(template_name: str, context: Dict[str, Any], template_path: str = None) -> str:
    """Render a template with the given context.

    Args:
        template_name: Name of template file
        context: Template context variables
        template_path: Optional custom template path

    Returns:
        Rendered HTML string
    """
    env = get_jinja_environment(template_path) if template_path else get_jinja_environment()
    template = env.get_template(template_name)

    # Validate context before rendering
    validate_template_context(context)

    # Use async rendering since environment has enable_async=True
    return await template.render_async(context)


async def render_component(component_name: str, context: Dict[str, Any], template_path: str = None) -> str:
    """Render a component template.

    Args:
        component_name: Name of component (without .html extension)
        context: Template context variables
        template_path: Optional custom template path

    Returns:
        Rendered component HTML
    """
    template_name = f"components/{component_name}.html"
    return await render_template(template_name, context, template_path)


# Custom filter functions


def filter_format_currency(value: Any, currency_symbol: str = "$", decimal_places: int = 2) -> str:
    """Format value as currency.

    Args:
        value: Value to format
        currency_symbol: Currency symbol to use
        decimal_places: Number of decimal places

    Returns:
        Formatted currency string
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        return f"{currency_symbol}{num_value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return html.escape(str(value))


def filter_format_percentage(value: Any, decimal_places: int = 1, multiply_by_100: bool = False) -> str:
    """Format value as percentage.

    Args:
        value: Value to format
        decimal_places: Number of decimal places
        multiply_by_100: Whether to multiply by 100 (for decimal percentages)

    Returns:
        Formatted percentage string
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        if multiply_by_100:
            num_value *= 100
        return f"{num_value:.{decimal_places}f}%"
    except (ValueError, TypeError):
        return html.escape(str(value))


def filter_format_number(value: Any, decimal_places: int = 2, thousands_separator: bool = True) -> str:
    """Format value as number.

    Args:
        value: Value to format
        decimal_places: Number of decimal places
        thousands_separator: Whether to include thousands separator

    Returns:
        Formatted number string
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        if thousands_separator:
            return f"{num_value:,.{decimal_places}f}"
        else:
            return f"{num_value:.{decimal_places}f}"
    except (ValueError, TypeError):
        return html.escape(str(value))


def filter_format_date(value: Any, format_string: str = "%Y-%m-%d") -> str:
    """Format value as date.

    Args:
        value: Value to format (datetime, string, or timestamp)
        format_string: Date format string

    Returns:
        Formatted date string
    """
    if value is None:
        return ""

    try:
        if isinstance(value, datetime):
            return value.strftime(format_string)
        elif isinstance(value, str):
            # Try to parse ISO format
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.strftime(format_string)
        elif isinstance(value, (int, float)):
            # Assume timestamp
            dt = datetime.fromtimestamp(value)
            return dt.strftime(format_string)
        else:
            return html.escape(str(value))
    except (ValueError, TypeError):
        return html.escape(str(value))


def filter_format_boolean(value: Any, true_text: str = "Yes", false_text: str = "No") -> str:
    """Format value as boolean.

    Args:
        value: Value to format
        true_text: Text for true values
        false_text: Text for false values

    Returns:
        Formatted boolean string
    """
    if value is None:
        return ""

    if isinstance(value, bool):
        return true_text if value else false_text
    elif isinstance(value, str):
        lower_val = value.lower()
        if lower_val in ("true", "1", "yes", "on"):
            return true_text
        elif lower_val in ("false", "0", "no", "off"):
            return false_text
        else:
            return html.escape(str(value))
    elif isinstance(value, (int, float)):
        return true_text if value else false_text
    else:
        return html.escape(str(value))


def filter_safe_string(value: Any) -> str:
    """Safely escape string for HTML.

    Args:
        value: Value to escape

    Returns:
        HTML-escaped string
    """
    if value is None:
        return ""
    return html.escape(str(value))


def filter_truncate_text(value: Any, max_length: int = 50, suffix: str = "...") -> str:
    """Truncate text to maximum length.

    Args:
        value: Text to truncate
        max_length: Maximum length
        suffix: Suffix to add when truncated

    Returns:
        Truncated text
    """
    if value is None:
        return ""

    text = str(value)
    if len(text) <= max_length:
        return html.escape(text)

    return html.escape(text[: max_length - len(suffix)]) + suffix


def filter_column_class(column: ColumnDefinition, additional_classes: str = "") -> str:
    """Generate CSS classes for table column.

    Args:
        column: Column definition
        additional_classes: Additional CSS classes

    Returns:
        CSS class string
    """
    classes = ["table-column", f"column-{column.type.value}"]

    if not column.visible:
        classes.append("column-hidden")
    if column.sortable:
        classes.append("column-sortable")
    if column.filterable:
        classes.append("column-filterable")

    if additional_classes:
        classes.extend(additional_classes.split())

    return " ".join(classes)


def filter_sort_icon(column_id: str, sorts: List[SortDefinition], base_class: str = "fas") -> str:
    """Generate sort icon for column.

    Args:
        column_id: Column ID
        sorts: List of sort definitions
        base_class: Base CSS class for icons

    Returns:
        Sort icon HTML
    """
    for sort_def in sorts:
        if sort_def.column_id == column_id:
            if sort_def.direction.value == "asc":
                return f'<i class="{base_class} fa-sort-up" title="Sorted ascending"></i>'
            else:
                return f'<i class="{base_class} fa-sort-down" title="Sorted descending"></i>'

    return f'<i class="{base_class} fa-sort" title="Click to sort"></i>'


def filter_operator_text(operator: str) -> str:
    """Convert filter operator to human-readable text.

    Args:
        operator: Filter operator

    Returns:
        Human-readable operator text
    """
    operator_map = {
        "equals": "equals",
        "not_equals": "does not equal",
        "contains": "contains",
        "not_contains": "does not contain",
        "starts_with": "starts with",
        "ends_with": "ends with",
        "greater_than": "greater than",
        "less_than": "less than",
        "greater_equal": "greater than or equal",
        "less_equal": "less than or equal",
        "is_null": "is empty",
        "is_not_null": "is not empty",
        "in": "is one of",
        "not_in": "is not one of",
    }

    return operator_map.get(operator, operator)


def filter_default_indicator(is_default: bool) -> str:
    """Generate default indicator icon.

    Args:
        is_default: Whether item is default

    Returns:
        Default indicator HTML or empty string
    """
    if is_default:
        return '<i class="fas fa-star default-indicator" title="Default"></i>'
    return ""


def filter_customization_indicator(is_customized: bool) -> str:
    """Generate customization indicator icon.

    Args:
        is_customized: Whether item is customized

    Returns:
        Customization indicator HTML or empty string
    """
    if is_customized:
        return '<i class="fas fa-user-edit customization-indicator" title="Customized"></i>'
    return ""


# Global template functions


def generate_table_id(table_name: str, user_id: str) -> str:
    """Generate unique table ID.

    Args:
        table_name: Name of table
        user_id: User ID

    Returns:
        Unique table ID
    """
    return f"table-{table_name}-{user_id}".replace("_", "-").replace("@", "-at-")


def get_column_width(column: ColumnDefinition, total_columns: int) -> str:
    """Calculate column width as CSS value.

    Args:
        column: Column definition
        total_columns: Total number of columns

    Returns:
        CSS width value
    """
    if hasattr(column, "width") and column.width:
        return f"{column.width}px"

    # Auto-calculate based on column type
    type_widths = {
        "NUMBER": "100px",
        "CURRENCY": "120px",
        "PERCENTAGE": "100px",
        "DATE": "120px",
        "DATETIME": "160px",
        "BOOLEAN": "80px",
        "EMAIL": "200px",
        "URL": "150px",
        "PHONE": "140px",
    }

    return type_widths.get(column.type.value, "auto")


def get_cell_style(value: Any, column: ColumnDefinition) -> str:
    """Generate inline CSS styles for table cell.

    Args:
        value: Cell value
        column: Column definition

    Returns:
        CSS style string
    """
    styles = []

    # Text alignment based on column type
    if column.type.value in ["NUMBER", "CURRENCY", "PERCENTAGE"]:
        styles.append("text-align: right")
    elif column.type.value == "BOOLEAN":
        styles.append("text-align: center")

    # Color coding for specific values
    if value is not None:
        if column.type.value == "BOOLEAN":
            if bool(value):
                styles.append("color: #28a745")  # Green for true
            else:
                styles.append("color: #dc3545")  # Red for false
        elif column.type.value in ["NUMBER", "CURRENCY", "PERCENTAGE"]:
            try:
                num_val = float(value)
                if num_val < 0:
                    styles.append("color: #dc3545")  # Red for negative
                elif num_val > 0:
                    styles.append("color: #28a745")  # Green for positive
            except (ValueError, TypeError):
                pass

    return "; ".join(styles)


def is_sortable_column(column: ColumnDefinition) -> bool:
    """Check if column is sortable.

    Args:
        column: Column definition

    Returns:
        True if column is sortable
    """
    return getattr(column, "sortable", True)


def is_filterable_column(column: ColumnDefinition) -> bool:
    """Check if column is filterable.

    Args:
        column: Column definition

    Returns:
        True if column is filterable
    """
    return getattr(column, "filterable", True)


def get_pagination_range(current_page: int, total_pages: int, max_pages: int = 10) -> List[int]:
    """Generate pagination page range.

    Args:
        current_page: Current page number
        total_pages: Total number of pages
        max_pages: Maximum pages to show

    Returns:
        List of page numbers to display
    """
    if total_pages <= max_pages:
        return list(range(1, total_pages + 1))

    # Calculate start and end pages
    half_max = max_pages // 2
    start_page = max(1, current_page - half_max)
    end_page = min(total_pages, start_page + max_pages - 1)

    # Adjust start if we're near the end
    if end_page - start_page < max_pages - 1:
        start_page = max(1, end_page - max_pages + 1)

    return list(range(start_page, end_page + 1))


def format_row_number(row_index: int, page: int, page_size: int) -> int:
    """Format row number for display.

    Args:
        row_index: Zero-based row index
        page: Current page number (1-based)
        page_size: Number of rows per page

    Returns:
        Display row number
    """
    return ((page - 1) * page_size) + row_index + 1


def get_column_type_icon(column_type: str) -> str:
    """Get icon for column type.

    Args:
        column_type: Column type string

    Returns:
        Icon HTML
    """
    icon_map = {
        "TEXT": "fas fa-font",
        "NUMBER": "fas fa-hashtag",
        "CURRENCY": "fas fa-dollar-sign",
        "PERCENTAGE": "fas fa-percentage",
        "DATE": "fas fa-calendar-day",
        "DATETIME": "fas fa-calendar-alt",
        "BOOLEAN": "fas fa-check-square",
        "EMAIL": "fas fa-envelope",
        "URL": "fas fa-link",
        "PHONE": "fas fa-phone",
        "JSON": "fas fa-code",
    }

    # Handle both uppercase and lowercase column types
    column_type_upper = column_type.upper()
    icon_class = icon_map.get(column_type_upper, "fas fa-question")
    return f'<i class="{icon_class}" title="{column_type}"></i>'


def calculate_table_stats(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate basic table statistics.

    Args:
        data: Table data rows

    Returns:
        Dictionary with statistics
    """
    if not data:
        return {"total_rows": 0, "total_columns": 0}

    total_rows = len(data)
    total_columns = len(data[0]) if data else 0

    return {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "has_data": total_rows > 0,
    }


def get_filter_operators_for_column(column_or_type) -> List[Dict[str, str]]:
    """
    Get filter operators available for a column type.

    Args:
        column_or_type: Either a string column type or ColumnDefinition object

    Returns:
        List of operator dictionaries with 'value' and 'label'
    """
    # Handle both string types and ColumnDefinition objects
    if hasattr(column_or_type, "type"):
        # ColumnDefinition object
        column_type = column_or_type.type.value
    elif hasattr(column_or_type, "value"):
        # Enum object
        column_type = column_or_type.value
    else:
        # String type
        column_type = str(column_or_type)

    operators_map = {
        "TEXT": [
            {"value": "equals", "label": "Equals"},
            {"value": "contains", "label": "Contains"},
            {"value": "starts_with", "label": "Starts with"},
            {"value": "ends_with", "label": "Ends with"},
            {"value": "not_equals", "label": "Not equals"},
            {"value": "is_empty", "label": "Is empty"},
            {"value": "is_not_empty", "label": "Is not empty"},
        ],
        "NUMBER": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Not equals"},
            {"value": "greater_than", "label": "Greater than"},
            {"value": "greater_than_equal", "label": "Greater than or equal"},
            {"value": "less_than", "label": "Less than"},
            {"value": "less_than_equal", "label": "Less than or equal"},
            {"value": "between", "label": "Between"},
            {"value": "is_null", "label": "Is null"},
            {"value": "is_not_null", "label": "Is not null"},
        ],
        "CURRENCY": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Not equals"},
            {"value": "greater_than", "label": "Greater than"},
            {"value": "greater_than_equal", "label": "Greater than or equal"},
            {"value": "less_than", "label": "Less than"},
            {"value": "less_than_equal", "label": "Less than or equal"},
            {"value": "between", "label": "Between"},
        ],
        "DATE": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Not equals"},
            {"value": "before", "label": "Before"},
            {"value": "after", "label": "After"},
            {"value": "between", "label": "Between"},
            {"value": "is_null", "label": "Is null"},
            {"value": "is_not_null", "label": "Is not null"},
        ],
        "BOOLEAN": [
            {"value": "equals", "label": "Equals"},
            {"value": "is_null", "label": "Is null"},
            {"value": "is_not_null", "label": "Is not null"},
        ],
        "EMAIL": [
            {"value": "equals", "label": "Equals"},
            {"value": "contains", "label": "Contains"},
            {"value": "starts_with", "label": "Starts with"},
            {"value": "ends_with", "label": "Ends with"},
            {"value": "is_empty", "label": "Is empty"},
            {"value": "is_not_empty", "label": "Is not empty"},
        ],
    }

    return operators_map.get(column_type.upper(), operators_map["TEXT"])


def validate_template_context(context: Dict[str, Any]) -> bool:
    """Validate template context has required variables.

    Args:
        context: Template context dictionary

    Returns:
        True if context is valid

    Raises:
        ValueError: If required context is missing
    """
    # Basic validation - ensure context is a dictionary
    if not isinstance(context, dict):
        raise ValueError("Template context must be a dictionary")

    # Add specific validations based on template requirements
    # This is a placeholder for more specific validation logic

    return True
