"""Rendering service functions for Table Module V2.

This module provides pure functions for HTML rendering using Jinja2 templates.
All dependencies are explicit parameters for clean library design.
"""

import html
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

from declaro_advise import error, info, success
from declaro_tablix.domain.models import (
    ColumnDefinition,
    FilterDefinition,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    TableHeaderConfig,
    TableState,
)
from declaro_tablix.domain.filter_layout import (
    FilterLayoutConfig,
    FilterControlConfig,
    FilterState,
    FilterOption,
)
from declaro_tablix.domain.protocols import (
    TableNotificationService,
    create_noop_notification_service,
)

# Customization metadata is obtained from table_state.data.metadata
from declaro_tablix.services.formula_engine import (
    evaluate_formula,
)


def _get_package_templates_path() -> str:
    """Get the path to the bundled templates directory.

    Returns:
        Path to the templates directory bundled with the package
    """
    try:
        # Python 3.9+ with files() API
        with resources.as_file(resources.files("declaro_tablix.templates")) as templates_dir:
            return str(templates_dir)
    except (TypeError, AttributeError):
        # Fallback: use the module's file location
        return str(Path(__file__).parent.parent / "templates")


def setup_jinja2_environment(template_path: Optional[str] = None) -> Environment:
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
    )

    # Add custom filters
    env.filters["format_cell"] = format_cell_filter
    env.filters["format_number"] = format_number_filter
    env.filters["format_currency"] = format_currency_filter
    env.filters["format_percentage"] = format_percentage_filter
    env.filters["format_date"] = format_date_filter
    env.filters["truncate_text"] = truncate_text_filter
    env.filters["highlight_search"] = highlight_search_filter
    env.filters["apply_value_translation"] = apply_value_translation_filter
    env.filters["highlight_customization"] = highlight_customization_filter

    # Add custom functions
    env.globals["get_column_class"] = get_column_class_function
    env.globals["get_sort_icon"] = get_sort_icon_function
    env.globals["get_filter_display"] = get_filter_display_function
    env.globals["generate_row_id"] = generate_row_id_function
    env.globals["render_customization_indicator"] = render_customization_indicator
    env.globals["render_formula_cell"] = render_formula_cell
    env.globals["render_default_indicator"] = render_default_indicator
    env.globals["get_formula_cell_class"] = get_formula_cell_class
    env.globals["get_custom_column_name"] = get_custom_column_name_function
    env.globals["is_formula_column"] = is_formula_column_function
    env.globals["render_calculated_cell"] = render_calculated_cell_function
    env.globals["get_column_customization_class"] = get_column_customization_class_function

    return env


async def render_complete_table(
    table_state: TableState,
    template_name: str = "table.html",
    context: Optional[Dict[str, Any]] = None,
    notification_service: TableNotificationService | None = None,
) -> str:
    """Render complete table HTML.

    Args:
        table_state: Complete table state
        template_name: Template name to use
        context: Optional additional context
        notification_service: Optional notification service

    Returns:
        Rendered HTML string
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Rendering complete table: {table_state.config.table_name}")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template(template_name)

        # Get customization metadata from table data
        customization_metadata = table_state.data.metadata.get("customizations", {}) if table_state.data.metadata else {}

        # Build template context
        template_context = {
            "table_state": table_state,
            "config": table_state.config,
            "data": table_state.data,
            "page_info": _get_page_info(table_state.config.pagination),
            "filter_summary": _get_filter_summary(table_state.config.filters),
            "sort_summary": _get_sort_summary(table_state.config.sorts),
            "user_id": table_state.user_id,
            "customization_metadata": customization_metadata,
            "has_customizations": bool(
                customization_metadata.get("column_aliases")
                or customization_metadata.get("column_renames")
                or customization_metadata.get("calculated_columns")
                or customization_metadata.get("value_translations")
            ),
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Table rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render table: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to render table: {str(e)}",
            notification_type="error",
            user_id=table_state.user_id,
            table_name=table_state.config.table_name,
        )
        return render_error_state(str(e))


async def render_table_header(
    columns: List[ColumnDefinition],
    sorts: Optional[List[SortDefinition]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render table header.

    Args:
        columns: Column definitions
        sorts: Optional sort definitions
        context: Optional additional context

    Returns:
        Rendered header HTML
    """
    try:
        info(f"Rendering table header with {len(columns)} columns")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/table_header.html")

        # Extract customization metadata from context
        customization_metadata = (context or {}).get("customization_metadata", {})

        # Build template context
        template_context = {
            "columns": columns,
            "sorts": sorts or [],
            "sort_summary": _get_sort_summary(sorts or []),
            "customization_metadata": customization_metadata,
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Table header rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render table header: {str(e)}")
        return f"<thead><tr><th>Error rendering header: {html.escape(str(e))}</th></tr></thead>"


async def render_table_body(
    data: TableData,
    columns: List[ColumnDefinition],
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render table body.

    Args:
        data: Table data
        columns: Column definitions
        context: Optional additional context

    Returns:
        Rendered body HTML
    """
    try:
        info(f"Rendering table body with {len(data.rows)} rows")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/table_body.html")

        # Extract customization metadata from data or context
        customization_metadata = (
            data.metadata.get("customizations", {}) if data.metadata else (context or {}).get("customization_metadata", {})
        )

        # Build template context
        template_context = {
            "data": data,
            "columns": columns,
            "rows": data.rows,
            "total_count": data.total_count,
            "metadata": data.metadata,
            "customization_metadata": customization_metadata,
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Table body rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render table body: {str(e)}")
        return f"<tbody><tr><td colspan='100%'>Error rendering table body: {html.escape(str(e))}</td></tr></tbody>"


async def render_table_controls(
    config: TableConfig,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render table controls (search, filters).

    Args:
        config: Table configuration
        context: Optional additional context

    Returns:
        Rendered controls HTML
    """
    try:
        info(f"Rendering table controls for: {config.table_name}")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/controls.html")

        # Build template context
        pagination = config.pagination
        template_context = {
            "config": config,
            "table_name": config.table_name,
            "columns": config.columns,
            "filters": config.filters,
            "sorts": config.sorts,
            "search_term": config.search_term,
            "pagination": pagination,
            "pagination_info": _get_page_info(pagination) if pagination else None,
            "filter_summary": _get_filter_summary(config.filters),
            "sort_summary": _get_sort_summary(config.sorts),
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Table controls rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render table controls: {str(e)}")
        return f"<div class='table-controls-error'>Error rendering controls: {html.escape(str(e))}</div>"


async def render_filter_layout(
    layout: FilterLayoutConfig,
    state: Optional[FilterState] = None,
    options: Optional[Dict[str, List[FilterOption]]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render a filter layout with all controls.

    Args:
        layout: Filter layout configuration
        state: Current filter state (selected values)
        options: Dictionary of filter options keyed by options_source
        context: Optional additional context

    Returns:
        Rendered filter layout HTML
    """
    try:
        info(f"Rendering filter layout: {layout.id}")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/filter_layout.html")

        # Build template context
        template_context = {
            "layout": layout,
            "state": state,
            "options": options or {},
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Filter layout rendered successfully: {layout.id}")
        return html_output

    except Exception as e:
        error(f"Failed to render filter layout: {str(e)}")
        return f"<div class='filter-layout-error'>Error rendering filters: {html.escape(str(e))}</div>"


async def render_table_header(
    header: TableHeaderConfig,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render table header/branding section with logo, title, and stats.

    Args:
        header: Header configuration with logo, title, subtitle, and stats
        context: Optional additional context

    Returns:
        Rendered header HTML
    """
    try:
        info(f"Rendering table header")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/header.html")

        # Build template context
        template_context = {
            "header": header,
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Table header rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render table header: {str(e)}")
        return f"<div class='table-header-error'>Error rendering header: {html.escape(str(e))}</div>"


async def render_pagination(
    pagination: PaginationSettings,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render pagination controls.

    Args:
        pagination: Pagination settings
        context: Optional additional context

    Returns:
        Rendered pagination HTML
    """
    try:
        info(f"Rendering pagination (page {pagination.page} of {pagination.total_pages})")

        # Set up Jinja2 environment
        env = setup_jinja2_environment()
        template = env.get_template("components/pagination.html")

        # Build template context
        template_context = {
            "pagination": pagination,
            "page_info": _get_page_info(pagination),
            "current_page": pagination.page,
            "total_pages": pagination.total_pages,
            "page_size": pagination.page_size,
            "total_count": pagination.total_count,
            **(context or {}),
        }

        # Render template
        html_output = template.render(template_context)

        success(f"Pagination rendered successfully")
        return html_output

    except Exception as e:
        error(f"Failed to render pagination: {str(e)}")
        return f"<div class='pagination-error'>Error rendering pagination: {html.escape(str(e))}</div>"


def render_loading_state(message: str = "Loading table data...") -> str:
    """Render loading state HTML.

    Args:
        message: Loading message

    Returns:
        Loading state HTML
    """
    return f"""
    <div class="table-loading">
        <div class="loading-spinner"></div>
        <p>{html.escape(message)}</p>
    </div>
    """


def render_error_state(error_message: str) -> str:
    """Render error state HTML.

    Args:
        error_message: Error message to display

    Returns:
        Error state HTML
    """
    return f"""
    <div class="table-error">
        <div class="error-icon">⚠️</div>
        <h3>Error Loading Table</h3>
        <p>{html.escape(error_message)}</p>
        <button onclick="window.location.reload()" class="btn btn-primary">
            Retry
        </button>
    </div>
    """


# Helper functions


def _get_page_info(pagination: PaginationSettings) -> Dict[str, Any]:
    """Get pagination information for templates.

    Args:
        pagination: Pagination settings

    Returns:
        Dictionary with pagination info
    """
    total_count = pagination.total_count or 0
    page = pagination.page or 1
    total_pages = pagination.total_pages or 0

    start_item = pagination.offset + 1 if total_count > 0 else 0
    end_item = min(pagination.offset + pagination.page_size, total_count)

    return {
        "current_page": page,
        "total_pages": total_pages,
        "page_size": pagination.page_size,
        "total_count": total_count,
        "start_item": start_item,
        "end_item": end_item,
        "has_previous": page > 1,
        "has_next": page < total_pages,
    }


def _get_filter_summary(filters: List[FilterDefinition]) -> Dict[str, Any]:
    """Get filter summary for templates.

    Args:
        filters: List of filter definitions

    Returns:
        Dictionary with filter summary
    """
    if not filters:
        return {"count": 0, "active": False, "summary": "No filters applied"}

    filter_descriptions = []
    for filter_def in filters:
        if filter_def.operator == "equals":
            filter_descriptions.append(f"{filter_def.column_id} = {filter_def.value}")
        elif filter_def.operator == "contains":
            filter_descriptions.append(f"{filter_def.column_id} contains '{filter_def.value}'")
        elif filter_def.operator == "is_null":
            filter_descriptions.append(f"{filter_def.column_id} is empty")
        elif filter_def.operator == "in":
            filter_descriptions.append(f"{filter_def.column_id} in {filter_def.values}")
        else:
            filter_descriptions.append(f"{filter_def.column_id} {filter_def.operator} {filter_def.value}")

    return {
        "count": len(filters),
        "active": True,
        "summary": f"{len(filters)} filter(s) applied",
        "descriptions": filter_descriptions,
    }


def _get_sort_summary(sorts: List[SortDefinition]) -> Dict[str, Any]:
    """Get sort summary for templates.

    Args:
        sorts: List of sort definitions

    Returns:
        Dictionary with sort summary
    """
    if not sorts:
        return {"count": 0, "active": False, "summary": "No sorting applied"}

    sort_descriptions = []
    for sort_def in sorted(sorts, key=lambda x: x.priority):
        direction = "↑" if sort_def.direction == "asc" else "↓"
        sort_descriptions.append(f"{sort_def.column_id} {direction}")

    return {
        "count": len(sorts),
        "active": True,
        "summary": f"Sorted by {', '.join(sort_descriptions)}",
        "descriptions": sort_descriptions,
    }


# Jinja2 filter functions


def format_cell_filter(value: Any, column_type: str, format_options: Optional[Dict[str, Any]] = None) -> str:
    """Format cell value for display.

    Args:
        value: Cell value
        column_type: Column type
        format_options: Optional formatting options

    Returns:
        Formatted cell value
    """
    if value is None:
        return ""

    if column_type == "currency":
        return format_currency_filter(value, format_options)
    elif column_type == "percentage":
        return format_percentage_filter(value, format_options)
    elif column_type == "number":
        return format_number_filter(value, format_options)
    elif column_type in ["date", "datetime"]:
        return format_date_filter(value, format_options)
    else:
        return html.escape(str(value))


def format_number_filter(value: Any, format_options: Optional[Dict[str, Any]] = None) -> str:
    """Format number for display.

    Args:
        value: Number value
        format_options: Optional formatting options

    Returns:
        Formatted number
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        decimal_places = format_options.get("decimal_places", 2) if format_options else 2
        return f"{num_value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return html.escape(str(value))


def format_currency_filter(value: Any, format_options: Optional[Dict[str, Any]] = None) -> str:
    """Format currency for display.

    Args:
        value: Currency value
        format_options: Optional formatting options

    Returns:
        Formatted currency
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        currency_symbol = format_options.get("currency_symbol", "$") if format_options else "$"
        decimal_places = format_options.get("decimal_places", 2) if format_options else 2
        return f"{currency_symbol}{num_value:,.{decimal_places}f}"
    except (ValueError, TypeError):
        return html.escape(str(value))


def format_percentage_filter(value: Any, format_options: Optional[Dict[str, Any]] = None) -> str:
    """Format percentage for display.

    Args:
        value: Percentage value
        format_options: Optional formatting options

    Returns:
        Formatted percentage
    """
    if value is None:
        return ""

    try:
        num_value = float(value)
        decimal_places = format_options.get("decimal_places", 1) if format_options else 1
        return f"{num_value:.{decimal_places}f}%"
    except (ValueError, TypeError):
        return html.escape(str(value))


def format_date_filter(value: Any, format_options: Optional[Dict[str, Any]] = None) -> str:
    """Format date for display.

    Args:
        value: Date value
        format_options: Optional formatting options

    Returns:
        Formatted date
    """
    if value is None:
        return ""

    # For now, just return the string representation
    # In a real implementation, you'd parse and format the date
    return html.escape(str(value))


def truncate_text_filter(value: Any, max_length: int = 50) -> str:
    """Truncate text for display.

    Args:
        value: Text value
        max_length: Maximum length

    Returns:
        Truncated text
    """
    if value is None:
        return ""

    text = str(value)
    if len(text) <= max_length:
        return html.escape(text)

    return html.escape(text[: max_length - 3]) + "..."


def highlight_search_filter(value: Any, search_term: Optional[str] = None) -> str:
    """Highlight search term in text.

    Args:
        value: Text value
        search_term: Search term to highlight

    Returns:
        Text with highlighted search term
    """
    if value is None or not search_term:
        return html.escape(str(value)) if value is not None else ""

    text = str(value)
    escaped_text = html.escape(text)
    escaped_search = html.escape(search_term)

    # Simple highlight - replace with <mark> tags
    highlighted = escaped_text.replace(escaped_search, f"<mark>{escaped_search}</mark>")

    return highlighted


def apply_value_translation_filter(value: Any, translations: Optional[Dict[str, str]] = None) -> str:
    """Apply value translations.

    Args:
        value: Original value
        translations: Translation dictionary

    Returns:
        Translated value or original if no translation
    """
    if value is None:
        return ""

    if not translations:
        return html.escape(str(value))

    str_value = str(value)
    translated = translations.get(str_value, str_value)
    return html.escape(translated)


def highlight_customization_filter(value: Any, is_customized: bool = False) -> str:
    """Highlight customized values.

    Args:
        value: Cell value
        is_customized: Whether the value is customized

    Returns:
        Value with customization highlighting
    """
    if value is None:
        return ""

    escaped_value = html.escape(str(value))

    if is_customized:
        return f"<span class='customized-value'>{escaped_value}</span>"

    return escaped_value


# Jinja2 global functions


def get_column_class_function(column: ColumnDefinition) -> str:
    """Get CSS class for column.

    Args:
        column: Column definition

    Returns:
        CSS class string
    """
    classes = ["table-column"]
    classes.append(f"column-{column.type}")

    if not column.visible:
        classes.append("column-hidden")

    if column.sortable:
        classes.append("column-sortable")

    if column.filterable:
        classes.append("column-filterable")

    return " ".join(classes)


def get_sort_icon_function(column_id: str, sorts: List[SortDefinition]) -> str:
    """Get sort icon for column.

    Args:
        column_id: Column identifier
        sorts: List of sort definitions

    Returns:
        Sort icon HTML
    """
    for sort_def in sorts:
        if sort_def.column_id == column_id:
            if sort_def.direction == "asc":
                return "<i class='fas fa-sort-up'></i>"
            else:
                return "<i class='fas fa-sort-down'></i>"

    return "<i class='fas fa-sort'></i>"


def get_filter_display_function(filters: List[FilterDefinition]) -> str:
    """Get filter display text.

    Args:
        filters: List of filter definitions

    Returns:
        Filter display text
    """
    if not filters:
        return "No filters"

    return f"{len(filters)} filter(s) applied"


def generate_row_id_function(row_index: int, row_data: Dict[str, Any]) -> str:
    """Generate unique row ID.

    Args:
        row_index: Row index
        row_data: Row data dictionary

    Returns:
        Unique row ID
    """
    return f"row-{row_index}"


def render_customization_indicator(is_customized: bool) -> str:
    """Render customization indicator.

    Args:
        is_customized: Whether item is customized

    Returns:
        Customization indicator HTML
    """
    if is_customized:
        return "<i class='fas fa-user-edit customization-indicator' title='Customized'></i>"
    return ""


def render_formula_cell(formula: str, result: Any) -> str:
    """Render formula cell.

    Args:
        formula: Formula string
        result: Formula result

    Returns:
        Formula cell HTML
    """
    return f"<span class='formula-cell' title='{html.escape(formula)}'>{html.escape(str(result))}</span>"


def render_default_indicator(is_default: bool) -> str:
    """Render default indicator.

    Args:
        is_default: Whether item is default

    Returns:
        Default indicator HTML
    """
    if is_default:
        return "<i class='fas fa-star default-indicator' title='Default'></i>"
    return ""


def get_formula_cell_class(has_formula: bool) -> str:
    """Get CSS class for formula cell.

    Args:
        has_formula: Whether cell has formula

    Returns:
        CSS class string
    """
    if has_formula:
        return "formula-cell"
    return "regular-cell"


def get_custom_column_name_function(column_id: str, customization_metadata: Optional[Dict[str, Any]] = None) -> str:
    """Get custom column name if available.

    Args:
        column_id: Original column ID
        customization_metadata: Customization metadata

    Returns:
        Custom column name or original column ID
    """
    if not customization_metadata:
        return column_id

    # Check for aliases first
    aliases = customization_metadata.get("column_aliases", {})
    if column_id in aliases:
        return aliases[column_id]

    # Check for renames
    renames = customization_metadata.get("column_renames", {})
    if column_id in renames:
        return renames[column_id]

    return column_id


def is_formula_column_function(column_id: str, customization_metadata: Optional[Dict[str, Any]] = None) -> bool:
    """Check if column is a calculated/formula column.

    Args:
        column_id: Column ID to check
        customization_metadata: Customization metadata

    Returns:
        True if column has formula, False otherwise
    """
    if not customization_metadata:
        return False

    calculated_columns = customization_metadata.get("calculated_columns", {})
    return column_id in calculated_columns


def render_calculated_cell_function(
    column_id: str, row_data: Dict[str, Any], customization_metadata: Optional[Dict[str, Any]] = None
) -> str:
    """Render calculated cell with formula indicator.

    Args:
        column_id: Column ID
        row_data: Row data
        customization_metadata: Customization metadata

    Returns:
        Rendered cell HTML with formula indicator
    """
    if not customization_metadata:
        return html.escape(str(row_data.get(column_id, "")))

    calculated_columns = customization_metadata.get("calculated_columns", {})

    if column_id in calculated_columns:
        formula = calculated_columns[column_id].get("formula", "")
        value = row_data.get(column_id, "")

        # Render with formula indicator
        return (
            f"<span class='formula-cell' "
            f"title='Formula: {html.escape(formula)}' "
            f"data-formula='{html.escape(formula)}'>"
            f"{html.escape(str(value))}"
            f"<i class='fas fa-calculator formula-indicator' title='Calculated column'></i>"
            f"</span>"
        )
    else:
        # Regular cell
        return html.escape(str(row_data.get(column_id, "")))


def get_column_customization_class_function(column_id: str, customization_metadata: Optional[Dict[str, Any]] = None) -> str:
    """Get CSS class based on column customization type.

    Args:
        column_id: Column ID
        customization_metadata: Customization metadata

    Returns:
        CSS class string indicating customization type
    """
    if not customization_metadata:
        return "regular-column"

    classes = ["table-column"]

    # Check for aliases
    aliases = customization_metadata.get("column_aliases", {})
    if column_id in aliases:
        classes.append("aliased-column")

    # Check for renames
    renames = customization_metadata.get("column_renames", {})
    if column_id in renames:
        classes.append("renamed-column")

    # Check for calculated columns
    calculated_columns = customization_metadata.get("calculated_columns", {})
    if column_id in calculated_columns:
        classes.append("calculated-column")

    # Check for value translations
    translations = customization_metadata.get("value_translations", {})
    if column_id in translations:
        classes.append("translated-column")

    # If no customizations, mark as regular
    if len(classes) == 1:
        classes.append("regular-column")
    else:
        classes.append("customized-column")

    return " ".join(classes)
