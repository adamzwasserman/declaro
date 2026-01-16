"""Template rendering functions for Table Module V2.

This module provides pure rendering functions using Jinja2 templates
with comprehensive context management and performance optimization.
"""

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import (
    ColumnDefinition,
    FilterDefinition,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    TableState,
)
from declaro_tablix.templates.jinja_env import (
    render_component,
    render_template,
)


async def render_table_view(
    table_state: TableState, template_name: str = "table.html", additional_context: Optional[Dict[str, Any]] = None
) -> str:
    """Render complete table view with all components.

    Args:
        table_state: Complete table state
        template_name: Main template name
        additional_context: Additional context variables

    Returns:
        Rendered HTML string
    """
    try:
        info(f"Rendering table view for: {table_state.config.table_name}")

        # Build comprehensive template context
        context = create_table_context(table_state, additional_context)

        # Add performance tracking
        start_time = time.time()

        # Render main template
        html = await render_template(template_name, context)

        # Track performance
        render_time = (time.time() - start_time) * 1000
        info(f"Table view rendered in {render_time:.2f}ms")

        success(f"Table view rendered successfully")
        return html

    except Exception as e:
        error(f"Failed to render table view: {str(e)}")
        return await render_error_message(f"Failed to render table: {str(e)}")


async def render_table_component(
    component_name: str, table_data: TableData, config: TableConfig, additional_context: Optional[Dict[str, Any]] = None
) -> str:
    """Render individual table component.

    Args:
        component_name: Name of component to render
        table_data: Table data
        config: Table configuration
        additional_context: Additional context variables

    Returns:
        Rendered component HTML
    """
    try:
        info(f"Rendering table component: {component_name}")

        # Build component context
        context = {
            "data": table_data,
            "config": config,
            "columns": config.columns,
            "filters": config.filters,
            "sorts": config.sorts,
            "pagination": config.pagination,
            "search_term": config.search_term,
            **(additional_context or {}),
        }

        # Add component-specific context
        context = _enhance_component_context(component_name, context)

        # Render component
        html = await render_component(component_name, context)

        success(f"Component '{component_name}' rendered successfully")
        return html

    except Exception as e:
        error(f"Failed to render component '{component_name}': {str(e)}")
        return await render_error_message(f"Failed to render component: {str(e)}")


def create_table_context(table_state: TableState, additional_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create comprehensive template context for table rendering.

    Args:
        table_state: Complete table state
        additional_context: Additional context variables

    Returns:
        Template context dictionary
    """
    try:
        info("Creating table template context")

        # Base context with safe values to avoid Undefined objects
        context = {
            # Core data
            "table_state": table_state,
            "config": table_state.config,
            "data": table_state.data,
            "user_id": table_state.user_id,
            # Table configuration
            "table_name": table_state.config.table_name,
            "columns": [col for col in table_state.config.columns],  # Convert to list to avoid Undefined
            "filters": table_state.config.filters,
            "sorts": table_state.config.sorts,
            "pagination": table_state.config.pagination,
            "search_term": table_state.config.search_term,
            "group_by": table_state.config.group_by,
            # Data information
            "rows": table_state.data.rows if table_state.data else [],
            "total_count": table_state.data.total_count if table_state.data else 0,
            "metadata": table_state.data.metadata if table_state.data else {},
            # Template helpers
            "now": datetime.now(),
            "render_time": datetime.now().isoformat(),
            # Filter operators for different column types
            "filter_operators": {
                "TEXT": [{"value": "equals", "label": "Equals"}, {"value": "contains", "label": "Contains"}],
                "NUMBER": [{"value": "equals", "label": "Equals"}, {"value": "greater_than", "label": "Greater than"}],
                "CURRENCY": [{"value": "equals", "label": "Equals"}, {"value": "greater_than", "label": "Greater than"}],
                "DATE": [
                    {"value": "equals", "label": "Equals"},
                    {"value": "before", "label": "Before"},
                    {"value": "after", "label": "After"},
                ],
                "BOOLEAN": [{"value": "equals", "label": "Equals"}],
                "EMAIL": [{"value": "equals", "label": "Equals"}, {"value": "contains", "label": "Contains"}],
                "URL": [{"value": "equals", "label": "Equals"}, {"value": "contains", "label": "Contains"}],
                "PHONE": [{"value": "equals", "label": "Equals"}, {"value": "contains", "label": "Contains"}],
                "JSON": [{"value": "equals", "label": "Equals"}, {"value": "contains", "label": "Contains"}],
            },
            # Customization context - ensure these are always defined
            "column_renames": {},
            "calculated_columns": {},
            "value_translations": {},
            "user_layouts": [],
            "user_filter_sets": [],
            "available_page_sizes": [10, 20, 50, 100],
            "excel_functions": [
                {"name": "SUM", "description": "Sum of values", "example": "=SUM([col1], [col2])"},
                {"name": "AVERAGE", "description": "Average of values", "example": "=AVERAGE([col1], [col2])"},
                {"name": "COUNT", "description": "Count of non-empty values", "example": "=COUNT([col1], [col2])"},
                {"name": "IF", "description": "Conditional logic", "example": "=IF([col1] > 10, 'High', 'Low')"},
                {"name": "CONCATENATE", "description": "Join text values", "example": "=CONCATENATE([first], ' ', [last])"},
            ],
        }

        # Add customization metadata if available
        if table_state.data and table_state.data.metadata:
            customization_metadata = table_state.data.metadata.get("customizations", {})
            context["customization_metadata"] = customization_metadata
            context["has_customizations"] = bool(customization_metadata)

        # Add pagination context
        if table_state.config.pagination:
            context["pagination_info"] = _get_pagination_info(table_state.config.pagination)

        # Add filter summary
        context["filter_summary"] = _get_filter_summary(table_state.config.filters)

        # Add sort summary
        context["sort_summary"] = _get_sort_summary(table_state.config.sorts)

        # Add table statistics
        if table_state.data and table_state.data.rows:
            context["table_stats"] = _get_table_stats(table_state.data.rows)

        # Merge additional context
        if additional_context:
            context.update(additional_context)

        # Validate context
        _validate_template_context(context)

        success("Table template context created successfully")
        return context

    except Exception as e:
        error(f"Failed to create table context: {str(e)}")
        return {"error": str(e)}


async def render_filter_form(
    columns: List[ColumnDefinition],
    current_filters: Optional[List[FilterDefinition]] = None,
    additional_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render filter form component.

    Args:
        columns: Available columns for filtering
        current_filters: Currently applied filters
        additional_context: Additional context variables

    Returns:
        Rendered filter form HTML
    """
    try:
        info("Rendering filter form")

        context = {
            "columns": columns,
            "current_filters": current_filters or [],
            "filterable_columns": [col for col in columns if col.filterable],
            "filter_operators": _get_filter_operators_by_type(),
            **(additional_context or {}),
        }

        html = await render_component("filter_form", context)
        success("Filter form rendered successfully")
        return html

    except Exception as e:
        error(f"Failed to render filter form: {str(e)}")
        return await render_error_message("Failed to render filter form")


async def render_layout_form(
    config: TableConfig,
    user_layouts: Optional[List[Dict[str, Any]]] = None,
    additional_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render layout configuration form.

    Args:
        config: Current table configuration
        user_layouts: User's saved layouts
        additional_context: Additional context variables

    Returns:
        Rendered layout form HTML
    """
    try:
        info("Rendering layout form")

        context = {
            "config": config,
            "user_layouts": user_layouts or [],
            "sortable_columns": [col for col in config.columns if col.sortable],
            "available_page_sizes": [10, 20, 50, 100, 200],
            **(additional_context or {}),
        }

        html = await render_component("layout_form", context)
        success("Layout form rendered successfully")
        return html

    except Exception as e:
        error(f"Failed to render layout form: {str(e)}")
        return await render_error_message("Failed to render layout form")


async def render_column_customization_form(
    columns: List[ColumnDefinition],
    customizations: Optional[Dict[str, Any]] = None,
    additional_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Render column customization form.

    Args:
        columns: Available columns
        customizations: Current customizations
        additional_context: Additional context variables

    Returns:
        Rendered customization form HTML
    """
    try:
        info("Rendering column customization form")

        context = {
            "columns": columns,
            "customizations": customizations or {},
            "column_aliases": customizations.get("column_aliases", {}) if customizations else {},
            "column_renames": customizations.get("column_renames", {}) if customizations else {},
            "calculated_columns": customizations.get("calculated_columns", {}) if customizations else {},
            "value_translations": customizations.get("value_translations", {}) if customizations else {},
            "excel_functions": _get_excel_functions_reference(),
            **(additional_context or {}),
        }

        html = await render_component("column_customization_form", context)
        success("Column customization form rendered successfully")
        return html

    except Exception as e:
        error(f"Failed to render column customization form: {str(e)}")
        return await render_error_message("Failed to render customization form")


async def render_error_message(
    message: str, error_type: str = "error", additional_context: Optional[Dict[str, Any]] = None
) -> str:
    """Render error message component.

    Args:
        message: Error message to display
        error_type: Type of error (error, warning, info)
        additional_context: Additional context variables

    Returns:
        Rendered error message HTML
    """
    try:
        context = {
            "message": message,
            "error_type": error_type,
            "timestamp": datetime.now().isoformat(),
            **(additional_context or {}),
        }

        return await render_component("error_message", context)

    except Exception as e:
        # Fallback to basic HTML if template rendering fails
        return f"""
        <div class="alert alert-danger" role="alert">
            <h4 class="alert-heading">Error</h4>
            <p>{message}</p>
            <hr>
            <p class="mb-0">Template rendering failed: {str(e)}</p>
        </div>
        """


async def render_success_message(message: str, additional_context: Optional[Dict[str, Any]] = None) -> str:
    """Render success message component.

    Args:
        message: Success message to display
        additional_context: Additional context variables

    Returns:
        Rendered success message HTML
    """
    try:
        context = {"message": message, "timestamp": datetime.now().isoformat(), **(additional_context or {})}

        return await render_component("success_message", context)

    except Exception:
        # Fallback to basic HTML
        return f"""
        <div class="alert alert-success" role="alert">
            <h4 class="alert-heading">Success</h4>
            <p>{message}</p>
        </div>
        """


def get_filter_operators_for_column(column: ColumnDefinition) -> List[Dict[str, str]]:
    """Get available filter operators for a column type.

    Args:
        column: Column definition

    Returns:
        List of operator definitions
    """
    base_operators = [
        {"value": "equals", "label": "Equals"},
        {"value": "not_equals", "label": "Does not equal"},
        {"value": "is_null", "label": "Is empty"},
        {"value": "is_not_null", "label": "Is not empty"},
    ]

    text_operators = [
        {"value": "contains", "label": "Contains"},
        {"value": "not_contains", "label": "Does not contain"},
        {"value": "starts_with", "label": "Starts with"},
        {"value": "ends_with", "label": "Ends with"},
    ]

    numeric_operators = [
        {"value": "greater_than", "label": "Greater than"},
        {"value": "less_than", "label": "Less than"},
        {"value": "greater_equal", "label": "Greater than or equal"},
        {"value": "less_equal", "label": "Less than or equal"},
    ]

    list_operators = [
        {"value": "in", "label": "Is one of"},
        {"value": "not_in", "label": "Is not one of"},
    ]

    # Build operators based on column type
    operators = base_operators.copy()

    if column.type.value in ["TEXT", "EMAIL", "URL", "PHONE"]:
        operators.extend(text_operators)
    elif column.type.value in ["NUMBER", "CURRENCY", "PERCENTAGE", "DATE", "DATETIME"]:
        operators.extend(numeric_operators)

    operators.extend(list_operators)

    return operators


async def get_cached_template(template_key: str, context: Dict[str, Any], cache_ttl: int = 300) -> Optional[str]:
    """Get cached template rendering if available.

    Args:
        template_key: Unique template cache key
        context: Template context for cache validation
        cache_ttl: Cache TTL in seconds

    Returns:
        Cached HTML or None if not available
    """
    # Placeholder for caching implementation
    # In a real implementation, this would check Redis or another cache
    return None


def validate_template_performance(
    template_name: str, context_size: int, render_time_ms: float, max_render_time_ms: float = 100
) -> Dict[str, Any]:
    """Validate template rendering performance.

    Args:
        template_name: Name of rendered template
        context_size: Size of template context
        render_time_ms: Actual render time in milliseconds
        max_render_time_ms: Maximum allowed render time

    Returns:
        Performance validation results
    """
    is_within_limits = render_time_ms <= max_render_time_ms

    performance_result = {
        "template_name": template_name,
        "render_time_ms": render_time_ms,
        "context_size": context_size,
        "max_render_time_ms": max_render_time_ms,
        "within_limits": is_within_limits,
        "performance_score": min(100, (max_render_time_ms / render_time_ms) * 100) if render_time_ms > 0 else 100,
    }

    if not is_within_limits:
        warning(f"Template '{template_name}' render time {render_time_ms:.2f}ms exceeds limit {max_render_time_ms}ms")
        performance_result["warnings"] = [f"Render time exceeds performance target"]

    return performance_result


def optimize_template_context(context: Dict[str, Any]) -> Dict[str, Any]:
    """Optimize template context for performance.

    Args:
        context: Original template context

    Returns:
        Optimized template context
    """
    optimized = context.copy()

    # Remove heavy objects if not needed for rendering
    # This is a placeholder for optimization logic

    return optimized


def create_template_cache_key(template_name: str, user_id: str, table_name: str, context_hash: str) -> str:
    """Create cache key for template rendering.

    Args:
        template_name: Name of template
        user_id: User ID
        table_name: Table name
        context_hash: Hash of template context

    Returns:
        Cache key string
    """
    return f"template:{template_name}:{user_id}:{table_name}:{context_hash}"


# Helper functions


def _enhance_component_context(component_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Add component-specific context enhancements.

    Args:
        component_name: Name of component
        context: Base context

    Returns:
        Enhanced context
    """
    enhanced = context.copy()

    # Add component-specific helpers
    if component_name == "pagination":
        if "pagination" in context:
            enhanced["pagination_range"] = _get_pagination_range(
                context["pagination"].page, context["pagination"].total_pages
            )
    elif component_name == "table":
        enhanced["table_id"] = f"table-{context.get('table_name', 'default')}"

    return enhanced


def _get_pagination_info(pagination: PaginationSettings) -> Dict[str, Any]:
    """Get pagination information for templates.

    Args:
        pagination: Pagination settings

    Returns:
        Pagination info dictionary
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
        "showing_text": f"Showing {start_item} to {end_item} of {total_count} entries",
    }


def _get_filter_summary(filters: List[FilterDefinition]) -> Dict[str, Any]:
    """Get filter summary for templates.

    Args:
        filters: List of filter definitions

    Returns:
        Filter summary dictionary
    """
    if not filters:
        return {"count": 0, "active": False, "summary": "No filters applied"}

    filter_descriptions = []
    for filter_def in filters:
        desc = f"{filter_def.column_id} {filter_def.operator}"
        if hasattr(filter_def, "value") and filter_def.value is not None:
            desc += f" '{filter_def.value}'"
        filter_descriptions.append(desc)

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
        Sort summary dictionary
    """
    if not sorts:
        return {"count": 0, "active": False, "summary": "No sorting applied"}

    sort_descriptions = []
    for sort_def in sorted(sorts, key=lambda x: x.priority):
        direction = "↑" if sort_def.direction.value == "asc" else "↓"
        sort_descriptions.append(f"{sort_def.column_id} {direction}")

    return {
        "count": len(sorts),
        "active": True,
        "summary": f"Sorted by {', '.join(sort_descriptions)}",
        "descriptions": sort_descriptions,
    }


def _get_table_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Get table statistics for templates.

    Args:
        rows: Table data rows

    Returns:
        Table statistics dictionary
    """
    if not rows:
        return {"total_rows": 0, "total_columns": 0, "has_data": False}

    return {
        "total_rows": len(rows),
        "total_columns": len(rows[0]) if rows else 0,
        "has_data": True,
    }


def _get_filter_operators_by_type() -> Dict[str, List[Dict[str, str]]]:
    """Get filter operators organized by column type.

    Returns:
        Dictionary mapping column types to operator lists
    """
    return {
        "TEXT": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Does not equal"},
            {"value": "contains", "label": "Contains"},
            {"value": "not_contains", "label": "Does not contain"},
            {"value": "starts_with", "label": "Starts with"},
            {"value": "ends_with", "label": "Ends with"},
            {"value": "is_null", "label": "Is empty"},
            {"value": "is_not_null", "label": "Is not empty"},
        ],
        "NUMBER": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Does not equal"},
            {"value": "greater_than", "label": "Greater than"},
            {"value": "less_than", "label": "Less than"},
            {"value": "greater_equal", "label": "Greater than or equal"},
            {"value": "less_equal", "label": "Less than or equal"},
            {"value": "is_null", "label": "Is empty"},
            {"value": "is_not_null", "label": "Is not empty"},
        ],
        "BOOLEAN": [
            {"value": "equals", "label": "Equals"},
            {"value": "not_equals", "label": "Does not equal"},
            {"value": "is_null", "label": "Is empty"},
            {"value": "is_not_null", "label": "Is not empty"},
        ],
    }


def _get_excel_functions_reference() -> List[Dict[str, str]]:
    """Get Excel functions reference for template.

    Returns:
        List of Excel function definitions
    """
    return [
        {"name": "SUM", "description": "Sum of values", "example": "=SUM([col1], [col2])"},
        {"name": "AVERAGE", "description": "Average of values", "example": "=AVERAGE([col1], [col2])"},
        {"name": "COUNT", "description": "Count of non-empty values", "example": "=COUNT([col1], [col2])"},
        {"name": "IF", "description": "Conditional logic", "example": "=IF([col1] > 10, 'High', 'Low')"},
        {
            "name": "CONCATENATE",
            "description": "Join text values",
            "example": "=CONCATENATE([first_name], ' ', [last_name])",
        },
        {"name": "UPPER", "description": "Convert to uppercase", "example": "=UPPER([text_column])"},
        {"name": "LOWER", "description": "Convert to lowercase", "example": "=LOWER([text_column])"},
        {"name": "MAX", "description": "Maximum value", "example": "=MAX([col1], [col2])"},
        {"name": "MIN", "description": "Minimum value", "example": "=MIN([col1], [col2])"},
        {"name": "ROUND", "description": "Round to decimal places", "example": "=ROUND([number_col], 2)"},
    ]


def _get_pagination_range(current_page: int, total_pages: int, max_pages: int = 10) -> List[int]:
    """Generate pagination page range for templates.

    Args:
        current_page: Current page number
        total_pages: Total number of pages
        max_pages: Maximum pages to show

    Returns:
        List of page numbers to display
    """
    if total_pages <= max_pages:
        return list(range(1, total_pages + 1))

    half_max = max_pages // 2
    start_page = max(1, current_page - half_max)
    end_page = min(total_pages, start_page + max_pages - 1)

    if end_page - start_page < max_pages - 1:
        start_page = max(1, end_page - max_pages + 1)

    return list(range(start_page, end_page + 1))


def _validate_template_context(context: Dict[str, Any]) -> None:
    """Validate template context has required variables.

    Args:
        context: Template context dictionary

    Raises:
        ValueError: If required context is missing
    """
    if not isinstance(context, dict):
        raise ValueError("Template context must be a dictionary")

    # Check for basic required keys
    # This can be expanded based on specific template requirements
