"""
TableV2 Formatting Module

This module provides function-based formatting capabilities for table data with plugin support.
Follows the function-based architecture pattern with no classes except for configuration objects.
"""

from .cell_formatters import (
    format_boolean_cell,
    format_currency_cell,
    format_custom_cell,
    format_date_cell,
    format_datetime_cell,
    format_email_cell,
    format_json_cell,
    format_number_cell,
    format_percentage_cell,
    format_phone_cell,
    format_text_cell,
    format_url_cell,
)
from .formatter_registry import (
    clear_formatter_registry,
    discover_formatter_plugins,
    get_formatter_plugin_stats,
    get_formatter_registry,
    register_formatter_plugin,
    unregister_formatter_plugin,
    validate_formatter_plugin,
)
from .formatting_strategy import (
    apply_formatting_strategy,
    create_formatting_context,
    format_cell_value,
    get_formatter_for_type,
    get_formatting_performance_metrics,
    list_available_formatters,
    register_formatter,
    validate_formatting_options,
)
from .style_generators import (
    build_css_class_map,
    generate_cell_css,
    generate_conditional_css,
    generate_header_css,
    generate_responsive_css,
    generate_row_css,
    generate_table_css,
    generate_theme_css,
    optimize_css_output,
)

__all__ = [
    # Strategy functions
    "format_cell_value",
    "get_formatter_for_type",
    "register_formatter",
    "list_available_formatters",
    "apply_formatting_strategy",
    "create_formatting_context",
    "validate_formatting_options",
    "get_formatting_performance_metrics",
    # Cell formatters
    "format_text_cell",
    "format_number_cell",
    "format_currency_cell",
    "format_percentage_cell",
    "format_date_cell",
    "format_datetime_cell",
    "format_boolean_cell",
    "format_email_cell",
    "format_url_cell",
    "format_phone_cell",
    "format_json_cell",
    "format_custom_cell",
    # Style generators
    "generate_cell_css",
    "generate_table_css",
    "generate_header_css",
    "generate_row_css",
    "generate_conditional_css",
    "generate_theme_css",
    "generate_responsive_css",
    "build_css_class_map",
    "optimize_css_output",
    # Plugin registry
    "register_formatter_plugin",
    "unregister_formatter_plugin",
    "get_formatter_registry",
    "clear_formatter_registry",
    "validate_formatter_plugin",
    "get_formatter_plugin_stats",
    "discover_formatter_plugins",
]
