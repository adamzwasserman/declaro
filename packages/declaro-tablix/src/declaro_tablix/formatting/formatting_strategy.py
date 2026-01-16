"""
Function-based formatting strategy pattern implementation for TableV2.

This module implements the strategy pattern using pure functions instead of classes,
following the project's function-based architecture guidelines.
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from declaro_advise import error, info, warning
from declaro_tablix.domain.models import ColumnType, TableData


class FormattingStrategy(Enum):
    """Enumeration of available formatting strategies."""

    DEFAULT = "default"
    COMPACT = "compact"
    DETAILED = "detailed"
    PRINTABLE = "printable"
    ACCESSIBLE = "accessible"
    MOBILE = "mobile"


@dataclass
class FormattingContext:
    """Context information for formatting operations."""

    strategy: FormattingStrategy
    locale: str = "en-US"
    timezone: str = "UTC"
    theme: str = "default"
    accessibility_mode: bool = False
    mobile_mode: bool = False
    print_mode: bool = False
    custom_options: Dict[str, Any] = None

    def __post_init__(self):
        if self.custom_options is None:
            self.custom_options = {}


@dataclass
class FormattingOptions:
    """Options for specific formatting operations."""

    decimal_places: Optional[int] = None
    currency_code: str = "USD"
    date_format: str = "MM/dd/yyyy"
    time_format: str = "HH:mm:ss"
    boolean_format: str = "yes_no"  # yes_no, true_false, on_off, 1_0
    null_display: str = "—"
    empty_display: str = ""
    max_length: Optional[int] = None
    truncate_with_ellipsis: bool = True
    escape_html: bool = True
    preserve_whitespace: bool = False
    custom_formatters: Dict[str, Callable] = None

    def __post_init__(self):
        if self.custom_formatters is None:
            self.custom_formatters = {}


# Global registry for formatters
_FORMATTER_REGISTRY: Dict[ColumnType, Callable] = {}
_PERFORMANCE_METRICS: Dict[str, List[float]] = {}


def register_formatter(
    column_type: ColumnType, formatter_func: Callable[[Any, FormattingOptions, FormattingContext], str]
) -> None:
    """
    Register a formatter function for a specific column type.

    Args:
        column_type: The column type to register the formatter for
        formatter_func: Function that takes (value, options, context) and returns formatted string
    """
    _FORMATTER_REGISTRY[column_type] = formatter_func
    info(f"Registered formatter for column type: {column_type.value}")


def get_formatter_for_type(column_type: ColumnType) -> Optional[Callable]:
    """
    Get the registered formatter function for a column type.

    Args:
        column_type: The column type to get formatter for

    Returns:
        The formatter function or None if not found
    """
    # Check custom registered formatters first
    if column_type in _FORMATTER_REGISTRY:
        return _FORMATTER_REGISTRY[column_type]

    # Fall back to built-in formatters from cell_formatters.py
    from .cell_formatters import COLUMN_TYPE_FORMATTERS

    # Handle both enum and string column types
    if hasattr(column_type, "value"):
        return COLUMN_TYPE_FORMATTERS.get(column_type.value.upper())
    else:
        return COLUMN_TYPE_FORMATTERS.get(str(column_type).upper())


def list_available_formatters() -> List[str]:
    """
    List all available formatter types.

    Returns:
        List of column type names that have registered formatters
    """
    return [col_type.value for col_type in _FORMATTER_REGISTRY.keys()]


def create_formatting_context(
    strategy: FormattingStrategy = FormattingStrategy.DEFAULT,
    locale: str = "en-US",
    timezone: str = "UTC",
    theme: str = "default",
    accessibility_mode: bool = False,
    mobile_mode: bool = False,
    print_mode: bool = False,
    custom_options: Optional[Dict[str, Any]] = None,
) -> FormattingContext:
    """
    Create a formatting context with the specified options.

    Args:
        strategy: The formatting strategy to use
        locale: Locale for formatting (e.g., "en-US", "es-ES")
        timezone: Timezone for date/time formatting
        theme: Theme name for styling
        accessibility_mode: Whether to enable accessibility features
        mobile_mode: Whether to optimize for mobile display
        print_mode: Whether to optimize for printing
        custom_options: Additional custom options

    Returns:
        FormattingContext object
    """
    return FormattingContext(
        strategy=strategy,
        locale=locale,
        timezone=timezone,
        theme=theme,
        accessibility_mode=accessibility_mode,
        mobile_mode=mobile_mode,
        print_mode=print_mode,
        custom_options=custom_options or {},
    )


def validate_formatting_options(options: FormattingOptions) -> List[str]:
    """
    Validate formatting options and return any validation errors.

    Args:
        options: The formatting options to validate

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    # Validate decimal places
    if options.decimal_places is not None and options.decimal_places < 0:
        errors.append("decimal_places cannot be negative")

    # Validate currency code
    if len(options.currency_code) != 3:
        errors.append("currency_code must be 3 characters long")

    # Validate boolean format
    valid_boolean_formats = ["yes_no", "true_false", "on_off", "1_0"]
    if options.boolean_format not in valid_boolean_formats:
        errors.append(f"boolean_format must be one of: {', '.join(valid_boolean_formats)}")

    # Validate max length
    if options.max_length is not None and options.max_length <= 0:
        errors.append("max_length must be positive if specified")

    return errors


def format_cell_value(value: Any, column_type: ColumnType, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a cell value using the appropriate formatter for its type.

    Args:
        value: The value to format
        column_type: The type of the column
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted string representation of the value
    """
    start_time = time.time()

    try:
        # Handle null/None values
        if value is None:
            return options.null_display

        # Handle empty string values
        if value == "":
            return options.empty_display

        # Get formatter for column type
        formatter = get_formatter_for_type(column_type)
        if formatter is None:
            column_type_str = column_type.value if hasattr(column_type, "value") else str(column_type)
            warning(f"No formatter found for column type: {column_type_str}")
            return str(value)

        # Apply formatting
        formatted_value = formatter(value, options, context)

        # Apply length restrictions if specified
        if options.max_length is not None and len(formatted_value) > options.max_length:
            if options.truncate_with_ellipsis:
                formatted_value = formatted_value[: options.max_length - 3] + "..."
            else:
                formatted_value = formatted_value[: options.max_length]

        # Record performance metrics
        execution_time = time.time() - start_time
        metric_key = f"format_{column_type.value}"
        if metric_key not in _PERFORMANCE_METRICS:
            _PERFORMANCE_METRICS[metric_key] = []
        _PERFORMANCE_METRICS[metric_key].append(execution_time)

        return formatted_value

    except Exception as e:
        column_type_str = column_type.value if hasattr(column_type, "value") else str(column_type)
        error(f"Error formatting value {value} for column type {column_type_str}: {str(e)}")
        return str(value)  # Fallback to string representation


def apply_formatting_strategy(table_data: TableData, context: FormattingContext, options: FormattingOptions) -> TableData:
    """
    Apply formatting strategy to entire table data.

    Args:
        table_data: The table data to format
        context: Formatting context
        options: Formatting options

    Returns:
        New TableData with formatted values
    """
    start_time = time.time()

    try:
        # Validate options
        validation_errors = validate_formatting_options(options)
        if validation_errors:
            error(f"Invalid formatting options: {', '.join(validation_errors)}")
            return table_data

        # Create column type mapping
        column_type_map = {col.id: col.type for col in table_data.columns}

        # Format each row
        formatted_rows = []
        for row in table_data.rows:
            formatted_row = {}
            for column_id, value in row.items():
                column_type = column_type_map.get(column_id, ColumnType.TEXT)
                formatted_value = format_cell_value(value, column_type, options, context)
                formatted_row[column_id] = formatted_value
            formatted_rows.append(formatted_row)

        # Create new TableData with formatted values
        formatted_table_data = TableData(
            table_name=table_data.table_name,
            columns=table_data.columns,
            rows=formatted_rows,
            total_count=table_data.total_count,
            filtered_count=table_data.filtered_count,
            pagination=table_data.pagination,
            sorts=table_data.sorts,
            filters=table_data.filters,
            search_term=table_data.search_term,
        )

        # Record performance metrics
        execution_time = time.time() - start_time
        if "format_table" not in _PERFORMANCE_METRICS:
            _PERFORMANCE_METRICS["format_table"] = []
        _PERFORMANCE_METRICS["format_table"].append(execution_time)

        info(
            f"Applied formatting strategy '{context.strategy.value}' to {len(formatted_rows)} rows in {execution_time:.3f}s"
        )

        return formatted_table_data

    except Exception as e:
        error(f"Error applying formatting strategy: {str(e)}")
        return table_data


def get_formatting_performance_metrics() -> Dict[str, Dict[str, float]]:
    """
    Get performance metrics for formatting operations.

    Returns:
        Dictionary with performance statistics for each formatter
    """
    metrics = {}

    for metric_key, times in _PERFORMANCE_METRICS.items():
        if times:
            metrics[metric_key] = {
                "count": len(times),
                "total_time": sum(times),
                "avg_time": sum(times) / len(times),
                "min_time": min(times),
                "max_time": max(times),
            }

    return metrics


def clear_performance_metrics() -> None:
    """Clear all performance metrics."""
    _PERFORMANCE_METRICS.clear()
    info("Cleared formatting performance metrics")


def get_strategy_configuration(strategy: FormattingStrategy) -> Dict[str, Any]:
    """
    Get the default configuration for a formatting strategy.

    Args:
        strategy: The formatting strategy

    Returns:
        Dictionary with strategy-specific configuration
    """
    configurations = {
        FormattingStrategy.DEFAULT: {
            "max_length": None,
            "truncate_with_ellipsis": True,
            "decimal_places": 2,
            "date_format": "MM/dd/yyyy",
            "time_format": "HH:mm:ss",
        },
        FormattingStrategy.COMPACT: {
            "max_length": 20,
            "truncate_with_ellipsis": True,
            "decimal_places": 1,
            "date_format": "MM/dd/yy",
            "time_format": "HH:mm",
        },
        FormattingStrategy.DETAILED: {
            "max_length": None,
            "truncate_with_ellipsis": False,
            "decimal_places": 4,
            "date_format": "MMMM dd, yyyy",
            "time_format": "HH:mm:ss.SSS",
        },
        FormattingStrategy.PRINTABLE: {
            "max_length": None,
            "truncate_with_ellipsis": False,
            "decimal_places": 2,
            "date_format": "MMMM dd, yyyy",
            "time_format": "HH:mm:ss",
            "escape_html": False,
        },
        FormattingStrategy.ACCESSIBLE: {
            "max_length": None,
            "truncate_with_ellipsis": False,
            "decimal_places": 2,
            "date_format": "MMMM dd, yyyy",
            "time_format": "HH:mm:ss",
            "preserve_whitespace": True,
        },
        FormattingStrategy.MOBILE: {
            "max_length": 15,
            "truncate_with_ellipsis": True,
            "decimal_places": 1,
            "date_format": "MM/dd",
            "time_format": "HH:mm",
        },
    }

    return configurations.get(strategy, configurations[FormattingStrategy.DEFAULT])


def create_formatting_options_from_strategy(
    strategy: FormattingStrategy, custom_overrides: Optional[Dict[str, Any]] = None
) -> FormattingOptions:
    """
    Create formatting options based on a strategy with optional custom overrides.

    Args:
        strategy: The formatting strategy to use as base
        custom_overrides: Optional dictionary of custom option overrides

    Returns:
        FormattingOptions configured for the strategy
    """
    base_config = get_strategy_configuration(strategy)

    # Apply custom overrides if provided
    if custom_overrides:
        base_config.update(custom_overrides)

    return FormattingOptions(**base_config)
