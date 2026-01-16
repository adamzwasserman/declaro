"""
Type-specific cell formatting functions for TableV2.

This module provides individual formatter functions for each column type,
following the function-based architecture pattern.
"""

import html
import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

from .formatting_strategy import FormattingContext, FormattingOptions, FormattingStrategy


def format_text_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a text cell value.

    Args:
        value: The text value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted text string
    """
    if value is None:
        return options.null_display

    text_value = str(value)

    # Handle empty strings
    if text_value == "":
        return options.empty_display

    # Preserve whitespace if requested
    if not options.preserve_whitespace:
        text_value = text_value.strip()

    # Escape HTML if requested
    if options.escape_html:
        text_value = html.escape(text_value)

    # Apply strategy-specific formatting
    if context.strategy == FormattingStrategy.COMPACT:
        # Remove extra whitespace for compact display
        text_value = re.sub(r"\s+", " ", text_value)
    elif context.strategy == FormattingStrategy.ACCESSIBLE:
        # Preserve all whitespace and add screen reader hints
        text_value = text_value.replace("\n", " [new line] ")
        text_value = text_value.replace("\t", " [tab] ")

    return text_value


def format_number_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a number cell value.

    Args:
        value: The number value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted number string
    """
    if value is None:
        return options.null_display

    try:
        # Convert to Decimal for precise formatting
        if isinstance(value, str):
            decimal_value = Decimal(value)
        else:
            decimal_value = Decimal(str(value))

        # Apply decimal places
        if options.decimal_places is not None:
            decimal_value = decimal_value.quantize(Decimal("0." + "0" * options.decimal_places))

        # Format based on strategy
        if context.strategy == FormattingStrategy.COMPACT:
            # Use scientific notation for very large/small numbers
            if abs(decimal_value) >= 1000000 or (abs(decimal_value) < 0.001 and decimal_value != 0):
                return f"{float(decimal_value):.2e}"

        # Format with locale-specific number formatting
        formatted_value = str(decimal_value)

        # Add thousands separators for accessibility
        if context.accessibility_mode and abs(decimal_value) >= 1000:
            parts = formatted_value.split(".")
            parts[0] = re.sub(r"(\d)(?=(\d{3})+(?!\d))", r"\1,", parts[0])
            formatted_value = ".".join(parts)

        return formatted_value

    except (InvalidOperation, ValueError):
        return str(value)


def format_currency_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a currency cell value.

    Args:
        value: The currency value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted currency string
    """
    if value is None:
        return options.null_display

    try:
        # Convert to Decimal for precise currency formatting
        if isinstance(value, str):
            decimal_value = Decimal(value)
        else:
            decimal_value = Decimal(str(value))

        # Apply currency decimal places (default 2)
        decimal_places = options.decimal_places if options.decimal_places is not None else 2
        decimal_value = decimal_value.quantize(Decimal("0." + "0" * decimal_places))

        # Currency symbol mapping
        currency_symbols = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CAD": "CA$", "AUD": "A$"}

        symbol = currency_symbols.get(options.currency_code, f"{options.currency_code} ")

        # Format the number part
        number_str = str(abs(decimal_value))

        # Add thousands separators
        if "." in number_str:
            integer_part, decimal_part = number_str.split(".")
        else:
            integer_part, decimal_part = number_str, "00"

        # Add thousands separators
        if len(integer_part) > 3:
            integer_part = re.sub(r"(\d)(?=(\d{3})+(?!\d))", r"\1,", integer_part)

        formatted_number = f"{integer_part}.{decimal_part}"

        # Handle negative values
        if decimal_value < 0:
            if context.strategy == FormattingStrategy.ACCESSIBLE:
                return f"negative {symbol}{formatted_number}"
            else:
                return f"-{symbol}{formatted_number}"
        else:
            return f"{symbol}{formatted_number}"

    except (InvalidOperation, ValueError):
        return str(value)


def format_percentage_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a percentage cell value.

    Args:
        value: The percentage value to format (0.25 = 25%)
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted percentage string
    """
    if value is None:
        return options.null_display

    try:
        # Convert to Decimal for precise percentage formatting
        if isinstance(value, str):
            decimal_value = Decimal(value)
        else:
            decimal_value = Decimal(str(value))

        # Convert to percentage (multiply by 100)
        percentage_value = decimal_value * 100

        # Apply decimal places (default 1 for percentages)
        decimal_places = options.decimal_places if options.decimal_places is not None else 1
        percentage_value = percentage_value.quantize(Decimal("0." + "0" * decimal_places))

        formatted_value = str(percentage_value)

        # Add accessibility context
        if context.accessibility_mode:
            return f"{formatted_value} percent"
        else:
            return f"{formatted_value}%"

    except (InvalidOperation, ValueError):
        return str(value)


def format_date_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a date cell value.

    Args:
        value: The date value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted date string
    """
    if value is None:
        return options.null_display

    try:
        # Parse date value
        if isinstance(value, str):
            # Try common date formats
            date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"]

            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(value, fmt).date()
                    break
                except ValueError:
                    continue

            if parsed_date is None:
                return str(value)

        elif isinstance(value, datetime):
            parsed_date = value.date()
        elif isinstance(value, date):
            parsed_date = value
        else:
            return str(value)

        # Format based on strategy
        if context.strategy == FormattingStrategy.COMPACT:
            return parsed_date.strftime("%m/%d/%y")
        elif context.strategy == FormattingStrategy.DETAILED:
            return parsed_date.strftime("%B %d, %Y")
        elif context.strategy == FormattingStrategy.ACCESSIBLE:
            return parsed_date.strftime("%B %d, %Y")
        else:
            # Convert our date format to Python strftime format
            python_format = options.date_format
            # Simple mapping of common formats
            python_format = python_format.replace("MM", "%m")
            python_format = python_format.replace("dd", "%d")
            python_format = python_format.replace("yyyy", "%Y")
            python_format = python_format.replace("yy", "%y")
            return parsed_date.strftime(python_format)

    except (ValueError, AttributeError):
        return str(value)


def format_datetime_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a datetime cell value.

    Args:
        value: The datetime value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted datetime string
    """
    if value is None:
        return options.null_display

    try:
        # Parse datetime value
        if isinstance(value, str):
            # Try common datetime formats
            datetime_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%m/%d/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
            ]

            parsed_datetime = None
            for fmt in datetime_formats:
                try:
                    parsed_datetime = datetime.strptime(value, fmt)
                    break
                except ValueError:
                    continue

            if parsed_datetime is None:
                return str(value)

        elif isinstance(value, datetime):
            parsed_datetime = value
        else:
            return str(value)

        # Format based on strategy
        if context.strategy == FormattingStrategy.COMPACT:
            return parsed_datetime.strftime("%m/%d/%y %H:%M")
        elif context.strategy == FormattingStrategy.DETAILED:
            return parsed_datetime.strftime("%B %d, %Y at %H:%M:%S")
        elif context.strategy == FormattingStrategy.ACCESSIBLE:
            return parsed_datetime.strftime("%B %d, %Y at %H:%M:%S")
        else:
            date_part = parsed_datetime.strftime(options.date_format)
            time_part = parsed_datetime.strftime(options.time_format)
            return f"{date_part} {time_part}"

    except (ValueError, AttributeError):
        return str(value)


def format_boolean_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a boolean cell value.

    Args:
        value: The boolean value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted boolean string
    """
    if value is None:
        return options.null_display

    # Convert to boolean
    if isinstance(value, str):
        bool_value = value.lower() in ("true", "yes", "on", "1", "y")
    else:
        bool_value = bool(value)

    # Format based on boolean_format option
    format_map = {"yes_no": ("Yes", "No"), "true_false": ("True", "False"), "on_off": ("On", "Off"), "1_0": ("1", "0")}

    true_text, false_text = format_map.get(options.boolean_format, ("Yes", "No"))

    # Apply accessibility formatting
    if context.accessibility_mode:
        result = true_text if bool_value else false_text
        return f"{result} (boolean value)"
    else:
        return true_text if bool_value else false_text


def format_email_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format an email cell value.

    Args:
        value: The email value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted email string
    """
    if value is None:
        return options.null_display

    email_str = str(value).strip()

    if email_str == "":
        return options.empty_display

    # Basic email validation
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(email_pattern, email_str):
        return email_str  # Return as-is if invalid

    # Escape HTML if requested
    if options.escape_html:
        email_str = html.escape(email_str)

    # Format based on strategy
    if context.strategy == FormattingStrategy.COMPACT:
        # Show only username part for compact display
        username = email_str.split("@")[0]
        return f"{username}@..."
    elif context.strategy == FormattingStrategy.ACCESSIBLE:
        return f"Email: {email_str}"
    else:
        return email_str


def format_url_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a URL cell value.

    Args:
        value: The URL value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted URL string
    """
    if value is None:
        return options.null_display

    url_str = str(value).strip()

    if url_str == "":
        return options.empty_display

    try:
        # Parse URL to validate and extract components
        parsed = urlparse(url_str)

        if not parsed.scheme:
            # Add http:// if no scheme provided
            url_str = f"http://{url_str}"
            parsed = urlparse(url_str)

        # Escape HTML if requested
        if options.escape_html:
            url_str = html.escape(url_str)

        # Format based on strategy
        if context.strategy == FormattingStrategy.COMPACT:
            # Show only domain for compact display
            return parsed.netloc or url_str
        elif context.strategy == FormattingStrategy.ACCESSIBLE:
            return f"Link: {url_str}"
        else:
            return url_str

    except Exception:
        return url_str


def format_phone_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a phone number cell value.

    Args:
        value: The phone number value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted phone number string
    """
    if value is None:
        return options.null_display

    phone_str = str(value).strip()

    if phone_str == "":
        return options.empty_display

    # Remove all non-digit characters
    digits = re.sub(r"\D", "", phone_str)

    # Format based on length
    if len(digits) == 10:
        # US phone number format
        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == "1":
        # US phone number with country code
        formatted = f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        # International or unknown format
        formatted = phone_str

    # Apply accessibility formatting
    if context.accessibility_mode:
        return f"Phone: {formatted}"
    else:
        return formatted


def format_json_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a JSON cell value.

    Args:
        value: The JSON value to format
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted JSON string
    """
    if value is None:
        return options.null_display

    try:
        # Parse JSON if it's a string
        if isinstance(value, str):
            json_obj = json.loads(value)
        else:
            json_obj = value

        # Format based on strategy
        if context.strategy == FormattingStrategy.COMPACT:
            # Single line, no indentation
            formatted = json.dumps(json_obj, separators=(",", ":"))
        elif context.strategy == FormattingStrategy.DETAILED:
            # Pretty print with indentation
            formatted = json.dumps(json_obj, indent=2, sort_keys=True)
        else:
            # Standard formatting
            formatted = json.dumps(json_obj, indent=None, sort_keys=True)

        # Escape HTML if requested
        if options.escape_html:
            formatted = html.escape(formatted)

        return formatted

    except (json.JSONDecodeError, TypeError):
        return str(value)


def format_custom_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format a cell value using custom formatters.

    Args:
        value: The value to format
        options: Formatting options (should contain custom_formatters)
        context: Formatting context

    Returns:
        Formatted string using custom formatter or fallback
    """
    if value is None:
        return options.null_display

    # Check if there are custom formatters available
    if not options.custom_formatters:
        return str(value)

    # Try to find a custom formatter for this value type
    value_type = type(value).__name__
    custom_formatter = options.custom_formatters.get(value_type)

    if custom_formatter and callable(custom_formatter):
        try:
            return custom_formatter(value, options, context)
        except Exception:
            # Fall back to string representation if custom formatter fails
            return str(value)

    return str(value)


# Mapping of column types to formatter functions
COLUMN_TYPE_FORMATTERS = {
    "TEXT": format_text_cell,
    "NUMBER": format_number_cell,
    "CURRENCY": format_currency_cell,
    "PERCENTAGE": format_percentage_cell,
    "DATE": format_date_cell,
    "DATETIME": format_datetime_cell,
    "BOOLEAN": format_boolean_cell,
    "EMAIL": format_email_cell,
    "URL": format_url_cell,
    "PHONE": format_phone_cell,
    "JSON": format_json_cell,
    "CUSTOM": format_custom_cell,
}


def get_formatter_for_column_type(column_type: str):
    """
    Get the appropriate formatter function for a column type.

    Args:
        column_type: The column type string

    Returns:
        The formatter function or None if not found
    """
    return COLUMN_TYPE_FORMATTERS.get(column_type.upper())


def register_column_type_formatter(column_type: str, formatter_func):
    """
    Register a custom formatter for a column type.

    Args:
        column_type: The column type string
        formatter_func: The formatter function
    """
    COLUMN_TYPE_FORMATTERS[column_type.upper()] = formatter_func


def list_supported_column_types():
    """
    List all supported column types.

    Returns:
        List of supported column type strings
    """
    return list(COLUMN_TYPE_FORMATTERS.keys())
