"""
Financial data formatters for TableV2.

This module provides specialized formatting functions for financial data types
following the function-based architecture pattern.
"""

import re
from decimal import Decimal
from typing import Any, Dict, Optional

from declaro_tablix.domain.financial_columns import FinancialColumnOptions, FinancialColumnType
from declaro_tablix.formatting.formatting_strategy import FormattingContext, FormattingOptions


def format_ticker_symbol_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format ticker symbol with optional exchange prefix and linking.

    Args:
        value: Ticker symbol string
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted ticker symbol HTML
    """
    if not value:
        return options.null_display

    # Get financial options from context
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Clean and uppercase the ticker symbol
    ticker = str(value).strip().upper()

    # Add exchange prefix if configured
    if financial_options.show_exchange_prefix and ":" not in ticker:
        # Infer exchange from context or use default
        exchange = getattr(context, "exchange", "NYSE")
        ticker = f"{exchange}:{ticker}"

    # Create clickable link if enabled
    if financial_options.ticker_link_enabled:
        return f'<a href="#" class="ticker-link" data-symbol="{ticker.split(":")[-1]}">{ticker}</a>'

    return ticker


def format_stock_price_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format stock price with currency symbol and proper decimal places.

    Args:
        value: Stock price as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted price string
    """
    if value is None or value == "":
        return options.null_display

    try:
        price = Decimal(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Format with specified decimal places
    decimal_places = financial_options.decimal_places
    currency_symbol = financial_options.currency_symbol

    # Format the price
    if price == 0:
        return f"{currency_symbol}0.00"

    formatted_price = f"{price:.{decimal_places}f}"
    return f"{currency_symbol}{formatted_price}"


def format_price_change_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format price change with color coding and optional plus sign.

    Args:
        value: Price change as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted price change HTML with color styling
    """
    if value is None or value == "":
        return options.null_display

    try:
        change = Decimal(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Determine color based on value
    if change > 0:
        color = financial_options.positive_color
        prefix = "+" if financial_options.show_plus_sign else ""
    elif change < 0:
        color = financial_options.negative_color
        prefix = ""
    else:
        color = financial_options.neutral_color
        prefix = ""

    # Format the change
    decimal_places = financial_options.decimal_places
    currency_symbol = financial_options.currency_symbol
    formatted_change = f"{change:.{decimal_places}f}"

    return f'<span style="color: {color}">{prefix}{currency_symbol}{formatted_change}</span>'


def format_price_change_percent_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format percentage change with color coding and plus sign.

    Args:
        value: Percentage change as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted percentage change HTML with color styling
    """
    if value is None or value == "":
        return options.null_display

    try:
        percent = Decimal(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Determine color based on value
    if percent > 0:
        color = financial_options.positive_color
        prefix = "+" if financial_options.show_plus_sign else ""
    elif percent < 0:
        color = financial_options.negative_color
        prefix = ""
    else:
        color = financial_options.neutral_color
        prefix = ""

    # Format the percentage
    decimal_places = financial_options.decimal_places
    percentage_symbol = financial_options.percentage_symbol
    formatted_percent = f"{percent:.{decimal_places}f}"

    return f'<span style="color: {color}">{prefix}{formatted_percent}{percentage_symbol}</span>'


def format_trading_volume_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format trading volume with abbreviated notation (K, M, B).

    Args:
        value: Trading volume as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted volume string
    """
    if value is None or value == "":
        return options.null_display

    try:
        volume = int(float(str(value)))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Format based on notation preference
    if financial_options.volume_notation == "full":
        return f"{volume:,}"
    elif financial_options.volume_notation == "scientific":
        return f"{volume:.2e}"
    else:  # abbreviated
        if volume >= 1_000_000_000:
            return f"{volume / 1_000_000_000:.1f}B"
        elif volume >= 1_000_000:
            return f"{volume / 1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"{volume / 1_000:.1f}K"
        else:
            return str(volume)


def format_market_cap_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format market capitalization with abbreviated notation.

    Args:
        value: Market cap as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted market cap string
    """
    if value is None or value == "":
        return options.null_display

    try:
        market_cap = float(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    currency_symbol = financial_options.currency_symbol

    # Format based on notation preference
    if financial_options.market_cap_notation == "full":
        return f"{currency_symbol}{market_cap:,.0f}"
    else:  # abbreviated
        if market_cap >= 1_000_000_000_000:  # Trillion
            return f"{currency_symbol}{market_cap / 1_000_000_000_000:.1f}T"
        elif market_cap >= 1_000_000_000:  # Billion
            return f"{currency_symbol}{market_cap / 1_000_000_000:.1f}B"
        elif market_cap >= 1_000_000:  # Million
            return f"{currency_symbol}{market_cap / 1_000_000:.1f}M"
        elif market_cap >= 1_000:  # Thousand
            return f"{currency_symbol}{market_cap / 1_000:.1f}K"
        else:
            return f"{currency_symbol}{market_cap:.0f}"


def format_pe_ratio_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format P/E ratio with proper decimal places and N/A handling.

    Args:
        value: P/E ratio as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted P/E ratio string
    """
    if value is None or value == "" or value == 0:
        return "N/A"

    try:
        pe_ratio = Decimal(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Handle negative or very high P/E ratios
    if pe_ratio < 0:
        return "N/A"
    elif pe_ratio > 1000:
        return ">1000"

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    decimal_places = financial_options.decimal_places
    return f"{pe_ratio:.{decimal_places}f}"


def format_sector_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format sector with proper capitalization and color coding.

    Args:
        value: Sector string
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted sector string with optional color coding
    """
    if not value:
        return options.null_display

    sector = str(value).strip()

    # Capitalize properly
    sector = " ".join(word.capitalize() for word in sector.split())

    # Optional: Add sector-specific color coding
    sector_colors = {
        "Technology": "#3b82f6",
        "Healthcare": "#10b981",
        "Financial Services": "#f59e0b",
        "Energy": "#ef4444",
        "Consumer Discretionary": "#8b5cf6",
        "Industrials": "#6b7280",
        "Materials": "#84cc16",
        "Utilities": "#06b6d4",
        "Real Estate": "#f97316",
        "Consumer Staples": "#ec4899",
    }

    color = sector_colors.get(sector, "#374151")
    return f'<span style="color: {color}; font-weight: 500">{sector}</span>'


def format_exchange_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format exchange with proper styling and badges.

    Args:
        value: Exchange string
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted exchange string with badge styling
    """
    if not value:
        return options.null_display

    exchange = str(value).strip().upper()

    # Exchange-specific styling
    exchange_styles = {
        "NYSE": "background: #1f2937; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;",
        "NASDAQ": "background: #059669; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;",
        "TSX": "background: #dc2626; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;",
        "LSE": "background: #7c3aed; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;",
    }

    style = exchange_styles.get(
        exchange, "background: #6b7280; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.75rem;"
    )
    return f'<span style="{style}">{exchange}</span>'


def format_rsi_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format RSI with color coding based on overbought/oversold levels.

    Args:
        value: RSI value as number
        options: Formatting options
        context: Formatting context with financial options

    Returns:
        Formatted RSI string with color coding
    """
    if value is None or value == "":
        return options.null_display

    try:
        rsi = Decimal(str(value))
    except (ValueError, TypeError):
        return options.error_display

    # Get financial options
    financial_options = getattr(context, "financial_options", None)
    if not financial_options:
        financial_options = FinancialColumnOptions()

    # Determine color based on RSI levels
    if rsi >= financial_options.rsi_overbought_threshold:
        color = financial_options.negative_color  # Red for overbought
        signal = " (Overbought)"
    elif rsi <= financial_options.rsi_oversold_threshold:
        color = financial_options.positive_color  # Green for oversold (buying opportunity)
        signal = " (Oversold)"
    else:
        color = financial_options.neutral_color
        signal = ""

    formatted_rsi = f"{rsi:.1f}"
    return f'<span style="color: {color}">{formatted_rsi}{signal}</span>'


def format_last_updated_cell(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
    """
    Format last updated timestamp with relative time.

    Args:
        value: Timestamp string or datetime object
        options: Formatting options
        context: Formatting context

    Returns:
        Formatted timestamp with relative time indication
    """
    if not value:
        return options.null_display

    from datetime import datetime, timezone

    import dateutil.parser

    try:
        # Parse the timestamp
        if isinstance(value, str):
            timestamp = dateutil.parser.parse(value)
        else:
            timestamp = value

        # Ensure timezone awareness
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        # Calculate time difference
        now = datetime.now(timezone.utc)
        diff = now - timestamp

        # Format relative time
        if diff.total_seconds() < 60:
            relative = "Just now"
            color = "#22c55e"  # Green for very recent
        elif diff.total_seconds() < 3600:  # Less than 1 hour
            minutes = int(diff.total_seconds() / 60)
            relative = f"{minutes}m ago"
            color = "#22c55e" if minutes < 5 else "#f59e0b"  # Green for recent, yellow for older
        elif diff.total_seconds() < 86400:  # Less than 1 day
            hours = int(diff.total_seconds() / 3600)
            relative = f"{hours}h ago"
            color = "#f59e0b"  # Yellow for hours old
        else:
            days = diff.days
            relative = f"{days}d ago"
            color = "#ef4444"  # Red for old data

        # Format the display
        formatted_time = timestamp.strftime("%H:%M")
        return f'<span style="color: {color}" title="{timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")}">{relative}</span>'

    except (ValueError, TypeError) as e:
        return options.error_display


# Registry of financial formatters
FINANCIAL_FORMATTERS = {
    FinancialColumnType.TICKER_SYMBOL: format_ticker_symbol_cell,
    FinancialColumnType.STOCK_PRICE: format_stock_price_cell,
    FinancialColumnType.PRICE_CHANGE: format_price_change_cell,
    FinancialColumnType.PRICE_CHANGE_PERCENT: format_price_change_percent_cell,
    FinancialColumnType.TRADING_VOLUME: format_trading_volume_cell,
    FinancialColumnType.MARKET_CAP: format_market_cap_cell,
    FinancialColumnType.PE_RATIO: format_pe_ratio_cell,
    FinancialColumnType.SECTOR: format_sector_cell,
    FinancialColumnType.EXCHANGE: format_exchange_cell,
    FinancialColumnType.RSI: format_rsi_cell,
    FinancialColumnType.LAST_UPDATED: format_last_updated_cell,
}


def get_financial_formatter(financial_type: FinancialColumnType):
    """
    Get the formatter function for a financial column type.

    Args:
        financial_type: Financial column type

    Returns:
        Formatter function or None if not found
    """
    return FINANCIAL_FORMATTERS.get(financial_type)


def register_financial_formatter(financial_type: FinancialColumnType, formatter_func):
    """
    Register a custom formatter for a financial column type.

    Args:
        financial_type: Financial column type
        formatter_func: Formatter function
    """
    FINANCIAL_FORMATTERS[financial_type] = formatter_func
