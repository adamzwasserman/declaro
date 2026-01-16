"""
Financial column type definitions for TableV2.

This module extends the base ColumnType enum with financial-specific
data types following the function-based architecture pattern.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from declaro_tablix.domain.models import ColumnDefinition, ColumnType


class FinancialColumnType(str, Enum):
    """Financial-specific column types extending base ColumnType."""

    # Basic financial data types
    TICKER_SYMBOL = "ticker_symbol"
    STOCK_PRICE = "stock_price"
    PRICE_CHANGE = "price_change"
    PRICE_CHANGE_PERCENT = "price_change_percent"

    # Volume and market data
    TRADING_VOLUME = "trading_volume"
    MARKET_CAP = "market_cap"
    SHARES_OUTSTANDING = "shares_outstanding"

    # Financial ratios and metrics
    PE_RATIO = "pe_ratio"
    EPS = "eps"
    DIVIDEND_YIELD = "dividend_yield"
    BETA = "beta"

    # Exchange and categorization
    EXCHANGE = "exchange"
    SECTOR = "sector"
    INDUSTRY = "industry"
    MARKET_CATEGORY = "market_category"

    # Time-based data
    LAST_UPDATED = "last_updated"
    TRADING_SESSION = "trading_session"

    # Advanced metrics
    RSI = "rsi"
    MOVING_AVERAGE = "moving_average"
    BOLLINGER_BANDS = "bollinger_bands"


class FinancialColumnOptions(BaseModel):
    """Configuration options for financial column types."""

    # Price formatting options
    currency_symbol: str = Field(default="$", description="Currency symbol to display")
    decimal_places: int = Field(default=2, description="Number of decimal places")

    # Volume formatting options
    volume_notation: str = Field(default="abbreviated", description="Volume display format (full, abbreviated, scientific)")

    # Percentage formatting
    percentage_symbol: str = Field(default="%", description="Percentage symbol")
    show_plus_sign: bool = Field(default=True, description="Show + sign for positive changes")

    # Color coding
    positive_color: str = Field(default="#22c55e", description="Color for positive values")
    negative_color: str = Field(default="#ef4444", description="Color for negative values")
    neutral_color: str = Field(default="#6b7280", description="Color for neutral values")

    # Ticker symbol options
    show_exchange_prefix: bool = Field(default=True, description="Show exchange prefix (TSX:, NYSE:)")
    ticker_link_enabled: bool = Field(default=True, description="Make ticker symbols clickable")

    # Market cap formatting
    market_cap_notation: str = Field(default="abbreviated", description="Market cap format (full, abbreviated)")

    # Technical indicators
    rsi_overbought_threshold: float = Field(default=70.0, description="RSI overbought threshold")
    rsi_oversold_threshold: float = Field(default=30.0, description="RSI oversold threshold")


class FinancialColumnDefinition(ColumnDefinition):
    """Extended column definition for financial data types."""

    financial_type: Optional[FinancialColumnType] = Field(None, description="Financial-specific column type")
    financial_options: Optional[FinancialColumnOptions] = Field(None, description="Financial formatting options")

    # Real-time data options
    real_time_enabled: bool = Field(default=False, description="Enable real-time data updates")
    update_interval_seconds: int = Field(default=30, description="Real-time update interval")

    # Calculation options
    calculated_field: bool = Field(default=False, description="Is this a calculated field")
    calculation_formula: Optional[str] = Field(None, description="Formula for calculated fields")

    # Historical data options
    historical_data_enabled: bool = Field(default=False, description="Enable historical data sparklines")
    historical_period: str = Field(default="1D", description="Historical data period (1D, 5D, 1M, etc.)")


def create_financial_column_definitions() -> Dict[str, FinancialColumnDefinition]:
    """
    Create standard financial column definitions for common market data tables.

    Returns:
        Dictionary of column definitions keyed by column ID
    """

    columns = {}

    # Ticker Symbol Column
    columns["ticker_symbol"] = FinancialColumnDefinition(
        id="ticker_symbol",
        name="Symbol",
        type=ColumnType.TEXT,
        financial_type=FinancialColumnType.TICKER_SYMBOL,
        width=120,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(show_exchange_prefix=True, ticker_link_enabled=True),
        format_options={"font_weight": "bold", "text_transform": "uppercase"},
    )

    # Company Name Column
    columns["company_name"] = FinancialColumnDefinition(
        id="company_name",
        name="Company",
        type=ColumnType.TEXT,
        width=200,
        sortable=True,
        filterable=True,
        visible=True,
        format_options={"max_length": 50, "truncate_with_tooltip": True},
    )

    # Stock Price Column
    columns["stock_price"] = FinancialColumnDefinition(
        id="stock_price",
        name="Price",
        type=ColumnType.CURRENCY,
        financial_type=FinancialColumnType.STOCK_PRICE,
        width=100,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(currency_symbol="$", decimal_places=2),
        real_time_enabled=True,
        update_interval_seconds=15,
        format_options={"text_align": "right", "font_family": "monospace", "currency_symbol": "$"},
    )

    # Price Change Column
    columns["price_change"] = FinancialColumnDefinition(
        id="price_change",
        name="Change",
        type=ColumnType.CURRENCY,
        financial_type=FinancialColumnType.PRICE_CHANGE,
        width=100,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(
            currency_symbol="$", decimal_places=2, show_plus_sign=True, positive_color="#22c55e", negative_color="#ef4444"
        ),
        real_time_enabled=True,
        format_options={"text_align": "right", "font_family": "monospace", "currency_symbol": "$"},
    )

    # Price Change Percentage Column
    columns["price_change_percent"] = FinancialColumnDefinition(
        id="price_change_percent",
        name="Change %",
        type=ColumnType.PERCENTAGE,
        financial_type=FinancialColumnType.PRICE_CHANGE_PERCENT,
        width=100,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(
            decimal_places=2, show_plus_sign=True, positive_color="#22c55e", negative_color="#ef4444"
        ),
        real_time_enabled=True,
        format_options={"text_align": "right", "font_family": "monospace"},
    )

    # Trading Volume Column
    columns["trading_volume"] = FinancialColumnDefinition(
        id="trading_volume",
        name="Volume",
        type=ColumnType.NUMBER,
        financial_type=FinancialColumnType.TRADING_VOLUME,
        width=120,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(volume_notation="abbreviated"),
        format_options={"text_align": "right", "font_family": "monospace"},
    )

    # Market Cap Column
    columns["market_cap"] = FinancialColumnDefinition(
        id="market_cap",
        name="Market Cap",
        type=ColumnType.NUMBER,
        financial_type=FinancialColumnType.MARKET_CAP,
        width=120,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(market_cap_notation="abbreviated", currency_symbol="$"),
        format_options={"text_align": "right", "font_family": "monospace"},
    )

    # P/E Ratio Column
    columns["pe_ratio"] = FinancialColumnDefinition(
        id="pe_ratio",
        name="P/E Ratio",
        type=ColumnType.NUMBER,
        financial_type=FinancialColumnType.PE_RATIO,
        width=80,
        sortable=True,
        filterable=True,
        visible=True,
        financial_options=FinancialColumnOptions(decimal_places=2),
        format_options={"text_align": "right", "font_family": "monospace"},
    )

    # Sector Column
    columns["sector"] = FinancialColumnDefinition(
        id="sector",
        name="Sector",
        type=ColumnType.TEXT,
        financial_type=FinancialColumnType.SECTOR,
        width=150,
        sortable=True,
        filterable=True,
        visible=True,
        format_options={"text_transform": "capitalize"},
    )

    # Exchange Column
    columns["exchange"] = FinancialColumnDefinition(
        id="exchange",
        name="Exchange",
        type=ColumnType.TEXT,
        financial_type=FinancialColumnType.EXCHANGE,
        width=100,
        sortable=True,
        filterable=True,
        visible=True,
        format_options={"text_transform": "uppercase", "font_weight": "bold"},
    )

    # Last Updated Column
    columns["last_updated"] = FinancialColumnDefinition(
        id="last_updated",
        name="Last Updated",
        type=ColumnType.DATETIME,
        financial_type=FinancialColumnType.LAST_UPDATED,
        width=150,
        sortable=True,
        filterable=False,
        visible=True,
        format_options={"datetime_format": "MMM dd, HH:mm", "relative_time": True},
    )

    return columns


def create_market_category_column_sets() -> Dict[str, List[str]]:
    """
    Create predefined column sets for different market categories.

    Returns:
        Dictionary mapping market category to list of column IDs
    """

    return {
        # Basic market overview columns
        "basic": ["ticker_symbol", "company_name", "stock_price", "price_change", "price_change_percent", "trading_volume"],
        # Extended market data with fundamentals
        "extended": [
            "ticker_symbol",
            "company_name",
            "stock_price",
            "price_change",
            "price_change_percent",
            "trading_volume",
            "market_cap",
            "pe_ratio",
            "sector",
            "exchange",
        ],
        # Full market data with all columns
        "comprehensive": [
            "ticker_symbol",
            "company_name",
            "stock_price",
            "price_change",
            "price_change_percent",
            "trading_volume",
            "market_cap",
            "pe_ratio",
            "sector",
            "exchange",
            "last_updated",
        ],
        # Mobile-optimized compact view
        "mobile": ["ticker_symbol", "stock_price", "price_change_percent", "trading_volume"],
        # Sector analysis view
        "sector_analysis": [
            "ticker_symbol",
            "company_name",
            "sector",
            "stock_price",
            "price_change_percent",
            "market_cap",
            "pe_ratio",
        ],
    }


def get_financial_table_configuration(
    table_name: str, column_set: str = "extended", user_customizations: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Generate complete table configuration for financial data.

    Args:
        table_name: Name of the table (e.g., "tsx_stocks", "nyse_stocks")
        column_set: Predefined column set to use
        user_customizations: User-specific customizations to apply

    Returns:
        Complete table configuration dictionary
    """

    # Get base column definitions
    all_columns = create_financial_column_definitions()
    column_sets = create_market_category_column_sets()

    # Select columns for this configuration
    selected_column_ids = column_sets.get(column_set, column_sets["extended"])
    selected_columns = [all_columns[col_id] for col_id in selected_column_ids if col_id in all_columns]

    # Market-specific default sorting
    market_specific_defaults = {
        "tsx_market": {"sort_column": "market_cap", "sort_direction": "desc"},
        "nyse_market": {"sort_column": "trading_volume", "sort_direction": "desc"},
        "nasdaq_market": {"sort_column": "price_change_percent", "sort_direction": "desc"},
    }

    # Get default sorting for this market
    default_sort = market_specific_defaults.get(table_name, {"sort_column": "market_cap", "sort_direction": "desc"})

    # Base table configuration
    config = {
        "table_name": table_name,
        "display_name": table_name.replace("_", " ").title(),
        "columns": selected_columns,
        "features": {
            "sorting": True,
            "filtering": True,
            "searching": True,
            "pagination": True,
            "export": True,
            "real_time_updates": True,
            "column_customization": True,
            "responsive": True,
        },
        "default_settings": {
            "page_size": 25,
            "sort_column": default_sort["sort_column"],
            "sort_direction": default_sort["sort_direction"],
            "real_time_interval": 30,
            "theme": "financial",
        },
        "real_time": {
            "enabled": True,
            "update_interval": 30,
            "endpoints": {
                "data": f"/api/v1/financial/market-data/{table_name}",
                "realtime": f"/api/v1/financial/realtime/{table_name}",
            },
        },
        "mobile_settings": {"column_set": "mobile", "horizontal_scroll": True, "compact_rows": True, "sticky_header": True},
    }

    # Apply user customizations if provided
    if user_customizations:
        # Apply column visibility customizations
        if "hidden_columns" in user_customizations:
            for column in config["columns"]:
                if column.id in user_customizations["hidden_columns"]:
                    column.visible = False

        # Apply column width customizations
        if "column_widths" in user_customizations:
            for column in config["columns"]:
                if column.id in user_customizations["column_widths"]:
                    column.width = user_customizations["column_widths"][column.id]

        # Apply sorting customizations
        if "default_sort" in user_customizations:
            config["default_settings"]["sort_column"] = user_customizations["default_sort"]["column"]
            config["default_settings"]["sort_direction"] = user_customizations["default_sort"]["direction"]

        # Apply display customizations
        if "page_size" in user_customizations:
            config["default_settings"]["page_size"] = user_customizations["page_size"]

    return config
