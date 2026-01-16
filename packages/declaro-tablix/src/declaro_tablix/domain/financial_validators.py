"""
Financial data validators for TableV2.

This module provides validation functions for financial data types
following the function-based architecture pattern.
"""

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from declaro_tablix.domain.financial_columns import FinancialColumnType


class ValidationResult(BaseModel):
    """Result of a validation operation."""

    is_valid: bool = Field(..., description="Whether the value is valid")
    cleaned_value: Optional[Any] = Field(None, description="Cleaned/normalized value")
    error_message: Optional[str] = Field(None, description="Error message if validation failed")
    warnings: List[str] = Field(default_factory=list, description="Non-fatal warnings")


def validate_ticker_symbol(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate ticker symbol format and constraints.

    Args:
        value: Ticker symbol to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=False, error_message="Ticker symbol cannot be empty")

    ticker = str(value).strip().upper()

    # Basic format validation
    if not re.match(r"^[A-Z]{1,6}$", ticker.split(":")[-1]):
        return ValidationResult(is_valid=False, error_message="Ticker symbol must be 1-6 uppercase letters")

    # Check for exchange prefix
    if ":" in ticker:
        exchange, symbol = ticker.split(":", 1)
        valid_exchanges = ["NYSE", "NASDAQ", "TSX", "LSE", "ASX", "HKEX"]
        if exchange not in valid_exchanges:
            return ValidationResult(
                is_valid=True, cleaned_value=ticker, warnings=[f"Exchange '{exchange}' not in common exchanges list"]
            )

    return ValidationResult(is_valid=True, cleaned_value=ticker)


def validate_stock_price(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate stock price value and range.

    Args:
        value: Stock price to validate
        options: Validation options (min_price, max_price, decimal_places)

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=False, error_message="Stock price cannot be empty")

    # Get validation options
    options = options or {}
    min_price = options.get("min_price", 0.01)
    max_price = options.get("max_price", 100000.00)
    max_decimal_places = options.get("decimal_places", 4)

    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ValidationResult(is_valid=False, error_message="Stock price must be a valid number")

    # Range validation
    if price < Decimal(str(min_price)):
        return ValidationResult(is_valid=False, error_message=f"Stock price must be at least ${min_price}")

    if price > Decimal(str(max_price)):
        return ValidationResult(is_valid=False, error_message=f"Stock price cannot exceed ${max_price}")

    # Check decimal places
    decimal_places = abs(price.as_tuple().exponent)
    if decimal_places > max_decimal_places:
        return ValidationResult(
            is_valid=True,
            cleaned_value=float(price.quantize(Decimal("0.01"))),
            warnings=[f"Price rounded to {max_decimal_places} decimal places"],
        )

    return ValidationResult(is_valid=True, cleaned_value=float(price))


def validate_price_change(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate price change value.

    Args:
        value: Price change to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=True, cleaned_value=0.0)

    try:
        change = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ValidationResult(is_valid=False, error_message="Price change must be a valid number")

    # Reasonable range check (could be configurable)
    if abs(change) > Decimal("10000"):
        return ValidationResult(
            is_valid=True, cleaned_value=float(change), warnings=["Unusually large price change detected"]
        )

    return ValidationResult(is_valid=True, cleaned_value=float(change))


def validate_percentage(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate percentage value and range.

    Args:
        value: Percentage to validate
        options: Validation options (min_percent, max_percent)

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=True, cleaned_value=0.0)

    # Remove percentage symbol if present
    if isinstance(value, str):
        value = value.replace("%", "").strip()

    try:
        percent = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ValidationResult(is_valid=False, error_message="Percentage must be a valid number")

    # Get validation options
    options = options or {}
    min_percent = options.get("min_percent", -100.0)
    max_percent = options.get("max_percent", 1000.0)
    warning_threshold = options.get("warning_threshold", 200.0)  # Warning for values over 200%

    # Range validation
    if percent < Decimal(str(min_percent)):
        return ValidationResult(is_valid=False, error_message=f"Percentage cannot be less than {min_percent}%")

    if percent > Decimal(str(max_percent)):
        return ValidationResult(
            is_valid=True, cleaned_value=float(percent), warnings=[f"Unusually high percentage change: {percent}%"]
        )

    # Check for warning threshold
    if percent > Decimal(str(warning_threshold)):
        return ValidationResult(
            is_valid=True, cleaned_value=float(percent), warnings=[f"Unusually high percentage change: {percent}%"]
        )

    return ValidationResult(is_valid=True, cleaned_value=float(percent))


def validate_trading_volume(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate trading volume value.

    Args:
        value: Trading volume to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=True, cleaned_value=0)

    try:
        volume = int(float(str(value)))
    except (ValueError, TypeError):
        return ValidationResult(is_valid=False, error_message="Trading volume must be a valid integer")

    # Volume must be non-negative
    if volume < 0:
        return ValidationResult(is_valid=False, error_message="Trading volume cannot be negative")

    # Reasonable upper limit check
    max_volume = options.get("max_volume", 10_000_000_000) if options else 10_000_000_000
    if volume > max_volume:
        return ValidationResult(is_valid=True, cleaned_value=volume, warnings=["Unusually high trading volume detected"])

    return ValidationResult(is_valid=True, cleaned_value=volume)


def validate_market_cap(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate market capitalization value.

    Args:
        value: Market cap to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=False, error_message="Market cap cannot be empty")

    try:
        market_cap = float(str(value))
    except (ValueError, TypeError):
        return ValidationResult(is_valid=False, error_message="Market cap must be a valid number")

    # Market cap must be positive
    if market_cap <= 0:
        return ValidationResult(is_valid=False, error_message="Market cap must be positive")

    # Get validation options
    options = options or {}
    min_market_cap = options.get("min_market_cap", 1_000_000)  # $1M minimum
    max_market_cap = options.get("max_market_cap", 10_000_000_000_000)  # $10T maximum

    if market_cap < min_market_cap:
        return ValidationResult(
            is_valid=True, cleaned_value=market_cap, warnings=["Very small market cap - may be a penny stock"]
        )

    if market_cap > max_market_cap:
        return ValidationResult(
            is_valid=True, cleaned_value=market_cap, warnings=["Extremely large market cap - please verify"]
        )

    return ValidationResult(is_valid=True, cleaned_value=market_cap)


def validate_pe_ratio(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate P/E ratio value.

    Args:
        value: P/E ratio to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "" or value == "N/A":
        return ValidationResult(is_valid=True, cleaned_value=None)

    try:
        pe_ratio = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ValidationResult(is_valid=False, error_message="P/E ratio must be a valid number or N/A")

    # P/E ratio should be positive (negative P/E is unusual but possible for loss-making companies)
    if pe_ratio < 0:
        return ValidationResult(
            is_valid=True, cleaned_value=None, warnings=["Negative P/E ratio converted to N/A"]  # Treat negative P/E as N/A
        )

    # Extremely high P/E ratios
    if pe_ratio > 1000:
        return ValidationResult(
            is_valid=True,
            cleaned_value=float(pe_ratio),
            warnings=["Very high P/E ratio - may indicate overvaluation or low earnings"],
        )

    return ValidationResult(is_valid=True, cleaned_value=float(pe_ratio))


def validate_sector(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate sector classification.

    Args:
        value: Sector name to validate
        options: Validation options (valid_sectors list)

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=False, error_message="Sector cannot be empty")

    sector = str(value).strip()

    # Standardize common sector names
    sector_mappings = {
        "tech": "Technology",
        "healthcare": "Healthcare",
        "health care": "Healthcare",
        "financial": "Financial Services",
        "financials": "Financial Services",
        "energy": "Energy",
        "consumer disc": "Consumer Discretionary",
        "consumer discretionary": "Consumer Discretionary",
        "consumer staples": "Consumer Staples",
        "industrials": "Industrials",
        "materials": "Materials",
        "utilities": "Utilities",
        "real estate": "Real Estate",
        "communication": "Communication Services",
        "communications": "Communication Services",
    }

    # Normalize the sector name
    normalized_sector = sector_mappings.get(sector.lower(), sector.title())

    # Get valid sectors from options
    if options and "valid_sectors" in options:
        valid_sectors = options["valid_sectors"]
        if normalized_sector not in valid_sectors:
            return ValidationResult(
                is_valid=True,
                cleaned_value=normalized_sector,
                warnings=[f"Sector '{normalized_sector}' not in predefined list"],
            )

    return ValidationResult(is_valid=True, cleaned_value=normalized_sector)


def validate_exchange(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate exchange name.

    Args:
        value: Exchange name to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=False, error_message="Exchange cannot be empty")

    exchange = str(value).strip().upper()

    # Known exchange mappings
    exchange_mappings = {
        "NEW YORK STOCK EXCHANGE": "NYSE",
        "NASDAQ": "NASDAQ",
        "TORONTO STOCK EXCHANGE": "TSX",
        "LONDON STOCK EXCHANGE": "LSE",
        "AUSTRALIAN SECURITIES EXCHANGE": "ASX",
        "HONG KONG STOCK EXCHANGE": "HKEX",
        "SHANGHAI STOCK EXCHANGE": "SSE",
        "TOKYO STOCK EXCHANGE": "TSE",
    }

    # Normalize exchange name
    normalized_exchange = exchange_mappings.get(exchange, exchange)

    # Common exchange validation
    common_exchanges = ["NYSE", "NASDAQ", "TSX", "LSE", "ASX", "HKEX", "SSE", "TSE"]
    if normalized_exchange not in common_exchanges:
        return ValidationResult(
            is_valid=True,
            cleaned_value=normalized_exchange,
            warnings=[f"Exchange '{normalized_exchange}' not in common exchanges list"],
        )

    return ValidationResult(is_valid=True, cleaned_value=normalized_exchange)


def validate_rsi(value: Any, options: Optional[Dict[str, Any]] = None) -> ValidationResult:
    """
    Validate RSI (Relative Strength Index) value.

    Args:
        value: RSI value to validate
        options: Validation options

    Returns:
        ValidationResult with validation status and cleaned value
    """
    if value is None or value == "":
        return ValidationResult(is_valid=True, cleaned_value=None)

    try:
        rsi = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return ValidationResult(is_valid=False, error_message="RSI must be a valid number between 0 and 100")

    # RSI must be between 0 and 100
    if rsi < 0 or rsi > 100:
        return ValidationResult(is_valid=False, error_message="RSI must be between 0 and 100")

    return ValidationResult(is_valid=True, cleaned_value=float(rsi))


# Registry of financial validators
FINANCIAL_VALIDATORS = {
    FinancialColumnType.TICKER_SYMBOL: validate_ticker_symbol,
    FinancialColumnType.STOCK_PRICE: validate_stock_price,
    FinancialColumnType.PRICE_CHANGE: validate_price_change,
    FinancialColumnType.PRICE_CHANGE_PERCENT: validate_percentage,
    FinancialColumnType.TRADING_VOLUME: validate_trading_volume,
    FinancialColumnType.MARKET_CAP: validate_market_cap,
    FinancialColumnType.PE_RATIO: validate_pe_ratio,
    FinancialColumnType.SECTOR: validate_sector,
    FinancialColumnType.EXCHANGE: validate_exchange,
    FinancialColumnType.RSI: validate_rsi,
}


def get_financial_validator(financial_type: FinancialColumnType):
    """
    Get the validator function for a financial column type.

    Args:
        financial_type: Financial column type

    Returns:
        Validator function or None if not found
    """
    return FINANCIAL_VALIDATORS.get(financial_type)


def validate_financial_data(data: Dict[str, Any], column_definitions: List[Dict[str, Any]]) -> Dict[str, ValidationResult]:
    """
    Validate a complete row of financial data.

    Args:
        data: Dictionary of column values
        column_definitions: List of column definitions with validation rules

    Returns:
        Dictionary mapping column names to validation results
    """
    results = {}

    for column_def in column_definitions:
        column_id = column_def.get("id")
        financial_type = column_def.get("financial_type")
        validation_options = column_def.get("validation_options", {})

        if column_id in data and financial_type:
            validator = get_financial_validator(financial_type)
            if validator:
                results[column_id] = validator(data[column_id], validation_options)
            else:
                # No specific validator, assume valid
                results[column_id] = ValidationResult(is_valid=True, cleaned_value=data[column_id])

    return results


def validate_bulk_financial_data(
    rows: List[Dict[str, Any]], column_definitions: List[Dict[str, Any]]
) -> List[Dict[str, ValidationResult]]:
    """
    Validate multiple rows of financial data.

    Args:
        rows: List of data rows
        column_definitions: Column definitions with validation rules

    Returns:
        List of validation results for each row
    """
    return [validate_financial_data(row, column_definitions) for row in rows]


def get_validation_summary(validation_results: Dict[str, ValidationResult]) -> Dict[str, Any]:
    """
    Generate a summary of validation results.

    Args:
        validation_results: Dictionary of validation results

    Returns:
        Summary statistics about validation
    """
    total_fields = len(validation_results)
    valid_fields = sum(1 for result in validation_results.values() if result.is_valid)
    invalid_fields = total_fields - valid_fields
    warnings_count = sum(len(result.warnings) for result in validation_results.values())

    errors = [f"{field}: {result.error_message}" for field, result in validation_results.items() if not result.is_valid]

    warnings = [f"{field}: {warning}" for field, result in validation_results.items() for warning in result.warnings]

    return {
        "total_fields": total_fields,
        "valid_fields": valid_fields,
        "invalid_fields": invalid_fields,
        "warnings_count": warnings_count,
        "success_rate": (valid_fields / total_fields) * 100 if total_fields > 0 else 0,
        "errors": errors,
        "warnings": warnings,
        "overall_valid": invalid_fields == 0,
    }
