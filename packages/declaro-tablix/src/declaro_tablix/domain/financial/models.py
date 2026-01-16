"""
Financial domain models for TableV2.

This module defines Pydantic models for financial data following clean architecture principles.
All models are Pydantic models with proper validation and normalization.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TickerData(BaseModel):
    """Ticker data model for financial instruments."""

    model_config = ConfigDict(use_enum_values=True, validate_assignment=True, str_strip_whitespace=True)

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique ticker ID")
    symbol: str = Field(..., description="Ticker symbol")
    name: str = Field(..., description="Company/instrument name")
    category: str = Field(..., description="Market category")
    price: Decimal = Field(..., description="Current price")
    currency: str = Field(..., description="Currency code")
    volume: int = Field(default=0, ge=0, description="Trading volume")
    day_change: Optional[Decimal] = Field(None, description="Daily price change")
    day_change_percent: Optional[Decimal] = Field(None, description="Daily change percentage")
    week_change: Optional[Decimal] = Field(None, description="Weekly price change")
    week_change_percent: Optional[Decimal] = Field(None, description="Weekly change percentage")
    quarter_change: Optional[Decimal] = Field(None, description="Quarterly price change")
    quarter_change_percent: Optional[Decimal] = Field(None, description="Quarterly change percentage")
    ytd_change: Optional[Decimal] = Field(None, description="Year-to-date price change")
    ytd_change_percent: Optional[Decimal] = Field(None, description="YTD change percentage")
    sector: Optional[str] = Field(None, description="Industry sector")
    industry: Optional[str] = Field(None, description="Industry classification")
    logo_url: Optional[str] = Field(None, description="Company logo URL")
    last_updated: datetime = Field(default_factory=datetime.now, description="Last update timestamp")

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Symbol cannot be empty")
        return v.strip().upper()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate name field."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Name cannot be empty")
        return v.strip()

    @field_validator("category")
    @classmethod
    def validate_category(cls, v):
        """Validate category field."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Category cannot be empty")
        return v.strip()

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v):
        """Validate currency code."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Currency cannot be empty")

        currency = v.strip().upper()
        # Basic currency code validation (ISO 4217 format)
        valid_currencies = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "SEK", "NOK"}
        if currency not in valid_currencies:
            raise ValueError(f"Invalid currency code: {currency}")

        return currency

    @field_validator("price")
    @classmethod
    def validate_price(cls, v):
        """Validate and round price to appropriate precision."""
        if v is None:
            raise ValueError("Price cannot be None")

        # Round to 4 decimal places for financial precision
        return round(Decimal(str(v)), 4)


class MarketCategory(BaseModel):
    """Market category model for grouping tickers."""

    model_config = ConfigDict(use_enum_values=True, validate_assignment=True, str_strip_whitespace=True)

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique category ID")
    name: str = Field(..., description="Category name")
    display_name: str = Field(..., description="Display name")
    is_currency: bool = Field(default=False, description="Is currency category")
    ticker_count: int = Field(default=0, ge=0, description="Number of tickers")
    gainers: List[str] = Field(default_factory=list, description="Top gainers")
    losers: List[str] = Field(default_factory=list, description="Top losers")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v):
        """Validate name field."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Name cannot be empty")
        return v.strip().lower()

    @field_validator("display_name")
    @classmethod
    def validate_display_name(cls, v):
        """Validate display name field."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Display name cannot be empty")
        return v.strip()

    @field_validator("ticker_count")
    @classmethod
    def validate_ticker_count(cls, v):
        """Validate ticker count is non-negative."""
        if v < 0:
            raise ValueError("Ticker count cannot be negative")
        return v


class PerformanceMetrics(BaseModel):
    """Performance metrics model for financial calculations."""

    model_config = ConfigDict(use_enum_values=True, validate_assignment=True, str_strip_whitespace=True)

    id: str = Field(default_factory=lambda: str(uuid4()), description="Unique metrics ID")
    symbol: str = Field(..., description="Ticker symbol")
    day_change: Decimal = Field(..., description="Daily change amount")
    day_change_percent: Decimal = Field(..., description="Daily change percentage")
    week_change: Decimal = Field(..., description="Weekly change amount")
    week_change_percent: Decimal = Field(..., description="Weekly change percentage")
    quarter_change: Decimal = Field(..., description="Quarterly change amount")
    quarter_change_percent: Decimal = Field(..., description="Quarterly change percentage")
    ytd_change: Decimal = Field(..., description="YTD change amount")
    ytd_change_percent: Decimal = Field(..., description="YTD change percentage")
    volume: int = Field(..., ge=0, description="Trading volume")
    avg_volume: int = Field(..., ge=0, description="Average volume")
    market_cap: Optional[Decimal] = Field(None, description="Market capitalization")
    pe_ratio: Optional[Decimal] = Field(None, description="Price-to-earnings ratio")

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v):
        """Normalize symbol to uppercase."""
        if not v or len(v.strip()) == 0:
            raise ValueError("Symbol cannot be empty")
        return v.strip().upper()

    @property
    def volume_formatted(self) -> str:
        """Format volume with appropriate suffixes (K, M, B)."""
        if self.volume >= 1_000_000_000:
            return f"{self.volume / 1_000_000_000:.1f}B"
        elif self.volume >= 1_000_000:
            return f"{self.volume / 1_000_000:.1f}M"
        elif self.volume >= 1_000:
            return f"{self.volume / 1_000:.1f}K"
        else:
            return str(self.volume)
