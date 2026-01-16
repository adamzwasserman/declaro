"""
Financial domain models package.

This package contains Pydantic models for financial data used in TableV2.
Following function-based architecture principles with no classes except Pydantic models.
"""

from .models import MarketCategory, PerformanceMetrics, TickerData

__all__ = ["TickerData", "MarketCategory", "PerformanceMetrics"]
