"""
Financial API integration for TableV2.

This module provides integration between TableV2 components and the
financial API endpoints, following function-based architecture patterns.

NOTE: The markets_routes integration has been deprecated. When deployed back
into buckler/idd, re-enable the lightning market data integration.
"""

import asyncio
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException

from declaro_tablix.domain.models import TableData
from declaro_tablix.financial_integration import ensure_financial_integration_initialized

# Backend logging - falls back to standard logging when not in buckler
try:
    from backend.utilities.logging_config import get_logger
except ImportError:
    import logging
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)


async def fetch_financial_table_data(
    table_name: str,
    market_category: str,
    page: int = 1,
    page_size: int = 25,
    sort_by: Optional[str] = None,
    sort_direction: str = "desc",
    filters: Optional[List[Dict[str, Any]]] = None,
    search_term: Optional[str] = None,
    user_context: Optional[Dict[str, Any]] = None,
) -> Tuple[TableData, Dict[str, Any]]:
    """
    Fetch financial table data from the markets controller and convert to TableData format.

    DEPRECATED: The markets_routes integration has been removed. This function
    returns empty data. When deployed back into buckler/idd, re-enable the
    lightning market data integration.

    Args:
        table_name: Name of the table
        market_category: Market category (tsx, nyse, nasdaq, etc.)
        page: Page number
        page_size: Items per page
        sort_by: Column to sort by
        sort_direction: Sort direction
        filters: Column filters to apply
        search_term: Search term for filtering
        user_context: User context for permissions

    Returns:
        Tuple of (TableData object, benchmark data dict)
    """
    warnings.warn(
        "fetch_financial_table_data: markets_routes integration is deprecated. "
        "Re-enable when deployed back into buckler/idd.",
        DeprecationWarning,
        stacklevel=2,
    )

    logger = get_logger(__name__)
    logger.warning(
        f"fetch_financial_table_data called for {market_category} but markets_routes "
        "integration is deprecated. Returning empty data."
    )

    # Return empty TableData
    table_data = TableData(
        rows=[],
        total_count=0,
        metadata={
            "market_category": market_category,
            "table_name": table_name,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "total_count": 0,
            },
            "last_updated": None,
            "data_source": "deprecated",
            "warning": "markets_routes integration deprecated",
        },
    )

    return table_data, {}


# Real-time data fetching now handled by lightning system

# Real-time updates now handled by lightning system with live data

# Data transformation now handled by lightning system

# Health status now monitored by lightning system


# Integration functions for use by TableV2 routes
async def create_financial_table_response(
    table_name: str,
    market_category: str,
    request_params: Dict[str, Any],
    user_context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a complete financial table response for API endpoints.

    Args:
        table_name: Name of the table
        market_category: Market category
        request_params: Request parameters (page, sort, etc.)
        user_context: User context

    Returns:
        Complete table response dictionary
    """
    from .financial_integration import create_financial_table_configuration_factory

    # Create table configuration
    config_factory = create_financial_table_configuration_factory()
    table_config = config_factory(
        table_name=table_name,
        market_category=market_category,
        column_set=request_params.get("column_set", "extended"),
        user_preferences=user_context.get("table_preferences"),
        real_time_enabled=request_params.get("real_time", True),
    )

    # Fetch table data
    table_data, benchmark_data = await fetch_financial_table_data(
        table_name=table_name,
        market_category=market_category,
        page=request_params.get("page", 1),
        page_size=request_params.get("page_size", 25),
        sort_by=request_params.get("sort_by"),
        sort_direction=request_params.get("sort_direction", "desc"),
        filters=request_params.get("filters"),
        search_term=request_params.get("search_term"),
        user_context=user_context,
    )

    # Add logging to debug benchmark data
    logger = get_logger(__name__)
    logger.info(f"DEBUG: Creating response for {market_category} with benchmark data: {benchmark_data}")

    # Build response
    response = {
        "success": True,
        "table_config": {
            "table_name": table_config.table_name,
            "display_name": table_config.display_name,
            "columns": [
                {
                    "id": col.id,
                    "name": col.name,
                    "type": col.type,
                    "financial_type": getattr(col, "financial_type", None),
                    "width": col.width,
                    "sortable": col.sortable,
                    "filterable": col.filterable,
                    "visible": col.visible,
                }
                for col in table_config.columns
            ],
            "features": table_config.features_enabled,
            "metadata": table_config.metadata,
        },
        "table_data": {
            "rows": table_data.rows,  # Lightning data rows are already dictionaries
            "total_count": table_data.total_count,
            "metadata": table_data.metadata,
        },
        "benchmark": benchmark_data,  # Add benchmark data for category headers
        "pagination": {
            "page": request_params.get("page", 1),
            "page_size": request_params.get("page_size", 25),
            "total_count": table_data.total_count,
            "total_pages": (table_data.total_count + request_params.get("page_size", 25) - 1)
            // request_params.get("page_size", 25),
        },
        "metadata": {
            "market_category": market_category,
            "request_timestamp": asyncio.get_event_loop().time(),
            "real_time_enabled": request_params.get("real_time", True),
        },
    }

    return response


# Convenience functions for common market categories
async def get_tsx_table_data(request_params: Dict[str, Any], user_context: Dict[str, Any]) -> Dict[str, Any]:
    """Get TSX market table data."""
    return await create_financial_table_response("tsx_market", "tsx", request_params, user_context)


async def get_nyse_table_data(request_params: Dict[str, Any], user_context: Dict[str, Any]) -> Dict[str, Any]:
    """Get NYSE market table data."""
    return await create_financial_table_response("nyse_market", "nyse", request_params, user_context)


async def get_nasdaq_table_data(request_params: Dict[str, Any], user_context: Dict[str, Any]) -> Dict[str, Any]:
    """Get NASDAQ market table data."""
    return await create_financial_table_response("nasdaq_market", "nasdaq", request_params, user_context)
