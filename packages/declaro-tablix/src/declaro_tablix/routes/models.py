"""
Pydantic models for TableV2 FastAPI routes.

This module provides request/response models with validation for all
TableV2 API endpoints, following the function-based architecture pattern.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from ..domain.models import (
    SortDirection,
    TableData,
)
from ..formatting.formatting_strategy import FormattingStrategy


class TableDataRequest(BaseModel):
    """Request model for table data operations."""

    table_name: str = Field(..., description="Name of the table")
    page: int = Field(default=1, ge=1, description="Page number for pagination")
    per_page: int = Field(default=50, ge=1, le=1000, description="Items per page")
    search_term: Optional[str] = Field(None, description="Search term for filtering")
    sort_column: Optional[str] = Field(None, description="Column to sort by")
    sort_direction: Optional[SortDirection] = Field(None, description="Sort direction")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Column filters")
    include_metadata: bool = Field(default=True, description="Include table metadata")

    @field_validator("filters")
    @classmethod
    def validate_filters(cls, v):
        """Validate filter format."""
        if v:
            for column, filter_config in v.items():
                if not isinstance(filter_config, dict):
                    raise ValueError(f"Filter for column '{column}' must be a dictionary")
                if "operator" not in filter_config:
                    raise ValueError(f"Filter for column '{column}' must include 'operator'")
                if "value" not in filter_config:
                    raise ValueError(f"Filter for column '{column}' must include 'value'")
        return v


class TableDataResponse(BaseModel):
    """Response model for table data operations."""

    success: bool = Field(..., description="Operation success status")
    data: Optional[TableData] = Field(None, description="Table data")
    message: Optional[str] = Field(None, description="Success/error message")
    pagination: Optional[Dict[str, Any]] = Field(None, description="Pagination metadata")
    performance: Optional[Dict[str, float]] = Field(None, description="Performance metrics")


class TableConfigRequest(BaseModel):
    """Request model for table configuration operations."""

    table_name: str = Field(..., description="Name of the table")
    config_name: str = Field(..., description="Configuration name")
    configuration: Dict[str, Any] = Field(..., description="Configuration data")
    is_default: bool = Field(default=False, description="Is this the default configuration")
    description: Optional[str] = Field(None, description="Configuration description")


class TableConfigResponse(BaseModel):
    """Response model for table configuration operations."""

    success: bool = Field(..., description="Operation success status")
    config_id: Optional[UUID] = Field(None, description="Configuration ID")
    message: Optional[str] = Field(None, description="Success/error message")
    configuration: Optional[Dict[str, Any]] = Field(None, description="Configuration data")


class CustomizationRequest(BaseModel):
    """Request model for customization operations."""

    table_name: str = Field(..., description="Name of the table")
    customization_type: str = Field(..., description="Type of customization")
    customization_data: Dict[str, Any] = Field(..., description="Customization data")
    name: Optional[str] = Field(None, description="Customization name")
    description: Optional[str] = Field(None, description="Customization description")

    @field_validator("customization_type")
    @classmethod
    def validate_customization_type(cls, v):
        """Validate customization type."""
        valid_types = ["column", "search", "filter", "layout"]
        if v not in valid_types:
            raise ValueError(f"Customization type must be one of: {valid_types}")
        return v


class CustomizationResponse(BaseModel):
    """Response model for customization operations."""

    success: bool = Field(..., description="Operation success status")
    customization_id: Optional[UUID] = Field(None, description="Customization ID")
    message: Optional[str] = Field(None, description="Success/error message")
    customization_data: Optional[Dict[str, Any]] = Field(None, description="Customization data")


class FormattingRequest(BaseModel):
    """Request model for formatting operations."""

    table_data: TableData = Field(..., description="Table data to format")
    strategy: FormattingStrategy = Field(default=FormattingStrategy.DEFAULT, description="Formatting strategy")
    options: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Formatting options")
    include_css: bool = Field(default=True, description="Include CSS styles")
    theme: Optional[str] = Field(default="default", description="Theme name")


class FormattingResponse(BaseModel):
    """Response model for formatting operations."""

    success: bool = Field(..., description="Operation success status")
    formatted_data: Optional[TableData] = Field(None, description="Formatted table data")
    css_styles: Optional[str] = Field(None, description="Generated CSS styles")
    message: Optional[str] = Field(None, description="Success/error message")
    performance: Optional[Dict[str, float]] = Field(None, description="Performance metrics")


class PluginRequest(BaseModel):
    """Request model for plugin operations."""

    operation: str = Field(..., description="Plugin operation")
    plugin_name: Optional[str] = Field(None, description="Plugin name")
    plugin_data: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Plugin data")
    context: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Operation context")

    @field_validator("operation")
    @classmethod
    def validate_operation(cls, v):
        """Validate plugin operation."""
        valid_operations = ["execute", "register", "unregister", "list", "status"]
        if v not in valid_operations:
            raise ValueError(f"Plugin operation must be one of: {valid_operations}")
        return v


class PluginResponse(BaseModel):
    """Response model for plugin operations."""

    success: bool = Field(..., description="Operation success status")
    result: Optional[Any] = Field(None, description="Plugin operation result")
    plugins: Optional[List[Dict[str, Any]]] = Field(None, description="Plugin list")
    message: Optional[str] = Field(None, description="Success/error message")
    performance: Optional[Dict[str, float]] = Field(None, description="Performance metrics")


class CacheRequest(BaseModel):
    """Request model for cache operations."""

    operation: str = Field(..., description="Cache operation")
    key: Optional[str] = Field(None, description="Cache key")
    value: Optional[Any] = Field(None, description="Cache value")
    ttl: Optional[int] = Field(None, description="Time to live in seconds")
    pattern: Optional[str] = Field(None, description="Key pattern for bulk operations")

    @field_validator("operation")
    @classmethod
    def validate_operation(cls, v):
        """Validate cache operation."""
        valid_operations = ["get", "set", "delete", "clear", "stats", "invalidate"]
        if v not in valid_operations:
            raise ValueError(f"Cache operation must be one of: {valid_operations}")
        return v


class CacheResponse(BaseModel):
    """Response model for cache operations."""

    success: bool = Field(..., description="Operation success status")
    value: Optional[Any] = Field(None, description="Cache value")
    statistics: Optional[Dict[str, Any]] = Field(None, description="Cache statistics")
    message: Optional[str] = Field(None, description="Success/error message")


class PreferencesRequest(BaseModel):
    """Request model for user preferences operations."""

    preference_type: str = Field(..., description="Type of preference")
    preferences: Dict[str, Any] = Field(..., description="Preference data")
    table_name: Optional[str] = Field(None, description="Table-specific preferences")

    @field_validator("preference_type")
    @classmethod
    def validate_preference_type(cls, v):
        """Validate preference type."""
        valid_types = ["user", "table_view", "dashboard"]
        if v not in valid_types:
            raise ValueError(f"Preference type must be one of: {valid_types}")
        return v


class PreferencesResponse(BaseModel):
    """Response model for user preferences operations."""

    success: bool = Field(..., description="Operation success status")
    preferences: Optional[Dict[str, Any]] = Field(None, description="Preference data")
    message: Optional[str] = Field(None, description="Success/error message")
    export_data: Optional[str] = Field(None, description="Exported preferences data")


class HealthCheckResponse(BaseModel):
    """Response model for health check operations."""

    status: str = Field(..., description="Health status")
    timestamp: datetime = Field(..., description="Check timestamp")
    services: Dict[str, str] = Field(..., description="Service health status")
    performance: Dict[str, float] = Field(..., description="Performance metrics")
    version: str = Field(..., description="API version")


class ErrorResponse(BaseModel):
    """Response model for error cases."""

    success: bool = Field(default=False, description="Operation success status")
    error_code: str = Field(..., description="Error code")
    error_message: str = Field(..., description="Error message")
    error_details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for tracking")


class BatchRequest(BaseModel):
    """Request model for batch operations."""

    operations: List[Dict[str, Any]] = Field(..., description="List of operations")
    fail_fast: bool = Field(default=False, description="Stop on first error")
    parallel: bool = Field(default=True, description="Execute operations in parallel")

    @field_validator("operations")
    @classmethod
    def validate_operations(cls, v):
        """Validate batch operations."""
        if not v:
            raise ValueError("At least one operation is required")
        if len(v) > 100:
            raise ValueError("Maximum 100 operations per batch")

        for i, op in enumerate(v):
            if "type" not in op:
                raise ValueError(f"Operation {i} missing 'type' field")
            if "params" not in op:
                raise ValueError(f"Operation {i} missing 'params' field")

        return v


class BatchResponse(BaseModel):
    """Response model for batch operations."""

    success: bool = Field(..., description="Overall batch success status")
    results: List[Dict[str, Any]] = Field(..., description="Individual operation results")
    summary: Dict[str, Any] = Field(..., description="Batch execution summary")
    performance: Dict[str, float] = Field(..., description="Performance metrics")
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="Error details")


# Standard response wrapper
class ApiResponse(BaseModel):
    """Standard API response wrapper."""

    success: bool = Field(..., description="Operation success status")
    data: Optional[Any] = Field(None, description="Response data")
    message: Optional[str] = Field(None, description="Response message")
    meta: Optional[Dict[str, Any]] = Field(None, description="Response metadata")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")


# Function to create standardized response
def create_api_response(
    success: bool, data: Optional[Any] = None, message: Optional[str] = None, meta: Optional[Dict[str, Any]] = None
) -> ApiResponse:
    """
    Create a standardized API response.

    Args:
        success: Operation success status
        data: Response data
        message: Response message
        meta: Response metadata

    Returns:
        Standardized API response
    """
    return ApiResponse(success=success, data=data, message=message, meta=meta)


# Function to create error response
def create_error_response(
    error_code: str, error_message: str, error_details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None
) -> ErrorResponse:
    """
    Create a standardized error response.

    Args:
        error_code: Error code
        error_message: Error message
        error_details: Additional error details
        request_id: Request ID for tracking

    Returns:
        Standardized error response
    """
    return ErrorResponse(
        error_code=error_code, error_message=error_message, error_details=error_details, request_id=request_id
    )
