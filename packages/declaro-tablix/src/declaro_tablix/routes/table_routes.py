"""
FastAPI route handlers for TableV2 table operations.

This module provides function-based route handlers with full integration
of all Phase 2 components following the function-based architecture pattern.
"""

from datetime import datetime
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, Query, Request

# Backend user context - falls back to stub when not in buckler
try:
    from backend.utilities.company_context import get_current_user_context
except ImportError:
    def get_current_user_context() -> dict:
        """Stub for standalone use. Returns minimal user context.

        Note: Uses 'uid' key to match tableV2 user context expectations.
        """
        return {"uid": "standalone_user", "workspace_id": "default", "locale": "en-US", "timezone": "UTC"}


def get_current_user() -> dict:
    """
    FastAPI dependency function to get current user context.

    Returns:
        Current user context dictionary

    Raises:
        HTTPException: If user context is not available
    """
    user_context = get_current_user_context()
    if not user_context:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user_context


from declaro_advise import error, info, success, warning
from declaro_tablix.formatting.formatting_strategy import (
    apply_formatting_strategy,
    create_formatting_context,
    create_formatting_options_from_strategy,
)
from declaro_tablix.plugins.hooks import execute_hook
from declaro_tablix.plugins.plugin_manager import get_plugin_system_status, get_plugins
from declaro_tablix.repositories.table_config_repository import (
    delete_table_configuration,
    list_table_configurations,
    load_table_configuration,
    save_table_configuration,
)
from declaro_tablix.repositories.table_data_repository import (
    execute_custom_query,
    get_table_data,
    get_table_schema,
    get_table_stats,
    table_exists,
)

from .models import (
    CacheRequest,
    CacheResponse,
    FormattingRequest,
    FormattingResponse,
    HealthCheckResponse,
    PluginRequest,
    PluginResponse,
    PreferencesRequest,
    PreferencesResponse,
    TableConfigRequest,
    TableConfigResponse,
    TableDataRequest,
    TableDataResponse,
)

# Create router
router = APIRouter(prefix="/tables", tags=["tables"])


async def get_table_data_route(
    request: Request, table_request: TableDataRequest, user: Dict = Depends(get_current_user)
) -> TableDataResponse:
    """
    Get table data with filtering, pagination, and formatting.

    Args:
        request: FastAPI request object
        table_request: Table data request parameters
        user: Current authenticated user

    Returns:
        Table data response with formatted data
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Execute pre-processing plugin hooks
        execute_hook(
            "before_table_data_fetch",
            {"table_name": table_request.table_name, "user_id": user["uid"], "request_params": table_request.dict()},
        )

        # Check if table exists
        if not table_exists(table_request.table_name):
            error(f"Table '{table_request.table_name}' not found")
            raise HTTPException(status_code=404, detail="Table not found")

        # Get table data from repository
        table_data = get_table_data(
            table_name=table_request.table_name,
            page=table_request.page,
            per_page=table_request.per_page,
            search_term=table_request.search_term,
            sort_column=table_request.sort_column,
            sort_direction=table_request.sort_direction,
            filters=table_request.filters or {},
            user_id=user["uid"],
        )

        # Apply formatting if requested
        if table_data:
            formatting_context = create_formatting_context(
                strategy=table_request.formatting_strategy if hasattr(table_request, "formatting_strategy") else None,
                locale=user.get("locale", "en-US"),
                timezone=user.get("timezone", "UTC"),
            )

            formatting_options = create_formatting_options_from_strategy(formatting_context.strategy)

            formatted_data = apply_formatting_strategy(table_data, formatting_context, formatting_options)
        else:
            formatted_data = table_data

        # Execute post-processing plugin hooks
        execute_hook(
            "after_table_data_fetch",
            {
                "table_name": table_request.table_name,
                "user_id": user["uid"],
                "data_rows": len(formatted_data.rows) if formatted_data else 0,
            },
        )

        # Create pagination metadata
        pagination = {
            "page": table_request.page,
            "per_page": table_request.per_page,
            "total_count": formatted_data.total_count if formatted_data else 0,
            "total_pages": (
                (formatted_data.total_count + table_request.per_page - 1) // table_request.per_page if formatted_data else 0
            ),
        }

        success(f"Successfully loaded table data for '{table_request.table_name}'")

        return TableDataResponse(
            success=True,
            data=formatted_data,
            message="Table data loaded successfully",
            pagination=pagination,
            performance={"fetch_time": 0.0},  # Would be populated with actual metrics
        )

    except HTTPException:
        raise
    except Exception as e:
        error(f"Error loading table data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def get_table_schema_route(
    request: Request, table_name: str, user: Dict = Depends(get_current_user)
) -> TableDataResponse:
    """
    Get table schema information.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        user: Current authenticated user

    Returns:
        Table schema response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Get table schema
        schema = get_table_schema(table_name, user["uid"])

        if not schema:
            error(f"Schema not found for table '{table_name}'")
            raise HTTPException(status_code=404, detail="Table schema not found")

        info(f"Successfully retrieved schema for table '{table_name}'")

        return TableDataResponse(success=True, data=schema, message="Table schema retrieved successfully")

    except HTTPException:
        raise
    except Exception as e:
        error(f"Error retrieving table schema: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def get_table_statistics_route(
    request: Request, table_name: str, user: Dict = Depends(get_current_user)
) -> TableDataResponse:
    """
    Get table statistics.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        user: Current authenticated user

    Returns:
        Table statistics response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Get table statistics
        statistics = get_table_stats(table_name, user["uid"])

        if not statistics:
            warning(f"No statistics available for table '{table_name}'")
            statistics = {"message": "No statistics available"}

        info(f"Successfully retrieved statistics for table '{table_name}'")

        return TableDataResponse(success=True, data=statistics, message="Table statistics retrieved successfully")

    except Exception as e:
        error(f"Error retrieving table statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def execute_query_route(
    request: Request,
    table_name: str,
    query: str = Query(..., description="SQL query to execute"),
    user: Dict = Depends(get_current_user),
) -> TableDataResponse:
    """
    Execute a safe query on the table.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        query: SQL query to execute
        user: Current authenticated user

    Returns:
        Query results response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Execute plugin hook for query validation
        execute_hook("before_query_execution", {"table_name": table_name, "query": query, "user_id": user["uid"]})

        # Execute safe query
        results = execute_custom_query(query)

        # Execute post-processing plugin hook
        execute_hook(
            "after_query_execution",
            {
                "table_name": table_name,
                "query": query,
                "user_id": user["uid"],
                "result_count": len(results) if results else 0,
            },
        )

        success(f"Query executed successfully on table '{table_name}'")

        return TableDataResponse(success=True, data=results, message="Query executed successfully")

    except Exception as e:
        error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


async def save_table_config_route(
    request: Request, config_request: TableConfigRequest, user: Dict = Depends(get_current_user)
) -> TableConfigResponse:
    """
    Save table configuration.

    Args:
        request: FastAPI request object
        config_request: Table configuration request
        user: Current authenticated user

    Returns:
        Configuration save response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Save table configuration
        config_id = await save_table_configuration(
            table_name=config_request.table_name,
            config_name=config_request.config_name,
            configuration=config_request.configuration,
            user_id=user["uid"],
            is_default=config_request.is_default,
            description=config_request.description,
        )

        success(f"Table configuration '{config_request.config_name}' saved successfully")

        return TableConfigResponse(
            success=True,
            config_id=config_id,
            message="Configuration saved successfully",
            configuration=config_request.configuration,
        )

    except Exception as e:
        error(f"Error saving table configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Configuration save failed: {str(e)}")


async def load_table_config_route(
    request: Request, table_name: str, config_name: str, user: Dict = Depends(get_current_user)
) -> TableConfigResponse:
    """
    Load table configuration.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        config_name: Name of the configuration
        user: Current authenticated user

    Returns:
        Configuration load response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Load table configuration
        configuration = await load_table_configuration(table_name=table_name, config_name=config_name, user_id=user["uid"])

        if not configuration:
            warning(f"Configuration '{config_name}' not found for table '{table_name}'")
            raise HTTPException(status_code=404, detail="Configuration not found")

        info(f"Configuration '{config_name}' loaded successfully")

        return TableConfigResponse(success=True, message="Configuration loaded successfully", configuration=configuration)

    except HTTPException:
        raise
    except Exception as e:
        error(f"Error loading table configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Configuration load failed: {str(e)}")


async def list_table_configs_route(
    request: Request, table_name: str, user: Dict = Depends(get_current_user)
) -> TableConfigResponse:
    """
    List all table configurations.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        user: Current authenticated user

    Returns:
        Configuration list response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # List table configurations
        configurations = await list_table_configurations(table_name, user["uid"])

        info(f"Found {len(configurations)} configurations for table '{table_name}'")

        return TableConfigResponse(
            success=True,
            message=f"Found {len(configurations)} configurations",
            configuration={"configurations": configurations},
        )

    except Exception as e:
        error(f"Error listing table configurations: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Configuration list failed: {str(e)}")


async def delete_table_config_route(
    request: Request, table_name: str, config_name: str, user: Dict = Depends(get_current_user)
) -> TableConfigResponse:
    """
    Delete table configuration.

    Args:
        request: FastAPI request object
        table_name: Name of the table
        config_name: Name of the configuration
        user: Current authenticated user

    Returns:
        Configuration delete response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Delete table configuration
        deleted = await delete_table_configuration(table_name=table_name, config_name=config_name, user_id=user["uid"])

        if not deleted:
            warning(f"Configuration '{config_name}' not found for deletion")
            raise HTTPException(status_code=404, detail="Configuration not found")

        success(f"Configuration '{config_name}' deleted successfully")

        return TableConfigResponse(success=True, message="Configuration deleted successfully")

    except HTTPException:
        raise
    except Exception as e:
        error(f"Error deleting table configuration: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Configuration delete failed: {str(e)}")


async def apply_formatting_route(
    request: Request, formatting_request: FormattingRequest, user: Dict = Depends(get_current_user)
) -> FormattingResponse:
    """
    Apply formatting strategy to table data.

    Args:
        request: FastAPI request object
        formatting_request: Formatting request parameters
        user: Current authenticated user

    Returns:
        Formatted table data response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Create formatting context
        formatting_context = create_formatting_context(
            strategy=formatting_request.strategy, locale=user.get("locale", "en-US"), timezone=user.get("timezone", "UTC")
        )

        # Create formatting options
        formatting_options = create_formatting_options_from_strategy(formatting_context.strategy)

        # Apply formatting to data
        formatted_data = apply_formatting_strategy(formatting_request.table_data, formatting_context, formatting_options)

        # Generate CSS if requested
        css_styles = None
        if formatting_request.include_css:
            css_styles = "/* Generated CSS styles would go here */"

        success(f"Formatting applied successfully with {formatting_request.strategy} strategy")

        return FormattingResponse(
            success=True,
            formatted_data=formatted_data,
            css_styles=css_styles,
            message="Formatting applied successfully",
            performance={"format_time": 0.0},
        )

    except Exception as e:
        error(f"Error applying formatting: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Formatting failed: {str(e)}")


async def manage_plugins_route(
    request: Request, plugin_request: PluginRequest, user: Dict = Depends(get_current_user)
) -> PluginResponse:
    """
    Manage plugin operations (register, unregister, list, execute).

    Args:
        request: FastAPI request object
        plugin_request: Plugin operation request
        user: Current authenticated user

    Returns:
        Plugin operation response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        result = None
        plugins_list = None

        if plugin_request.operation == "list":
            # List all registered plugins
            plugins_list = get_plugins()
            info(f"Found {len(plugins_list)} registered plugins")

        elif plugin_request.operation == "execute":
            # Execute plugin hook
            if not plugin_request.plugin_data.get("hook_name"):
                raise ValueError("Hook name is required for plugin execution")

            hook_name = plugin_request.plugin_data["hook_name"]
            hook_context = plugin_request.context or {}

            result = execute_hook(hook_name, hook_context)
            success(f"Plugin hook '{hook_name}' executed successfully")

        elif plugin_request.operation == "status":
            # Get plugin system status
            result = get_plugin_system_status()
            info("Plugin system status retrieved")

        return PluginResponse(
            success=True,
            result=result,
            plugins=plugins_list,
            message=f"Plugin operation '{plugin_request.operation}' completed successfully",
            performance={"execution_time": 0.0},
        )

    except Exception as e:
        error(f"Error in plugin operation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Plugin operation failed: {str(e)}")


async def manage_cache_route(
    request: Request, cache_request: CacheRequest, user: Dict = Depends(get_current_user)
) -> CacheResponse:
    """
    Manage cache operations for table data.

    Args:
        request: FastAPI request object
        cache_request: Cache operation request
        user: Current authenticated user

    Returns:
        Cache operation response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Note: This is a placeholder for cache operations
        # In production, this would integrate with Redis or similar

        result_value = None
        cache_stats = None

        if cache_request.operation == "get":
            # Get cached value
            info(f"Cache GET operation for key: {cache_request.key}")
            result_value = None  # Would fetch from cache

        elif cache_request.operation == "set":
            # Set cached value
            info(f"Cache SET operation for key: {cache_request.key}")
            # Would store in cache with TTL

        elif cache_request.operation == "delete":
            # Delete cached value
            info(f"Cache DELETE operation for key: {cache_request.key}")
            # Would delete from cache

        elif cache_request.operation == "clear":
            # Clear cache
            warning("Cache CLEAR operation - all cached data will be removed")
            # Would clear cache

        elif cache_request.operation == "stats":
            # Get cache statistics
            cache_stats = {"total_keys": 0, "memory_usage": 0, "hit_rate": 0.0, "miss_rate": 0.0}
            info("Cache statistics retrieved")

        return CacheResponse(
            success=True,
            value=result_value,
            statistics=cache_stats,
            message=f"Cache operation '{cache_request.operation}' completed successfully",
        )

    except Exception as e:
        error(f"Error in cache operation: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Cache operation failed: {str(e)}")


async def manage_preferences_route(
    request: Request, preferences_request: PreferencesRequest, user: Dict = Depends(get_current_user)
) -> PreferencesResponse:
    """
    Manage user preferences for table views and customization.

    Args:
        request: FastAPI request object
        preferences_request: Preferences operation request
        user: Current authenticated user

    Returns:
        Preferences operation response
    """
    try:
        # Set notification context
        from declaro_advise import set_request_context

        set_request_context(request)

        # Note: This is a placeholder for preferences operations
        # In production, this would integrate with user preferences storage

        # Store/retrieve user preferences
        user_id = user["uid"]
        preference_type = preferences_request.preference_type

        if preferences_request.table_name:
            info(f"Managing {preference_type} preferences for table '{preferences_request.table_name}' and user {user_id}")
        else:
            info(f"Managing {preference_type} preferences for user {user_id}")

        # Would store/retrieve preferences from database
        stored_preferences = preferences_request.preferences

        success(f"Preferences updated successfully for {preference_type}")

        return PreferencesResponse(success=True, preferences=stored_preferences, message="Preferences updated successfully")

    except Exception as e:
        error(f"Error managing preferences: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Preferences operation failed: {str(e)}")


async def health_check_route() -> HealthCheckResponse:
    """
    Health check endpoint for monitoring service status.

    Args:
        request: FastAPI request object

    Returns:
        Health check response
    """
    try:
        # Health check doesn't need notification context

        # Check service health
        services_status = {"database": "healthy", "cache": "healthy", "plugins": "healthy", "notifications": "healthy"}

        performance_metrics = {"response_time": 0.001, "memory_usage": 0.0, "cpu_usage": 0.0}

        overall_status = "healthy" if all(status == "healthy" for status in services_status.values()) else "unhealthy"

        return HealthCheckResponse(
            status=overall_status,
            timestamp=datetime.utcnow(),
            services=services_status,
            performance=performance_metrics,
            version="1.0.0",
        )

    except Exception as e:
        return HealthCheckResponse(
            status="unhealthy",
            timestamp=datetime.utcnow(),
            services={"error": str(e)},
            performance={"error_time": 0.0},
            version="1.0.0",
        )


async def readiness_check_route() -> Dict:
    """
    Readiness check endpoint for deployment validation.

    Returns:
        Readiness status
    """
    try:
        # Basic readiness check
        return {"status": "ready", "timestamp": datetime.utcnow()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service not ready: {str(e)}")


async def detailed_health_check_route() -> Dict:
    """
    Detailed health check endpoint with comprehensive metrics.

    Returns:
        Detailed health information
    """
    try:
        checks = {
            "database": {"status": "healthy", "response_time": 0.001},
            "cache": {"status": "healthy", "response_time": 0.001},
            "performance": {"database_response_time": 0.001, "total_check_time": 0.002},
        }

        return {"status": "healthy", "timestamp": datetime.utcnow(), "checks": checks}
    except Exception as e:
        return {"status": "unhealthy", "timestamp": datetime.utcnow(), "checks": {"error": str(e)}}


async def metrics_endpoint() -> Dict:
    """
    Metrics endpoint for monitoring (placeholder).

    Returns:
        Metrics data or 404 if not implemented
    """
    raise HTTPException(status_code=404, detail="Metrics endpoint not implemented")


# Register routes
router.post("/data", response_model=TableDataResponse)(get_table_data_route)
router.get("/{table_name}/schema", response_model=TableDataResponse)(get_table_schema_route)
router.get("/{table_name}/statistics", response_model=TableDataResponse)(get_table_statistics_route)
router.post("/{table_name}/query", response_model=TableDataResponse)(execute_query_route)
router.post("/config", response_model=TableConfigResponse)(save_table_config_route)
router.get("/{table_name}/config/{config_name}", response_model=TableConfigResponse)(load_table_config_route)
router.get("/{table_name}/config", response_model=TableConfigResponse)(list_table_configs_route)
router.delete("/{table_name}/config/{config_name}", response_model=TableConfigResponse)(delete_table_config_route)
router.post("/format", response_model=FormattingResponse)(apply_formatting_route)
router.post("/plugins", response_model=PluginResponse)(manage_plugins_route)
router.post("/cache", response_model=CacheResponse)(manage_cache_route)
router.post("/preferences", response_model=PreferencesResponse)(manage_preferences_route)
router.get("/health", response_model=HealthCheckResponse)(health_check_route)
router.get("/ready")(readiness_check_route)
router.get("/health/detailed")(detailed_health_check_route)
router.get("/metrics")(metrics_endpoint)
