"""Table service functions for Table Module V2.

This module provides pure functions for table operations.
Functions accept explicit dependencies as parameters with sensible defaults.
"""

from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success
from declaro_tablix.domain.models import (
    FilterDefinition,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    TableState,
)
from declaro_tablix.domain.protocols import (
    TableCacheService,
    TableDataRepository,
    TableNotificationService,
    create_noop_cache_service,
    create_noop_data_repository,
    create_noop_notification_service,
)
from declaro_tablix.domain.validators import (
    validate_filter_definition,
    validate_pagination_settings,
    validate_sort_definition,
    validate_table_config,
)
from declaro_tablix.services.customization_service import (
    apply_column_customizations,
)
from declaro_tablix.services.layout_persistence_service import (
    apply_default_layout,
)


def sort_table_data(data: TableData, sorts: List[SortDefinition]) -> TableData:
    """Apply sorting to table data (sync version for direct use).

    This is a pure function version of sorting that doesn't require
    FastAPI dependencies. Use this for testing or direct calls.

    Args:
        data: Table data to sort
        sorts: List of sort definitions

    Returns:
        Sorted table data
    """
    if not sorts or not data.rows:
        return data

    # Sort data by priority (0 = highest priority)
    sorted_definitions = sorted(sorts, key=lambda x: x.priority)

    # Apply sorts in reverse order (highest priority last)
    sorted_rows = data.rows.copy()
    for sort_def in reversed(sorted_definitions):
        reverse = sort_def.direction.value == "desc" or sort_def.direction == "desc"
        sorted_rows = sorted(
            sorted_rows,
            key=lambda row, col=sort_def.column_id: _get_sort_key_sync(row, col),
            reverse=reverse,
        )

    # Create sorted data
    metadata = data.metadata.copy() if data.metadata else {}
    metadata["sorts_applied"] = len(sorts)
    metadata["sort_columns"] = [s.column_id for s in sorted_definitions]

    return TableData(
        rows=sorted_rows,
        total_count=data.total_count,
        metadata=metadata,
    )


def _get_sort_key_sync(row: dict, column_id: str) -> tuple:
    """Get sort key for a row and column (sync version).

    Returns a tuple to handle None values properly (None sorts to end).
    """
    value = row.get(column_id)
    if value is None:
        return (1, "")  # Sort None to end
    return (0, value)


async def get_table_configuration(
    table_name: str,
    user_id: str,
    data_repo: TableDataRepository | None = None,
    cache_service: TableCacheService | None = None,
    notification_service: TableNotificationService | None = None,
) -> Optional[TableConfig]:
    """Get table configuration with caching and validation.

    Args:
        table_name: Name of the table
        user_id: User ID for user-specific customizations
        data_repo: Data repository (optional, uses no-op if not provided)
        cache_service: Cache service (optional, uses no-op if not provided)
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Table configuration or None if not found
    """
    # Use no-op services/repos if not provided
    if data_repo is None:
        data_repo = create_noop_data_repository()
    if cache_service is None:
        cache_service = create_noop_cache_service()
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Loading configuration for table: {table_name}")

        # Check cache first
        cache_key = f"table_config:{table_name}:{user_id}"
        cached_config = await cache_service.get_cached_table_data(cache_key, user_id)

        if cached_config:
            success(f"Table configuration loaded from cache")
            return cached_config

        # Check if table exists
        if not await data_repo.table_exists(table_name):
            error(f"Table '{table_name}' not found")
            return None

        # Get table schema
        columns = await data_repo.get_table_schema(table_name)

        # Create basic configuration
        base_config = {
            "table_name": table_name,
            "columns": columns,
            "filters": [],
            "sorts": [],
            "pagination": {"page": 1, "page_size": 20},
            "search_term": None,
            "group_by": None,
        }

        # Apply user default layout preferences
        applied_config = await apply_default_layout(
            user_id=user_id,
            table_id=table_name,
            base_configuration=base_config,
        )

        # Create final configuration object
        config = TableConfig(
            table_name=applied_config["table_name"],
            columns=applied_config["columns"],
            filters=applied_config.get("filters", []),
            sorts=applied_config.get("sorts", []),
            pagination=PaginationSettings(**applied_config.get("pagination", {"page": 1, "page_size": 20})),
            search_term=applied_config.get("search_term"),
            group_by=applied_config.get("group_by"),
        )

        # Validate configuration
        is_valid, errors = validate_table_config(config)
        if not is_valid:
            error(f"Invalid table configuration: {', '.join(errors)}")
            return None

        # Cache the configuration
        await cache_service.cache_table_data(cache_key, config, ttl_seconds=3600, user_id=user_id)

        success(f"Table configuration loaded successfully")
        return config

    except Exception as e:
        error(f"Failed to load table configuration: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to load table configuration: {str(e)}",
            notification_type="error",
            user_id=user_id,
            table_name=table_name,
        )
        return None


async def get_table_data(
    table_name: str,
    user_id: str,
    data_repo: TableDataRepository | None = None,
    filters: Optional[List[FilterDefinition]] = None,
    sorts: Optional[List[SortDefinition]] = None,
    pagination: Optional[PaginationSettings] = None,
    search_term: Optional[str] = None,
    cache_service: TableCacheService | None = None,
    notification_service: TableNotificationService | None = None,
) -> Optional[TableData]:
    """Get table data with filtering, sorting, and pagination.

    Args:
        table_name: Name of the table
        user_id: User ID for user-specific data
        data_repo: Data repository (optional, uses no-op if not provided)
        filters: Optional list of filters to apply
        sorts: Optional list of sort definitions
        pagination: Optional pagination settings
        search_term: Optional global search term
        cache_service: Cache service (optional, uses no-op if not provided)
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Table data or None if error
    """
    # Use no-op services/repos if not provided
    if data_repo is None:
        data_repo = create_noop_data_repository()
    if cache_service is None:
        cache_service = create_noop_cache_service()
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Loading data for table: {table_name}")

        # Validate filters
        if filters:
            for filter_def in filters:
                is_valid, errors = validate_filter_definition(filter_def)
                if not is_valid:
                    error(f"Invalid filter: {', '.join(errors)}")
                    return None

        # Validate sorts
        if sorts:
            for sort_def in sorts:
                is_valid, errors = validate_sort_definition(sort_def)
                if not is_valid:
                    error(f"Invalid sort: {', '.join(errors)}")
                    return None

        # Validate pagination
        if pagination:
            is_valid, errors = validate_pagination_settings(pagination)
            if not is_valid:
                error(f"Invalid pagination: {', '.join(errors)}")
                return None

        # Generate cache key
        cache_key = (
            f"table_data:{table_name}:{user_id}:"
            f"{hash(str(filters))}{hash(str(sorts))}"
            f"{hash(str(pagination))}{hash(search_term or '')}"
        )

        # Check cache first
        cached_data = await cache_service.get_cached_table_data(cache_key, user_id)
        if cached_data:
            success("Table data loaded from cache")
            return cached_data

        # Get data from repository
        data = await data_repo.get_table_data(
            table_name=table_name,
            filters=filters,
            sorts=sorts,
            pagination=pagination,
            search_term=search_term,
            user_id=user_id,
        )

        # Apply customizations (column aliases, renames, calculated columns, translations)
        customized_data, customization_metadata = await apply_column_customizations(
            table_data=data,
            user_id=user_id,
            table_name=table_name,
        )

        # Add customization metadata to data
        if customized_data.metadata is None:
            customized_data.metadata = {}
        customized_data.metadata["customizations"] = customization_metadata

        # Cache the customized data
        await cache_service.cache_table_data(cache_key, customized_data, ttl_seconds=300, user_id=user_id)

        success(
            f"Table data loaded successfully ({customized_data.total_count} rows, {customization_metadata.get('processing_time_ms', 0)}ms customization)"
        )
        return customized_data

    except Exception as e:
        error(f"Failed to load table data: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to load table data: {str(e)}",
            notification_type="error",
            user_id=user_id,
            table_name=table_name,
        )
        return None


async def filter_table_data(
    data: TableData,
    filters: List[FilterDefinition],
    user_id: str,
    notification_service: TableNotificationService | None = None,
) -> TableData:
    """Apply filters to table data.

    Args:
        data: Table data to filter
        filters: List of filter definitions
        user_id: User ID for notifications
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Filtered table data
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Applying {len(filters)} filters to table data")

        # Validate filters
        for filter_def in filters:
            is_valid, errors = validate_filter_definition(filter_def)
            if not is_valid:
                error(f"Invalid filter: {', '.join(errors)}")
                return data

        # Apply filters to data
        filtered_rows = []
        for row in data.rows:
            include_row = True

            for filter_def in filters:
                if not _apply_filter_to_row(row, filter_def):
                    include_row = False
                    break

            if include_row:
                filtered_rows.append(row)

        # Create filtered data
        filtered_data = TableData(
            rows=filtered_rows,
            total_count=len(filtered_rows),
            metadata={
                **data.metadata,
                "filters_applied": len(filters),
                "original_count": data.total_count,
            },
        )

        success(f"Filters applied successfully ({len(filtered_rows)} rows remaining)")
        return filtered_data

    except Exception as e:
        error(f"Failed to apply filters: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to apply filters: {str(e)}",
            notification_type="error",
            user_id=user_id,
        )
        return data


async def sort_table_data_async(
    data: TableData,
    sorts: List[SortDefinition],
    user_id: str,
    notification_service: TableNotificationService | None = None,
) -> TableData:
    """Apply sorting to table data (async version).

    Args:
        data: Table data to sort
        sorts: List of sort definitions
        user_id: User ID for notifications
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Sorted table data
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Applying {len(sorts)} sorts to table data")

        # Validate sorts
        for sort_def in sorts:
            is_valid, errors = validate_sort_definition(sort_def)
            if not is_valid:
                error(f"Invalid sort: {', '.join(errors)}")
                return data

        # Use the sync version for actual sorting
        sorted_data = sort_table_data(data, sorts)

        success(f"Sorting applied successfully")
        return sorted_data

    except Exception as e:
        error(f"Failed to apply sorting: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to apply sorting: {str(e)}",
            notification_type="error",
            user_id=user_id,
        )
        return data


async def search_table_data(
    data: TableData,
    search_term: str,
    user_id: str,
    notification_service: TableNotificationService | None = None,
) -> TableData:
    """Perform full-text search on table data.

    Args:
        data: Table data to search
        search_term: Search term to find
        user_id: User ID for notifications
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Filtered table data with search results
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Searching table data for term: '{search_term}'")

        if not search_term or not search_term.strip():
            return data

        search_term_lower = search_term.lower().strip()

        # Search through all rows
        matching_rows = []
        for row in data.rows:
            # Check if any column value contains the search term
            row_matches = False
            for value in row.values():
                if value is not None and search_term_lower in str(value).lower():
                    row_matches = True
                    break

            if row_matches:
                matching_rows.append(row)

        # Create search results
        search_data = TableData(
            rows=matching_rows,
            total_count=len(matching_rows),
            metadata={
                **data.metadata,
                "search_term": search_term,
                "search_matches": len(matching_rows),
                "original_count": data.total_count,
            },
        )

        success(f"Search completed ({len(matching_rows)} matches found)")
        return search_data

    except Exception as e:
        error(f"Failed to search table data: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to search table data: {str(e)}",
            notification_type="error",
            user_id=user_id,
        )
        return data


async def paginate_table_data(
    data: TableData,
    pagination: PaginationSettings,
    user_id: str,
    notification_service: TableNotificationService | None = None,
) -> TableData:
    """Apply pagination to table data.

    Args:
        data: Table data to paginate
        pagination: Pagination settings
        user_id: User ID for notifications
        notification_service: Notification service (optional, uses no-op if not provided)

    Returns:
        Paginated table data
    """
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Applying pagination (page {pagination.page}, size {pagination.page_size})")

        # Validate pagination
        is_valid, errors = validate_pagination_settings(pagination)
        if not is_valid:
            error(f"Invalid pagination: {', '.join(errors)}")
            return data

        # Calculate pagination bounds
        start_index = pagination.offset
        end_index = start_index + pagination.page_size

        # Get paginated rows
        paginated_rows = data.rows[start_index:end_index]

        # Update pagination with total count
        pagination.total_count = data.total_count

        # Create paginated data
        paginated_data = TableData(
            rows=paginated_rows,
            total_count=data.total_count,
            metadata={
                **data.metadata,
                "pagination": {
                    "page": pagination.page,
                    "page_size": pagination.page_size,
                    "total_pages": pagination.total_pages,
                    "total_count": pagination.total_count,
                    "start_index": start_index,
                    "end_index": min(end_index, data.total_count),
                },
            },
        )

        success(f"Pagination applied successfully " f"({len(paginated_rows)} rows on page {pagination.page})")
        return paginated_data

    except Exception as e:
        error(f"Failed to apply pagination: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to apply pagination: {str(e)}",
            notification_type="error",
            user_id=user_id,
        )
        return data


async def get_table_stats(
    table_name: str,
    user_id: str,
    data_repo: TableDataRepository | None = None,
    notification_service: TableNotificationService | None = None,
) -> Optional[Dict[str, Any]]:
    """Get table statistics.

    Args:
        table_name: Name of the table
        user_id: User ID for user-specific stats
        data_repo: Data repository (optional, uses no-op if not provided)
        notification_service: Optional notification service

    Returns:
        Dictionary with table statistics
    """
    if data_repo is None:
        data_repo = create_noop_data_repository()
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Getting statistics for table: {table_name}")

        # Get stats from repository
        stats = await data_repo.get_table_stats(table_name)

        success(f"Table statistics retrieved successfully")
        return stats

    except Exception as e:
        error(f"Failed to get table statistics: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to get table statistics: {str(e)}",
            notification_type="error",
            user_id=user_id,
            table_name=table_name,
        )
        return None


async def build_table_state(
    table_name: str,
    user_id: str,
    data_repo: TableDataRepository | None = None,
    filters: Optional[List[FilterDefinition]] = None,
    sorts: Optional[List[SortDefinition]] = None,
    pagination: Optional[PaginationSettings] = None,
    search_term: Optional[str] = None,
    cache_service: TableCacheService | None = None,
    notification_service: TableNotificationService | None = None,
) -> Optional[TableState]:
    """Build complete table state with configuration and data.

    Args:
        table_name: Name of the table
        user_id: User ID for user-specific data
        data_repo: Data repository (optional, uses no-op if not provided)
        filters: Optional list of filters
        sorts: Optional list of sort definitions
        pagination: Optional pagination settings
        search_term: Optional search term
        cache_service: Optional cache service
        notification_service: Optional notification service

    Returns:
        Complete table state or None if error
    """
    if data_repo is None:
        data_repo = create_noop_data_repository()
    if cache_service is None:
        cache_service = create_noop_cache_service()
    if notification_service is None:
        notification_service = create_noop_notification_service()

    try:
        info(f"Building table state for: {table_name}")

        # Get table configuration
        config = await get_table_configuration(
            table_name=table_name,
            user_id=user_id,
            data_repo=data_repo,
            cache_service=cache_service,
            notification_service=notification_service,
        )

        if not config:
            return None

        # Override configuration with provided parameters
        if filters:
            config.filters = filters
        if sorts:
            config.sorts = sorts
        if pagination:
            config.pagination = pagination
        if search_term:
            config.search_term = search_term

        # Get table data
        data = await get_table_data(
            table_name=table_name,
            user_id=user_id,
            filters=config.filters,
            sorts=config.sorts,
            pagination=config.pagination,
            search_term=config.search_term,
            data_repo=data_repo,
            cache_service=cache_service,
            notification_service=notification_service,
        )

        if not data:
            return None

        # Build table state
        state = TableState(
            config=config,
            data=data,
            user_id=user_id,
            created_at=None,
            updated_at=None,
        )

        success(f"Table state built successfully")
        return state

    except Exception as e:
        error(f"Failed to build table state: {str(e)}")
        await notification_service.send_table_notification(
            message=f"Failed to build table state: {str(e)}",
            notification_type="error",
            user_id=user_id,
            table_name=table_name,
        )
        return None


# Helper functions


def _apply_filter_to_row(row: Dict[str, Any], filter_def: FilterDefinition) -> bool:
    """Apply a single filter to a row.

    Args:
        row: Row data dictionary
        filter_def: Filter definition to apply

    Returns:
        True if row matches filter, False otherwise
    """
    if filter_def.column_id not in row:
        return False

    value = row[filter_def.column_id]

    # Handle null checks
    if filter_def.operator == "is_null":
        return value is None
    elif filter_def.operator == "is_not_null":
        return value is not None

    # Handle null values for other operators
    if value is None:
        return False

    # Convert to string for comparison
    str_value = str(value).lower()

    # Apply operator
    if filter_def.operator == "equals":
        return str_value == str(filter_def.value).lower()
    elif filter_def.operator == "not_equals":
        return str_value != str(filter_def.value).lower()
    elif filter_def.operator == "contains":
        return str(filter_def.value).lower() in str_value
    elif filter_def.operator == "not_contains":
        return str(filter_def.value).lower() not in str_value
    elif filter_def.operator == "starts_with":
        return str_value.startswith(str(filter_def.value).lower())
    elif filter_def.operator == "ends_with":
        return str_value.endswith(str(filter_def.value).lower())
    elif filter_def.operator == "in":
        return str_value in [str(v).lower() for v in filter_def.values or []]
    elif filter_def.operator == "not_in":
        return str_value not in [str(v).lower() for v in filter_def.values or []]

    return True


def _get_sort_key(row: Dict[str, Any], column_id: str) -> Any:
    """Get sort key for a row and column.

    Args:
        row: Row data dictionary
        column_id: Column ID to sort by

    Returns:
        Sort key value
    """
    value = row.get(column_id)

    # Handle None values (sort to end)
    if value is None:
        return ""

    # Return value for sorting
    return value
