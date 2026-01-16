"""Domain service protocols for Table Module V2.

This module defines service interfaces using Python's typing.Protocol for
dependency injection and clean architecture separation.
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from .models import (
    ChildColumnDefinition,
    ColumnAlias,
    ColumnDefinition,
    FilterDefinition,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    TableState,
    UserColumnRename,
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)


@runtime_checkable
class TableDataRepository(Protocol):
    """Protocol for table data repository operations."""

    @abstractmethod
    async def get_table_data(
        self,
        table_name: str,
        filters: Optional[List[FilterDefinition]] = None,
        sorts: Optional[List[SortDefinition]] = None,
        pagination: Optional[PaginationSettings] = None,
        search_term: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> TableData:
        """Retrieve table data with optional filtering, sorting, and pagination.

        Args:
            table_name: Name of the table to query
            filters: Optional filters to apply
            sorts: Optional sort definitions
            pagination: Optional pagination settings
            search_term: Optional global search term
            user_id: Optional user ID for user-specific data

        Returns:
            TableData object with rows and metadata
        """
        ...

    @abstractmethod
    async def get_table_schema(self, table_name: str) -> List[ColumnDefinition]:
        """Get table schema as column definitions.

        Args:
            table_name: Name of the table

        Returns:
            List of column definitions
        """
        ...

    @abstractmethod
    async def table_exists(self, table_name: str) -> bool:
        """Check if table exists.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        ...

    @abstractmethod
    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get table statistics.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table statistics
        """
        ...

    @abstractmethod
    async def execute_custom_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None, user_id: Optional[str] = None
    ) -> TableData:
        """Execute custom SQL query safely.

        Args:
            query: SQL query string
            parameters: Optional query parameters
            user_id: Optional user ID for security context

        Returns:
            TableData object with query results
        """
        ...


@runtime_checkable
class TableConfigRepository(Protocol):
    """Protocol for table configuration repository operations."""

    @abstractmethod
    async def save_table_configuration(self, config: TableConfig, user_id: str) -> str:
        """Save table configuration.

        Args:
            config: Table configuration to save
            user_id: User ID for ownership

        Returns:
            Configuration ID
        """
        ...

    @abstractmethod
    async def load_table_configuration(self, config_id: str, user_id: str) -> Optional[TableConfig]:
        """Load table configuration.

        Args:
            config_id: Configuration ID
            user_id: User ID for ownership verification

        Returns:
            Table configuration or None if not found
        """
        ...

    @abstractmethod
    async def list_table_configurations(self, table_name: str, user_id: str) -> List[Dict[str, Any]]:
        """List table configurations for user.

        Args:
            table_name: Name of the table
            user_id: User ID

        Returns:
            List of configuration metadata
        """
        ...

    @abstractmethod
    async def delete_table_configuration(self, config_id: str, user_id: str) -> bool:
        """Delete table configuration.

        Args:
            config_id: Configuration ID
            user_id: User ID for ownership verification

        Returns:
            True if deleted, False if not found
        """
        ...


@runtime_checkable
class TableRenderingService(Protocol):
    """Protocol for table rendering service operations."""

    @abstractmethod
    async def render_table_html(
        self, table_state: TableState, template_name: str = "table.html", context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Render complete table HTML.

        Args:
            table_state: Complete table state
            template_name: Template name to use
            context: Optional additional context

        Returns:
            Rendered HTML string
        """
        ...

    @abstractmethod
    async def render_table_headers(
        self,
        columns: List[ColumnDefinition],
        sorts: Optional[List[SortDefinition]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render table headers.

        Args:
            columns: Column definitions
            sorts: Optional sort definitions
            context: Optional additional context

        Returns:
            Rendered headers HTML
        """
        ...

    @abstractmethod
    async def render_table_rows(
        self, data: TableData, columns: List[ColumnDefinition], context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Render table rows.

        Args:
            data: Table data
            columns: Column definitions
            context: Optional additional context

        Returns:
            Rendered rows HTML
        """
        ...

    @abstractmethod
    async def render_table_pagination(self, pagination: PaginationSettings, context: Optional[Dict[str, Any]] = None) -> str:
        """Render pagination controls.

        Args:
            pagination: Pagination settings
            context: Optional additional context

        Returns:
            Rendered pagination HTML
        """
        ...

    @abstractmethod
    async def render_table_controls(self, config: TableConfig, context: Optional[Dict[str, Any]] = None) -> str:
        """Render table controls (search, filters).

        Args:
            config: Table configuration
            context: Optional additional context

        Returns:
            Rendered controls HTML
        """
        ...


@runtime_checkable
class TableExportService(Protocol):
    """Protocol for table export service operations."""

    @abstractmethod
    async def export_table_data(
        self,
        data: TableData,
        format: str,
        columns: List[ColumnDefinition],
        filename: Optional[str] = None,
        include_metadata: bool = False,
    ) -> bytes:
        """Export table data in specified format.

        Args:
            data: Table data to export
            format: Export format (csv, excel, json, pdf)
            columns: Column definitions
            filename: Optional filename
            include_metadata: Whether to include metadata

        Returns:
            Exported data as bytes
        """
        ...

    @abstractmethod
    async def export_csv(
        self, data: TableData, columns: List[ColumnDefinition], delimiter: str = ",", include_headers: bool = True
    ) -> bytes:
        """Export data as CSV.

        Args:
            data: Table data
            columns: Column definitions
            delimiter: CSV delimiter
            include_headers: Whether to include headers

        Returns:
            CSV data as bytes
        """
        ...

    @abstractmethod
    async def export_excel(
        self, data: TableData, columns: List[ColumnDefinition], sheet_name: str = "Sheet1", include_formatting: bool = True
    ) -> bytes:
        """Export data as Excel.

        Args:
            data: Table data
            columns: Column definitions
            sheet_name: Excel sheet name
            include_formatting: Whether to include formatting

        Returns:
            Excel data as bytes
        """
        ...

    @abstractmethod
    async def export_json(
        self, data: TableData, columns: List[ColumnDefinition], pretty: bool = False, include_schema: bool = False
    ) -> bytes:
        """Export data as JSON.

        Args:
            data: Table data
            columns: Column definitions
            pretty: Whether to pretty-print JSON
            include_schema: Whether to include schema

        Returns:
            JSON data as bytes
        """
        ...

    @abstractmethod
    async def export_pdf(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        title: Optional[str] = None,
        include_page_numbers: bool = True,
    ) -> bytes:
        """Export data as PDF.

        Args:
            data: Table data
            columns: Column definitions
            title: Optional document title
            include_page_numbers: Whether to include page numbers

        Returns:
            PDF data as bytes
        """
        ...


@runtime_checkable
class TableCacheService(Protocol):
    """Protocol for table caching service operations."""

    @abstractmethod
    async def get_cached_table_data(self, cache_key: str, user_id: Optional[str] = None) -> Optional[TableData]:
        """Get cached table data.

        Args:
            cache_key: Cache key
            user_id: Optional user ID for user-specific cache

        Returns:
            Cached table data or None
        """
        ...

    @abstractmethod
    async def cache_table_data(
        self, cache_key: str, data: TableData, ttl_seconds: int = 300, user_id: Optional[str] = None
    ) -> None:
        """Cache table data.

        Args:
            cache_key: Cache key
            data: Table data to cache
            ttl_seconds: Time to live in seconds
            user_id: Optional user ID for user-specific cache
        """
        ...

    @abstractmethod
    async def invalidate_table_cache(self, table_name: str, user_id: Optional[str] = None) -> None:
        """Invalidate table cache.

        Args:
            table_name: Name of the table
            user_id: Optional user ID for user-specific cache
        """
        ...

    @abstractmethod
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        ...


@runtime_checkable
class TableNotificationService(Protocol):
    """Protocol for table notification service operations."""

    @abstractmethod
    async def send_table_notification(
        self,
        message: str,
        notification_type: str,
        user_id: str,
        table_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send table-related notification.

        Args:
            message: Notification message
            notification_type: Type of notification (success, error, warning, info)
            user_id: User ID
            table_name: Optional table name
            metadata: Optional additional metadata
        """
        ...

    @abstractmethod
    async def send_export_notification(
        self,
        export_status: str,
        user_id: str,
        table_name: str,
        format: str,
        file_size: Optional[int] = None,
        download_url: Optional[str] = None,
    ) -> None:
        """Send export-related notification.

        Args:
            export_status: Export status (started, completed, failed)
            user_id: User ID
            table_name: Table name
            format: Export format
            file_size: Optional file size
            download_url: Optional download URL
        """
        ...


@runtime_checkable
class TableFormattingService(Protocol):
    """Protocol for table formatting service operations."""

    @abstractmethod
    async def format_cell_value(
        self, value: Any, column: ColumnDefinition, format_options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Format individual cell value.

        Args:
            value: Cell value to format
            column: Column definition
            format_options: Optional formatting options

        Returns:
            Formatted value as string
        """
        ...

    @abstractmethod
    async def generate_table_styles(
        self, config: TableConfig, style_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """Generate table styles.

        Args:
            config: Table configuration
            style_options: Optional style options

        Returns:
            Dictionary of CSS classes and styles
        """
        ...


@runtime_checkable
class TablePluginService(Protocol):
    """Protocol for table plugin service operations."""

    @abstractmethod
    async def register_plugin(self, plugin_name: str, plugin_instance: Any, plugin_type: str) -> bool:
        """Register a table plugin.

        Args:
            plugin_name: Name of the plugin
            plugin_instance: Plugin instance
            plugin_type: Type of plugin

        Returns:
            True if registered successfully
        """
        ...

    @abstractmethod
    async def execute_plugin_hooks(
        self, hook_name: str, context: Dict[str, Any], user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute plugin hooks.

        Args:
            hook_name: Name of the hook
            context: Hook execution context
            user_id: Optional user ID

        Returns:
            Updated context after plugin execution
        """
        ...

    @abstractmethod
    async def get_plugin_statistics(self) -> Dict[str, Any]:
        """Get plugin system statistics.

        Returns:
            Dictionary with plugin statistics
        """
        ...


@runtime_checkable
class TableMonitoringService(Protocol):
    """Protocol for table monitoring service operations."""

    @abstractmethod
    async def log_table_operation(
        self,
        operation: str,
        table_name: str,
        user_id: str,
        duration_ms: float,
        success: bool,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log table operation.

        Args:
            operation: Operation name
            table_name: Table name
            user_id: User ID
            duration_ms: Operation duration in milliseconds
            success: Whether operation succeeded
            metadata: Optional additional metadata
        """
        ...

    @abstractmethod
    async def get_table_metrics(self, table_name: str, time_window_minutes: int = 60) -> Dict[str, Any]:
        """Get table performance metrics.

        Args:
            table_name: Table name
            time_window_minutes: Time window for metrics

        Returns:
            Dictionary with performance metrics
        """
        ...

    @abstractmethod
    async def check_table_health(self, table_name: str) -> Dict[str, Any]:
        """Check table health status.

        Args:
            table_name: Table name

        Returns:
            Dictionary with health status
        """
        ...


@runtime_checkable
class TableAggregationService(Protocol):
    """Protocol for table aggregation service operations."""

    @abstractmethod
    async def calculate_aggregations(
        self, data: TableData, columns: List[ColumnDefinition], group_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate table aggregations.

        Args:
            data: Table data
            columns: Column definitions
            group_by: Optional group by column

        Returns:
            Dictionary with aggregation results
        """
        ...

    @abstractmethod
    async def get_column_statistics(self, data: TableData, column_id: str, column_type: str) -> Dict[str, Any]:
        """Get column statistics.

        Args:
            data: Table data
            column_id: Column ID
            column_type: Column type

        Returns:
            Dictionary with column statistics
        """
        ...


# Customization-specific protocols


@runtime_checkable
class ColumnCustomizationRepository(Protocol):
    """Protocol for column customization repository operations."""

    @abstractmethod
    async def save_column_alias(self, alias: ColumnAlias) -> str:
        """Save column alias.

        Args:
            alias: Column alias to save

        Returns:
            Alias ID
        """
        ...

    @abstractmethod
    async def get_column_aliases(self, table_id: str) -> List[ColumnAlias]:
        """Get column aliases for table.

        Args:
            table_id: Table ID

        Returns:
            List of column aliases
        """
        ...

    @abstractmethod
    async def save_user_column_rename(self, rename: UserColumnRename) -> str:
        """Save user column rename.

        Args:
            rename: User column rename to save

        Returns:
            Rename ID
        """
        ...

    @abstractmethod
    async def get_user_column_renames(self, user_id: str, table_id: str) -> List[UserColumnRename]:
        """Get user column renames.

        Args:
            user_id: User ID
            table_id: Table ID

        Returns:
            List of user column renames
        """
        ...


@runtime_checkable
class FormulaEvaluationService(Protocol):
    """Protocol for formula evaluation service operations."""

    @abstractmethod
    async def evaluate_formula(self, formula: str, context: Dict[str, Any], user_id: str) -> Any:
        """Evaluate Excel-like formula.

        Args:
            formula: Formula to evaluate
            context: Evaluation context
            user_id: User ID for security

        Returns:
            Evaluated result
        """
        ...

    @abstractmethod
    async def validate_formula_security(self, formula: str, user_id: str) -> bool:
        """Validate formula security.

        Args:
            formula: Formula to validate
            user_id: User ID for security context

        Returns:
            True if safe, False otherwise
        """
        ...


@runtime_checkable
class TableCustomizationCache(Protocol):
    """Protocol for table customization cache operations."""

    @abstractmethod
    async def get_cached_customizations(self, user_id: str, table_id: str) -> Optional[Dict[str, Any]]:
        """Get cached customizations.

        Args:
            user_id: User ID
            table_id: Table ID

        Returns:
            Cached customizations or None
        """
        ...

    @abstractmethod
    async def cache_customizations(
        self, user_id: str, table_id: str, customizations: Dict[str, Any], ttl_seconds: int = 3600
    ) -> None:
        """Cache customizations.

        Args:
            user_id: User ID
            table_id: Table ID
            customizations: Customizations to cache
            ttl_seconds: Time to live in seconds
        """
        ...

    @abstractmethod
    async def invalidate_customization_cache(self, user_id: str, table_id: str) -> None:
        """Invalidate customization cache.

        Args:
            user_id: User ID
            table_id: Table ID
        """
        ...


@runtime_checkable
class UserLayoutRepository(Protocol):
    """Protocol for user layout repository operations."""

    @abstractmethod
    async def save_user_layout(self, layout: UserSavedLayout) -> str:
        """Save user layout.

        Args:
            layout: User layout to save

        Returns:
            Layout ID
        """
        ...

    @abstractmethod
    async def get_user_layouts(self, user_id: str, table_id: str) -> List[UserSavedLayout]:
        """Get user layouts.

        Args:
            user_id: User ID
            table_id: Table ID

        Returns:
            List of user layouts
        """
        ...

    @abstractmethod
    async def delete_user_layout(self, layout_id: str, user_id: str) -> bool:
        """Delete user layout.

        Args:
            layout_id: Layout ID
            user_id: User ID for ownership verification

        Returns:
            True if deleted, False if not found
        """
        ...


@runtime_checkable
class DefaultPreferencesService(Protocol):
    """Protocol for default preferences service operations."""

    @abstractmethod
    async def get_user_preferences(self, user_id: str, table_id: str) -> Optional[UserDefaultPreferences]:
        """Get user default preferences.

        Args:
            user_id: User ID
            table_id: Table ID

        Returns:
            User preferences or None
        """
        ...

    @abstractmethod
    async def save_user_preferences(self, preferences: UserDefaultPreferences) -> str:
        """Save user preferences.

        Args:
            preferences: User preferences to save

        Returns:
            Preferences ID
        """
        ...

    @abstractmethod
    async def apply_user_defaults(self, user_id: str, table_id: str, config: TableConfig) -> TableConfig:
        """Apply user defaults to table configuration.

        Args:
            user_id: User ID
            table_id: Table ID
            config: Base table configuration

        Returns:
            Configuration with user defaults applied
        """
        ...

    @abstractmethod
    async def validate_default_uniqueness(self, user_id: str, table_id: str, item_type: str, item_id: str) -> bool:
        """Validate default uniqueness.

        Args:
            user_id: User ID
            table_id: Table ID
            item_type: Type of item (search, filter_set, layout)
            item_id: Item ID

        Returns:
            True if unique, False otherwise
        """
        ...


# Service Layer Protocols


@runtime_checkable
class TableServiceProtocol(Protocol):
    """Protocol for table service operations."""

    @abstractmethod
    async def get_table_configuration(self, table_name: str, user_id: str) -> Optional[TableConfig]:
        """Get table configuration with caching and validation."""
        ...

    @abstractmethod
    async def get_table_data(
        self,
        table_name: str,
        user_id: str,
        filters: Optional[List[FilterDefinition]] = None,
        sorts: Optional[List[SortDefinition]] = None,
        pagination: Optional[PaginationSettings] = None,
        search_term: Optional[str] = None,
    ) -> Optional[TableData]:
        """Get table data with filtering, sorting, and pagination."""
        ...

    @abstractmethod
    async def filter_table_data(self, data: TableData, filters: List[FilterDefinition], user_id: str) -> TableData:
        """Apply filters to table data."""
        ...

    @abstractmethod
    async def sort_table_data(self, data: TableData, sorts: List[SortDefinition], user_id: str) -> TableData:
        """Apply sorting to table data."""
        ...

    @abstractmethod
    async def search_table_data(self, data: TableData, search_term: str, user_id: str) -> TableData:
        """Perform full-text search on table data."""
        ...

    @abstractmethod
    async def paginate_table_data(self, data: TableData, pagination: PaginationSettings, user_id: str) -> TableData:
        """Apply pagination to table data."""
        ...

    @abstractmethod
    async def get_table_stats(self, table_name: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get table statistics."""
        ...

    @abstractmethod
    async def build_table_state(
        self,
        table_name: str,
        user_id: str,
        filters: Optional[List[FilterDefinition]] = None,
        sorts: Optional[List[SortDefinition]] = None,
        pagination: Optional[PaginationSettings] = None,
        search_term: Optional[str] = None,
    ) -> Optional[TableState]:
        """Build complete table state with configuration and data."""
        ...


@runtime_checkable
class RenderingServiceProtocol(Protocol):
    """Protocol for rendering service operations."""

    @abstractmethod
    async def render_complete_table(
        self,
        table_state: TableState,
        template_name: str = "table.html",
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render complete table HTML."""
        ...

    @abstractmethod
    async def render_table_header(
        self,
        columns: List[ColumnDefinition],
        sorts: Optional[List[SortDefinition]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render table header."""
        ...

    @abstractmethod
    async def render_table_body(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render table body."""
        ...

    @abstractmethod
    async def render_table_controls(
        self,
        config: TableConfig,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render table controls (search, filters)."""
        ...

    @abstractmethod
    async def render_pagination(
        self,
        pagination: PaginationSettings,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render pagination controls."""
        ...


@runtime_checkable
class ExportServiceProtocol(Protocol):
    """Protocol for export service operations."""

    @abstractmethod
    async def export_table_data(
        self,
        data: TableData,
        format: str,
        columns: List[ColumnDefinition],
        filename: Optional[str] = None,
        include_metadata: bool = False,
        user_id: Optional[str] = None,
    ) -> bytes:
        """Export table data in specified format."""
        ...

    @abstractmethod
    async def export_csv(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        delimiter: str = ",",
        include_headers: bool = True,
    ) -> bytes:
        """Export data as CSV."""
        ...

    @abstractmethod
    async def export_excel(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        sheet_name: str = "Sheet1",
        include_formatting: bool = True,
    ) -> bytes:
        """Export data as Excel."""
        ...

    @abstractmethod
    async def export_json(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        pretty: bool = False,
        include_schema: bool = False,
    ) -> bytes:
        """Export data as JSON."""
        ...

    @abstractmethod
    async def export_pdf(
        self,
        data: TableData,
        columns: List[ColumnDefinition],
        title: Optional[str] = None,
        include_page_numbers: bool = True,
    ) -> bytes:
        """Export data as PDF."""
        ...

    @abstractmethod
    async def export_table_by_format(
        self,
        table_state: TableState,
        format: str,
        filename: Optional[str] = None,
        include_metadata: bool = False,
    ) -> Any:
        """Export table by format as streaming response."""
        ...


# FastAPI dependency injection functions


def get_table_data_repository() -> TableDataRepository:
    """Get table data repository dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_config_repository() -> TableConfigRepository:
    """Get table config repository dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_rendering_service() -> TableRenderingService:
    """Get table rendering service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_export_service() -> TableExportService:
    """Get table export service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_cache_service() -> TableCacheService:
    """Get table cache service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_notification_service() -> TableNotificationService:
    """Get table notification service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_formatting_service() -> TableFormattingService:
    """Get table formatting service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_plugin_service() -> TablePluginService:
    """Get table plugin service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_monitoring_service() -> TableMonitoringService:
    """Get table monitoring service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_aggregation_service() -> TableAggregationService:
    """Get table aggregation service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_column_customization_repository() -> ColumnCustomizationRepository:
    """Get column customization repository dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_formula_evaluation_service() -> FormulaEvaluationService:
    """Get formula evaluation service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_table_customization_cache() -> TableCustomizationCache:
    """Get table customization cache dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_user_layout_repository() -> UserLayoutRepository:
    """Get user layout repository dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_default_preferences_service() -> DefaultPreferencesService:
    """Get default preferences service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


# Service Layer Dependency Functions


def get_table_service() -> TableServiceProtocol:
    """Get table service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_rendering_service() -> RenderingServiceProtocol:
    """Get rendering service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


def get_export_service() -> ExportServiceProtocol:
    """Get export service dependency."""
    raise NotImplementedError("Must be implemented by dependency injection")


# =============================================================================
# No-Op Service Implementations
# =============================================================================
# These are default implementations that do nothing or return sensible defaults.
# They are used when library functions are called without explicit dependencies.


class NoOpNotificationService:
    """No-op notification service that logs but doesn't send notifications.

    Use this as a default when notification service is not provided.
    """

    async def send_table_notification(
        self,
        message: str,
        notification_type: str,
        user_id: str,
        table_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log notification but don't send."""
        # Silent no-op - notifications are optional
        pass

    async def send_export_notification(
        self,
        export_status: str,
        user_id: str,
        table_name: str,
        format: str,
        file_size: Optional[int] = None,
        download_url: Optional[str] = None,
    ) -> None:
        """Log export notification but don't send."""
        # Silent no-op - notifications are optional
        pass


class NoOpCacheService:
    """No-op cache service that always returns cache miss.

    Use this as a default when cache service is not provided.
    """

    async def get_cached_table_data(
        self, cache_key: str, user_id: Optional[str] = None
    ) -> Optional[TableData]:
        """Always return None (cache miss)."""
        return None

    async def cache_table_data(
        self,
        cache_key: str,
        data: TableData,
        ttl_seconds: int = 300,
        user_id: Optional[str] = None,
    ) -> None:
        """Do nothing - no caching."""
        pass

    async def invalidate_table_cache(
        self, table_name: str, user_id: Optional[str] = None
    ) -> None:
        """Do nothing - no cache to invalidate."""
        pass

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Return empty stats."""
        return {"enabled": False, "hits": 0, "misses": 0}


# Factory functions for no-op services
_noop_notification_service: Optional[NoOpNotificationService] = None
_noop_cache_service: Optional[NoOpCacheService] = None


def create_noop_notification_service() -> NoOpNotificationService:
    """Create or return singleton no-op notification service."""
    global _noop_notification_service
    if _noop_notification_service is None:
        _noop_notification_service = NoOpNotificationService()
    return _noop_notification_service


def create_noop_cache_service() -> NoOpCacheService:
    """Create or return singleton no-op cache service."""
    global _noop_cache_service
    if _noop_cache_service is None:
        _noop_cache_service = NoOpCacheService()
    return _noop_cache_service


class NoOpTableCustomizationCache:
    """No-op customization cache that always returns cache miss."""

    async def get_cached_customizations(
        self, user_id: str, table_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return None (cache miss)."""
        return None

    async def cache_customizations(
        self,
        user_id: str,
        table_id: str,
        customizations: Dict[str, Any],
        ttl_seconds: int = 3600,
    ) -> None:
        """No-op cache storage."""
        pass

    async def invalidate_customization_cache(self, user_id: str, table_id: str) -> None:
        """No-op cache invalidation."""
        pass

    async def get_cached_customization_data(
        self, cache_key: str, user_id: Optional[str] = None
    ) -> Optional[Any]:
        """Return None (cache miss)."""
        return None

    async def cache_customization_data(
        self,
        cache_key: str,
        data: Any,
        ttl_seconds: int = 3600,
        user_id: Optional[str] = None,
    ) -> None:
        """No-op cache storage."""
        pass


class NoOpFormulaEvaluationService:
    """No-op formula evaluation service that returns None."""

    async def evaluate_formula(
        self, formula: str, context: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Return None for all formula evaluations."""
        return None

    async def validate_formula_security(self, formula: str, user_id: str) -> bool:
        """Always return True (safe) for no-op."""
        return True


# Factory functions for customization no-op services
_noop_customization_cache: Optional[NoOpTableCustomizationCache] = None
_noop_formula_service: Optional[NoOpFormulaEvaluationService] = None


def create_noop_customization_cache() -> NoOpTableCustomizationCache:
    """Create or return singleton no-op customization cache."""
    global _noop_customization_cache
    if _noop_customization_cache is None:
        _noop_customization_cache = NoOpTableCustomizationCache()
    return _noop_customization_cache


def create_noop_formula_service() -> NoOpFormulaEvaluationService:
    """Create or return singleton no-op formula service."""
    global _noop_formula_service
    if _noop_formula_service is None:
        _noop_formula_service = NoOpFormulaEvaluationService()
    return _noop_formula_service


class NoOpDefaultPreferencesService:
    """No-op default preferences service that does nothing."""

    async def get_user_preferences(
        self, user_id: str, table_id: str
    ) -> Optional[UserDefaultPreferences]:
        """Return None (no preferences)."""
        return None

    async def save_user_preferences(self, preferences: UserDefaultPreferences) -> str:
        """No-op save, return empty string."""
        return ""

    async def apply_user_defaults(
        self, user_id: str, table_id: str, config: TableConfig
    ) -> TableConfig:
        """Return config unchanged."""
        return config

    async def validate_default_uniqueness(
        self, user_id: str, table_id: str, item_type: str, item_id: str
    ) -> bool:
        """Always return True (valid)."""
        return True

    async def get_cached_preferences(
        self, user_id: str, table_id: str
    ) -> Optional[UserDefaultPreferences]:
        """Return None (cache miss)."""
        return None

    async def cache_preferences(
        self, preferences: UserDefaultPreferences, ttl_seconds: int = 1800
    ) -> None:
        """No-op cache."""
        pass

    async def invalidate_preferences_cache(self, user_id: str, table_id: str) -> None:
        """No-op cache invalidation."""
        pass


# Factory function for no-op default preferences service
_noop_prefs_service: Optional[NoOpDefaultPreferencesService] = None


def create_noop_preferences_service() -> NoOpDefaultPreferencesService:
    """Create or return singleton no-op preferences service."""
    global _noop_prefs_service
    if _noop_prefs_service is None:
        _noop_prefs_service = NoOpDefaultPreferencesService()
    return _noop_prefs_service


# =============================================================================
# No-Op Repository Implementations
# =============================================================================


class NoOpTableDataRepository:
    """No-op table data repository that returns empty data.

    Use this as a default when no real database connection is available.
    Useful for testing or running the library standalone.
    """

    async def get_table_data(
        self,
        table_name: str,
        filters: Optional[List[FilterDefinition]] = None,
        sorts: Optional[List[SortDefinition]] = None,
        pagination: Optional[PaginationSettings] = None,
        search_term: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> TableData:
        """Return empty table data."""
        return TableData(rows=[], total_count=0)

    async def get_table_schema(self, table_name: str) -> List[ColumnDefinition]:
        """Return empty schema."""
        return []

    async def table_exists(self, table_name: str) -> bool:
        """Return False (no tables exist in no-op mode)."""
        return False

    async def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Return empty stats."""
        return {"row_count": 0, "column_count": 0}

    async def execute_custom_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None, user_id: Optional[str] = None
    ) -> TableData:
        """Return empty data for custom queries."""
        return TableData(rows=[], total_count=0)


class NoOpUserLayoutRepository:
    """No-op user layout repository that stores nothing.

    Use this as a default when no persistence is needed.
    All save operations succeed silently, all get operations return empty lists.
    """

    async def save_user_layout(self, layout: UserSavedLayout) -> str:
        """No-op save, return empty ID."""
        return ""

    async def get_user_layouts(self, user_id: str, table_id: str) -> List[UserSavedLayout]:
        """Return empty list."""
        return []

    async def get_user_saved_layouts(self, user_id: str, table_id: str) -> List[UserSavedLayout]:
        """Return empty list."""
        return []

    async def delete_user_layout(self, user_id: str, table_id: str, layout_name: str) -> bool:
        """Return False (nothing to delete)."""
        return False

    async def save_user_search(self, search: UserSavedSearch) -> UserSavedSearch:
        """No-op save, return the search unchanged."""
        return search

    async def get_user_saved_searches(self, user_id: str, table_id: str) -> List[UserSavedSearch]:
        """Return empty list."""
        return []

    async def delete_user_search(self, user_id: str, table_id: str, search_name: str) -> bool:
        """Return False (nothing to delete)."""
        return False

    async def save_user_filter_set(self, filter_set: UserFilterSet) -> UserFilterSet:
        """No-op save, return the filter set unchanged."""
        return filter_set

    async def get_user_filter_sets(self, user_id: str, table_id: str) -> List[UserFilterSet]:
        """Return empty list."""
        return []

    async def delete_user_filter_set(self, user_id: str, table_id: str, filter_set_name: str) -> bool:
        """Return False (nothing to delete)."""
        return False


class NoOpColumnCustomizationRepository:
    """No-op column customization repository that stores nothing.

    Use this as a default when no customization persistence is needed.
    """

    async def save_column_alias(self, alias: ColumnAlias) -> str:
        """No-op save, return empty ID."""
        return ""

    async def get_column_aliases(self, table_id: str) -> List[ColumnAlias]:
        """Return empty list."""
        return []

    async def save_user_column_rename(self, rename: UserColumnRename) -> str:
        """No-op save, return empty ID."""
        return ""

    async def get_user_column_renames(self, user_id: str, table_id: str) -> List[UserColumnRename]:
        """Return empty list."""
        return []

    async def save_child_column_definition(self, child_column: ChildColumnDefinition) -> str:
        """No-op save, return empty ID."""
        return ""

    async def get_child_column_definitions(
        self, user_id: str, table_id: str
    ) -> List[ChildColumnDefinition]:
        """Return empty list."""
        return []

    async def save_value_translations(
        self, user_id: str, table_id: str, column_id: str, translations: Dict[str, str]
    ) -> None:
        """No-op save."""
        pass

    async def get_value_translations(self, user_id: str, table_id: str) -> Dict[str, Dict[str, str]]:
        """Return empty dict."""
        return {}


# Factory functions for no-op repositories
_noop_data_repo: Optional[NoOpTableDataRepository] = None
_noop_layout_repo: Optional[NoOpUserLayoutRepository] = None
_noop_customization_repo: Optional[NoOpColumnCustomizationRepository] = None


def create_noop_data_repository() -> NoOpTableDataRepository:
    """Create or return singleton no-op data repository."""
    global _noop_data_repo
    if _noop_data_repo is None:
        _noop_data_repo = NoOpTableDataRepository()
    return _noop_data_repo


def create_noop_layout_repository() -> NoOpUserLayoutRepository:
    """Create or return singleton no-op layout repository."""
    global _noop_layout_repo
    if _noop_layout_repo is None:
        _noop_layout_repo = NoOpUserLayoutRepository()
    return _noop_layout_repo


def create_noop_customization_repository() -> NoOpColumnCustomizationRepository:
    """Create or return singleton no-op customization repository."""
    global _noop_customization_repo
    if _noop_customization_repo is None:
        _noop_customization_repo = NoOpColumnCustomizationRepository()
    return _noop_customization_repo
