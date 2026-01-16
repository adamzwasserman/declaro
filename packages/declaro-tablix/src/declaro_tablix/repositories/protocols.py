"""Repository protocols for Table Module V2.

This module defines Protocol interfaces for all repository operations,
following the function-based architecture pattern with FastAPI dependency injection.
"""

from typing import Any, Dict, List, Optional, Protocol

from declaro_tablix.domain.models import (
    ColumnDefinition,
    FilterDefinition,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    UserColumnRename,
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)


class TableDataRepository(Protocol):
    """Protocol for table data access operations."""

    def get_table_data(
        self,
        table_name: str,
        user_id: str,
        filters: Optional[List[FilterDefinition]] = None,
        sorts: Optional[List[SortDefinition]] = None,
        pagination: Optional[PaginationSettings] = None,
        search_term: Optional[str] = None,
    ) -> TableData:
        """Get table data with filtering, sorting, and pagination."""
        ...

    def get_table_schema(self, table_name: str) -> List[ColumnDefinition]:
        """Get table schema information."""
        ...

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""
        ...

    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get table statistics (row count, size, etc.)."""
        ...

    def execute_custom_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute custom SQL query safely."""
        ...


class TableConfigRepository(Protocol):
    """Protocol for table configuration persistence."""

    def save_table_configuration(self, user_id: str, table_name: str, config: TableConfig) -> bool:
        """Save table configuration for user."""
        ...

    def load_table_configuration(self, user_id: str, table_name: str) -> Optional[TableConfig]:
        """Load saved table configuration."""
        ...

    def list_table_configurations(self, user_id: str) -> List[Dict[str, Any]]:
        """List all user table configurations."""
        ...

    def delete_table_configuration(self, user_id: str, table_name: str, config_name: str) -> bool:
        """Delete table configuration."""
        ...


class CustomizationRepository(Protocol):
    """Protocol for user customizations and preferences."""

    def save_column_customization(self, user_id: str, table_name: str, customization: UserColumnRename) -> bool:
        """Save column customization."""
        ...

    def load_column_customizations(self, user_id: str, table_name: str) -> List[UserColumnRename]:
        """Load user column customizations."""
        ...

    def save_saved_search(self, user_id: str, table_name: str, search: UserSavedSearch) -> bool:
        """Save named search configuration."""
        ...

    def load_saved_searches(self, user_id: str, table_name: str) -> List[UserSavedSearch]:
        """Load user saved searches."""
        ...

    def save_filter_set(self, user_id: str, table_name: str, filter_set: UserFilterSet) -> bool:
        """Save filter set configuration."""
        ...

    def load_filter_sets(self, user_id: str, table_name: str) -> List[UserFilterSet]:
        """Load user filter sets."""
        ...

    def save_layout_preset(self, user_id: str, table_name: str, layout: UserSavedLayout) -> bool:
        """Save complete layout preset."""
        ...

    def load_layout_presets(self, user_id: str, table_name: str) -> List[UserSavedLayout]:
        """Load user layout presets."""
        ...


class CacheRepository(Protocol):
    """Protocol for Redis-based caching operations."""

    def cache_table_data(self, key: str, data: TableData, ttl_seconds: int = 300) -> bool:
        """Cache table data with TTL."""
        ...

    def get_cached_table_data(self, key: str) -> Optional[TableData]:
        """Retrieve cached table data."""
        ...

    def cache_table_config(self, key: str, config: TableConfig, ttl_seconds: int = 3600) -> bool:
        """Cache table configuration with TTL."""
        ...

    def get_cached_table_config(self, key: str) -> Optional[TableConfig]:
        """Retrieve cached table configuration."""
        ...

    def cache_query_result(self, key: str, result: List[Dict[str, Any]], ttl_seconds: int = 300) -> bool:
        """Cache arbitrary query results."""
        ...

    def get_cached_query_result(self, key: str) -> Optional[List[Dict[str, Any]]]:
        """Retrieve cached query results."""
        ...

    def invalidate_table_cache(self, table_name: str, user_id: str) -> bool:
        """Invalidate all cache entries for table and user."""
        ...

    def invalidate_cache_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        ...

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        ...


class PreferenceRepository(Protocol):
    """Protocol for user preference persistence."""

    def save_user_preferences(self, user_id: str, table_name: str, preferences: UserDefaultPreferences) -> bool:
        """Save user default preferences."""
        ...

    def load_user_preferences(self, user_id: str, table_name: str) -> Optional[UserDefaultPreferences]:
        """Load user default preferences."""
        ...

    def clear_user_preferences(self, user_id: str, table_name: str) -> bool:
        """Clear user preferences for table."""
        ...

    def get_user_preference_summary(self, user_id: str) -> Dict[str, Any]:
        """Get summary of user preferences across all tables."""
        ...


# FastAPI Dependency Injection Functions
def get_table_data_repository() -> TableDataRepository:
    """FastAPI dependency for table data repository."""
    ...


def get_table_config_repository() -> TableConfigRepository:
    """FastAPI dependency for table config repository."""
    ...


def get_customization_repository() -> CustomizationRepository:
    """FastAPI dependency for customization repository."""
    ...


def get_cache_repository() -> CacheRepository:
    """FastAPI dependency for cache repository."""
    ...


def get_preference_repository() -> PreferenceRepository:
    """FastAPI dependency for preference repository."""
    ...
