"""Caching layer for declaro-tablix.

Provides Redis-based caching for table data following tableV2 patterns.
"""

from declaro_tablix.caching.cache_service import (
    DEFAULT_TTL,
    CACHE_PRIORITIES,
    initialize_caching_service,
    setup_cache_warming_pipeline,
    smart_cache_get,
    smart_cache_set,
    schedule_cache_refresh,
    warm_cache_for_user,
    intelligent_cache_invalidation,
    get_cache_health_metrics,
    optimize_cache_performance,
    reset_cache_metrics,
    generate_cache_report,
    get_cache_service_for_dependency,
    get_smart_cache_for_dependency,
    get_cache_health_for_dependency,
    get_cache_warming_for_dependency,
)
from declaro_tablix.repositories.cache_repository import (
    generate_cache_key,
    initialize_cache_connection,
    get_redis_client,
    cache_table_data,
    get_cached_table_data,
    cache_table_config,
    get_cached_table_config,
    cache_query_result,
    get_cached_query_result,
    invalidate_cache_pattern,
    get_cache_stats,
)

__all__ = [
    # Constants
    "DEFAULT_TTL",
    "CACHE_PRIORITIES",
    # Service functions
    "initialize_caching_service",
    "setup_cache_warming_pipeline",
    "smart_cache_get",
    "smart_cache_set",
    "schedule_cache_refresh",
    "warm_cache_for_user",
    "intelligent_cache_invalidation",
    "get_cache_health_metrics",
    "optimize_cache_performance",
    "reset_cache_metrics",
    "generate_cache_report",
    # Repository functions
    "generate_cache_key",
    "initialize_cache_connection",
    "get_redis_client",
    "cache_table_data",
    "get_cached_table_data",
    "cache_table_config",
    "get_cached_table_config",
    "cache_query_result",
    "get_cached_query_result",
    "invalidate_cache_pattern",
    "get_cache_stats",
    # Dependency injection
    "get_cache_service_for_dependency",
    "get_smart_cache_for_dependency",
    "get_cache_health_for_dependency",
    "get_cache_warming_for_dependency",
]
